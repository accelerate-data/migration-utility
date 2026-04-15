# Procedure Analysis

Deep-dive analysis of a single stored procedure. Produces call graph, statement classification, logic summary, migration guidance, and persists resolved statements to catalog.

The procedure name is the candidate writer identified by the parent analyzing-table skill.

## Pipeline

Follow these steps in order. Do not abbreviate — every step must complete before moving to the next.

### Step 1 — Fetch object data

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover show \
  --name <proc>
```

### Step 2 — Classify statements

Check the `needs_llm` field and `statements` array:

- **`needs_llm: false`** with `statements` populated and no `action: "needs_llm"` entries — `refs` and `statements` are pre-classified by the AST. Use them alongside the body as the authoritative source of truth.
- **`needs_llm: false`** but `statements` is null, empty, or contains `action: "needs_llm"` entries — safety-net fallback. Treat as needs_llm: classify each statement yourself from `raw_ddl`. See the dialect-appropriate statement classification via [`statement-classification.md`](statement-classification.md).
- **`needs_llm: true`** or `statements` is null — classify each statement yourself from `raw_ddl`. See the dialect-appropriate statement classification via [`statement-classification.md`](statement-classification.md).

### Step 3 — Resolve call graph

Read/write targets come from `refs`. Resolve to base tables: if a ref is a view, function, or procedure (not a base table), run `discover show` on it to get its refs and follow the chain until you reach base tables. Present the full lineage:

```text
silver.usp_load_DimCustomer  (direct writer)
  +-- reads: silver.vw_ProductCatalog (view)
  |     +-- reads: bronze.Customer        <- resolved via discover show
  |     +-- reads: bronze.Product         <- resolved via discover show
  +-- reads: bronze.Person
  +-- writes: silver.DimCustomer
```

### Step 4 — Logic summary

Read `raw_ddl` and produce a plain-language description of what the procedure does, step by step. No tags, no classification — just explain the logic.

### Step 5 — Migration guidance

Tag each statement as `migrate` or `skip`:

| Action | Meaning |
|---|---|
| `migrate` | Core transformation (INSERT, UPDATE, DELETE, MERGE, SELECT INTO) — becomes the dbt model |
| `skip` | Operational overhead (SET, TRUNCATE, DROP/CREATE INDEX) — dbt handles or ignores |

Use this table for edge cases:

| Statement pattern | Action | Notes |
|---|---|---|
| In-scope INSERT/UPDATE/DELETE/MERGE/SELECT INTO that materially writes the target | `migrate` | Core transformation logic |
| Operational statements (`SET`, `TRUNCATE`, index maintenance) | `skip` | Operational overhead |
| Cross-database or linked-server `EXEC` | `skip` | Unsupported statement; explain in `rationale` whether it is ancillary or the core write path |
| Dynamic SQL that can be reconstructed from literals, local assignments, or simple concatenation | `migrate` | Persist the resolved inner SQL text, not the wrapper `EXEC(@sql)` or builder assignment |
| Dynamic SQL builder setup (`DECLARE`, `SET @sql = ...`, `SET @target = ...`) | `skip` | Always setup only; never persist builder assignments as `migrate` even when they contain the write text as a string |
| Dynamic or opaque statement whose target behavior cannot be resolved | `skip` or stop | Use `skip` only if the remaining target-table write path is still defensible; otherwise stop and report the unresolved path to the parent skill |

Present the tagged list:

```text
Migration Guidance
  1. [skip]    TRUNCATE TABLE silver.DimCustomer
  2. [migrate] INSERT INTO silver.DimCustomer from vw_ProductCatalog JOIN bronze.Person
  3. [migrate] Computes DateFirstPurchase via OUTER APPLY on bronze.SalesOrderHeader
```

If the procedure includes a cross-database or linked-server `EXEC`, persist that statement as `action: "skip"` and explain in `rationale` that it is out-of-scope. Do not invent a third action such as `unsupported`.

Do not reject the whole procedure just because one statement is remote. The parent skill decides writer selection based on whether the remaining local statements are sufficient to represent the target-table write path:

- if the remote `EXEC` is ancillary and the target table still has sufficient local `migrate` statements, the proc can remain selectable
- if the remote `EXEC` is the only meaningful write path for the target table, the parent skill should reject the proc with `REMOTE_EXEC_UNSUPPORTED`

When dynamic SQL is resolvable, reconstruct the inner write statement and persist that as the statement `sql`. Do not persist the execution wrapper or builder text as the migrated statement.

Examples:

- `EXEC(@sql)` where `@sql` resolves to an insert:
  persist `INSERT INTO silver.ExecVariableTarget ... FROM bronze.Product`
- `EXEC sp_executesql @sql` where `@sql` resolves to a merge:
  persist `MERGE silver.DimDynamicBranch ...`
- `SET @sql = 'INSERT INTO ' + @target + ...` followed by `EXEC(@sql)`:
  persist the reconstructed `INSERT INTO ... SELECT ... FROM ...`, and keep the `SET` / `DECLARE` statements as `skip`
- if both the builder assignment and the `EXEC` are present:
  record only one `migrate` statement for the reconstructed inner SQL, not two migrate entries

### Step 6 — Persist resolved statements

After presenting the analysis, persist resolved statements to catalog.

Treat any existing `statements` section as non-authoritative on reruns. Re-resolve the procedure from current `raw_ddl`, `references`, and routing metadata, then overwrite `statements` with the newly resolved canonical list. Do not preserve stale migrate/skip decisions just because they already exist in the catalog.

**Deterministic procedures** (`needs_llm: false`, no `action: "needs_llm"` entries in statements): all statements are already classified by the AST. Persist immediately after presenting Migration Guidance — no additional user confirmation needed. All statements get `source: "ast"`.

**LLM-assisted procedures** (`needs_llm: true` or statements containing `action: "needs_llm"`):

1. Read `raw_ddl` and analyse each `needs_llm` statement — follow the call graph, resolve dynamic SQL, and classify as `migrate` or `skip`.
2. For dynamic SQL, rebuild the final inner statement text whenever the target write can be recovered from local literals, assignments, or simple concatenation. The persisted `sql` must be the resolved statement itself, not `EXEC(@sql)`, `EXEC sp_executesql @sql`, or `SET @sql = ...`.
3. Build the final resolved statement list and persist it immediately. Do not ask for confirmation before writing — this is a write-through workflow.
4. After the write succeeds, present the resolved statement list and the persisted outcome to the user. All resolved statements get `source: "llm"`. Each statement must include a `rationale` field (1-2 sentences) explaining why it is `migrate` or `skip`.

Example for a remote delegate statement:

```json
{
  "type": "Command",
  "action": "skip",
  "sql": "EXEC [ArchiveDB].[silver].[usp_stage_DimCrossDbProfile]",
  "source": "llm",
  "rationale": "Cross-database EXEC delegate to an external procedure. Skip in statement persistence; the parent scoping step decides whether this remote call is ancillary or the core write path."
}
```

Example for a resolved dynamic SQL write:

```json
{
  "type": "Command",
  "action": "migrate",
  "sql": "INSERT INTO silver.ExecVariableTarget (ProductAlternateKey, EnglishProductName) SELECT CAST(ProductID AS NVARCHAR(25)), ProductName FROM bronze.Product",
  "source": "llm",
  "rationale": "Resolved from EXEC(@sql) after reconstructing the local string assignment. This is the core transformation that writes the target table."
}
```

No `needs_llm` actions are written to catalog — all must be resolved before persisting.

Create the temp file first, then persist it to the catalog:

```bash
mkdir -p .staging
cat > .staging/statements.json <<'EOF'
<statements JSON>
EOF
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover write-statements \
  --name <procedure_name> --statements-file .staging/statements.json && rm -rf .staging
```

`discover write-statements` reads `.staging/statements.json` and persists those statements into the catalog.

After `discover write-statements` succeeds, report that the statements were persisted and summarize the migrate/skip decisions.

## Common mistakes

- Do not invent `unsupported` as a statement action. `discover write-statements` accepts only `migrate` or `skip`.
- Do not reject the entire procedure at statement-classification time just because one statement is remote. Record the remote statement as `skip`; the parent skill decides whether the proc is still selectable.
- Do not persist `EXEC(@sql)`, `EXEC sp_executesql @sql`, or `SET @sql = ...` as the migrated statement when the inner SQL can be reconstructed. Persist the reconstructed inner write statement instead.
- Do not mark the SQL-builder assignment itself as `migrate`. The builder setup is `skip`; the reconstructed write is `migrate`.
- Do not create duplicate `migrate` entries for a concatenated dynamic SQL path. The assignment that builds the SQL string stays `skip`; only the reconstructed inner statement is `migrate`.

## Error handling

| Command | Exit code | Action |
|---|---|---|
| `discover show` | 1 | Object not found or catalog file missing. Report and stop |
| `discover show` | 2 | Catalog directory unreadable (IO error). Report and stop |
| `discover show` | 0 + `parse_error` set | Still loaded — `raw_ddl` preserved. Report parse error, proceed with `raw_ddl`-based analysis |
| `discover write-statements` | 1 | Procedure not found or invalid statements. Report validation error |
| `discover write-statements` | 2 | Invalid JSON input. Report and stop |
| call graph resolution | — | Circular reference: stop recursion and report the cycle |
| dynamic SQL reconstruction | — | If the target write cannot be reconstructed and no remaining local write path is defensible, stop and report the unresolved path to the parent skill so it can persist `SCOPING_FAILED` |
