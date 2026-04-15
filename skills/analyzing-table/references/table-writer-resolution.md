# Table Writer Resolution

Use this reference after all writer candidates have completed procedure analysis and statement persistence.

## Happy path

When the table has one clearly defensible local writer, select it and persist scoping with:

- `selected_writer`
- `selected_writer_rationale`
- candidate context
- canonical `warnings` / `errors` entries when needed

## Resolution table

| Situation | Select proc? | Persisted outcome |
|---|---|---|
| Single defensible local writer | yes | persist `selected_writer` with rationale |
| Multiple defensible writers, one clearly primary | yes | persist the best-supported `selected_writer` with rationale |
| Multiple writers, no defensible tie-break | no | persist candidate context and let status resolve to `ambiguous_multi_writer` |
| Multi-table writer with a clean table-specific slice | yes | write the slice, then evaluate and select normally if it is the best writer |
| Multi-table writer with interleaved target logic | no | persist candidate context plus canonical error entry |
| Remote or linked-server `EXEC` is ancillary and local target-table writes are sufficient | yes | keep the proc selectable; persist the remote statement as `skip` and mention the skipped out-of-scope behavior in rationale or warnings |
| Remote or linked-server `EXEC` is the only meaningful write path for the target table | no | persist canonical `REMOTE_EXEC_UNSUPPORTED` error |
| Dynamic or opaque write path leaves the target-table transformation materially unresolved | no | persist canonical `SCOPING_FAILED` error and do not select the proc |
| No writers found | no | `no_writer_found` |

Common outcomes:

- **1 writer** -- auto-select and persist when it remains defensible under the decision table
- **2+ writers** -- present candidates, choose the best-supported writer, and persist with clear rationale
- **0 writers** -- report `no_writer_found`

## Multi-table writers

If a candidate proc has a `MULTI_TABLE_WRITE` warning, do **not** disqualify it automatically.

### Interleaved target logic

If a single MERGE/INSERT block writes to multiple tables simultaneously, or the logic uses shared variables or transaction semantics that cannot be cleanly attributed to one table:

- do not write `status` manually
- persist no `selected_writer`
- include the candidate context you gathered
- include a canonical error entry such as:

```json
{
  "code": "SCOPING_FAILED",
  "severity": "error",
  "message": "Writer logic is interleaved across multiple target tables and cannot be attributed to a table-specific slice."
}
```

Stop evaluating that candidate after persisting the error outcome.

### Separable target logic

If distinct MERGE/INSERT/UPDATE blocks handle each target table:

1. Identify the block or blocks that write to this target table only, plus any shared setup they depend on.
2. Persist the slice:

   ```bash
   mkdir -p .staging
   cat > .staging/slice.sql <<'EOF'
   <slice SQL>
   EOF
   uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover write-slice \
     --proc <proc_fqn> --table <target_table_fqn> --slice-file .staging/slice.sql
   rm -rf .staging
   ```

   `discover write-slice` reads `.staging/slice.sql` and persists that slice into the catalog.

3. Evaluate the sliced candidate normally.
4. If selected, mention in `selected_writer_rationale` that this is a multi-table proc and name the other written tables.

## External delegates and remote EXEC

If all discovered candidates are unsupported external delegates, persist table scoping without `selected_writer`. In that case:

- omit `selected_writer`
- explain in `selected_writer_rationale` that the apparent writer delegates to an out-of-scope external procedure and cannot be migrated from this project
- include an `errors` entry such as:

```json
{
  "code": "REMOTE_EXEC_UNSUPPORTED",
  "severity": "error",
  "message": "The apparent writer delegates through a cross-database or linked-server EXEC, so the writer cannot be resolved from this project."
}
```

## Scoping payload

Treat any existing `scoping` section as non-authoritative on reruns. Recompute scoping from current catalog evidence, then overwrite it with the new canonical payload.

Create the temp file first, then persist it to the catalog:

```bash
mkdir -p .staging
cat > .staging/scoping.json <<'EOF'
<scoping JSON>
EOF
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover write-scoping \
  --name <table> --scoping-file .staging/scoping.json && rm -rf .staging
```

`discover write-scoping` reads `.staging/scoping.json` and persists that scoping payload into the catalog.

The payload must include `selected_writer_rationale`, even when no writer is selected.

Example:

```json
{
  "selected_writer": "silver.usp_load_dimcustomer_full",
  "selected_writer_rationale": "Full loader is the primary writer because it independently rebuilds the target table from source data.",
  "candidates": [
    {
      "procedure_name": "silver.usp_load_dimcustomer_full",
      "rationale": "Direct full-load writer for the target table.",
      "dependencies": {
        "tables": ["bronze.customer", "bronze.person"],
        "views": [],
        "functions": []
      }
    }
  ],
  "warnings": [],
  "errors": []
}
```

## Common mistakes

- Do not put `status` in the scoping JSON.
- Do not reject every `MULTI_TABLE_WRITE` candidate. Separable writers stay valid after slicing.
- Do not omit `selected_writer_rationale` when the result is ambiguous or unsupported.
- For multi-writer cases, every entry in `candidates` must use `procedure_name` and `rationale`. `dependencies` is optional.
- Do not use legacy fields such as `procedure`, `write_type`, or `selected`.
