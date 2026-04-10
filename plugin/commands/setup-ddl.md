# Setting Up DDL

Extract DDL from a live SQL Server or Oracle database and write local artifact files that the `ddl` MCP server used by `listing-objects` / `analyzing-table` skills reads for schema information.

## Guard check

Run the stage guard before doing anything else:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util guard _ setup-ddl
```

If `passed` is `false`, report the failing guard's `code` and `message` and stop.

## Confirm project root

Run `pwd` and show the resolved path. Ask the user: "Is this the correct project root?" If the user says no, tell them to `cd` to the correct directory and re-run the skill. Stop.

## Read manifest

Read `manifest.json`. Determine `technology` — one of `sql_server`, `fabric_warehouse`, or `oracle`.

If `ddl/` or `catalog/` already exists in the project root, warn the user:

> Re-running will **fully rebuild** both `ddl/` and `catalog/`. All previously extracted files will be replaced. (LLM-enriched catalog fields such as scoping, profile, and refactor results are preserved.)

Ask for confirmation before proceeding. If they decline, stop immediately.

## Progress Tracking

Use `TaskCreate` and `TaskUpdate` to track the automated phases of this command. After the user confirms and before extraction begins, create tasks for each automated step that will run (e.g. `Extract DDL`). Update each task to `in_progress` when it starts and to `completed` or `cancelled` (include the error reason) when it finishes. Do not create tasks for interactive steps (database/schema selection, confirmation prompts).

---

## SQL Server / Fabric Warehouse flow

Use this branch when `technology` is `sql_server` or `fabric_warehouse`.

`<shared-path>` refers to `${CLAUDE_PLUGIN_ROOT}/lib`.

### Step 1 — Select database

> **Skip this step** if `manifest.json` already has `source_database` set. Show the user:
>
> ```text
> Previously extracted database: <source_database>
> Previously extracted schemas:  <extracted_schemas>
> ```
>
> Then proceed directly to Step 2.

List user databases:

```bash
uv run --project <shared-path> setup-ddl list-databases
```

The command returns `{"databases": [...]}`. Present the list and ask the user to pick one. Do not proceed without a selection.

### Step 2 — Select schemas

```bash
uv run --project <shared-path> setup-ddl list-schemas --database <database>
```

The command returns `{"schemas": [{"schema": name, "tables": N, "procedures": N, "views": N, "functions": N}, ...]}`.

Present the schemas with their counts and offer an `all` option. Ask the user to pick one or more (or `all`). Store the selected schemas for the next step.

### Step 3 — Preview and confirm

Build a summary table from the `list-schemas` output for the selected schemas:

```text
Extraction preview for [database]
Schemas: <selected-schemas>

  Object counts across selected schemas:
    Tables:     N
    Procedures: N
    Views:      N
    Functions:  N

  DDL files will be written to:     ./ddl/
  Catalog files will be written to: ./catalog/
  manifest.json will be updated at: ./manifest.json
```

Ask the user for confirmation before extraction proceeds. If they decline, stop immediately — no files are written.

### Step 4 — Extract

```bash
uv run --project <shared-path> setup-ddl extract \
  --database <database> \
  --schemas <comma-separated-schemas>
```

### Step 5 — Report

Report a summary using the counts from the `extract` output:

```text
DDL extraction complete → ./
Database: <database>
Schemas:  <selected-schemas>

  DDL files (ddl/):
    tables.sql     : N tables
    procedures.sql : N procedures
    views.sql      : N views
    functions.sql  : N functions

  Catalog files (catalog/):
    tables/     : N files
    procedures/ : N files
    views/      : N files
    functions/  : N files

  manifest.json at ./manifest.json
```

Tell the user: "To browse the extracted objects, run `/listing-objects list tables`. To start scoping tables for migration, run `/scope`."

If `dbt/models/staging/sources.yml` already exists, warn: "sources.yml already exists and may be stale after this extraction. Run `/analyzing-table` on new tables, then re-run `/init-dbt` to regenerate."

### Step 6 — Stale catalog cleanup

Check all catalog files for `"stale": true` by scanning `catalog/tables/*.json`, `catalog/procedures/*.json`, `catalog/views/*.json`, and `catalog/functions/*.json`.

If any stale files exist:

1. List them:

   ```text
   The following N catalog file(s) are from a prior extraction and are now stale:
     catalog/tables/silver.dimcustomer.json
   ```

2. Ask: **"Remove these N stale catalog files?"**

3. If the user confirms: delete each file. (They will be included in the commit as deletions.)

4. If the user declines: leave the files intact. The commit will still proceed.

If no stale files exist, skip this step.

### Step 7 — Commit extracted artifacts

Stage and commit the extracted artifacts:

```bash
git add ddl/ catalog/ manifest.json
git commit -m "extract DDL from <database> (<schemas>)"
```

Where `<database>` is the selected database name and `<schemas>` is the comma-separated list of extracted schema names (e.g. `extract DDL from AdventureWorks2022 (silver, gold)`).

If `git` is not available or the project root is not a git repository (i.e. `git status` exits non-zero), warn the user and skip this step without failing:

> git not available or project root is not a git repository — skipping commit. Artifacts written to disk.

---

## Oracle flow

Use this branch when `technology` is `oracle`.

`<shared-path>` refers to `${CLAUDE_PLUGIN_ROOT}/lib`.

### Step 1 — Select schemas

```bash
uv run --project <shared-path> setup-ddl list-schemas
```

The command returns `{"schemas": [{"owner": name, "tables": N, "procedures": N, "views": N, "functions": N}, ...]}`.

Present the owners with their counts and offer an `all` option. Ask the user to pick one or more (or `all`). Store the selected owners for the next step.

### Step 2 — Preview and confirm

Build a summary table from the `list-schemas` output for the selected owners:

```text
Extraction preview
Schemas: <selected-owners>

  Object counts across selected schemas:
    Tables:     N
    Procedures: N
    Views:      N
    Functions:  N

  DDL files will be written to:     ./ddl/
  Catalog files will be written to: ./catalog/
  manifest.json will be updated at: ./manifest.json
```

Ask the user for confirmation before extraction proceeds. If they decline, stop immediately — no files are written.

### Step 3 — Extract

```bash
uv run --project <shared-path> setup-ddl extract \
  --schemas <comma-separated-owners>
```

### Step 4 — Report

Report a summary using the counts from the `extract` output:

```text
DDL extraction complete → ./
Schemas: <selected-owners>

  DDL files (ddl/):
    tables.sql     : N tables
    procedures.sql : N procedures
    views.sql      : N views
    functions.sql  : N functions

  Catalog files (catalog/):
    tables/     : N files
    procedures/ : N files
    views/      : N files
    functions/  : N files

  manifest.json at ./manifest.json
```

Tell the user: "To browse the extracted objects, run `/listing-objects list tables`. To start scoping tables for migration, run `/scope`."

If `dbt/models/staging/sources.yml` already exists, warn: "sources.yml already exists and may be stale after this extraction. Run `/analyzing-table` on new tables, then re-run `/init-dbt` to regenerate."

### Step 5 — Stale catalog cleanup

Check all catalog files for `"stale": true` by scanning `catalog/tables/*.json`, `catalog/procedures/*.json`, `catalog/views/*.json`, and `catalog/functions/*.json`.

If any stale files exist:

1. List them:

   ```text
   The following N catalog file(s) are from a prior extraction and are now stale:
     catalog/procedures/sh.usp_load.json
   ```

2. Ask: **"Remove these N stale catalog files?"**

3. If the user confirms: delete each file. (They will be included in the commit as deletions.)

4. If the user declines: leave the files intact. The commit will still proceed.

If no stale files exist, skip this step.

### Step 6 — Commit extracted artifacts

Stage and commit the extracted artifacts:

```bash
git add ddl/ catalog/ manifest.json
git commit -m "extract DDL from <schemas>"
```

Where `<schemas>` is the comma-separated list of extracted schema names (e.g. `extract DDL from SH, HR`).

If `git` is not available or the project root is not a git repository (i.e. `git status` exits non-zero), warn the user and skip this step without failing:

> git not available or project root is not a git repository — skipping commit. Artifacts written to disk.

---

## Safety

- Use the CLI commands for all database interaction — never query the database directly via MCP tools or ad-hoc scripts.
- Do not log `SA_PASSWORD`, `ORACLE_PASSWORD`, or any connection string values.
