---
name: exclude-table
description: >
  Mark one or more tables or views as excluded from the migration pipeline.
  Sets excluded: true in each named catalog file. Excluded objects are hidden
  from /status and skipped by batch scheduling. To re-include, edit the catalog
  JSON directly and remove the excluded field.
user-invocable: true
argument-hint: "<schema.table_or_view> [schema.table_or_view ...]"
---

# Exclude Table

Mark one or more tables or views as excluded from the migration pipeline by setting `excluded: true` in their catalog files. Excluded objects are hidden from `/status` and skipped by batch scheduling. Explicit single-object commands (e.g. `/scope silver.MyTable`) still work on excluded objects.

## Guards

- `manifest.json` must exist. If missing, tell the user to run `/setup-ddl` first.

## Pipeline

### Step 1 — Parse arguments

Parse `$ARGUMENTS` as a space-separated list of fully-qualified names. If no arguments are provided:

```text
Usage: /exclude-table <schema.table_or_view> [schema.table_or_view ...]

Example: /exclude-table silver.AuditLog silver.StagingTemp
```

Stop and show this message.

### Step 2 — Mark objects as excluded

Run:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util exclude <fqn1> [fqn2 ...]
```

Parse the JSON output:

- `marked` — FQNs successfully marked excluded
- `not_found` — FQNs for which no catalog file (table or view) existed

### Step 3 — Commit

For each FQN in `marked`, stage its catalog file. Tables live in `catalog/tables/<fqn>.json`, views in `catalog/views/<fqn>.json`. Stage whichever file exists:

```bash
git add catalog/tables/<fqn>.json 2>/dev/null || git add catalog/views/<fqn>.json
```

Then commit:

```bash
git commit -m "chore: exclude <fqn-list> from migration pipeline"
```

Where `<fqn-list>` is the space-separated list of marked FQNs (truncated to 60 chars if long).

If nothing was marked (all not_found), skip the commit.

### Step 4 — Report

Present the result:

```text
Excluded from pipeline:
  silver.AuditLog         catalog/tables/silver.auditlog.json
  silver.StagingTemp      catalog/tables/silver.stagingtemp.json

Not found (no catalog file):
  silver.Nonexistent      — run /setup-ddl to extract this object first
```

If `not_found` is empty, omit that section. If `marked` is empty, say:

```text
No objects were excluded — none of the provided FQNs have catalog files.
Run /setup-ddl to extract objects first.
```

After a successful exclusion, tell the user:

```text
Run /status to see the updated pipeline. Objects that depend on the excluded
table(s) will show an EXCLUDED_DEP warning in the catalog diagnostics section.
```

## Idempotency

Running `/exclude-table` on an already-excluded object is safe — it re-writes `excluded: true` and reports it in `marked`. The catalog is not otherwise modified.

## Error handling

| Situation | Action |
|---|---|
| `manifest.json` missing | Tell user to run `/setup-ddl` first |
| FQN has no catalog file | Report in `not_found`; continue with remaining FQNs |
| `migrate-util exclude` returns exit code 2 | Report IO error and suggest checking project setup |
| `git commit` fails (nothing to commit) | Skip silently — catalog was already excluded |
