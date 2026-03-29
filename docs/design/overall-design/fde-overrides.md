# FDE Overrides

FDE overrides are direct edits to catalog files committed to git. There is no SQLite table, no `fde_overrides` schema, no separate override storage.

## How It Works

Catalog files in `catalog/tables/` and `catalog/procedures/` contain the canonical state for each object -- profile answers, statement classifications, writer selections. When the FDE wants to override an agent's output, they edit the catalog file directly.

In the interactive path, this happens naturally: after each skill step, Claude presents the results and the FDE reviews, edits, and approves before the skill writes to the catalog file. The approved values (including any FDE edits) are what get persisted.

In the batch path, the agent writes catalog files autonomously. The FDE can review and edit them after the fact by modifying the JSON and committing.

## Override Visibility

All overrides are visible in git history. A `git log` or `git diff` on any catalog file shows exactly what changed, when, and by whom. No separate audit table is needed.

## Editable Fields

### Scoping Stage

The FDE can edit `catalog/procedures/<writer>.json` to change the selected writer for a table. The `selected_writer` field in the scoping output determines which procedure is used for downstream stages.

### Profiling Stage

The FDE can edit `catalog/tables/<table>.json` to change profile answers:

- `classification` -- table classification (fact, dimension, bridge, etc.)
- `primary_key` -- key columns and type (surrogate, natural, composite)
- `natural_key` -- natural key columns
- `watermark` -- incremental watermark column
- `foreign_keys` -- FK relationships
- `pii_actions` -- PII column handling (mask, hash, redact)
- `materialization` -- dbt materialization strategy
- `documentation` -- model name, description, owner, tags

### Migration and Test Generation

These are final output stages. Their outputs (dbt model SQL, schema.yml, test fixtures) are files in the `dbt/` directory, not catalog entries. The FDE reviews them as normal code files.
