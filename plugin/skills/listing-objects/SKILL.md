---
name: listing-objects
description: >
  Read-only catalog viewer. Use when the user asks to "list tables", "list procedures", "show me object X", "what references Y", or wants to browse catalog contents.
user-invocable: true
argument-hint: "<subcommand> [object]"
---

# Listing Objects

Read-only catalog viewer. Displays whatever state exists in the catalog — columns, refs, scoping results, analyzed statements. Never writes to the catalog.

## Arguments

`$ARGUMENTS` is a subcommand followed by a type or object name. If no subcommand is given, default to `list`.

| Subcommand | Argument | Description |
|---|---|---|
| `list` | `tables`, `procedures`, `views`, or `functions` | Enumerate objects by type |
| `show` | `<schema.object>` | Display catalog state for one object |
| `refs` | `<schema.object>` | Show procedures/views that reference an object |

## Before invoking

For object-specific subcommands (`show <schema.object>`, `refs <schema.object>`), run the same stage readiness guard used by `analyzing-table`:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util ready <fqn> scope
```

If `ready` is `false`, report the failing `code` and `reason` to the user and stop.
If `code` is absent, report the `reason`.

For `list <type>`, do not run a per-item guard because there is no specific FQN yet.

Use the canonical surfaced code list in `../../lib/shared/listing_objects_error_codes.md`. Do not define a competing public error-code list in this skill.

## Output schemas

| Subcommand | Schema |
|---|---|
| `list` | `../../lib/shared/schemas/discover_list_output.json` |
| `show` | `../../lib/shared/schemas/discover_show_output.json` |
| `refs` | `../../lib/shared/schemas/discover_refs_output.json` |

## list

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover list \
  --type <type>
```

Present as a numbered list. If the user selects an object, proceed to `show`. If they ask what references it, proceed to `refs`.

## show

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover show \
  --name <fqn>
```

Present whatever the catalog currently holds for the object.

**Tables:** columns, plus scoping results and analyzed statements if present.

**Views:** refs and definition.

**Procedures:** parameters, refs, statements if analyzed, raw DDL summary.

## refs

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover refs \
  --name <fqn>
```

Present `writers` (procs that modify the object) and `readers` (procs/views that select from it), grouped.

**Known limitation:** Procs that write only via dynamic SQL (`EXEC(@sql)`, `sp_executesql`) will not appear as writers.

## Error handling

Use the canonical `listing-objects` code list in [`../../lib/shared/listing_objects_error_codes.md`](../../lib/shared/listing_objects_error_codes.md).

Map failures like this:

| Command | Exit code / payload | Action |
|---|---|---|
| `discover list/show/refs` | 1 | Surface `OBJECT_NOT_FOUND` or `UNSUPPORTED_OBJECT_TYPE` based on the returned message, then stop |
| `discover list/show/refs` | 2 | Surface `CATALOG_IO_ERROR`, then stop |
| `discover show` | `parse_error` / parse warning in payload | Surface `PARSE_ERROR` or `DDL_PARSE_ERROR` as a warning and continue showing available raw catalog state |
