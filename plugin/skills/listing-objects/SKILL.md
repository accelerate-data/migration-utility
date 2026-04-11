---
name: listing-objects
description: >
  Use when the user asks to "list tables", "list procedures", "show me object X", "what references Y", or wants to browse catalog contents.
user-invocable: true
argument-hint: "<subcommand> [object]"
---

# Listing Objects

Read-only catalog viewer. Displays whatever state exists in the catalog.

## Arguments

`$ARGUMENTS` is a subcommand followed by a type or object name. If no subcommand is given, default to `list`.

| Subcommand | Argument | Description |
|---|---|---|
| `list` | `tables`, `procedures`, `views`, or `functions` | Enumerate objects by type |
| `show` | `<schema.object>` | Display catalog state for one object |
| `refs` | `<schema.object>` | Show procedures/views that reference an object |

## Before invoking

No stage guard. This skill is read-only — it shows whatever exists in the catalog. The CLI commands handle missing catalog files via their own exit codes.

Use the canonical surfaced code list in `../../lib/shared/listing_objects_error_codes.md`.

## Output shapes

All CLI commands return JSON. Key fields: `objects[]` (list), `columns`/`refs`/`statements` (show), `readers`/`writers` (refs). Fields are `null` or `[]` when not applicable to the object type.

## list

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover list \
  --type <type>
```

Present as a numbered list. If the user selects an object, proceed to `show`. If they ask what references it, proceed to `refs`.

If `objects[]` is empty, say there are no objects of that type in the current catalog state. Do not troubleshoot fixtures or infer missing extraction steps unless the user asks.

## show

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover show \
  --name <fqn>
```

Present whatever the catalog currently holds for the object.

Do not special-case `is_source` tables. If `discover show` returns columns for a source table, present them as normal catalog state.
For a direct `show <schema.object>` request, run `discover show` once and answer from that payload. Do not ask follow-up questions before presenting the result.

**Tables:** columns, plus scoping results and analyzed statements if present.

**Views:** refs and definition.

**Procedures:** parameters, refs, statements if analyzed, raw DDL summary.

## refs

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover refs \
  --name <fqn>
```

Present `writers` (procs that modify the object) and `readers` (procs/views that select from it), grouped.

If the payload contains `error`, surface that error instead of inventing refs output from other catalog data.

## Error handling

Use the canonical `listing-objects` code list in [`../../lib/shared/listing_objects_error_codes.md`](../../lib/shared/listing_objects_error_codes.md).

Map command results like this:

| Command | Exit code / payload | Action |
|---|---|---|
| `discover list/show` | 1 | Surface `OBJECT_NOT_FOUND`, then stop |
| `discover list/show/refs` | 2 | Surface `CATALOG_IO_ERROR`, then stop |
| `discover refs` | payload `error` saying the target is a procedure or unsupported target for refs | Surface `UNSUPPORTED_OBJECT_TYPE`, include the returned guidance, do not ask a follow-up question, do not fall back to `show`, then stop |
| `discover refs` | payload `error` saying `no catalog file for ...` | Surface `OBJECT_NOT_FOUND`, then stop |
| `discover show` | `parse_error` / parse warning in payload | Surface `PARSE_ERROR` or `DDL_PARSE_ERROR` as a warning and continue showing available raw catalog state |
