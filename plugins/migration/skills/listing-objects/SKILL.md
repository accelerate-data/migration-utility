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

Read `manifest.json` from the current working directory to confirm a valid project root. If missing, tell the user to run `setup-ddl` first.

## Output schemas

| Subcommand | Schema |
|---|---|
| `list` | `lib/shared/schemas/discover_list_output.json` |
| `show` | `lib/shared/schemas/discover_show_output.json` |
| `refs` | `lib/shared/schemas/discover_refs_output.json` |

## list

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/../../lib" discover list \
  --type <type>
```

Present as a numbered list. If the user selects an object, proceed to `show`. If they ask what references it, proceed to `refs`.

## show

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/../../lib" discover show \
  --name <fqn>
```

Present whatever the catalog currently holds for the object.

**Tables:** columns, plus scoping results and analyzed statements if present.

**Views:** refs and definition.

**Procedures:** parameters, refs, statements if analyzed, raw DDL summary.

## refs

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/../../lib" discover refs \
  --name <fqn>
```

Present `writers` (procs that modify the object) and `readers` (procs/views that select from it), grouped.

**Known limitation:** Procs that write only via dynamic SQL (`EXEC(@sql)`, `sp_executesql`) will not appear as writers.

## Error handling

| Condition | Action |
|---|---|
| `discover list/show/refs` exits 1 | Object not found or catalog file missing. Report which object and stop |
| `discover list/show/refs` exits 2 | Catalog directory unreadable (IO error). Report and stop |
| Procedure has `parse_error` | Still loaded — `raw_ddl` preserved for inspection |
