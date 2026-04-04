# Browsing the Catalog

The `/listing-objects` skill is a read-only catalog viewer for exploring tables, procedures, views, and functions before running pipeline stages. It never writes to the catalog.

## Subcommands

### `list` -- enumerate objects by type

```text
/listing-objects list tables
/listing-objects list procedures
/listing-objects list views
/listing-objects list functions
```

Lists all objects of the given type from the catalog as a numbered list. If no subcommand is given, `list` is the default.

### `show` -- inspect a single object

```text
/listing-objects show silver.DimCustomer
/listing-objects show dbo.usp_load_dimcustomer
```

Displays whatever state the catalog currently holds for the object:

- **Tables:** columns, plus scoping results and analyzed statements if present
- **Procedures:** parameters, references, statements (if analyzed), raw DDL summary
- **Views:** references and definition

### `refs` -- trace references to an object

```text
/listing-objects refs silver.DimCustomer
```

Shows which procedures and views reference the given object, grouped into:

- **Writers** -- procedures that modify the object (INSERT, UPDATE, MERGE, DELETE)
- **Readers** -- procedures and views that select from it

Known limitation: procedures that write only via dynamic SQL (`EXEC(@sql)`, `sp_executesql`) will not appear as writers.

## Example workflow

A typical exploration session before scoping:

1. **List all tables** to see what is in the catalog:

   ```text
   /listing-objects list tables
   ```

2. **Pick a table** and inspect its details:

   ```text
   /listing-objects show silver.DimCustomer
   ```

3. **Check what writes to it** to understand which procedures are candidates for scoping:

   ```text
   /listing-objects refs silver.DimCustomer
   ```

4. **Inspect a candidate procedure** to see its parameters, references, and raw DDL:

   ```text
   /listing-objects show dbo.usp_load_dimcustomer
   ```

5. **Decide what to scope** based on the catalog state, then proceed to `/analyzing-table` or `/scope`.

## When to use

- **Before scoping** -- understand which tables exist, which procedures write to them, and what the catalog contains before committing to a migration batch
- **During debugging** -- inspect catalog state to understand why a guard is failing (e.g. check if `selected_writer` is set, or if statements are resolved)
- **After setup** -- verify that `/setup-ddl` populated the catalog correctly

## Prerequisites

`manifest.json` must exist in the project root. If missing, run `/setup-ddl` first.

## Error handling

| Situation | Behavior |
|---|---|
| Object not found | Reports which object was not found and stops |
| Catalog directory unreadable | Reports IO error and stops |
| Procedure with parse error | Shows `raw_ddl` for manual inspection |

## Related pages

- [[Skill Listing Objects]] -- full schema details and CLI reference
- [[Stage 1 Scoping]] -- using catalog data for writer discovery
- [[Glossary]] -- definitions of catalog, item_id, routing flags
