# Modularity

Source: https://www.getdbt.com/blog/modular-data-modeling-techniques

| code | rule | severity |
|------|------|----------|
| MOD_001 | Staging models must not contain joins — joins belong in mart or intermediate models | error |
| MOD_002 | Mart models must reference staging models via `ref()`, not `source()` directly | error |
| MOD_003 | One staging model per source table — do not merge multiple source tables in a single staging model | warning |
| MOD_004 | Staging models must be materialized as `ephemeral` | error |
| MOD_005 | All business logic (joins, CASE WHEN, aggregations, window functions) lives in mart or intermediate models, not staging | error |
| MOD_006 | Transformations shared across multiple models must be extracted into a foundational model and referenced via `ref()` | warning |
| MOD_007 | Staging layer transformations are limited to: field type casting, column renaming, filtering deleted records, and light standardization | warning |
| MOD_008 | Individual model files should not exceed approximately 100 lines — split larger models into intermediate steps | info |
