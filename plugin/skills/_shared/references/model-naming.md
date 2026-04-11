# Model Naming

Source: https://docs.getdbt.com/best-practices/how-we-style/1-how-we-style-our-dbt-models

| code | rule | severity |
|------|------|----------|
| MDL_001 | Model name must match the target table name in `snake_case` (e.g., `dimcustomer` for `silver.DimCustomer`) | error |
| MDL_002 | Generate one reviewable model artifact per target. Do not split one target across multiple helper SQL files. | error |
| MDL_003 | `stg_`-prefixed model names are valid when the selected target artifact belongs in the staging layer. | info |
| MDL_004 | All model names must be `snake_case` — no dots, dashes, or mixed case | error |
| MDL_005 | `_dbt_run_id = {{ invocation_id }}` must be present in all materializations | error |
| MDL_006 | `_loaded_at = current_timestamp()` must be present for table and snapshot materializations | error |
| MDL_007 | `_loaded_at` must NOT appear in incremental models — incremental models use a watermark column instead | warning |
| MDL_008 | Locked column names from the target schema must not be renamed or removed | error |
| MDL_009 | Primary keys follow the pattern `<object>_id` (e.g., `account_id`) | warning |
| MDL_010 | Boolean columns are prefixed with `is_` or `has_` | warning |
| MDL_011 | Timestamp columns follow the pattern `<event>_at` in UTC (e.g., `created_at`) | info |
| MDL_012 | Do not use database reserved words as column names | error |
| MDL_013 | Use business terminology over source system terminology where the business has an established name | info |
