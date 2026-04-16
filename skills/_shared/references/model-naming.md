# Model Naming

Source: https://docs.getdbt.com/best-practices/how-we-style/1-how-we-style-our-dbt-models

| code | rule | severity |
|------|------|----------|
| MDL_001 | First-pass generated mart model names must match the normalized lowercase target object name (e.g., `dimcustomer` for `silver.DimCustomer`, `dim_customer` for `silver.dim_customer`) | error |
| MDL_002 | Generate one reviewable model artifact per target. Do not split one target across multiple helper SQL files. | error |
| MDL_003 | Do not add layer prefixes such as `stg_` or `int_` to first-pass generated mart target models. Layer prefixes are valid only for staging and intermediate models. | error |
| MDL_004 | Model names must be lowercase identifiers with only letters, numbers, and existing underscores. Preserve underscores already present in the target name, but do not invent new separators. | error |
| MDL_005 | `_dbt_run_id = '{{ invocation_id }}'` must be present in all materializations | error |
| MDL_006 | `_loaded_at = {{ current_timestamp() }}` must be present for table mart and snapshot materializations | error |
| MDL_007 | `_loaded_at` must NOT appear in incremental models — incremental models use a watermark column instead | warning |
| MDL_008 | Locked column names from the target schema must not be renamed or removed | error |
| MDL_009 | Primary keys follow the pattern `<object>_id` (e.g., `account_id`) | warning |
| MDL_010 | Boolean columns are prefixed with `is_` or `has_` | warning |
| MDL_011 | Timestamp columns follow the pattern `<event>_at` in UTC (e.g., `created_at`) | info |
| MDL_012 | Do not use database reserved words as column names | error |
| MDL_013 | Use business terminology over source system terminology where the business has an established name | info |
| MDL_014 | Staging wrappers follow dbt's `stg_[source]__[entity]` pattern; generated migration projects use `source = bronze`, producing `stg_bronze__<entity>` | error |
| MDL_015 | Intermediate models follow `int_<entity>_<purpose>` when created by refactor workflows | error |
| MDL_016 | Mart models must reference confirmed source dependencies through `ref('stg_bronze__<entity>')`, not direct `source()` calls or raw warehouse names | error |
| MDL_017 | Mart models must reference catalog tables marked `is_seed: true` through `ref('<seed_name>')`, not `source()` calls or raw warehouse names | error |

Derive first-pass mart `model_name` mechanically from the target object name:
drop the schema, lowercase the object name, and preserve only underscores that
already exist. Do not split CamelCase or invent separators. For example,
`silver.InsertSelectTarget` becomes `insertselecttarget`, `silver.FactSales`
becomes `factsales`, and `silver.dim_customer` remains `dim_customer`.
