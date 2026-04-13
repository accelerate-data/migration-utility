# CTE Structure

Source: https://docs.getdbt.com/guides/refactoring-legacy-sql?step=5

| code | rule | severity |
|------|------|----------|
| CTE_001 | Import CTEs must come first — all `{{ source() }}` and `{{ ref() }}` calls placed at the top of the file | error |
| CTE_002 | The final transformation CTE must be named `final` | error |
| CTE_003 | The last statement in the file must be `select * from final` | error |
| CTE_004 | Each CTE performs one logical unit of work | warning |
| CTE_005 | CTE names are descriptive and reflect the transformation performed (e.g., `events_joined_to_users`) | warning |
| CTE_006 | No nested CTEs — every nested subquery must be extracted to its own CTE | error |
| CTE_007 | Import CTEs use simple `select *` (optionally with column selection and `where` filters) — no transformation logic | warning |
| CTE_008 | CTEs are ordered sequentially so each CTE depends only on CTEs defined above it | warning |
