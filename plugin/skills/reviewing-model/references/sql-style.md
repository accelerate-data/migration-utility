# SQL Style

Source: https://docs.getdbt.com/best-practices/how-we-style/2-how-we-style-our-sql

| code | rule | severity |
|------|------|----------|
| SQL_001 | Keywords, field names, and function names must be lowercase | error |
| SQL_002 | Use 4-space indentation per indent level | warning |
| SQL_003 | Use trailing commas at the end of each item in a list | warning |
| SQL_004 | One column per line in SELECT statements | warning |
| SQL_005 | Always prefix column names with table name or alias when joining multiple tables | warning |
| SQL_006 | No `SELECT *` in mart models — enumerate columns explicitly | error |
| SQL_007 | Use the `as` keyword explicitly when aliasing a field or table | warning |
| SQL_008 | Avoid abbreviated table aliases — use descriptive names instead of initialisms | warning |
| SQL_009 | Write explicit join types (`inner join`, `left join`) rather than bare `join` | warning |
| SQL_010 | Favor `union all` over `union` unless duplicate removal is specifically required | warning |
| SQL_011 | Place regular fields before aggregates and window functions in SELECT | info |
| SQL_012 | Group by column name rather than numeric position references (e.g., `group by customer_id` not `group by 1`) | warning |
| SQL_013 | Lines of SQL should be no longer than 80 characters | info |
