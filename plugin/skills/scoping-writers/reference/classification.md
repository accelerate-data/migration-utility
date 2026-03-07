# Write Classification

Perform **structural analysis** on each procedure body — understand the code, not just
keyword scanning. Detect writes to the target table and any view that maps to it.

| Statement | Classification |
|---|---|
| `INSERT [INTO] <target>` | `direct` |
| `UPDATE <target>` | `direct` |
| `DELETE [FROM] <target>` | `direct` |
| `MERGE [INTO] <target>` | `direct` |
| `TRUNCATE TABLE <target>` | `direct` |
| Calls a procedure confirmed to write to target | `indirect` |
| No write to target | `read_only` |

Flag dynamic SQL patterns: `EXEC(@sql)`, `sp_executesql @stmt`, string-built table names.
These reduce confidence but do not disqualify.
