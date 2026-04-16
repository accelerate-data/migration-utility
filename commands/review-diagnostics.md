---
name: review-diagnostics
description: >
  Review active catalog diagnostics for one table or migration object, fixing
  catalog state where possible or writing reviewed-warning artifacts.
user-invocable: true
argument-hint: "<schema.table>"
---

# Review Diagnostics

Review all active catalog diagnostics for one table.

Run the `reviewing-diagnostics` skill with `$ARGUMENTS` and follow its workflow:

```text
skills/reviewing-diagnostics/SKILL.md
```

Do not review all tables at once. If `$ARGUMENTS` is missing, ask for one table
FQN before continuing.
