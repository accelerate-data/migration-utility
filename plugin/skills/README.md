# Skills

Migration pipeline skills for the Claude Code plugin. Each skill is a self-contained workflow that the agent follows to complete one stage of the migration process.

## Directory layout

```text
plugin/skills/
  _shared/references/     Shared reference files used by multiple skills
  <skill-name>/
    SKILL.md              Required — skill definition and instructions
    references/           Optional — skill-specific reference docs
```

## Skill template

Every skill SKILL.md follows this structure:

```markdown
---
name: <gerund-noun>            # e.g. generating-model, profiling-table
description: >
  <what it does, when to trigger>
user-invocable: true | false   # true = user can invoke directly; false = pipeline-internal
argument-hint: "<args>"        # shown in skill picker UI
---

# <Title>

<One-line summary.>

## Arguments

`$ARGUMENTS` is ... Ask the user if missing.

## Schema discipline                          # if skill writes to catalog

Use the canonical `/<stage>` surfaced code list in `../../lib/shared/<stage>_error_codes.md`.
Do not define a competing public error-code list in this skill.

## Before invoking                            # stage guard

Check stage readiness:
...

## Steps                                      # numbered, sequential

### Step 1 -- ...
### Step 2 -- ...

## References                                 # links to reference files

## Error handling                             # exit-code-to-action table

| Command | Exit code | Action |
|---|---|---|
| ... | 1 | ... |
```

## Conventions

### Naming

Skill directories use gerund naming: `generating-tests`, `profiling-table`, `analyzing-table`.

### Frontmatter fields

| Field | Required | Notes |
|---|---|---|
| `name` | yes | Matches directory name |
| `description` | yes | When to trigger and what the skill does |
| `user-invocable` | yes | `true` for entry-point skills, `false` for pipeline-internal |
| `argument-hint` | yes | Describes expected arguments |
| `context` | no | Set to `fork` when spawned as an isolated sub-agent |

### Stage guards

All skills check readiness before proceeding:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util ready <fqn> <stage>
```

If `ready` is `false`, report the failing check and stop.

### Error codes

Each skill references its canonical error codes file at `../../lib/shared/<stage>_error_codes.md`. Skills must not define competing error-code lists.

### Staging directory

Skills that write multi-line content to CLI commands use `.staging/` to avoid shell quoting issues. Always clean up with `&&` (not `;`) so cleanup only runs on success:

```bash
mkdir -p .staging
# write files to .staging/
uv run ... --file .staging/payload.json && rm -rf .staging
```

### Write-through pattern

Skills that persist to catalog do not ask for user confirmation before writing. The skill is a write-through workflow — validation happens via Pydantic contracts at the CLI boundary.

### Shared references

Reference files used by multiple skills live in `_shared/references/`. Skill-specific references live in `<skill-name>/references/`.

Currently shared:

- `_shared/references/sql-style.md` — SQL formatting rules (SQL_001--SQL_013)
- `_shared/references/cte-structure.md` — CTE pattern rules (CTE_001--CTE_008)
- `_shared/references/model-naming.md` — Model naming rules (MDL_001--MDL_013)
- `_shared/references/yaml-style.md` — YAML formatting rules (YML_001--YML_008)
- `_shared/references/branch-patterns.md` — Conditional branch enumeration patterns for tables and views
- `_shared/references/dialects/` — dialect-specific references (statement classification, routine migration patterns). Skills read `dialect` from `manifest.json` to select the right file.
  - `tsql/statement-classification.md` — T-SQL statement migrate/skip classification
  - `tsql/routine-migration-ref.md` — T-SQL DML extraction and CTE refactoring patterns
  - `oracle/statement-classification.md` — Oracle PL/SQL statement classification
  - `oracle/routine-migration-ref.md` — Oracle PL/SQL extraction and CTE refactoring patterns

### Error handling table

Every skill ends with a table mapping CLI exit codes to agent actions. Format:

```markdown
| Command | Exit code | Action |
|---|---|---|
| `<cli command>` | 1 | <what to do> |
| `<cli command>` | 2 | <what to do> |
```
