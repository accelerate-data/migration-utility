# Fast Path — Small Issues

## When to Use

Use when ALL are true:
- Estimate is XS or S (1-2 points)
- Changes are isolated to one area of the codebase
- User can override in either direction

## How It Works

Skip team orchestration. Spawn a **single `general-purpose` sub-agent** that implements, updates/adds/removes tests as needed, commits, pushes, and checks off ACs on Linear. The agent must read existing tests before writing any — update broken tests, remove redundant ones, and only add tests for genuinely new behavior.
Only the **code reviewed** and **final validation** gates apply — the single agent handles tests and logging inline. Code review and PR creation are never skipped.

**Always run `npx tsc --noEmit`** (from `app/`) before committing — catches type errors in untouched files that reference changed interfaces.

Linear updates still apply — write what was done, tests, and PR link.
