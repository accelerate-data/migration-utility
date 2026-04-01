# Command Orchestration Design

## Overview

Commands are plugin command files (`.md` files), not skills. They run inside the Claude Code session and can spawn sub-agents. Each command handles both single-table and multi-table invocation through the same code path — single-table is just a batch of one.

## Commands

| Command | Plugin | Skill per table | Notes |
|---|---|---|---|
| `/scope` | `migration` | `/scoping-table` | |
| `/profile` | `migration` | `/profiling-table` | |
| `/generate-tests` | `ground-truth-harness` | `/generating-tests` | Includes test-reviewer sub-agent loop |
| `/generate-model` | `migration` | `/generating-model` | Includes code-reviewer sub-agent loop |

## Invocation

The FDE types a command followed by fully-qualified table names:

```text
/scoping silver.DimCustomer silver.DimProduct silver.FactSales
```

Table names are `schema.table`. Single table = one sub-agent, FDE reviews inline. Multiple tables = parallel sub-agents, FDE reviews summary at end.

## Command Lifecycle

Every command follows the same lifecycle:

1. Accept table names from FDE.
2. Create a worktree: `git worktree add ../worktrees/run/<command>-batch-N`. Isolates the batch so the FDE can run multiple commands in parallel.
3. Clear `.migration-runs/`, write `meta.json`.
4. Spawn one sub-agent per table in parallel.
5. Each sub-agent follows the corresponding skill's per-table processing rules.
6. Sub-agents run autonomously; on error, skip the table and continue.
7. Each sub-agent writes its result to `.migration-runs/<schema>.<table>.json`.
8. Command aggregates results into `.migration-runs/summary.json`.
9. Present summary to FDE.
10. Ask FDE: commit and open PR?

## Run Log Structure

```text
.migration-runs/
  meta.json                        # stage, tables, started_at
  <schema>.<table>.json            # one per item — sub-agent writes on completion
  summary.json                     # command writes after all sub-agents finish
```

- Cleared at the start of each command invocation.
- `.gitignore`d — never committed.
- No run IDs, no nesting.
- Consumed at commit/PR time for rich commit messages and PR bodies.

## Relationship to Skills

Skills define per-table processing rules. Commands reference skills when constructing sub-agent prompts. The skill is unaware of whether it was invoked directly by the FDE or by a command's sub-agent.

## Relationship to Agents

There are no standalone agent definition files for command orchestration. The command constructs sub-agent prompts inline from skill rules and catalog context.
