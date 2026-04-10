# Migration Project Management

## Goal

Use Paperclip as the orchestration layer for one migration engagement while the migration repo and plugin commands remain the execution and state authority.

## Scope

- One Paperclip project represents one migration.
- One company may run many migration projects in parallel.
- Paperclip manages planning, task projection, assignment, heartbeat reconciliation, budget visibility, and human escalation.
- Repo commands perform all migration work and all durable scope changes.

## System Roles

### CDO

- Owns the migration project budget and overall objective.
- Bootstraps the project after the human provides external bindings.
- Reconciles repo status into Paperclip on heartbeat.
- Creates and updates the task graph.
- Assigns runnable execution tasks to workers.
- Owns clarification and manual-fix tasks.
- Escalates to humans through Slack/OpenClaw and Paperclip comments.

### Data Engineer

- Generic execution worker.
- Runs atomic repo commands only.
- Retries bounded runtime failures.
- Reports results, evidence, and exhausted retries back to the planner.
- Does not make arbitrary repo edits outside approved commands.

### Human

- Provides external bindings and the migration objective.
- Approves or edits the initial migration scope.
- Can add or remove tables later.
- Responds to clarifications and manual-fix requests.

## Source of Truth

- Repo status output is the single source of truth for migration state.
- Paperclip task state is a projection of repo state.
- Scope changes must be written through repo commands, then reflected into Paperclip on the next heartbeat.
- Slack replies are authoritative only after OpenClaw mirrors them into Paperclip task comments.

## Project Setup

The human must provide:

- repo target
- source database connection and credentials
- Slack/OpenClaw routing target
- project budget
- migration objective and initial boundaries

The CDO then:

1. bootstraps the migration repo state
2. runs discovery
3. proposes the initial migration scope
4. waits for human approval or edits
5. creates the Paperclip task graph from approved repo state

## Task Model

- Durable planning object: one table- or view-level issue per migration target
- Child tasks: one task per pipeline phase
- Planner-owned exception tasks: `manual-fix`, `clarification`, `recheck`

Day 1 task creation is full-chain, not lazy. The CDO creates the phase tasks from repo status, then assigns only the runnable tasks.

## Lifecycle Mapping

Paperclip task lifecycle follows its native statuses. Repo-derived readiness determines which status a task should hold.

- `backlog`: future phase exists but is not yet actionable
- `todo`: phase is ready but not assigned
- `in_progress`: worker is actively executing the phase command
- `in_review`: planner-owned waiting state for human clarification, approval, or manual-fix triage
- `blocked`: repo status or planner triage says the phase cannot proceed
- `done`: repo status shows the phase is complete

Paperclip does not lead state back into the repo.

## Execution Model

- Workers finish tasks and stop.
- The heartbeat loop performs all graph recomputation.
- Completion of one task may unblock many other tasks, but only the heartbeat assigns them.
- Workers may do bounded self-retry for transient or locally fixable failures.
- Semantic blockers, unsupported patterns, and exhausted retries return control to the CDO.

## Scope Management

Day 1 requires deterministic repo commands for both directions of scope change:

- mark a table as a source
- clear source status
- exclude a table or view from migration
- re-include a previously excluded object

Paperclip should never hand-edit catalog JSON.

## Human Engagement Rules

The CDO should auto-advance normal migration flow and only interrupt the human for exceptions:

- missing business choice or scope clarification
- diagnostic or semantic blockers
- manual-fix approval or direction

Unblocked work should continue in parallel while affected tables remain planner-owned.

## Cost Model

- Budget is tracked at the migration-project level and owned by the CDO.
- Worker spend rolls up into the same project budget.
- Paperclip dashboards and cost APIs provide budget visibility and alerting.

## Day 1 Implementation Boundary

Paperclip should orchestrate, not replace, the existing migration pipeline.

Day 1 expects:

- repo bootstrap and discovery commands
- reversible scope-management commands
- stable machine-readable status and batch schedule output
- worker-safe atomic commands for each migration phase

Anything that changes migration semantics belongs in the repo, not in Paperclip.
