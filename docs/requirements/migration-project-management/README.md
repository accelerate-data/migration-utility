# Migration Project Management

## Goal

Use Paperclip as the orchestration layer for one migration engagement while the migration repo and plugin commands remain the execution and state authority.

## Scope

- One Paperclip project represents one migration.
- One company may run many migration projects in parallel.
- The project spans migration, ingestion, validation, and cutover for the approved in-scope objects.
- Paperclip manages planning, task projection, assignment, heartbeat reconciliation, budget visibility, and human escalation.
- Repo commands perform all migration work and all durable scope changes.

## Terminology

- `migration object`: an approved in-scope deliverable such as a migrated table, source or lakehouse table, or deployed pipeline
- `Paperclip parent issue`: the planning container for one migration object
- `Paperclip phase task`: a child issue representing one executable phase for that migration object
- `GitHub issue`: a linked defect record for code, CI, validation, data-quality, or operational problems

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

- Repo state is authoritative for migration readiness, completion, and durable scope.
- Paperclip is authoritative for assignment, comments, escalation, approvals, and orchestration history.
- GitHub Issues are authoritative for code, CI, pipeline, data-quality, and validation defects.
- Paperclip task state is derived from repo readiness plus linked blocking defects and human decisions.
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

- Durable planning unit: one Paperclip parent issue per approved migration object
- Child tasks: one Paperclip phase task per phase in that migration object's template
- Planner-owned exception tasks: `manual-fix`, `clarification`, `recheck`

Each migration object may also link to zero or more GitHub Issues. Those defects do not replace the parent Paperclip issue; they provide defect tracking for the same migration object.

Day 1 task creation is full-chain, not lazy. The CDO creates the phase tasks from repo status, then assigns only the runnable tasks.

## Object Types

Day 1 project planning recognizes these object types:

- migrated data mart tables
- migrated source or lakehouse tables
- deployed pipelines

Different object types may use different phase templates. The project stays unified even when objects follow different work patterns.

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

## External Run Model

- Long-running external work uses a `launch -> poll -> reconcile` pattern.
- Repo commands or external runtimes perform the actual run; Paperclip phase tasks track the orchestration state around that run.
- Workers do not hold ownership indefinitely while an external system is running unattended.
- Heartbeats reconcile external status back into Paperclip statuses, comments, and linked defects.

## Scope Management

Day 1 requires deterministic repo commands for both directions of scope change:

- mark a table as a source
- clear source status
- exclude a table or view from migration
- re-include a previously excluded object

Paperclip should never hand-edit catalog JSON.

The approved object set is the project contract. If an object is added to or removed from scope, the change requires human approval and the CDO re-plans the affected lanes from repo state.

## Parallel Run and Validation

Parallel run and cutover are part of the same migration project, not a follow-on project.

- Soft runs may happen during the project as readiness improves.
- The formal run happens after all approved readiness conditions are met.
- Formal validation compares only like-to-like approved in-scope deliverables, not internal intermediate artifacts created by refactoring.
- Required lakehouse objects and pipelines are still first-class project deliverables because they are needed to make the formal run and cutover possible.
- A single migration object may consolidate multiple validation or pipeline defects.
- Paperclip orchestrates run and validation tasks; repo commands and external execution surfaces perform the actual run, output collection, and comparison.

## GitHub and Paperclip Responsibilities

- GitHub Issues are the source of truth for pipeline failures, data-quality failures, and validation mismatches.
- Paperclip is the source of truth for project planning, execution state, assignment, escalation, and human approvals or overrides.
- The CDO may create GitHub Issues automatically when validation or operational defects are detected.
- The CDO may create new Paperclip phase tasks automatically when new non-scope work is discovered from repo or GitHub state.
- Humans and agents may update the GitHub Issue lifecycle there.
- Only scope changes require human approval before re-planning.
- A migration object may link to multiple GitHub Issues, and only linked blocking defects prevent the affected Paperclip phase from advancing.
- Human approvals and overrides for migration readiness or cutover are recorded in Paperclip against the affected migration object, even when the underlying defect lives in GitHub.

## Cutover Concept

- The project ends only when approved in-scope objects are migrated, validated, and ready for cutover.
- If an approved object is not part of the formal run, it must first be removed from scope through the approved project workflow.
- The formal run starts automatically when readiness conditions are met, with human notification.
- Human approval can override unresolved issues, but the override must be attached to the affected migration object.

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
