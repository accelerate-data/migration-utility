# Inversion Analysis: Where the Proposed Flow Breaks

We applied inversion thinking to the proposed agentic migration flow -- asking "how will this fail?" instead of "how will this work?" This document captures 12 failure scenarios across 5 categories, along with the first-principles resolutions we reached for each.

## Category 1: Domain Boundaries Are Messy

### Scenario: Conformed dimensions have no home

`dim_date`, `dim_currency`, `dim_geography` are used by every fact table across every domain. Assigning them to a domain is arbitrary -- they're shared infrastructure.

**Resolution**: Foundation layer concept. Auto-detect conformed dimensions (referenced by 3+ fact tables across 2+ schemas). Process them first, independently of domain assignment. They're prerequisites, not domain members.

### Scenario: Bridge tables straddle domains

`bridge_customer_product` -- is that customer domain or product domain? Domain assignment debates for cross-domain objects add no migration value.

**Resolution**: Agent proposes, human adjusts during triage. Bridge tables go to whichever domain the agent suggests based on reference weight. If genuinely ambiguous, either domain works -- the `{{ ref() }}` resolves regardless of folder location.

### Scenario: Circular cross-domain dependencies

Customer domain has `fact_customer_orders` -> `dim_product` (product domain). Product domain has `fact_product_returns` -> `dim_customer` (customer domain).

**Resolution**: This only happens at the fact layer. Dims in both domains complete first (no circular dep at dim level). Facts in both domains run in parallel since they only READ from already-completed dims. The circle breaks because cross-domain dependencies at the fact level are read-only references to completed dimension models.

## Category 2: Test Blind Spots

### Scenario: Ground truth from buggy source validates bug reproduction

Source procedure has a known bug. Tests capture buggy ground truth. Generated model reproduces bug. Tests pass.

**Resolution (user's call)**: Migration means "reproduce what exists." Source code IS ground truth. If you want to fix bugs, that's a separate, explicit decision -- not something the migration tool does silently. This is correct behavior, not a blind spot.

### Scenario: Branch coverage does not equal semantic coverage

A MERGE statement's behavior depends on source/target data state combinations, not explicit IF/ELSE branches. The test generator might miss implicit branches.

**Resolution (user's call)**: This is a quality-of-implementation issue with the test generator, not a fundamental flaw. The test generator must treat MERGE WHEN clauses, CASE arms, NULL handling, and JOIN filter conditions as branches. The LLM can identify these from code. Solvable by making the test generator thorough.

### Scenario: Tests cannot validate transactional semantics

Source procedure writes to table A and table B in a single transaction. dbt models execute independently. If model B fails, model A's changes persist (no rollback).

**Resolution (first principles)**: In a DW context, the business requirement is "consistent at rest after load completes," not atomic mid-load. dbt achieves this through DAG ordering + idempotent models + re-runnable loads. The source transaction boundary is an implementation detail, not a business requirement.

**Exception**: SCD2 current+history patterns (same entity, different time granularity) MUST be handled as a single dbt snapshot model, not two separate models. The model generator should detect this pattern.

### Scenario: Tests pass at sandbox scale, fail at production scale

10 rows in sandbox, 10M in production. Non-deterministic ORDER BY, floating-point precision, query timeouts.

**Status**: Acknowledged risk. Not solvable in the migration tool -- this is a deployment concern. The tool validates logical correctness; performance testing is a separate phase.

## Category 3: Error Cascading in Autonomous Pipeline

### Scenario: Wrong writer selection compounds through 4 autonomous steps

Agent picks wrong writer -> profile built on wrong logic -> refactored SQL from wrong procedure -> model generated from wrong source. Tests fail, but debugging through 4 layers is expensive.

**Resolution**: Self-checks between each step catch ~80% of errors early. After scope: "does selected writer actually INSERT/UPDATE/MERGE into this table?" After profile: "does classification match procedure patterns?" These are machine-verifiable postconditions. Late detection still happens for the ~20% that pass self-checks but fail tests -- accepted cost of autonomy.

### Scenario: Agent is confidently wrong (high confidence, wrong answer)

The LLM selects a plausible-looking wrong writer, self-checks pass (the writer DOES write to the table, just not the one the human would choose), and the human skips review because it's flagged "high confidence."

**Status**: Genuine risk. Mitigation: tests as final validation. If the wrong writer produces wrong output, tests fail. If the wrong writer produces correct output (because both writers produce equivalent results), then the choice doesn't matter. The only dangerous case is: wrong writer, tests pass, production behavior differs -- which means the test generator missed a scenario. This chains back to test generator thoroughness.

## Category 4: Review Bottlenecks

### Scenario: Large domain blocks on single failure

"Sales" domain has 30 objects. 1 complex fact table fails refactoring. Cannot sign off on domain as complete.

**Resolution**: Domain is the review unit, but objects within it have independent status. "Sales domain: 29/30 complete. 1 escalated: fact_sales_complex -- refactoring failed on line 340, procedure has dynamic SQL that couldn't be statically analyzed. Options: [manual refactor] [exclude and migrate separately]." The human approves the 29 and handles the 1 separately.

### Scenario: Sequential domain review serializes parallel work

Three independent domains could run and be reviewed concurrently, but domain-level processing implies serial.

**Resolution**: Independent domains run and are reviewed in parallel. "Domain level" means the domain is the unit of work, not that domains are serialized. If customer and product domains have no interdependency (beyond shared foundation dims already completed), both execute concurrently.

## Category 5: Cost and Scope

### Scenario: Whole-DB extraction + triage is expensive for phased migrations

200 tables, 300 procedures. Even with triage excluding ~40%, deep analysis on 120+ tables is significant API cost. Real migrations are phased -- not everything migrated at once.

**Resolution**: Extraction and triage are cheap (I/O + metadata-level classification). The expensive work (scope + profile + refactor + generate) only runs on domains the user selects after triage. Flow: Extract whole DB (cheap) -> Triage whole DB (medium) -> User picks domains for this wave -> Deep analysis on selected domains only (expensive but scoped).

## Summary Risk Matrix

| Risk | Severity | Mitigated By |
|---|---|---|
| Conformed dims have no domain | High | Foundation layer (auto-detected) |
| Bridge tables straddle domains | Low | Agent proposes, human adjusts |
| Circular cross-domain deps | Medium | Dims-first ordering breaks cycles |
| Bug reproduction via tests | N/A | Correct behavior (source = ground truth) |
| Implicit branch coverage | Medium | Thorough test generator implementation |
| Transactional semantics | Medium | DW loads don't need mid-load atomicity; SCD2 -> snapshot |
| Scale-dependent failures | Low | Out of scope (deployment concern) |
| Error cascading (4 autonomous steps) | Medium | Self-checks between steps |
| Confident wrong answer | Medium | Tests as final validation + test generator thoroughness |
| Large domain partial failure | Low | Per-object status within domain |
| Sequential domain review | Low | Parallel execution of independent domains |
| Cost of whole-DB analysis | Low | Triage gates expensive work to selected domains |
