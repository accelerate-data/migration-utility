# First Principles Resolutions

After identifying failure modes via inversion thinking, we applied first principles analysis to five key challenges. Each section states the fundamental truths, derives the logical conclusion, and lands on a concrete design decision.

## 1. What's the Right Grouping Unit?

### Fundamental Truths
- Objects have dependencies (FK chains, procedure references) — this is physics, not opinion
- Humans need bounded, coherent sets to review — this is cognitive science
- Execution must respect dependency order — this is a DAG constraint
- The target architecture should be domain-organized (modern DW best practice)

### The Insight
Execution order and review grouping are two different things that the current design conflates. Think of it like a build system: you say `make customer-service`, and make knows it needs `common-libs` first. You don't manually sequence that — make does. But the unit you reason about is `customer-service`.

### Design Decision: Three-Layer Model

**Foundation layer** — conformed/shared dimensions (dim_date, dim_currency, dim_geography). Auto-detected (referenced by 3+ fact tables across 2+ schemas). Always builds first, automatically. Nobody "reviews" dim_date as part of the customer domain.

**Domains** — vertical slices the human defines/validates at triage. Agent proposes based on naming + reference clustering. Domain assignment is a first-class deliverable — it becomes the target folder structure in dbt. Bridge tables go where the agent suggests; human adjusts if wrong.

**Dependency scheduler** — within and across domains, the agent resolves execution order from the DAG. If customer domain needs `dim_geography` from foundation, the agent pulls it in. The human never sees this plumbing.

## 2. What Role Do Tests Play?

### Fundamental Truths
- Migration means "reproduce what exists" — source code is ground truth
- Branch coverage requires identifying ALL code paths, including implicit ones (MERGE clauses, CASE arms, NULL handling)
- DW loads require consistency at rest, not atomic mid-load transactions
- dbt achieves consistency through DAG ordering + idempotent models

### Design Decisions

**Source = ground truth.** If the source has a bug, the migration reproduces it. Fixing bugs is a separate, explicit decision. The migration tool does not silently change behavior.

**Test generator must be thorough about implicit branches.** MERGE WHEN clauses, CASE arms, NULL coalescing, and JOIN filter conditions are branches. The LLM can identify these from code. This is a quality-of-implementation requirement, not an architectural issue.

**Transactional semantics are an implementation detail.** The business requirement in a DW is consistent data at rest. dbt handles this via DAG ordering. Exception: SCD2 current+history patterns must be handled as a single snapshot model, not two separate models. The model generator should detect this pattern (procedure writes to both a current-state table and a history table for the same entity).

## 3. How Do We Balance Autonomy vs. Safety?

### Fundamental Truths
- LLMs make mistakes (~5-15% on complex reasoning)
- Errors compound: 4 steps at 90% accuracy each = 66% all correct
- Late detection is expensive (unwind 4 steps vs 1)
- Human involvement is ALSO expensive (blocking, context switching, serial processing)

### The Optimization Problem
Minimize `total_cost = human_involvement_cost + error_recovery_cost`

Neither "human at every step" nor "human only at the end" is optimal. The answer depends on whether we can catch errors without human judgment.

### Design Decision: Self-Checks (CI Pipeline Model)

Each pipeline step has machine-verifiable postconditions:

```
Scope  -> Does selected writer INSERT/UPDATE/MERGE into this table?
          Does reference graph match catalog?
Profile -> Does classification match procedure patterns?
           (dim_scd2 should have valid_from/valid_to logic)
Refactor -> Does refactored SQL compile against source DB?
            Does it reference same source tables as original?
Generate -> dbt compile + dbt test pass?
```

If postcondition holds -> proceed autonomously.
If postcondition fails -> self-correct up to N times -> if still fails, escalate with specific failure context.

The human's role shifts from "approve each step" to:
1. Resolve ambiguities the agent can't self-check
2. Review completed domain output
3. Focus attention on medium/low confidence items

## 4. How Should Domains Work as an Execution Unit?

### Fundamental Truths
- We WANT domain-driven target architecture (this is a deliverable, not just a convenience)
- Humans think in domains — they review coherently within a domain
- Dependencies cross domain boundaries (fact_sales references dim_customer from another domain)
- The dependency within a domain follows a chain; the output of one domain is input for another

### Design Decision: Domain = Scope + Review Unit, with Inter-Domain Ordering

Domain assignment happens once, at triage, for all objects. This is the one place human judgment is irreplaceable. After that:

- **Within a domain**: agent executes in dependency order (staging -> dims -> facts)
- **Across domains**: inter-domain ordering handles dependencies (date domain completes before sales domain starts, because sales needs dim_date)
- **Independent domains run in parallel**: if customer and product have no interdependency beyond foundation dims, both execute concurrently
- **Partial completion is fine**: 9/10 objects done, 1 escalated — human can approve the 9 and handle the 1 separately

## 5. How Do We Handle Whole-DB Scope Without Cost Explosion?

### Fundamental Truths
- You need the full picture for cross-schema dependency discovery
- Real migrations are phased (not everything at once)
- Extraction and triage are cheap; deep analysis is expensive

### Design Decision: Cheap-First, Gate-Before-Expensive

| Step | Cost | Scope |
|---|---|---|
| Extract | Low (pure I/O) | Whole DB |
| Triage | Medium (metadata-level LLM classification) | Whole DB |
| Deep analysis | High (full procedure reading, profiling, refactoring) | Selected domains only |

The user picks which domains to migrate after reviewing the triage output. Expensive work only runs on selected domains. This naturally supports phased migration: triage everything once, migrate wave by wave.

## Synthesis: The Revised Pipeline

```
INIT (Fabric Lakehouse target, scaffold project + dbt upfront)
  |
EXTRACT (whole DB -- cheap I/O)
  |
TRIAGE (whole DB -- metadata-level classification)
  -> Auto-detect foundation layer
  -> Propose domain groupings
  -> Flag multi-table writers, sourceless tables, circular deps
  -> HUMAN: validate domain map, pick wave domains
  |
FOUNDATION (autonomous -- conformed dims, shared objects)
  -> Self-checks validate each object
  |
DOMAIN EXECUTION (parallel across independent domains)
  For each domain:
    Scope -> self-check -> Profile -> self-check -> Refactor -> self-check -> Generate -> dbt test
    Escalate only on self-check failures
  -> HUMAN: review completed domain (confidence flags, test results, escalated items)
  |
CROSS-DOMAIN INTEGRATION
  -> Relationship tests across domain boundaries
  -> Final dbt build
```
