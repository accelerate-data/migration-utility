# Showcase Migration — Review Package

Redesign of the migration utility: user specifies 2-3 target facts, agent traces the dependency tree backwards, autonomously migrates everything needed, and generates sourcing + comparison manifests for the 3-5 day validation window. Foundation quality — models become the starting point for production migration.

---

## Start Here

**[one-pager.md](one-pager.md)** — The full proposal in one page: why the current pipeline is the wrong abstraction, the reverse-dependency showcase flow, sourcing and comparison strategy. Read this first.

---

## Decisions

All design decisions have been resolved. See [design-decisions.md](open-questions-analysis.md) for the complete record with rationale for each.

Key decisions:
- **Purpose**: Showcase migration (2-3 facts + deps), not full warehouse migration
- **Flow**: Reverse dependency from target facts — dependency tree IS the scope
- **Quality**: Foundation — full test coverage, models become production starting point
- **Ingestion**: Manifest (agent-ready YAML + human-readable MD) grouped by origin type (database/app/file/API), recommends method per group
- **Parallel Run**: Manifest (agent-ready YAML + human-readable MD) + setup prompt for Fabric Semantic Model + Report via DirectQuery
- **Detection**: All algorithms naming-agnostic — structure over names
- **SCD2 / multi-table**: Agent detects structurally, human gates
- **Implementation**: Incremental refactor — new `/showcase` command calling existing CLI tools

---

## How We Got Here

The conversation started with a review of the migration utility's end-to-end flow — mapping all 16 commands and examining a session (157796ea) where a user spent the entire time fighting prerequisite gates. This established the core problem: the pipeline treats the LLM as a wizard assistant rather than an autonomous agent.

We initially designed a full-migration flow with domain classification, triage phases, foundation layer detection, and inter-domain ordering. Through three rounds of debate — pushback, inversion thinking (12 failure scenarios), and first-principles resolution — we arrived at a sophisticated architecture. The inversion analysis and first-principles work remain valuable as the intellectual foundation for the self-check and detection designs that survived into the final flow.

Then the user reframed the problem: the plugin's purpose is not full warehouse migration. It minimizes the effort to bring 2-3 fact tables + dependencies to Fabric Lakehouse so vibeData can be showcased. This dissolved most of the complexity — no triage, no domains, no foundation layer. The flow collapsed to a single reverse-dependency traversal with autonomous execution.

Two practical questions then sharpened the design: how does source data get into the lakehouse (sourcing manifest with per-group method recommendations, supporting database/app/file/API origins), and how do you prove source and target match over 3-5 days (Fabric Semantic Model with DirectQuery into both, not dbt-audit-helper which can't cross databases). The comparison report built on Fabric is itself proof that vibeData works.

---

## Supporting Documents

| Step | File | What It Contains | When to Read |
|---|---|---|---|
| 1 | [current-flow-analysis.md](current-flow-analysis.md) | Mapping of the existing 8-step pipeline and evidence of the wizard-driven problem | If you want to understand what exists today and why it's insufficient |
| 2 | [proposed-agentic-flow.md](proposed-agentic-flow.md) | The showcase flow: reverse dependency, autonomous pipeline, self-checks, manifests, single-command UX | If you want the full specification of the proposed flow |
| 3 | [open-questions-analysis.md](open-questions-analysis.md) | All 14 design decisions resolved with rationale | If you want to understand WHY each design choice was made |
| 4 | [inversion-analysis.md](inversion-analysis.md) | 12 failure scenarios from stress-testing the earlier full-migration design | If you want the intellectual history — failure modes informed the self-check and detection designs |
| 5 | [first-principles-resolutions.md](first-principles-resolutions.md) | First-principles derivations for self-checks, test role, naming-agnostic detection | If you want the reasoning chain behind specific design principles |

## Chat History

The full conversation that produced this package is logged for transparency. The session went through three major pivots: full migration → domain-driven architecture → showcase reframe.

**[chat-history.md](chat-history.md)** — Condensed, readable log of the session.

**[session-transcript.jsonl](session-transcript.jsonl)** — Raw Claude Code conversation log in JSONL format.

---

## Key Numbers

- **8** discrete commands in the current pipeline, each requiring manual invocation
- **1** command in the proposed flow (`/showcase <fact1> <fact2>`)
- **10-15** objects in a typical showcase dependency tree
- **8** output artifacts: dbt models, dbt seeds, sources.yml, ingestion manifest (YAML + MD), parallel run manifest (YAML + MD), parallel run setup prompt
- **0** open design questions
