# Vibedata — Strategy

## 1. What is Vibedata

Vibedata is an agentic platform that powers data engineering workflows on Microsoft Fabric — from build through production operations. Vibedata is built and published by Accelerate Data.

Vibedata has 3 core functions:

- **Ingest** data from source systems into the lakehouse
- **Transform** raw data into curated silver and gold tables
- **Operate** the resulting pipelines in production

Most data tools address one of these in isolation — ingestion (Fivetran, Airbyte), transformation (dbt Cloud), or observability (Elementary, Monte Carlo). Vibedata owns the full lifecycle and connects them through an improvement flywheel that makes the platform smarter with every pipeline built and every incident resolved (see Section 7.2).

The platform is built on four integrated pillars — LLM, Agentic Workflow, Skills, and MCPs (see Section 7.1). Users maintain control throughout — all requirements and design decisions captured in Github, all code follows GitHub Flow, users approve deployments, and all issues managed via Github Issues.

---

## 2. Vision

> Every data practitioner ships trusted silver and gold pipelines in hours instead of weeks, and keeps them running . Institutional knowledge is persisted alongside the code in git (the 'why’ is never lost) and data practitioners maintain full control throughout.

---

## 3. Problem Statement

### Who experiences this pain?

Data Engineers, Analytics Engineers, and Data Reliability Engineers in early/growth-stage companies using Azure Fabric.

| Pain Point | Quantification | Current State |
| -------- | -------- | -------- |
| Pipeline creation time | 2-4 weeks → 2-4 hours target | Manual, fragmented workflow |
| Maintenance burden | 40% → 15% of time target | Reactive firefighting |
| Explaining data | 40% → 10% of time target | 70% reactive responses |
| Alert noise | 97% false positives → <50% target | 2000+ weekly alerts, 3% actionable |
| MTTR | 15+ hours → <30 min target | 68% take 4+ hours just to detect; root cause analysis dominates resolution time |
| Knowledge dependency | Person-dependent → Reusable skills | "When Sarah left, nobody knew why that pipeline was built that way" |

*Sources: Problem Statement References (original context docs removed)*

### Why current solutions fail

| Failure Category | Problem | Evidence |
| -------- | -------- | -------- |
| **Incomplete planning** | Requirements miss edge cases, schema changes, upstream dependencies | 70% of failures stem from poor requirements; 60% of critical requirements never documented |
| **Testing at wrong layer** | Over-reliance on data tests (run post-build) instead of unit tests (run pre-build) | dbt data tests require full materialization; issues caught late cost 10-100x more to fix |
| **No golden data validation** | Over-reliance on UAT; scenarios not carried over to automated tests for Day 2 | 80% of UAT failures from expectation mismatch; 73% of test automation projects fail to deliver ROI |
| **Broken feedback loops** | Production issues don't trigger new tests to prevent recurrence | Thousands of incidents tracked but not converted to preventive tests |
| **Knowledge walks out the door** | Institutional knowledge trapped in people's heads | 80% of critical knowledge undocumented; $47M/year lost per large company |

*Sources: Problem Statement References (original context docs removed)*

### Why existing tools don't solve this

| Tool | What It Does Well | Why It Doesn't Close the Loop |
| -------- | -------- | -------- |
| **ChatGPT/Claude + copy-paste** | Rapid prototyping, explaining concepts | Context resets each session; "lost in the middle" problem degrades quality in long prompts; no codebase awareness; copy-paste between tools kills flow; no day2 data or pipeline observability |
| **AI coding assistants (Claude Code, Cursor, Codex)** | Codebase-aware code generation, multi-file editing, terminal integration, extensible via skills/MCP, can read specs and logs | Capable platform, but requires building: system prompts for data engineering use cases, MCP tools for Fabric/catalogs/observability, skills for industry/source/institutional knowledge, orchestration harness. Build vs buy decision—Vibedata is this assembly, purpose-built and maintained. |
| **dbt Cloud + Elementary Cloud** | Model building, metrics, semantic layer, DAG visualization, version control; Elementary adds data observability, AI-assisted triage, remediation | Specs are outside the code (no context capture); no agentic support for model building (Copilot is assistive only); no fully autonomous Day 2 operations (requires human approval); no closed loop from issues to tests |
| **Fabric Copilot** | Natural language queries, SQL/DAX generation, error fixing via quick actions, pipeline debugging assistance, emerging agentic capabilities via Copilot Studio | Schema-only data access (no actual data values for validation); non-deterministic outputs; agentic features require Copilot Studio setup; no intent/specs capture; no closed loop from issues to tests |
| **Osmos / Fabric AI Data Agents** | Schema drift handling, messy file normalization, PySpark code generation, native Fabric integration | Files in ADLS/blob only (no API ingestion); silver tables only (no gold/analytics); no specs capture; no unit tests or DQ checks; no Day 2 operations; schema-only self-healing (not business logic) |
| **Fivetran + dbt** | 700+ managed connectors, schema evolution, zero-maintenance EL, dbt integration for transforms | Merger unifies tooling but not effort—still manual dbt development, manual test writing, no AI/agentic capabilities; no specs capture; Day 2 requires Monte Carlo/Metaplane (additional $$$); no knowledge capture |

*Note: Ingestion tools (dlthub, Airbyte) focus narrowly on source connectivity. Observability and catalog tools (Monte Carlo, Datafold, Great Expectations, Atlan) address specific slices—data diff, validation rules, cataloging. None attempt end-to-end workflow integration.*
*Source note: Fivetran connector count from Fivetran Deep Dive (original context docs removed).*

### The fundamental gap

Teams cobble together 5-7+ tools, creating integration overhead, responsibility gaps, and knowledge silos.

No existing tool provides spec driven development (intent), shift-left methodology (tests and golden data validation), bidirectional feedback from production to development (retro agent), or persistent organizational knowledge (skills) to support data engineering workflows.

**The 1:10:100 rule**: Addressing issue in requirements/build/unit test phase cost ~$1, $10 in UAT and $100 in prod. Current tools focus on the $10-$100 stages.
*Sources: Modern data stack complexity survey, Problem Statement References (original context docs removed)*

### Why now: Industry shifts creating urgency

Three fundamental industry shifts make agentic data engineering not just possible, but necessary:

| Shift | From | To | Implication |
| -------- | -------- | -------- | -------- |
| **Data systems** | Important (analytics) | Mission critical (operations) | Reverse ETL and AI agents consuming data in real-time mean bad data causes immediate business harm. Gartner: $12.9M/year cost of poor data quality. |
| **Engineer role** | Code writer | Orchestrator | 84% of developers using AI tools (Stack Overflow 2025). Engineers shift from writing code to directing agents and owning quality. |
| **Services delivery** | Offshore headcount | Embedded domain experts + AI | India's Big Four lost $150B market value in 9 months. Outcomes, not headcount, now define value. |
| **Build economics** | Implementation cost acts as quality filter | Zero-cost build removes the filter; bad specs execute at scale | Specification precision becomes the binding constraint. Vibedata's Requirements Agent and intent-capture workflow address exactly this gap. AWS launched Kiro around the same premise — specs before code. |

**Key data points:**

- **Gartner**: 40% of enterprise apps will have AI agents by 2026 (up from <5% in 2025)
- **McKinsey**: AI agents could handle 44% of US work hours; AI fluency is fastest-growing skill (7x in 2 years)
- **Deloitte**: 75% of companies planning agentic AI deployment within 2 years
- **GitHub**: 55.8% faster task completion with AI coding tools; 90% of Fortune 100 using Copilot
- **CodeRabbit**: AI-generated code produces 1.7x more logic issues than human-written code (470 PR analysis) — not syntax, but doing the wrong thing correctly
- **Google DORA**: 9% bug rate climb correlated with 90% AI adoption increase; 91% increase in code review time

---

## 4. User Personas

### 4.1 Primary Persona: Full-Stack Analyst (D010-D011)

The Full-Stack Analyst is an emerging role that Vibedata enables. As AI collapses the cost of production across knowledge work, two classes of practitioner are emerging: those who specify precisely and orchestrate agent fleets (the specification class), and those using AI as a copilot to do the same work faster (being commoditized). The Full-Stack Analyst is Vibedata's instantiation of the specification class for data engineering. This role combines the business context of a Business Analyst/Data Analyst, modeling skills of an Analytics Engineer with the technical execution of a Data Engineer. Today, these responsibilities are split across roles or handled by individuals wearing multiple hats (or completely missing in smaller organizations).

Vibedata's agentic workflow allows a single practitioner to own the full pipeline from intent to production.

| Dimension | Definition |
| -------- | -------- |
| **Who they are** | Mid/Senior data practitioners bridging business context with technical execution |
| **Job-to-be-done** | Get trusted data to stakeholders faster, reduce maintenance toil |
| **Current pain** | 2-4 weeks to build pipelines, 40% time on maintenance, won't use CLI tools |
| **Success looks like** | 4 pipelines/user/month (8x improvement), 2-4 hour pipeline creation |

### 4.2 Co-Primary Persona: Data Reliability Engineer (D074, D089)

In smaller teams, the DRE is often the same person as the data engineer or analytics engineer — owning both pipeline creation and ongoing operations. In larger teams, this may be a dedicated role focused on reliability and incident response.

| Dimension | Definition |
| -------- | -------- |
| **Who they are** | Operational excellence specialists ($130K-$250K); in small teams, often the same Data Engineer or Analytics Engineer wearing multiple hats |
| **Job-to-be-done** | Ensure data systems run reliably; catch issues before stakeholders do; reduce firefighting |
| **Current pain** | No tests or DQ checks in prod; issues found by stakeholders not alerts; 70% time reactive (clarifying, investigating); depend on others for knowledge of what was built and why; depend on business users for what's expected; no specs to check back against |
| **Success looks like** | Issues caught proactively; MTTR <30 min; <20% time on toil; runbooks and tests prevent recurrence |

### 4.3 Secondary Personas (D027, D104, D105)

| Persona | Relationship |
| -------- | -------- |
| Head of Data/VP Analytics | Buyer (purchase decision maker) |
| Data Scientists (small cos) | User (feature engineering, training pipelines) |
| Data Engineers (small cos) | User (multi-hat BA+AE+DRE work) |

### 4.4 Anti-Personas (D014, D105)

| Anti-Persona | Why We Exclude Them |
| -------- | -------- |
| Enterprise teams (10+ engineers) | Heavy governance, specialized roles, different tooling needs |
| Non-Fabric users | Vibedata supports Microsoft Fabric only |
| Real-time/streaming teams | Batch processing focus initially |
| Large company Data Engineers | Don't need domain convergence |

---

## 5. Customer Journey

### 5.1 Journey Stages (D028-D030, D068)

| Term | Definition |
| -------- | -------- |
| Discovery | User becomes aware of Vibedata and understands the core problem we solve. |
| CRP | Confirmed Reference Point: user sees the intent-to-pipeline flow on sample data. |
| POC | Proof of Concept: user deploys with their own data to validate fit and feasibility. |
| MVP | Minimum Viable Production: user runs in production and measures business value. |
| Habit | User repeats core workflows regularly, not as a one-off experiment. |
| Growth | Usage expands from initial users to broader team adoption. |

| Stage | User Goal | Proof Question | Exit Criteria |
| -------- | -------- | -------- | -------- |
| Discovery | Learn we exist | Do they understand the problem and why Vibedata is different? | Enters CRP evaluation path |
| CRP | Experience Vibedata with sample data (studio) | **Aha #1**: Can they see intent-to-pipeline in action? | Completes CRP flow and requests POC |
| POC | Experience with their own data (studio + CI) | **Aha #2**: Can they create/use a skill and deploy with their data? Deploy agent validates the first PR. | Deploys pipeline with custom skill |
| MVP | See it running in their environment (studio + CI + issues) | **Aha #3**: Does the workflow reduce delivery effort by at least 50%? Operator agent triages a real production incident. | Production deployment with measured improvement |
| Habit | Regular usage | Are users returning for repeat pipeline work? | 4+ pipelines/user/month |
| Growth | Expand usage | Is value clear enough to add users/seats? | 1-5 → 6-20 → 20+ seats |

*Note: Journey targets are internal operating targets for CRP/POC/MVP stage-gate validation.*

### 5.2 Critical Moments of Truth

| Moment | Stage | Success | Failure |
| -------- | -------- | -------- | -------- |
| Aha #1: Intent-to-pipeline | CRP | Completes flow, requests POC | Abandons or doesn't request POC |
| Aha #2: Skill creation | POC | Creates skill, deploys pipeline with own data | Can't create skill or pipeline fails |
| Aha #3: Effort reduction | MVP | Measures 60%+ effort reduction | <40% effort reduction or reverts to manual |
| 30-day retention | Post-MVP | >25% active | <25% active |

---

## 6. Success Metrics

### 6.1 North Star Metrics (D064, D079)

| Persona | North Star | Target |
| -------- | -------- | -------- |
| Full-Stack Analyst | Pipelines deployed/user/month | 4 (8x vs 0.5 baseline) |
| Data Reliability Engineer | MTTR for data incidents | <30 minutes |

### 6.2 Success by Timeframe

| Metric | Launch | 90 Days | Year 1 |
| -------- | -------- | -------- | -------- |
| CRP → POC conversion (Aha #1) | >30% | >50% | >60% |
| POC → MVP conversion (Aha #2) | >30% | >50% | >60% |
| MVP effort reduction (Aha #3) | >50% | >60% | >60% |
| Pipeline deployment rate | 2/user/month | 4/user/month | 4/user/month |
| 30-day retention (post-MVP) | >25% | >25% | >30% |
| NPS | — | — | >30 |

### 6.3 Failure Criteria

| Metric | Failure Threshold | Response |
| -------- | -------- | -------- |
| CRP → POC conversion | <20% | Redesign CRP experience |
| POC → MVP conversion | <20% | Simplify skill creation |
| MVP effort reduction | <50% | Reassess agentic workflow value |
| 30-day retention | <25% active | Reassess value prop |
| Pipeline deployment rate | <2/user/month at 90 days | Reassess core workflow |
| Skills effectiveness | <60% successful | Reassess skills architecture |
| NPS | <30 after 6 months | Major product reassessment |

*Note: Metrics and thresholds in Sections 6.1-6.3 are internal operating targets.*
*Benchmark context references: SaaS conversion benchmarks, enterprise retention benchmarks, NPS interpretation framework (original context docs removed).*

---

## 7. Core Differentiation

Vibedata's moat is built on four integrated pillars plus a compounding flywheel.

### 7.1 The Four Pillars

| Pillar | Role | What It Enables |
| -------- | -------- | -------- |
| LLM | The brain | Reasoning, code generation, and decision-making that powers every agent |
| Agentic Workflow | The spine | End-to-end automation from intent → deployed objects → observability → improvement, delivered across build, deploy, and operate contexts |
| Skills | The domain memory | Domain expertise that compounds with every pipeline built and every incident resolved. Skills encode source-system patterns, business logic, and institutional knowledge — distributed via repo marketplace, executable by agents across build, deploy, and operate contexts |
| MCP | The hands | Open connectivity to live systems (lakehouse, specs, tickets) |

> **Why this matters**: An LLM alone is generic. Skills alone can be copied. MCP alone is a protocol. The agentic workflow alone is a feature. Together, they create a platform that takes significant effort to build and maintain. Vibedata is this purpose-built application for agentic data engineering, curated and evolving.
>
> **Why this matters now**: When the cost of building collapses, the cost of specifying badly compounds faster than ever — you can build the wrong thing at unprecedented speed and scale. Vibedata's four pillars address this directly: the agentic workflow captures intent before generating code, Skills encode the domain knowledge that prevents bad specs, and the improvement flywheel means every resolved incident makes future specifications more precise.

#### How the Agentic Workflow Spans Surfaces

The agentic workflow is not confined to a single interface. Agents are placed where users already work — in their build environment when they design pipelines, in their CI pipeline when they deploy, and in their issue tracker when things break in production.

**Build context**: When a practitioner describes business intent, a builder agent in the studio environment guides them through spec-driven pipeline creation — capturing requirements, generating code, and producing tests. The agent draws on Skills to validate domain fit before generating any code, ensuring that the data actually answers the business question.

**Deploy context**: When a PR is opened, deploy agents run skills-based quality gates — checking documentation completeness, code quality, test coverage, and data quality coverage. Skills are repo-distributed and CI-invokable, meaning the same domain expertise that guided the build is enforced automatically at deployment time. A skills marketplace with automated dependency updates keeps CI checks current and shareable across projects.

**Operate context**: When a production anomaly fires, an operator agent creates and triages an issue with diagnostic context — root cause hypothesis, affected pipelines, and proposed remediation. This closes the loop: the operator agent can update Skills and add new tests that the deploy agent will enforce on future PRs, preventing recurrence.

This multi-surface model means the agentic workflow is not just a build-time feature — it is a continuous presence across the entire pipeline lifecycle.

### 7.2 The Improvement Flywheel

Every action makes the platform smarter through two reinforcing loops:

| Loop | Trigger | What Improves |
| -------- | -------- | -------- |
| Build | User intent → deployed pipeline (studio + CI) | **Skills**: Organizational knowledge, Source Knowledge, Validation patterns, edge cases discovered |
| Operate | Production alert → issue resolved (issues + CI) | **Skills + Runbooks + Code + Tests**: Root causes documented, fix patterns recorded, new tests prevent recurrence |

**Cross-loop reinforcement**:

- Operate issues reveal gaps → Skills updated → Build avoids the same issue
- Operate issues reveal missing tests → Tests added → Issues caught earlier (build/PR/pipeline)
- Build context (intent, plan) preserved → Operate diagnosis understands original requirements

> **The compounding moat**: Every pipeline built and every issue resolved makes the platform smarter. Competitors start at zero; Vibedata customers accumulate domain expertise in skills, runbooks, code patterns, and test coverage.

---

## 8. Key Assumptions

1. **Market**: AI-enabled role convergence is real — FSA/DRE personas are emerging. *Evidence: McKinsey reports AI fluency as fastest-growing skill; Microsoft says 82% of leaders consider AI skills essential. Nate's Newsletter identifies two classes of knowledge worker emerging: specification-class (who define what to build) and production-class (who execute, being commoditized). StrongDM's three engineers deployed a 'software factory' equivalent to a ten-person team from 18 months prior. Knowledge work is converging on software — the underlying cognitive task is translating vague human intent into precise executable instructions.*
2. **User behavior**: Users will trust AI-generated pipelines with trust-but-verify model. *Evidence: Stack Overflow 2025 shows 46% distrust AI output—human oversight remains essential.*
3. **Technology**: Skills architecture effectively guides agent behavior. *Evidence: Gartner predicts 40% of enterprise apps will have task-specific AI agents by 2026.*
4. **Context preservation**: Build context (intent, plan, validation history) is required for Day 2 operations. *Evidence: Deloitte notes only 21% have mature agent governance—context and traceability are key gaps.*
5. **Platform**: Fabric adoption continues growing in target segment
6. **Business model**: Hybrid GTM (PLG → sales → partners) scales effectively
7. **Industry timing**: Data engineering is shifting from "important" to "mission critical" as reverse ETL and AI agents consume data operationally. *Evidence: Industry Changes Analysis (original context docs removed).*

---

## 9. Risks (D035, D071)

| Risk | Likelihood | Impact | Trigger | Response |
| -------- | -------- | -------- | -------- | -------- |
| Microsoft builds natively | Medium | High | MS expands Osmos to cover more agentic data engineering scenarios | Accelerate skills marketplace; pivot to "open ecosystem" |
| Can't reach personas | Low | High | <50 CRPs after 90 days | Engage additional SI partners; pivot to sales-led |
| Skills don't guide agent | Medium | High | >40% intent failures | Pause MVP; invest in skills research |
| Fabric adoption slow | Low | Low | <20% YoY growth | Deepen Fabric integration; leverage Microsoft partnership |

*Note: Risk triggers are internal operating thresholds; Fabric market context reference: Microsoft Fabric adoption update (original context docs removed).*

---

**Statistics Traceability**: [statistics-traceability.md](statistics-traceability.md)
**Decision Log**: See [decisions.md](../decisions.md)
