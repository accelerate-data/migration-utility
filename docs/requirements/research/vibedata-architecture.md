# Vibedata Product Architecture

Level 2 Architecture for Vibedata — the unified view of how modules and capabilities work together.

**Status**: Draft
**Prerequisites**: [vibedata-strategy.md](vibedata-strategy.md)

---

## 1. Architecture Overview

Vibedata is an agentic platform that powers data engineering workflows on Microsoft Fabric — from build through production operations.

Vibedata has 3 core functions:

- **Ingest** data from source systems into the lakehouse
- **Transform** raw data into curated silver and gold tables
- **Operate** the resulting pipelines in production

Most data tools address one of these in isolation — ingestion (Fivetran, Airbyte), transformation (dbt Cloud), or observability (Elementary, Monte Carlo). Vibedata owns the full lifecycle and connects them through an improvement flywheel that makes the platform smarter with every pipeline built and every incident resolved (see [vibedata-strategy.md Section 7.2](vibedata-strategy.md)).

The architecture is built on four integrated pillars — LLM, Agentic Workflow, Skills, and MCPs. Users maintain control throughout — all requirements and design decisions captured in GitHub, all code follows GitHub Flow, users approve deployments, and all issues managed via GitHub Issues.

### 1.1 The Four Pillars

| Pillar | Role | What It Enables |
| -------- | -------- | -------- |
| LLM | The brain | Reasoning, code generation, and decision-making that powers every agent |
| Agentic Workflow | The spine | End-to-end automation from intent → deployed objects → observability → improvement, delivered across build, deploy, and operate contexts |
| Skills | The domain memory | Domain expertise that compounds with every pipeline built and every incident resolved. Skills encode source-system patterns, business logic, and institutional knowledge — distributed via repo marketplace, executable by agents across build, deploy, and operate contexts |
| MCP | The hands | Open connectivity to live systems (lakehouse, specs, tickets) |

The lifecycle phases supported by these pillars are detailed in Section 3. The modules that implement them (Foundation, Studio, Agents) are detailed in Section 6. The skills architecture is detailed in 6.2.4. MCP server configuration is in Settings (6.2.5).

### 1.2 Relationship to Fabric IQ

Vibedata provides the data foundation layer that Fabric IQ depends on.

- Simplifies bronze data ingestion
- Uses agents as teammates to help practitioners build silver and gold transformations
- Uses agents as teammates to triage, diagnose, and resolve production issues
- Continuously improves agent skills from every pipeline built and every incident resolved.

| Fabric IQ Component | Vibedata's Role |
| -------- | -------- |
| Data Agents | Vibedata provides curated, tested data for conversational Q&A |
| Operations Agent | Vibedata provides real-time business data for monitoring and action recommendations |
| Semantic Models | Vibedata will accelerate creation (planned) |
| Ontology | Vibedata will accelerate definition (planned) |

Today Vibedata accelerates the build of data foundation (pipelines, tests, quality). Near-term planned phases extend into Data Agents, Operational Agents, Semantic Models and Ontology to stay aligned with Fabric's evolution.

---

## 2. Data Platform Concepts

Vibedata has two foundational data platform concepts to enable data mesh principles. This model shifts ownership from centralized data teams to domain experts, treating data as a product to improve accuracy and accountability.

### 2.1 Source

A logical grouping representing a replica of a source system. Sources define the bronze data layer (raw ingested data) in the lakehouse and are owned by source system experts. Sources are not tied to a specific Fabric workspace—bronze data can land in a dedicated sources lakehouse or within a Data Domain's lakehouse, depending on configuration.

Vibedata ingests data from API source using dlt pipelines. Mirroring sources (database sources or sources ingested via Open Mirroring) are configured and managed within Fabric directly—Vibedata consumes their output as bronze data but does not manage the mirroring itself.

| Attribute | Description |
| -------- | -------- |
| Ownership | Source system experts (e.g., Salesforce admin owns Salesforce source) |
| Data layer | Bronze tables (raw replica of source system) |
| GitHub repo | API sources only — hosts dlt pipelines and ingestion configuration (see Section 3.1) |
| Fabric workspace | Linked to the repo (see Section 3.1) |

### 2.2 Data Domain

Represents silver and gold tables owned and managed by the functional team closest to that data (e.g., Marketing, Sales, Product).

| Attribute | Description |
| -------- | -------- |
| Ownership | Functional teams (e.g., Marketing team owns Marketing domain) |
| Data layers | Silver and gold tables |
| GitHub repo | Specified during domain creation (see Section 3.1) |
| Fabric workspace | Linked to the repo (see Section 3.1) |

### 2.3 Alignment to Data Mesh Principles

Sources and Data Domains implement data mesh principles within the Fabric lakehouse:

| Principle | How Vibedata Implements It |
| -------- | -------- |
| Domain-oriented decentralized ownership | Functional teams own their Data Domain (silver/gold); source experts own their Source (bronze). Clear boundaries reduce coordination overhead |
| Data as a product | Each Data Domain publishes curated datasets (silver/gold) that are governed, discoverable, observable, and trustworthy (see 2.3.1) |
| Self-serve Data infra | Domain teams operate independently: domain/source work within their own repo/fabric workspace; each capture their organizational specifics in skills; skills reduce the knowledge gap and the improvement flywheel means the platform learns from every pipeline built and every incident resolved |
| Federated governance | Platform-level policies (naming, testing, DQ thresholds) are enforced at the domain/source level (via Skills and CI validation) by respective domain/source owner. Domain teams retain autonomy over their business logic. Similarly, RBAC (data level) is managed by each domain/source team for their respective fabric workspace |

#### 2.3.1 Data as a Product

Each Data Domain publishes **data products** — curated datasets (silver/gold tables) designed for downstream consumption. Vibedata-built data products are:

- **Governed** —
  - Schema: dbt contracts enforce column names, types, and constraints at build time
  - Standards: Skills and `claude.md` enforce naming conventions, coding standards, and business rules
  - Quality gates: dbt tests (unit + data) and Elementary DQ checks run in CI; must pass before merge
  - Change management: all changes PR-gated with CI validation (linting, tests, coverage)
  - Ownership: domain teams control access via Fabric workspace RBAC
  - Data governance: domain teams leverage Fabric-native capabilities (Purview sensitivity labels, data classification, masking, DLP) within their workspace
- **Discoverable** — Available via Fabric SQL endpoint; dbt docs provide lineage and schema documentation
- **Observable** — Elementary anomaly detection monitors freshness, volume, and distribution drift in production; failures raise GitHub Issues to the domain team
- **Trustworthy** — dbt tests and Elementary DQ checks run in production to validate data quality continuously; agents auto-remediate common incidents; the improvement flywheel captures fix patterns so issues don't recur
- **Contract-backed** — each model can define a dbt contract (column names, types, constraints) enforced at build time, guaranteeing downstream consumers a stable schema; breaking changes caught in CI before merge
- **Semantically defined** — each model can include a dbt semantic model (entities, measures, dimensions, relationships) giving all consumers (BI tools, analysts, agents) a single agreed-upon definition of business metrics; can serve as the foundation for Fabric IQ components in the future (see Section 1.2)

---

## 3. Development and Deployment

### 3.1 Repository and Workspace Model

Each source or data domain is linked to a GitHub repository and a Fabric workspace. Customers decide how to organize these — a single repo/workspace can host multiple sources and domains, or each can have its own dedicated repo/workspace.

### 3.2 Branch-Based Development

Vibedata enforces branch-based development. All changes — new sources, new transformations, hotfixes — happen on feature branches, never directly on `main`.

- **`main` is the single long-running branch** and represents what is deployed to the production Fabric workspace.
- **Feature branches** are created automatically when a user starts an intent or adds a source. Each feature branch gets an **ephemeral Fabric workspace** for isolated development and testing against real data.
- When work is complete, a **PR merges the feature branch into `main`**, triggering deployment to the production workspace. Ephemeral workspaces are fully isolated — source and cross-domain reads point to production workspaces only.

### 3.3 CI/CD

Vibedata provides tailored GitHub Actions workflows for pre-merge validation and post-merge deployment. CI is a first-class agent surface — not just a test runner.

**Pre-merge (deploy agents in CI)**: When a PR is opened, GitHub Actions invokes Claude CLI to run repo-published skills as quality gates. Deploy agents check documentation completeness, code quality, test coverage, and data quality coverage. Skills are repo-distributed and CI-invokable — the same domain expertise that guided the build in studio is enforced automatically at deployment time.

**Pre-merge (recommender agents in CI)**: Advisor agents (Test Recommender, Performance Analyzer) can post recommendations directly on PRs when invoked via CI, providing contextual suggestions alongside code review.

**Post-merge**: Deployment to the production Fabric workspace via GitHub Actions.

**Skills marketplace**: Skills used in CI are managed via the repo's skills directory with automated dependency updates (dependabot pattern), keeping quality gates current and shareable across domains.

### 3.4 Production Operations

uk: I am just wondering how much of our day2 agents are tied to  studio - if they are really two products (to me they are pretty indepdnet), we shoudl let them be so. Like how databricks does not mix their products  like  UC and Genei and Lakeflow. They converge and have touch point. We let folks orcestrate/ensemble/ make it work as per what they want. Why should we be prescriptive and limit their creativity. The data products created via studio are richer and will throw more signal and are more observable and hence agents will be more precise to to remediate while legacy data products wont be so.

Once artifacts are deployed to the production Fabric workspace, scheduling and execution are managed directly in Fabric. Vibedata's role shifts from build to observe-and-remediate.

```text
┌─────────────────┐
│  Azure Monitor  │───┐
│  (pipeline logs)│   │     ┌─────────────────┐     ┌─────────────────┐
└─────────────────┘   │     │                 │     │                 │
                      ├────>│  Function App   │────>│  GitHub Issue   │
┌─────────────────┐   │     │                 │     │  Created        │
│ Data Activator  │───┘     └─────────────────┘     └────────┬────────┘
│ (DQ from ops)   │                                          │
└─────────────────┘                                          ▼
                                                    ┌─────────────────┐
                                                    │  Operator       │
                                                    │  Agents Act     │
                                                    └─────────────────┘
```

Pipeline and data quality errors are raised as GitHub Issues on the respective domain or source repo, keeping incidents co-located with the code they relate to. Operator Agents (see 6.3.4) act on these issues to triage, diagnose, and remediate.

---

## 4. Technology Stack

| Category | Technology | Customer Interaction |
| -------- | -------- | -------- |
| Lakehouse | Microsoft Fabric | Customer-provisioned capacity and workspaces |
| Ingestion | dlt | Python pipelines in Fabric notebooks |
| Transformation | dbt | SQL models executed via Spark in Fabric notebooks |
| Data Quality | Elementary | Anomaly detection, freshness, volume checks |
| Testing | dbt tests (unit + data), dbt-coverage | Included in CI; visible in PR checks |
| Orchestration | Fabric pipelines | Customer-managed scheduling |
| Version Control | GitHub | Repos, PRs, Issues, CI/CD via GitHub Actions |
| AI | Azure AI Foundry | Customer-provisioned LLM endpoints (*future Agent hosting*) |

---

## 5. Deployment Model

Vibedata is deployed as an Azure Managed Application in the customer's Azure tenant.

| Component | Location | Purpose |
| -------- | -------- | -------- |
| Publisher | Accelerate Data Azure tenant | Control plane for provisioning, updates, instance monitoring |
| Managed App | Customer Azure tenant | All customer data, workloads, and Fabric resources |

### 5.1 Key Principles

| Principle | Description |
| -------- | -------- |
| Data residency | All customer data stays in customer tenant; no customer data is sent to the publisher |
| Customer ownership | Customer owns all traffic logs, audit trails, Fabric capacity, AI endpoints, and data |

### 5.2 Topology

A **Vibedata instance** is a single deployment of the Managed App in a customer's Azure tenant. Each instance is a self-contained set of the resources listed below, connected to the customer's GitHub organization and Fabric capacity. No resources are shared across instances. Most customers run one instance; multiple instances are useful when regulatory, regional, or organizational boundaries require full isolation.

The network is zero-trust by default: all PaaS services use private endpoints, all compute is VNet-integrated, and traffic flows through Front Door → APIM → backend services with no public backend endpoints.

| Resource | Purpose |
| -------- | -------- |
| Azure Front Door | Per-instance ingress; sole public entry point for all traffic; WAF, DDoS protection, TLS termination; traffic logs stay in customer subscription |
| API Management | API gateway behind Front Door for Studio and Foundation API endpoints; provides JWT validation and rate limiting |
| App Service | Hosts Studio web application, Studio API, and agent runtimes; VNet-integrated, accepts Front Door traffic only (service tag + `X-Azure-FDID` header) |
| Function App | Hosts Foundation app and associated durable functions; VNet-integrated, accepts Front Door traffic only (service tag + `X-Azure-FDID` header) |
| Azure AI Foundry | LLM model endpoints for agents (*Foundry-managed agent hosting is future*) |
| Azure Container Registry | Stores application container images |
| SQLite | Application operational database |
| Storage Table (immutable) | Audit logs, chat history |
| Storage Account | Application state, uploads |
| Key Vault | All secrets (Publisher ACR tokens, Source credentials) |
| Azure Monitor | Log Analytics workspace in the managed resource group for diagnostic logs and pipeline logs |

### 5.3 External Resources

Vibedata depends on two external platforms that are owned and managed by the customer.

| Resource | Owned By | Purpose |
| -------- | -------- | -------- |
| Microsoft Fabric | Customer (Fabric capacity + workspaces) | Lakehouse storage, Spark compute, pipeline orchestration, observability |
| GitHub | Customer (organization + repositories) | Specs versioning, Code versioning, CI/CD, IR/SR/CR (Github Issues) |

Vibedata does not own or manage these resources. The customer provisions Fabric capacity and GitHub repositories, grants Vibedata access through the identities described below, and retains full ownership of all code, data, and logs. If the customer removes Vibedata, all artifacts remain in their GitHub and Fabric accounts.

### 5.4 Identity Model

Vibedata uses a layered identity model spanning infrastructure operations and application-level access to external resources.

#### 5.4.1 Infrastructure Identities

Cross-tenant access between Accelerate Data (publisher) and Managed App uses a controlled identity model.

| Identity | Location | Purpose |
| -------- | -------- | -------- |
| Publisher SPN (`managed_application_operator`) | Accelerate Data tenant | Calls Foundation APIs in the managed app for provisioning, updates, and lifecycle operations |
| Managed App UAMI (`vibedata-uami`) | Customer tenant | Executes all internal automation (deployments, rotation, health checks); also used by Studio/Monitor backend for zero-credential access to Azure data plane (Key Vault and AI Foundry) via `DefaultAzureCredential`; access revoked when Managed App is deleted |

*Both identities enforce one-way control: the publisher initiates operations via Foundation APIs; the Managed App never calls the publisher.*

#### 5.4.2 Application Identities

When users interact with Studio, their own identities are used to access external resources (Fabric and GitHub). This preserves audit trails and ensures actions are attributed to the user who initiated them.

| Identity | Authentication | Used For |
| -------- | -------- | -------- |
| User's Entra ID (SSO) | OAuth 2.0 / OIDC via Microsoft Entra ID | Studio login; Fabric API calls on behalf of user (delegated token with Fabric scope) |
| User's GitHub OAuth token | GitHub App OAuth flow (user authorizes Vibedata GitHub App) | Clone, push, pull, commit—all attributed to user's GitHub identity |

**GitHub**: A Vibedata GitHub App is installed once per customer organization with read/write access to repository contents. Each user additionally authorizes via OAuth so that all commits are attributed to their GitHub account. The user's OAuth token is refreshed automatically; if revoked, the user is prompted to re-authenticate.

**Fabric**: Interactive API calls (e.g., workspace browsing, settings) use the user's Entra ID delegated token. Production pipeline scheduling and execution is intended to use a non-human execution identity configured by the domain admin (e.g., Fabric service principal/workspace identity), so runs are auditable and not tied to an individual employee account.

### 5.5 Audit Logging

All operations produce structured audit records stored in two locations:

| Location | Content | Retention |
| -------- | -------- | -------- |
| Publisher Storage Tables | Instance lifecycle events (provisioning, updates, deletion) | 90 days |
| Managed App Storage Tables | Component updates, credential rotations, configuration changes (immutable append-only) | 90 days |

*Storage Tables provide tamper-evident, immutable audit trails for SOC 2 Type II compliance. All audit records include timestamp, actor identity, operation type, and outcome.*

---

## 6. Modules

Vibedata serves two co-primary personas with equal importance:

| Persona | Focus | Vibedata Module |
| -------- | -------- | -------- |
| Full-Stack Analyst (FSA) | Pipeline creation: intent → production | Studio |
| Data Reliability Engineer (DRE) | Operations: monitor → resolve → improve | Operator Agents |

Vibedata has three main modules:

| Module | Purpose |
| -------- | -------- |
| Foundation | Control plane for instance lifecycle |
| Studio | UI for data ingestion, transformation, orchestration and monitoring |
| Agents | AI agents that automate DE, AE and DRE workflows |

### 6.1 Foundation (Control Plane)

Manages the customer instance lifecycle. The publisher is the master and triggers all actions in the managed app. All operations use Durable Functions for orchestration with polling-based status tracking.

| Function | Description |
| -------- | -------- |
| Bootstrapping | Marketplace webhook triggers provisioning |
| Instance Monitoring | Hourly reachability probes (72h unreachable → alert operations) |
| Credential Rotation | Daily credential rotation workflows. Any user-managed secrets are raised as tickets |
| Upgrade | A new release on the publisher initiates update for the managed instances; automatic rollback on deployment failure |
| Deletion | Marketplace webhook triggers clean up of the instance from the publisher registry and any resources associated with the instance |
| Registry | Configuration APIs for component versions, policies, feature flags, and entitlements—consumed by Studio and Operator Agents |

### 6.2 Studio (UI)

Main user interface for data practitioners.

| Component | Description |
| -------- | -------- |
| Source | CRUD of Sources (*currently in settings*) |
| Transform | CRUD of silver and gold tables |
| Monitor | Data and pipeline observability for the sources and domains |
| Skills | CRUD of skills (*currently in settings*) |
| Settings | Configuration management for Vibedata |

#### 6.2.1 Source

Configure and manage source data ingestion. Supports three source types:

##### API Sources (DLT)

DLT pipelines ingest data from API sources into the lakehouse as bronze. Source configurations are stored in the linked GitHub repo. The target lakehouse is selected by the user.

| Aspect | Implementation |
| -------- | -------- |
| Code format | DLT in Fabric notebooks |
| Scheduling | Fabric scheduler |
| Output | Data landed in lakehouse |

For the development and deployment flow (feature branches, ephemeral workspaces, CI/CD), see Section 3.

##### Database Sources (Fabric Mirroring)

Database sources are ingested using Fabric mirroring. Users can configure target lakehouse per source. This is not managed in Vibedata.

##### Open Mirroring Sources

Applications that support Open Mirroring can be ingested into the Fabric lakehouse directly. This is not managed in Vibedata.

#### 6.2.2 Transform

Build dbt models to transform data into silver and gold tables.

| Aspect | Implementation |
| -------- | -------- |
| Code format | dbt project |
| Execution | Wrapped in Fabric notebook |
| Scheduling | Fabric scheduler |
| Output | Silver and gold tables |

For the development and deployment flow (plan → execute → deploy, feature branches, ephemeral workspaces, CI/CD), see Section 3.

#### 6.2.3 Monitor

Data and pipeline observability scoped to the Fabric workspace. Monitor tracks all scheduled pipelines — ingestion, transformation, and data quality (Elementary anomaly detection for freshness, volume, column drift, and distribution shifts).

##### Telemetry Collection

All pipeline telemetry flows to two complementary destinations, linked by `correlation_id` for end-to-end tracing:

| Destination | Content | Why | Retention |
| -------- | -------- | -------- | -------- |
| Azure Log Analytics | Spark logs (driver/executor), crash evidence (OOM, timeouts) from ingestion and transformation pipelines | Survives ungraceful failures; needed for crash forensics | 31 days (cost-optimized) |
| Lakehouse (ops schema) | Structured telemetry from all pipelines — dlt loads, dbt runs, pipeline status, capacity metrics, DQ errors and warnings | SQL-queryable by agents; historical analysis; cheaper storage ($0.023/GB vs $2.30/GB) | Customer-configured |

The `correlation_id` traces to the originating Fabric pipeline execution, enabling the Diagnose Agent to correlate cascading failures (e.g., dlt failure causing downstream dbt failure) across the full pipeline chain.

##### Alerting

Alert routing is handled by the Alert Engine (see 6.2.6). Azure Monitor and Data Activator feed alerts through a webhook bridge into GitHub Issues, where Operator Agents act on them (see 3.4). Alert storms are deduplicated by the alert engine.

#### 6.2.4 Skills

Browse, create, and manage the skills library. Skills are the domain memory of the platform — domain expertise that compounds with every pipeline built and every incident resolved. Skills encode knowledge that transforms generic agents into domain experts, distributed via repo marketplace and executable by agents across build (studio), deploy (CI), and operate (issues) contexts.

The Skills UI uses the Retro Agent, which analyzes chat history and issues for a specific domain or source and recommends skill updates (see 6.3.5).

##### What Skills Contain

| Knowledge Type | Examples |
| -------- | -------- |
| Real-world knowledge | Common pitfalls when analyzing sales pipeline |
| Institutional knowledge | For PS projects > 12 months, only 1st year ACV is recognized as booking amount, not the full TCV |
| Pattern knowledge | SCD Type 2 implementation, dbt best practices, incremental loading strategies |
| Source knowledge | Salesforce object relationships, QuickBooks data model |

As organizations get leaner, coordination overhead (reports, status updates, cross-team alignment) gets deleted — it doesn't transform, it disappears. Skills replace coordination with encoded knowledge: instead of asking Sarah why the pipeline was built that way, the agent reads the skill.

##### Skill Types

| Type | Description | Managed By |
| -------- | -------- | -------- |
| Platform | dbt, dlt, Fabric, Azure, etc. | Vibedata (seeded) |
| Data Engineering | SCD, accumulating snapshots, etc. | Vibedata (seeded) |
| Domain | Functional domains (e.g., pipeline analysis for tech services) | Community/Customer |
| Source | Source-specific (e.g., Salesforce skills) | Community/Customer |

##### Governance Tiers

Skills follow a tiering model to ensure reliability as the library grows:

| Tier | Description | Review Process |
| -------- | -------- | -------- |
| Official | Curated, actively maintained, tested against known scenarios | Accelerate Data engineering review |
| Community | User-created, experimental, no guarantees | Self-published |

Customer-created skills (Domain and Source types) start at Community tier. The Retro Agent can recommend skill updates based on chat history and incident patterns, which are reviewed before promotion.

##### Distribution Model

Skills are stored in the domain or source repo's skills directory and follow the same version control practices as code. This enables:

| Capability | Description |
| -------- | -------- |
| CI-invokable | Deploy agents invoke skills during PR validation to enforce domain-specific quality gates |
| Repo-distributed | Skills travel with the code they relate to — no separate skill server |
| Dependency-managed | Skills can be kept current via automated dependency updates (dependabot pattern) |
| Cross-surface | The same skill file is available to builders in studio, deploy agents in CI, and operator agents on issues |

Skills improve over time through the improvement flywheel — every pipeline built and every incident resolved feeds back into skills via the Retro Agent (see 6.3.5). See [vibedata-strategy.md Section 7.2](vibedata-strategy.md) for the full flywheel description.

#### 6.2.5 Settings

Configuration management for Vibedata.

| Setting | Description |
| -------- | -------- |
| Data Domains | Manage data domains and their GitHub repos/Fabric workspaces |
| Sources | Configure source ingestion (see 6.2.1) |
| Skills | Manage skills; invoke retro Agent to analyze chat history and issues per domain/source and recommend new or updated skills |
| MCP Servers | Connect to external systems for additional context (Confluence/Notion for specs, Linear/Jira for tickets, etc.) |
| System Prompts | Customize agent system prompts and behavior |
| Usage & Logs | Vibedata usage, logs, and token consumption by agents |
| Profile | Personal profile and LLM provider credentials (only required when using direct provider APIs) |
| Users | RBAC for Vibedata and data domains |

#### 6.2.6 Engine Layer

Studio delegates platform integrations to a set of engines. Each engine owns a specific external system interaction.

| Engine | Responsibility | Key Details |
| -------- | -------- | -------- |
| dlt | Ingestion configuration | CRUD operations for ingestion configuration and associated fabric artifacts |
| dbt | dbt Generation and Validation | CRUD operations for dbt artifacts and performs validation to ensure generated code aligns to the requirements |
| Git Integration | Code versioning and incident tracking | Perform GitHub operations for code versioning and issue tracking. |
| Fabric Integration | Workspace and compute management | CRUD operations for ephemeral workspaces, access the schema and data for agents to generate/modify mappings |
| Alert | Alert routing | Data Activator (Lakehouse) + Azure Monitor (Log Analytics) → webhook bridge (Azure Function) for deduplication and enrichment → GitHub Issues |
| Git Webhook | Inbound GitHub sync | Receives GitHub webhook events (push, installation, repo changes) to keep Studio's local git clones in sync with remote |

#### 6.2.7 Backend API Service

Studio exposes a backend API that connects the UI, agents, and engines.

| Capability | Description |
| -------- | -------- |
| SSE Streaming | Real-time agent responses with event types: delta, tool_use, artifact, todo_add, todo_update, git_commit |
| Session Management | File-system-based state (session_context.json, progress.json, todo.json) |
| Phase Transitions | Validation-enforced phase progression (Plan → Execute → Deploy); no regression, cannot skip Plan |
| Git Operations | Branch management, status (staged/unstaged/untracked), commit, PR creation |
| Artifact Persistence | File-system storage at deterministic paths for agent/UI coordination |

#### 6.2.8 Security Architecture

All secrets management uses Azure Key Vault with Managed Identity access. No secrets are stored in code, config files, or environment variables.

#### 6.2.9 Observability

Studio provides structured observability for all agent and service activity:

- **End-to-end tracing** — every log entry, tool call, and telemetry record includes `session id` and `intent_id` for correlation from user message through agent processing to pipeline execution
- **LLM tracing** — via Azure AI Foundry (*future*); full agent lifecycle tracking including tool calls, token usage, and cost per generation
- **Structured logging** — consistent JSON format across all services for querying and correlation
- **Persistent history** — chat, tool call, and agent execution logs retained per intent for debugging and audit

### 6.3 Agents

AI agents that automate data engineering workflows. Agents are organized into three groups:

| Group | Purpose | Persona |
| -------- | -------- | -------- |
| Builders | Create and modify pipelines (intent → production) | Full-Stack Analyst |
| Operators | Monitor, triage, diagnose, and remediate production issues | Data Reliability Engineer |
| Advisors | Provide on-demand recommendations to improve quality, performance, and skills | Both |

#### 6.3.0 Agent Surfaces

Agents are placed where users already work. The surface determines what context the agent has, what actions it can take, and whose attention it claims.

| Surface | Context | Agent Groups | Trigger |
| -------- | -------- | -------- | -------- |
| Studio | Interactive build environment | Builders, Advisors | User submits intent or requests advice |
| CI (GitHub Actions) | Pre-merge validation and post-merge deployment | Deploy agents (skills-based quality gates), Advisors (PR recommendations) | PR opened or code pushed |
| GitHub Issues | Production incident management | Operators (Triage, Diagnose, Remediation) | Alert fires and creates/updates issue |

This multi-surface model means agents participate in the entire pipeline lifecycle without requiring users to context-switch to a separate tool. Skills flow across all surfaces — the same domain knowledge that guides a builder in studio enforces quality gates in CI and informs root cause analysis on issues.

#### 6.3.1 Agent Overview

*Agent names are provisional and subject to further planning.*

| Agent | Group | Purpose |
| -------- | -------- | -------- |
| Requirements Agent | Builder | Gather requirements and design for new or modified silver and gold tables. Addresses the specification bottleneck: when build cost collapses, requirements quality becomes the binding constraint on pipeline value. |
| Build Agent | Builder | Generate or refactor dbt artifacts — models, tests (data and unit), dbt contract, dbt semantic model |
| Validation Agent | Builder | Perform golden data validation |
| Triage Agent | Operator | Prioritize and categorize GitHub issues; alert suppression |
| Diagnose Agent | Operator | Analyze telemetry, identify root cause |
| Remediation Agent | Operator | Execute approved runbooks, propose fixes and assist DREs to resolve production issues |
| Test Recommender Agent | Advisor | Suggest tests based on dbt model, chat history, issue log, and conversations; test coverage analysis |
| Performance Analyzer Agent | Advisor | Review dbt code and data model to recommend performance tuning (Day 1 and Day 2) |
| Retro Agent (Skill Builder) | Advisor | Analyze chat history and issues for a specific domain or source; suggest skill updates |

```text
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              BUILDERS                                           │
│                                                                                 │
│  ┌────────────────┐  ┌──────────────┐  ┌────────────────┐  ┌────────────────┐   │
│  │ Requirements   │  │ Build        │  │ Validation     │  │    Deploy      │   │
│  │ Agent          │─>│ Agent        │─>│ Agent          │─>│ (via GitHub    │   │
│  │                │  │              │  │                │  │  Actions)      │   │
│  │ Intent → Plan  │  │ Plan → Code  │  │ Code → Verify  │  │ Code → Fabric  │   │
│  └────────────────┘  └──────────────┘  └────────────────┘  └────────────────┘   │
│       PLAN              EXECUTE            VALIDATE             DEPLOY          │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────┐
│                              OPERATORS                                          │
│                                                                                 │
│   ┌──────────────┐    ┌───────────────┐    ┌───────────────────┐                │
│   │ Triage Agent │───>│ Diagnose Agent│───>│ Remediation Agent │                │
│   └──────────────┘    └───────────────┘    └───────────────────┘                │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────┐
│                              ADVISORS                                           │
│                                                                                 │
│   ┌──────────────────────────┐  ┌─────────────────────────────────┐             │
│   │ Test Recommender Agent   │  │ Performance Analyzer Agent      │             │
│   └──────────────────────────┘  └─────────────────────────────────┘             │
│   ┌──────────────────────────┐                                                  │
│   │ Retro Agent              │                                                  │
│   │ (Skill Builder)          │                                                  │
│   └──────────────────────────┘                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

#### 6.3.2 Agent Knowledge

Agents draw on four artifact types that together preserve institutional knowledge:

| Artifact | Scope | Purpose |
| -------- | -------- | -------- |
| Skills (`.skill` files) | Reusable | Source, domain, and pattern knowledge consumed by all agents |
| `claude.md` | Per repo | Coding standards, naming conventions, business rules |
| `intent.md` | Per intent | Business context, requirements, success criteria |
| `plan.md` | Per intent | Transformation plan and technical decisions |

#### 6.3.3 Builders

Builders serve the Full-Stack Analyst through the transform workflow. Requirements, Build, and Validation form the sequential pipeline from intent to deployed code.

| Agent | Trigger | Input | Output |
| -------- | -------- | -------- | -------- |
| Requirements | User submits intent via Studio chat | User intent, domain context, source schemas, skills | Requirements document, execution plan, success criteria |
| Build | User approves execution plan | Execution plan, source schemas, skills, validation report | dbt models, data tests, unit tests, dbt contract, dbt semantic model |
| Validation | Build agent completes code generation | Generated dbt artifacts, golden data | Validation report with pass/fail |

#### 6.3.4 Operators

Operators serve the Data Reliability Engineer during production operations. They act on GitHub Issues created by the alerting system.

| Agent | Trigger | Input | Output |
| -------- | -------- | -------- | -------- |
| Triage | Alert fires | GitHub issue, alert context | Categorized issue with priority; duplicate alerts suppressed |
| Diagnose | Issue triaged | Triaged issue, telemetry, logs | Root cause analysis |
| Remediation | Diagnosis complete | Root cause analysis, runbooks | Applied fix or fix proposal |

#### 6.3.5 Advisors

Advisors provide on-demand recommendations to both personas. They do not execute changes directly.

| Agent | Trigger | Input | Output |
| -------- | -------- | -------- | -------- |
| Test Recommender | On demand or post-build | dbt model, chat history, issue log | Test recommendations, coverage report |
| Performance Analyzer | On demand or post-build | dbt code, data model, query patterns | Performance tuning recommendations |
| Retro (Skill Builder) | User invokes from Skills UI, scoped to a domain or source | Chat history, GitHub issues, existing skills | Skill recommendations (new skills to create, existing skills to enhance) |

#### 6.3.6 Agent Autonomy Model

Agents operate at different autonomy levels based on risk and confidence.

| Level | Name | Description | Example |
| -------- | -------- | -------- | -------- |
| L1 | Automated | Agent acts without approval for known, low-risk actions | Triage categorizes issue, Remediation runs approved runbook |
| L2 | Propose | Agent proposes action, user approves before execution | Requirements proposes plan, Diagnose recommends fix |
| L3 | Trusted | Agent acts autonomously for complex tasks after earning trust | Remediation for novel issues after pattern established |

The graduation model (L1→L2→L3) reflects the J-curve of AI adoption: initial deployments reduce productivity before surging past manual baselines. Starting agents at L2 (propose, human approves) absorbs the J-curve trough while the platform accumulates domain knowledge in skills. L3 trust is earned, not assumed.

#### 6.3.7 Seeded Artifacts

Vibedata seeds agents with artifacts in two storage categories, published as versioned artifacts and updated by the control plane as part of the upgrade flow.

| Artifact | Storage | Purpose |
| -------- | -------- | -------- |
| System Prompts | Application storage | System prompts for the agents |
| Skills | Application storage | Prompt-based knowledge packages for agent reasoning |
| Tools | Application storage | MCP integrations for agent actions |
| Alert Templates | Customer GitHub repo | Query definitions that detect conditions and fire alerts |
| Runbooks | Customer GitHub repo | Remediation procedures (advisory or executable) |

Repo-based artifacts (alert templates, runbooks) are customer-overridable.

---

## 7. Cross-References

| Topic | Document |
| -------- | -------- |
| Product strategy | [vibedata-strategy.md](vibedata-strategy.md) |
| Studio overview | [module-specs/studio/specs/overview.md](module-specs/studio/specs/overview.md) |
| Studio Agent (Builders) | [module-specs/studio/specs/agent--studio-agent.md](module-specs/studio/specs/agent--studio-agent.md) |
| Monitoring Agents (Operators) | [module-specs/studio/specs/agent--monitoring-agent.md](module-specs/studio/specs/agent--monitoring-agent.md) |
| Alert Engine | [module-specs/studio/specs/engine--alert-engine.md](module-specs/studio/specs/engine--alert-engine.md) |
| Git Webhook Engine | [module-specs/studio/specs/engine--git-webhook.md](module-specs/studio/specs/engine--git-webhook.md) |
| Studio observability | [module-specs/studio/specs/cross-cutting--observability.md](module-specs/studio/specs/cross-cutting--observability.md) |
| Monitor module overview | [module-specs/monitor/monitor.md](module-specs/monitor/monitor.md) |
| Telemetry collection | [module-specs/monitor/modules/telemetry-collection.md](module-specs/monitor/modules/telemetry-collection.md) |
| Ops schema (database) | [module-specs/monitor/detailed-design/database-design.md](module-specs/monitor/detailed-design/database-design.md) |
| Testing strategy | [context/testing-strategy-policy.md](context/testing-strategy-policy.md) |
