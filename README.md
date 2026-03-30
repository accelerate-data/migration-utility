# Migration Utility

A desktop app that migrates Microsoft Fabric Warehouse stored procedures to dbt models on Vibedata's agentic data engineering platform.

Connect it to your Fabric workspace, select which tables to migrate, review and confirm the AI's analysis, then launch. The migration runs as a headless pipeline in your GitHub repo — producing tested, ready-to-merge dbt models at the end.

**Scope:** Silver and gold transformations from Fabric Warehouse (T-SQL). Lakehouse / Spark is not supported.

---

## Prerequisites

- Access to the Fabric workspace you want to migrate (Viewer role or higher)
- A GitHub account — used to clone and push to the migration repo
- An Anthropic API key — used by the migration agents during execution

---

## Installation

Download the latest release from the [Releases page](https://github.com/hbanerjee74/migration-utility/releases).

| Platform | File |
|---|---|
| macOS (Apple Silicon) | `migration-utility_aarch64.dmg` |
| macOS (Intel) | `migration-utility_x86_64.dmg` |
| Windows | `migration-utility_x86_64-setup.exe` |
| Linux | `migration-utility_amd64.AppImage` |

**macOS:** Open the `.dmg`, drag Migration Utility to Applications. On first launch, right-click → Open if macOS shows an unverified developer warning.

---

## Workflow

### 1 — Connect your accounts

Open **Settings → Connections** and complete the one-time setup:

- **GitHub** — authenticate to allow the app to clone and push to your migration repo
- **Anthropic API key** — the key the pipeline agents will use during execution

These are reusable across migrations. You only do this once.

Then open **Settings → Workspace** and configure the current migration:

- **Fabric workspace URL** and service principal credentials — the source workspace containing the stored procedures to migrate
- **Migration repo** — the GitHub repo where migration state and agent outputs are committed (separate from your production repo)
- **Working directory** — where the migration repo is cloned locally (default: `~/vibedata-migration`)

Once Connections and Workspace are both configured, **Home** shows you're ready to begin.

---

### 2 — Select your tables

Open **Scope**. You'll see every table in your Fabric Warehouse, grouped by schema. Select the tables that belong to this domain migration.

The app traces each selected table back to the stored procedure that produces it — you don't need to know which procedures are involved.

---

### 3 — Review candidacy

The app analyses each stored procedure and classifies it as:

| Tier | Meaning |
|---|---|
| **Migrate** | Straightforward T-SQL — the utility handles it automatically |
| **Review** | Complex patterns (dynamic SQL, MERGE, cursors) — migrate manually in parallel |
| **Reject** | Cannot be migrated automatically — migrate manually in parallel |

Review the classifications and expand any row to see the agent's reasoning. You can override a classification at any time — select a different tier and add a short reason.

Review and Reject procedures are not blockers. You migrate those manually while the automated track runs, and both converge at the same pull request.

---

### 4 — Confirm table config

For each table, confirm the migration settings:

- **Table type** — Fact, Dimension, or Other (drives snapshot strategy)
- **Snapshot strategy** — 1-day sample (facts) or full copy (dimensions)
- **Incremental column** — the date or sequence column used for incremental loads (pre-filled by the agent, confirm before launch)
- **PII columns** — columns containing personal data; masked before fixture generation

Fields pre-filled by the agent are marked with an "AI suggested" indicator. Edit any field to remove the indicator and take ownership of that value.

Each table requires an explicit **Confirm** click. The left panel tracks not started / opened / confirmed.

---

### 5 — Launch

Once all tables are confirmed, open **Monitor**. A summary shows your confirmed procedure and table counts across all three tiers.

Click **Launch migration**. The app writes your configuration to `plan.md`, commits it to the migration repo, and starts the GitHub Actions pipeline. The Scope wizard locks to read-only at this point.

Monitor switches to the running view, which shows:

- **Progress** — procedures complete vs total
- **Agent phases** — Discovery · Candidacy · Translation · Tests · Validation, updated in real time
- **Log stream** — live output from the running agents

---

## Handling blocked procedures

If a Migrate-tier procedure depends on a Review or Reject procedure that hasn't been manually migrated yet, it is marked **BLOCKED** in `plan.md`. The app highlights these in Monitor.

To unblock: migrate the upstream procedure manually, mark it `RESOLVED` in `plan.md`, then relaunch. The pipeline resumes from where it stopped — completed procedures are not re-run.

---

## After the pipeline completes

When all Migrate-tier procedures pass their tests and validation, the utility pushes a branch to your production repo. Open a pull request from that branch. Your team's standard CI/CD takes over from there — ephemeral environment, parallel run, domain owner sign-off, then merge.

---

## Session resumption

The app saves state to SQLite continuously. If you close it mid-setup (between conversations with a domain owner) or mid-run (pipeline interrupted), reopening the app restores exactly where you left off — including partial candidacy and table config work.

---

## Development

### Loading plugins

To run Claude Code with all migration plugins loaded:

```bash
claude --plugin-dir ./bootstrap \
       --plugin-dir ./migration \
       --plugin-dir ./test-generation
```

See the plugin README files in `bootstrap/`, `migration/`, and `test-generation/` for plugin prerequisites, skills, MCP servers, and environment variables.

---

## License

[Elastic License 2.0](LICENSE) — free to use, not available as a managed service.
