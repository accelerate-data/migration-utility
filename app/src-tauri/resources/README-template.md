# Migration Repository

This repository is managed by the **Vibedata Migration Utility**. It stores migration artifacts for
one or more projects migrating Microsoft Fabric Warehouse stored procedures to dbt models.

> **Do not edit files in this repository manually.** Agent output files are immutable records.
> FDE overrides and edits are stored in the desktop app — not here.

---

## Repository Structure

```text
{project-slug}/
  artifacts/
    dacpac/
      {dacpac-filename}          # Source database schema — tracked via Git LFS
      metadata.json              # Project metadata (customer, system, db_name, extraction date)
    scoping-agent/
      {run_id}.json              # Writer procedure mapping results
    profiler-agent/
      {run_id}.json              # Candidate migration decisions
    decomposer-agent/
      {run_id}.json              # SQL decomposition and split-point proposals
    planner-agent/
      {run_id}.json              # dbt model design manifest
    test-generator-agent/
      {run_id}.json              # Branch-covering unit test fixtures
    migrator-agent/
      {run_id}.json              # Final dbt artifacts
```

Each project has its own top-level directory named after its slug (kebab-case of the project name).
Multiple projects can share this repository.

---

## Migration Pipeline

The utility migrates stored procedures through six sequential stages. Each stage is submitted from
the desktop app and executed as a GitHub Actions workflow.

| Stage | Agent | What it does |
|---|---|---|
| **Scope** | `scoping-agent` | Maps each target table to its writer stored procedure |
| **Profile** | `profiler-agent` | Proposes classification, keys, watermarks, FK, and PII decisions for FDE approval |
| **Decompose** | `decomposer-agent` | Segments the writer SQL into reusable logical blocks and split points |
| **Plan** | `planner-agent` | Produces a dbt model design manifest from approved decisions |
| **Generate Tests** | `test-generator-agent` | Generates branch-covering `unit_tests:` YAML fixtures |
| **Migrate** | `migrator-agent` | Materialises final dbt model files from the plan and test fixtures |

FDE review and approval happens in the desktop app between stages. Agent output files committed here
are immutable — a re-run produces a new file; it does not overwrite the prior one.

---

## Git LFS

`.dacpac` files are tracked via **Git LFS**. Do not remove the `.gitattributes` file or the LFS
tracking entry. Losing LFS tracking will break GitHub Actions restore caching.

To pull LFS objects after cloning:

```bash
git lfs pull
```

---

## GitHub Actions

Each agent run triggers a GitHub Actions workflow (`workflow_dispatch`). Runs are submitted by the
desktop app — do not trigger them manually.

To view logs for a specific run, right-click the table row in the desktop app and choose
**View run log**. This opens the GitHub Actions run URL directly in your browser.

Workflow files live in `.github/workflows/` and are managed by the Migration Utility team.

---

## FDE Overrides

Scope, Profile, Decompose, and Plan stage outputs can be reviewed and edited in the desktop app
before the next stage is submitted. These edits are stored **locally** in the app's SQLite database
and are never committed to this repository.

Generate Tests and Migrate outputs are final and read-only — no overrides are permitted for those
stages.
