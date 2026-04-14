# VU-1073 Reset Migration Global Mode Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `/reset-migration all` so a project can be reset to a clean post-`/init-ad-migration` state, with explicit destructive confirmation, sandbox teardown first, and a fresh `/setup-ddl` as the next required step.

**Architecture:** Keep the existing split between the thin command wrapper and deterministic CLI core. Put global inventory and deletion logic in `lib/shared/dry_run_core.py`, extend the `migrate-util` output contract to report global reset results, and update `commands/reset-migration.md` so the command performs an evidence-based dry run, explicit confirmation, and teardown-before-reset orchestration.

**Tech Stack:** Python 3.11, Typer CLI, Pydantic v2 output models, pytest, Promptfoo command evals, Markdown command specs.

---

## Task 1: Extend the Reset Output Contract

**Files:**

- Modify: `lib/shared/output_models/dry_run.py`
- Test: `tests/unit/dry_run/test_dry_run.py`

- [ ] **Step 1: Write the failing contract test**

Add a unit test near the existing reset-migration tests that asserts the CLI can serialize a global-reset result shape with deleted paths and cleared manifest sections.

```python
def test_reset_migration_global_output_contract() -> None:
    result = dry_run.ResetMigrationOutput(
        stage="all",
        targets=[],
        reset=[],
        noop=[],
        blocked=[],
        not_found=[],
        deleted_paths=["catalog", "ddl"],
        missing_paths=["test-specs"],
        cleared_manifest_sections=["runtime.source", "runtime.target"],
    )

    payload = result.model_dump(mode="json")
    assert payload["stage"] == "all"
    assert payload["deleted_paths"] == ["catalog", "ddl"]
    assert payload["missing_paths"] == ["test-specs"]
    assert payload["cleared_manifest_sections"] == ["runtime.source", "runtime.target"]
```

- [ ] **Step 2: Run the single test to verify it fails**

Run:

```bash
cd lib && uv run pytest ../tests/unit/dry_run/test_dry_run.py -k global_output_contract -v
```

Expected: FAIL because `ResetMigrationOutput` does not accept `stage="all"` or the new fields.

- [ ] **Step 3: Extend the output model minimally**

Update `ResetMigrationOutput` in `lib/shared/output_models/dry_run.py` to support the new global mode without breaking table-stage callers.

```python
class ResetMigrationOutput(BaseModel):
    model_config = OUTPUT_CONFIG

    stage: Literal["scope", "profile", "generate-tests", "refactor", "all"]
    targets: list[ResetTargetResult]
    reset: list[str]
    noop: list[str]
    blocked: list[str]
    not_found: list[str]
    deleted_paths: list[str] = Field(default_factory=list)
    missing_paths: list[str] = Field(default_factory=list)
    cleared_manifest_sections: list[str] = Field(default_factory=list)
```

- [ ] **Step 4: Re-run the contract test**

Run:

```bash
cd lib && uv run pytest ../tests/unit/dry_run/test_dry_run.py -k global_output_contract -v
```

Expected: PASS

- [ ] **Step 5: Commit the contract change**

```bash
git add lib/shared/output_models/dry_run.py tests/unit/dry_run/test_dry_run.py
git commit -m "feat: extend reset migration output contract"
```

## Task 2: Add Deterministic Global Reset Logic in the Core

**Files:**

- Modify: `lib/shared/dry_run_core.py`
- Modify: `tests/unit/dry_run/test_dry_run.py`

- [ ] **Step 1: Write the failing core tests**

Add focused tests covering:

- inventory + deletion of existing artifact directories
- preservation of init scaffolding files
- clearing `runtime.source`, `runtime.target`, `runtime.sandbox`, `extraction`, and `init_handoff`
- no-op reporting for paths that do not exist

Use a helper fixture project that contains the full artifact surface:

```python
def test_run_reset_migration_all_deletes_artifacts_and_clears_manifest(tmp_path: Path) -> None:
    dst = _make_reset_project(tmp_path)
    (dst / "ddl").mkdir()
    (dst / ".staging").mkdir()
    (dst / "dbt").mkdir(exist_ok=True)
    (dst / "dbt" / "dbt_project.yml").write_text("name: demo\n", encoding="utf-8")
    (dst / "CLAUDE.md").write_text("# keep\n", encoding="utf-8")
    manifest_path = dst / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["runtime"]["target"] = {
        "technology": "sql_server",
        "dialect": "tsql",
        "connection": {"database": "TargetDB"},
    }
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    result = dry_run.run_reset_migration(dst, "all", [])

    assert sorted(result.deleted_paths) == sorted(["catalog", "ddl", ".staging", "test-specs", "dbt"])
    assert "runtime.source" in result.cleared_manifest_sections
    assert "runtime.target" in result.cleared_manifest_sections
    assert "runtime.sandbox" in result.cleared_manifest_sections
    assert "extraction" in result.cleared_manifest_sections
    assert "init_handoff" in result.cleared_manifest_sections
    assert not (dst / "catalog").exists()
    assert not (dst / "dbt").exists()
    assert (dst / "CLAUDE.md").exists()
```

```python
def test_run_reset_migration_all_reports_missing_paths_as_noop(tmp_path: Path) -> None:
    tmp, dst = _make_bare_project()
    with tmp:
        result = dry_run.run_reset_migration(dst, "all", [])
        assert "catalog" in result.deleted_paths
        assert "ddl" in result.missing_paths
        assert "test-specs" in result.missing_paths
```

- [ ] **Step 2: Run the targeted tests to verify they fail**

Run:

```bash
cd lib && uv run pytest ../tests/unit/dry_run/test_dry_run.py -k "reset_migration_all" -v
```

Expected: FAIL because `run_reset_migration()` rejects `all`.

- [ ] **Step 3: Add focused global-reset helpers**

In `lib/shared/dry_run_core.py`, keep table-stage logic intact and add a separate path for global reset:

```python
_GLOBAL_RESET_DIRS = ("catalog", "ddl", ".staging", "test-specs", "dbt")
_GLOBAL_RESET_MANIFEST_SECTIONS = (
    "runtime.source",
    "runtime.target",
    "runtime.sandbox",
    "extraction",
    "init_handoff",
)


def _delete_tree_if_present(project_root: Path, rel_path: str) -> bool:
    path = project_root / rel_path
    if not path.exists():
        return False
    if path.is_dir():
        shutil.rmtree(path)
    else:
        path.unlink()
    return True


def _clear_global_manifest_state(project_root: Path) -> list[str]:
    manifest_path = project_root / "manifest.json"
    if not manifest_path.exists():
        return []

    manifest = _read_catalog_json(manifest_path)
    cleared: list[str] = []

    runtime = manifest.get("runtime")
    if isinstance(runtime, dict):
        for role in ("source", "target", "sandbox"):
            if role in runtime:
                del runtime[role]
                cleared.append(f"runtime.{role}")
        if not runtime:
            manifest.pop("runtime", None)

    for key in ("extraction", "init_handoff"):
        if key in manifest:
            del manifest[key]
            cleared.append(key)

    write_json(manifest_path, manifest)
    return cleared


def _run_reset_migration_all(project_root: Path) -> ResetMigrationOutput:
    deleted_paths: list[str] = []
    missing_paths: list[str] = []
    for rel_path in _GLOBAL_RESET_DIRS:
        if _delete_tree_if_present(project_root, rel_path):
            deleted_paths.append(rel_path)
        else:
            missing_paths.append(rel_path)

    cleared_manifest_sections = _clear_global_manifest_state(project_root)
    return ResetMigrationOutput(
        stage="all",
        targets=[],
        reset=[],
        noop=[],
        blocked=[],
        not_found=[],
        deleted_paths=deleted_paths,
        missing_paths=missing_paths,
        cleared_manifest_sections=cleared_manifest_sections,
    )
```

Add an early branch at the top of `run_reset_migration()`:

```python
if stage == "all":
    if fqns:
        raise ValueError("Global reset does not accept table arguments")
    return _run_reset_migration_all(project_root)
```

Also update the stage validation set to include `all` only for reset, not for readiness.

- [ ] **Step 4: Re-run the targeted tests**

Run:

```bash
cd lib && uv run pytest ../tests/unit/dry_run/test_dry_run.py -k "reset_migration_all" -v
```

Expected: PASS

- [ ] **Step 5: Run the broader dry-run unit suite**

Run:

```bash
cd lib && uv run pytest ../tests/unit/dry_run/test_dry_run.py -v
```

Expected: PASS

- [ ] **Step 6: Commit the core implementation**

```bash
git add lib/shared/dry_run_core.py tests/unit/dry_run/test_dry_run.py
git commit -m "feat: add global reset migration core"
```

## Task 3: Update the Typer CLI for `all`

**Files:**

- Modify: `lib/shared/dry_run.py`
- Modify: `tests/unit/dry_run/test_dry_run.py`

- [ ] **Step 1: Write the failing CLI tests**

Add one success test and one validation test:

```python
def test_reset_migration_cli_all(tmp_path: Path) -> None:
    dst = _make_reset_project(tmp_path)
    (dst / "ddl").mkdir()

    result = _cli_runner.invoke(
        dry_run.app,
        ["reset-migration", "all", "--project-root", str(dst)],
    )

    assert result.exit_code == 0, result.output
    output = json.loads(result.stdout)
    assert output["stage"] == "all"
    assert "ddl" in output["deleted_paths"]
```

```python
def test_reset_migration_cli_all_rejects_table_args(tmp_path: Path) -> None:
    dst = _make_reset_project(tmp_path)

    result = _cli_runner.invoke(
        dry_run.app,
        ["reset-migration", "all", "silver.DimCustomer", "--project-root", str(dst)],
    )

    assert result.exit_code == 1
    output = json.loads(result.stdout)
    assert "error" in output
```

- [ ] **Step 2: Run the CLI tests to verify they fail**

Run:

```bash
cd lib && uv run pytest ../tests/unit/dry_run/test_dry_run.py -k "cli_all" -v
```

Expected: FAIL because the CLI currently requires one or more FQNs.

- [ ] **Step 3: Adjust the CLI signature without disturbing stage mode**

Update `reset_migration_cmd()` in `lib/shared/dry_run.py` so `fqns` can be optional and the core enforces the mode rules:

```python
@app.command("reset-migration")
def reset_migration_cmd(
    stage: str = typer.Argument(..., help="Pre-model stage to reset, or 'all' for global reset"),
    fqns: List[str] = typer.Argument(
        None,
        help="Fully-qualified table names to reset",
    ),
    project_root: Optional[Path] = typer.Option(
        None, "--project-root", help="Project root directory",
    ),
) -> None:
    ...
    result = run_reset_migration(root, stage, list(fqns or []))
```

Keep the existing error handling branches intact.

- [ ] **Step 4: Re-run the CLI tests**

Run:

```bash
cd lib && uv run pytest ../tests/unit/dry_run/test_dry_run.py -k "cli_all" -v
```

Expected: PASS

- [ ] **Step 5: Commit the CLI change**

```bash
git add lib/shared/dry_run.py tests/unit/dry_run/test_dry_run.py
git commit -m "feat: support global reset migration CLI mode"
```

## Task 4: Update the `/reset-migration` Command Contract

**Files:**

- Modify: `commands/reset-migration.md`
- Test: `tests/evals/packages/cmd-reset-migration/cmd-reset-migration.yaml`
- Test: `tests/evals/fixtures/cmd-reset-migration/base/manifest.json`
- Test: `tests/evals/fixtures/cmd-reset-migration/model-complete/manifest.json`
- Test: `tests/evals/prompts/cmd-reset-migration.txt`

- [ ] **Step 1: Update the command spec to branch on `all`**

Rewrite `commands/reset-migration.md` so it explicitly documents two modes:

- stage mode for `scope|profile|generate-tests|refactor <schema.table>...`
- global mode for `all`

Add the global-mode preflight contract:

```text
For `/reset-migration all`, inspect the current project first and show exactly what exists and will be removed:

- `runtime.sandbox` configured / not configured
- `catalog/`, `ddl/`, `.staging/`, `test-specs/`, `dbt/` exist / absent
- manifest sections present and subject to clearing:
  `runtime.source`, `runtime.target`, `runtime.sandbox`, `extraction`, `init_handoff`

Require explicit confirmation:

Type `reset all` to confirm global reset.
```

Document that sandbox teardown must run first when configured and that the next required step after success is `/setup-ddl`.

- [ ] **Step 2: Update eval coverage for the new mode**

Add Promptfoo cases for:
- global reset with existing artifacts
- global reset with configured sandbox in manifest inventory
- global reset when some paths are already absent

Example entry:

```yaml
  - description: "all — global reset deletes migration artifacts and clears runtime state"
    vars:
      fixture_path: "tests/evals/fixtures/cmd-reset-migration/base"
      instruction: "Run `/reset-migration all`."
      expected_item_statuses: '{}'
      expected_output_terms: "reset-migration,all,catalog,ddl,dbt,setup-ddl"
```

Keep the eval prompt override that skips the confirmation pause, but update the wording so the harness still expects the preflight summary.

- [ ] **Step 3: Run the command eval package**

Run:

```bash
cd tests/evals && npm run eval:cmd-reset-migration
```

Expected: PASS

- [ ] **Step 4: Commit the command/eval contract update**

```bash
git add commands/reset-migration.md tests/evals/packages/cmd-reset-migration/cmd-reset-migration.yaml tests/evals/prompts/cmd-reset-migration.txt tests/evals/fixtures/cmd-reset-migration
git commit -m "feat: document global reset migration command"
```

## Task 5: Wire Sandbox Teardown into the Command Flow

**Files:**

- Modify: `commands/reset-migration.md`
- Reference: `commands/teardown-sandbox.md`
- Test: `tests/evals/packages/cmd-reset-migration/cmd-reset-migration.yaml`

- [ ] **Step 1: Document the orchestration explicitly**

In `commands/reset-migration.md`, add the exact sequence for `all`:

```text
If `runtime.sandbox` is configured:

1. Show it in the preflight summary.
1. After explicit confirmation, run:

   `uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" test-harness sandbox-down`

1. If sandbox teardown fails, stop and report the failure. Do not run the global reset CLI afterward.
```

This keeps teardown orchestration in the command wrapper, not in `migrate-util`.

- [ ] **Step 2: Add an eval expectation for teardown-first behavior**

Add expected output terms for the global reset case that mention sandbox teardown when the fixture manifest includes `runtime.sandbox`.

```yaml
expected_output_terms: "sandbox,teardown,reset-migration,all"
```

- [ ] **Step 3: Re-run the command eval package**

Run:

```bash
cd tests/evals && npm run eval:cmd-reset-migration
```

Expected: PASS

- [ ] **Step 4: Commit the teardown orchestration update**

```bash
git add commands/reset-migration.md tests/evals/packages/cmd-reset-migration/cmd-reset-migration.yaml
git commit -m "feat: require sandbox teardown before global reset"
```

## Task 6: Final Verification

**Files:**

- Verify only

- [ ] **Step 1: Run the dry-run unit tests**

Run:

```bash
cd lib && uv run pytest ../tests/unit/dry_run/test_dry_run.py -v
```

Expected: PASS

- [ ] **Step 2: Run the command eval package**

Run:

```bash
cd tests/evals && npm run eval:cmd-reset-migration
```

Expected: PASS

- [ ] **Step 3: Inspect the final diff**

Run:

```bash
git status --short
git diff --stat
```

Expected: only the planned files are modified, with no accidental repo-wide churn.

- [ ] **Step 4: Create the final implementation commit**

If any verification-driven adjustments were needed:

```bash
git add lib/shared/output_models/dry_run.py lib/shared/dry_run_core.py lib/shared/dry_run.py tests/unit/dry_run/test_dry_run.py commands/reset-migration.md tests/evals/packages/cmd-reset-migration/cmd-reset-migration.yaml tests/evals/prompts/cmd-reset-migration.txt tests/evals/fixtures/cmd-reset-migration
git commit -m "VU-1073: add global reset migration mode"
```

## Self-Review

- Spec coverage:
  - global `all` mode is covered in Tasks 1-4
  - explicit destructive confirmation and dry-run inventory are covered in Tasks 4-5
  - sandbox teardown-first behavior is covered in Task 5
  - full artifact deletion including `dbt/` is covered in Task 2 and documented in Task 4
  - manifest cleanup while preserving init scaffolding is covered in Task 2
- Placeholder scan:
  - no `TBD`/`TODO` placeholders remain
  - every code-changing task includes concrete file targets, commands, and snippets
- Type consistency:
  - the plan uses one result model, `ResetMigrationOutput`, with `stage="all"` and `deleted_paths` / `missing_paths` / `cleared_manifest_sections`
  - the plan keeps command-level sandbox teardown separate from CLI-core deletion logic
