from __future__ import annotations

import json
from pathlib import Path


def _append_procedure(project_root: Path, ddl_sql: str) -> None:
    ddl_path = project_root / "ddl" / "procedures.sql"
    existing = ddl_path.read_text()
    ddl_path.write_text(f"{existing.rstrip()}\nGO\n{ddl_sql.strip()}\nGO\n")

def _write_catalog(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n")

def _seed_migrate_fixture(
    project_root: Path,
    table_fqn: str,
    writer_fqn: str,
    proc_sql: str,
    proc_catalog: dict[str, object],
    table_catalog: dict[str, object],
) -> None:
    _append_procedure(project_root, proc_sql)
    _write_catalog(
        project_root / "catalog" / "procedures" / f"{writer_fqn.lower()}.json",
        proc_catalog,
    )
    _write_catalog(
        project_root / "catalog" / "tables" / f"{table_fqn.lower()}.json",
        table_catalog,
    )

def _seed_refactor_fixture(
    tmp_path: Path,
    writer_fqn: str,
    proc_refactor: dict | None,
) -> None:
    """Write a minimal table catalog (with selected_writer) and procedure catalog."""
    table_dir = tmp_path / "catalog" / "tables"
    table_dir.mkdir(parents=True)
    proc_dir = tmp_path / "catalog" / "procedures"
    proc_dir.mkdir(parents=True)
    (tmp_path / "manifest.json").write_text("{}")
    (table_dir / "silver.mytable.json").write_text(json.dumps({
        "schema": "silver",
        "name": "mytable",
        "scoping": {"status": "resolved", "selected_writer": writer_fqn},
    }))
    proc_catalog: dict = {"schema": "dbo", "name": "usp_writer"}
    if proc_refactor is not None:
        proc_catalog["refactor"] = proc_refactor
    (proc_dir / f"{writer_fqn.lower()}.json").write_text(json.dumps(proc_catalog))

def _seed_generate_fixture(
    project_root: Path,
    fqn: str,
    *,
    kind: str = "tables",
    create_model: bool = True,
) -> Path:
    """Create catalog + dbt project tree for run_write_generate tests.

    Returns the project_root.
    """
    # Catalog file
    cat_path = project_root / "catalog" / kind / f"{fqn}.json"
    cat_path.parent.mkdir(parents=True, exist_ok=True)
    cat_path.write_text(json.dumps({"schema": "dbo", "name": fqn}))

    # dbt project
    dbt = project_root / "dbt"
    marts = dbt / "models" / "marts"
    marts.mkdir(parents=True, exist_ok=True)
    (dbt / "dbt_project.yml").write_text("name: test\nversion: '1.0.0'\nconfig-version: 2\n")

    if create_model:
        model_file = marts / "foo.sql"
        model_file.write_text("SELECT 1 AS id")

    return project_root
