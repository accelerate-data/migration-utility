#!/usr/bin/env python3
"""One-time script to extract per-scenario fixtures from the monolithic migration-test fixture.

For each test scenario in each package YAML, creates a minimal fixture directory containing
only the files that scenario needs. Throwaway — delete after fixture migration is verified.

Usage:
    cd tests/evals
    python3 ../../scripts/extract-scenario-fixtures.py
"""

import json
import re
import shutil
from pathlib import Path

import yaml

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

EVALS_DIR = Path(".")  # run from tests/evals/
PACKAGES_DIR = EVALS_DIR / "packages"
MONOLITHIC = EVALS_DIR / "fixtures" / "migration-test"
FIXTURES_OUT = EVALS_DIR / "fixtures"

# Skills that need source table catalogs (proc's references.tables.in_scope)
NEEDS_SOURCE_TABLES = {
    "generating-tests",
    "reviewing-tests",
}

# Skills that need dbt project files
NEEDS_DBT = {
    "generating-model",
    "reviewing-tests",
    "reviewing-model",
}

# Skills that need test-specs
NEEDS_TEST_SPECS = {
    "refactoring-sql",
    "generating-model",
    "generating-tests",
    "reviewing-tests",
    "reviewing-model",
}

# Skills where we need ALL candidate writer procedures (not just selected)
NEEDS_ALL_CANDIDATES = {
    "analyzing-table",
}

# View pipeline uses views, not tables/procedures
VIEW_SKILLS = {
    "view-pipeline",
}

# Command packages that operate on multiple tables
CMD_PACKAGES = {
    "cmd-scope",
    "cmd-profile",
    "cmd-generate-model",
    "cmd-generate-tests",
    "cmd-refactor",
    "cmd-status",
}

# ---------------------------------------------------------------------------
# DDL Splitting
# ---------------------------------------------------------------------------


def split_ddl_by_go(ddl_path: Path) -> dict[str, str]:
    """Split a DDL file by GO delimiters, returning {lowercase_fqn: chunk}."""
    if not ddl_path.exists():
        return {}
    text = ddl_path.read_text(encoding="utf-8")
    # Split on GO that's on its own line (possibly with whitespace)
    chunks = re.split(r"\n\s*GO\s*\n", text, flags=re.IGNORECASE)
    result = {}
    for chunk in chunks:
        # Find CREATE PROCEDURE or CREATE VIEW name
        m = re.search(
            r"CREATE\s+(?:PROCEDURE|PROC|VIEW)\s+(\[?\w+\]?\.\[?\w+\]?)",
            chunk,
            re.IGNORECASE,
        )
        if m:
            name = m.group(1).replace("[", "").replace("]", "").lower()
            result[name] = chunk.strip()
    return result


def reassemble_ddl(chunks: dict[str, str], needed_names: set[str]) -> str:
    """Reassemble DDL chunks for the needed names."""
    parts = []
    for name in sorted(needed_names):
        if name in chunks:
            parts.append(chunks[name])
    return "\n\nGO\n\n".join(parts) + "\n\nGO\n" if parts else ""


# ---------------------------------------------------------------------------
# Catalog helpers
# ---------------------------------------------------------------------------


def load_json(path: Path) -> dict | None:
    """Load JSON file, return None if not found."""
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def get_referenced_by_procs(table_catalog: dict) -> list[str]:
    """Get all procedure FQNs that reference this table (from referenced_by)."""
    procs = []
    ref_by = table_catalog.get("referenced_by", {})
    for scope in ["in_scope", "out_of_scope"]:
        for p in ref_by.get("procedures", {}).get(scope, []):
            schema = p.get("schema", "")
            name = p.get("name", "")
            if schema and name:
                procs.append(f"{schema}.{name}".lower())
    return procs


def get_proc_callees(proc_catalog: dict) -> list[str]:
    """Get procedures called by this proc (EXEC chain callees)."""
    callees = []
    refs = proc_catalog.get("references", {})
    for scope in ["in_scope", "out_of_scope"]:
        for p in refs.get("procedures", {}).get(scope, []):
            schema = p.get("schema", "")
            name = p.get("name", "")
            if schema and name:
                callees.append(f"{schema}.{name}".lower())
    return callees


def get_proc_source_tables(proc_catalog: dict) -> list[str]:
    """Get source tables referenced by this proc."""
    tables = []
    refs = proc_catalog.get("references", {})
    for scope in ["in_scope", "out_of_scope"]:
        for t in refs.get("tables", {}).get(scope, []):
            schema = t.get("schema", "")
            name = t.get("name", "")
            if schema and name:
                tables.append(f"{schema}.{name}".lower())
    return tables


def find_stg_model_for_table(target_table: str) -> tuple[str | None, str | None]:
    """Find staging model SQL and YAML for a target table."""
    table_name = target_table.split(".")[-1].lower()
    stg_dir = MONOLITHIC / "dbt" / "models" / "staging"
    sql_file = stg_dir / f"stg_{table_name}.sql"
    yml_file = stg_dir / f"_stg_{table_name}.yml"
    return (
        sql_file if sql_file.exists() else None,
        yml_file if yml_file.exists() else None,
    )


# ---------------------------------------------------------------------------
# YAML parsing
# ---------------------------------------------------------------------------


def parse_package_yaml(yaml_path: Path) -> list[dict]:
    """Parse a package YAML and return list of {description, vars, package_name}."""
    data = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
    if not data:
        return []

    default_vars = {}
    if "defaultTest" in data and "vars" in data["defaultTest"]:
        default_vars = data["defaultTest"]["vars"]

    tests = data.get("tests", [])
    results = []
    for test in tests:
        desc = test.get("description", "")
        merged_vars = {**default_vars, **(test.get("vars", {}))}
        results.append(
            {
                "description": desc,
                "vars": merged_vars,
            }
        )
    return results


def slug_from_description(desc: str) -> str:
    """Derive a filesystem-safe slug from the scenario description."""
    # Take everything before " — " if present, else first 60 chars
    parts = desc.split(" — ")
    slug = parts[0].strip()
    # Sanitize
    slug = re.sub(r"[^a-zA-Z0-9_-]", "-", slug)
    slug = re.sub(r"-+", "-", slug).strip("-").lower()
    return slug[:80]


def package_name_from_path(yaml_path: Path) -> str:
    """Derive package name from YAML path."""
    # packages/profiling-table/skill-profiling-table.yaml -> profiling-table
    return yaml_path.parent.name


# ---------------------------------------------------------------------------
# Fixture generation
# ---------------------------------------------------------------------------


def create_scenario_fixture(
    package_name: str,
    slug: str,
    scenario_vars: dict,
    proc_chunks: dict[str, str],
    view_chunks: dict[str, str],
) -> Path:
    """Create a per-scenario fixture directory with minimal files."""
    out_dir = FIXTURES_OUT / package_name / slug
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    target_table = scenario_vars.get("target_table", "").lower()
    target_view = scenario_vars.get("target_view", "").lower()
    writer = scenario_vars.get("writer", "").lower()
    target_procedure = scenario_vars.get("target_procedure", "").lower()
    target_tables_str = scenario_vars.get("target_tables", "")

    # For command packages, parse space-separated table list
    target_tables = []
    if target_tables_str:
        target_tables = [t.lower() for t in target_tables_str.split()]
    elif target_table:
        target_tables = [target_table]

    # Determine writer(s)
    writers = []
    if writer:
        writers.append(writer)
    if target_procedure and target_procedure not in writers:
        writers.append(target_procedure)

    # 1. Copy manifest.json
    src_manifest = MONOLITHIC / "manifest.json"
    if src_manifest.exists():
        shutil.copy2(src_manifest, out_dir / "manifest.json")

    # --- VIEW PIPELINE ---
    if target_view:
        # Copy view catalog
        view_cat_dir = out_dir / "catalog" / "views"
        view_cat_dir.mkdir(parents=True, exist_ok=True)
        src = MONOLITHIC / "catalog" / "views" / f"{target_view}.json"
        if src.exists():
            shutil.copy2(src, view_cat_dir / f"{target_view}.json")

        # Copy view DDL
        if view_chunks:
            needed = {target_view}
            ddl_text = reassemble_ddl(view_chunks, needed)
            if ddl_text:
                ddl_dir = out_dir / "ddl"
                ddl_dir.mkdir(parents=True, exist_ok=True)
                (ddl_dir / "views.sql").write_text(ddl_text, encoding="utf-8")

        return out_dir

    # --- TABLE-BASED SCENARIOS ---

    # 2. Copy table catalog(s)
    table_cat_dir = out_dir / "catalog" / "tables"
    table_cat_dir.mkdir(parents=True, exist_ok=True)

    all_needed_procs: set[str] = set()
    all_source_tables: set[str] = set()

    for tbl in target_tables:
        src = MONOLITHIC / "catalog" / "tables" / f"{tbl}.json"
        if src.exists():
            shutil.copy2(src, table_cat_dir / f"{tbl}.json")

            # Load catalog to discover procedures and sources
            cat = load_json(src)
            if cat:
                # Get candidate writers
                if package_name in NEEDS_ALL_CANDIDATES:
                    # Copy ALL candidate procs for this table
                    all_needed_procs.update(get_referenced_by_procs(cat))
                elif package_name in CMD_PACKAGES:
                    # Commands also need all referenced procs
                    all_needed_procs.update(get_referenced_by_procs(cat))

    # Add explicit writer(s)
    all_needed_procs.update(writers)

    # 3. Load each needed proc's catalog, discover callees and sources
    proc_cat_dir = out_dir / "catalog" / "procedures"
    proc_cat_dir.mkdir(parents=True, exist_ok=True)

    visited_procs: set[str] = set()
    proc_queue = list(all_needed_procs)

    while proc_queue:
        proc_fqn = proc_queue.pop()
        if proc_fqn in visited_procs:
            continue
        visited_procs.add(proc_fqn)

        src = MONOLITHIC / "catalog" / "procedures" / f"{proc_fqn}.json"
        if src.exists():
            shutil.copy2(src, proc_cat_dir / f"{proc_fqn}.json")

            proc_cat = load_json(src)
            if proc_cat:
                # EXEC chain callees
                callees = get_proc_callees(proc_cat)
                for c in callees:
                    if c not in visited_procs:
                        proc_queue.append(c)

                # Source tables for skills that need them
                if package_name in NEEDS_SOURCE_TABLES or package_name in CMD_PACKAGES:
                    all_source_tables.update(get_proc_source_tables(proc_cat))

    # 4. Copy source table catalogs
    for src_tbl in all_source_tables:
        src = MONOLITHIC / "catalog" / "tables" / f"{src_tbl}.json"
        if src.exists() and not (table_cat_dir / f"{src_tbl}.json").exists():
            shutil.copy2(src, table_cat_dir / f"{src_tbl}.json")

    # 5. Build per-scenario procedures.sql
    needed_proc_names = visited_procs.copy()
    # Also include callees' callees (already resolved via BFS above)
    ddl_text = reassemble_ddl(proc_chunks, needed_proc_names)
    if ddl_text:
        ddl_dir = out_dir / "ddl"
        ddl_dir.mkdir(parents=True, exist_ok=True)
        (ddl_dir / "procedures.sql").write_text(ddl_text, encoding="utf-8")

    # 6. Copy test-specs if needed
    if package_name in NEEDS_TEST_SPECS or package_name in CMD_PACKAGES:
        for tbl in target_tables:
            src = MONOLITHIC / "test-specs" / f"{tbl}.json"
            if src.exists():
                ts_dir = out_dir / "test-specs"
                ts_dir.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, ts_dir / f"{tbl}.json")

    # 7. Create minimal dbt project if needed
    if package_name in NEEDS_DBT or package_name in CMD_PACKAGES:
        dbt_dir = out_dir / "dbt"
        dbt_dir.mkdir(parents=True, exist_ok=True)

        # dbt_project.yml
        src = MONOLITHIC / "dbt" / "dbt_project.yml"
        if src.exists():
            shutil.copy2(src, dbt_dir / "dbt_project.yml")

        # profiles.yml
        src = MONOLITHIC / "dbt" / "profiles.yml"
        if src.exists():
            shutil.copy2(src, dbt_dir / "profiles.yml")

        # models/staging/ dir
        stg_dir = dbt_dir / "models" / "staging"
        stg_dir.mkdir(parents=True, exist_ok=True)

        # Build minimal _sources.yml from proc's source tables
        source_tables_for_yml = set()
        for proc_fqn in visited_procs:
            pc = load_json(MONOLITHIC / "catalog" / "procedures" / f"{proc_fqn}.json")
            if pc:
                for tbl_fqn in get_proc_source_tables(pc):
                    source_tables_for_yml.add(tbl_fqn)

        # Also add target tables (they might be sources for other queries)
        source_tables_for_yml.update(target_tables)

        _write_minimal_sources_yml(stg_dir / "_sources.yml", source_tables_for_yml)

        # Copy pre-seeded staging models for target tables
        for tbl in target_tables:
            sql_file, yml_file = find_stg_model_for_table(tbl)
            if sql_file:
                shutil.copy2(sql_file, stg_dir / sql_file.name)
            if yml_file:
                shutil.copy2(yml_file, stg_dir / yml_file.name)

        # Also copy staging models for source tables (they might be ref'd)
        for src_tbl in source_tables_for_yml:
            sql_file, yml_file = find_stg_model_for_table(src_tbl)
            if sql_file and not (stg_dir / sql_file.name).exists():
                shutil.copy2(sql_file, stg_dir / sql_file.name)
            if yml_file and not (stg_dir / yml_file.name).exists():
                shutil.copy2(yml_file, stg_dir / yml_file.name)

        # .gitkeep to preserve empty dirs
        (stg_dir / ".gitkeep").touch()

    # 8. Create empty output dirs that skills expect
    for d in ["test-review-results", "model-review-results", ".migration-runs"]:
        (out_dir / d).mkdir(parents=True, exist_ok=True)
        (out_dir / d / ".gitkeep").touch()

    return out_dir


def _write_minimal_sources_yml(
    out_path: Path, source_tables: set[str]
) -> None:
    """Write a minimal _sources.yml with only the needed source tables."""
    # Load the full sources file to get column definitions
    full_sources = MONOLITHIC / "dbt" / "models" / "staging" / "_sources.yml"
    if not full_sources.exists():
        return

    full_data = yaml.safe_load(full_sources.read_text(encoding="utf-8"))
    if not full_data or "sources" not in full_data:
        return

    # Group needed tables by schema
    needed_by_schema: dict[str, set[str]] = {}
    for fqn in source_tables:
        parts = fqn.split(".")
        if len(parts) == 2:
            schema, name = parts[0], parts[1]
            needed_by_schema.setdefault(schema, set()).add(name)

    # Build filtered sources
    new_sources = []
    for source in full_data["sources"]:
        source_name = source.get("name", "")
        schema = source.get("schema", source_name)
        needed_tables = needed_by_schema.get(schema, set())
        if not needed_tables:
            continue

        new_tables = []
        for tbl in source.get("tables", []):
            if tbl["name"].lower() in needed_tables:
                new_tables.append(tbl)

        if new_tables:
            new_source = {
                "name": source_name,
                "schema": schema,
                "tables": new_tables,
            }
            new_sources.append(new_source)

    if new_sources:
        out_data = {"version": 2, "sources": new_sources}
        out_path.write_text(
            yaml.dump(out_data, default_flow_style=False, sort_keys=False),
            encoding="utf-8",
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    # Pre-split DDL files
    proc_chunks = split_ddl_by_go(MONOLITHIC / "ddl" / "procedures.sql")
    view_chunks = split_ddl_by_go(MONOLITHIC / "ddl" / "views.sql")
    print(f"Split procedures.sql into {len(proc_chunks)} chunks")
    print(f"Split views.sql into {len(view_chunks)} chunks")

    # Find all package YAMLs
    yaml_files = sorted(PACKAGES_DIR.glob("*/*.yaml"))
    print(f"Found {len(yaml_files)} package YAMLs")

    total_created = 0
    total_skipped = 0

    for yaml_path in yaml_files:
        package_name = package_name_from_path(yaml_path)
        scenarios = parse_package_yaml(yaml_path)
        print(f"\n--- {package_name} ({len(scenarios)} scenarios) ---")

        for scenario in scenarios:
            desc = scenario["description"]
            v = scenario["vars"]
            fixture_path = v.get("fixture_path", "")

            # Skip scenarios that already use non-migration-test fixtures
            if fixture_path and "migration-test" not in fixture_path:
                print(f"  SKIP (custom fixture): {desc[:60]}")
                total_skipped += 1
                continue

            slug = slug_from_description(desc)
            out_dir = create_scenario_fixture(
                package_name, slug, v, proc_chunks, view_chunks
            )

            # Count files created
            file_count = sum(1 for _ in out_dir.rglob("*") if _.is_file())
            print(f"  OK ({file_count} files): {slug}")
            total_created += 1

    print(f"\n{'=' * 60}")
    print(f"Created: {total_created} fixture dirs")
    print(f"Skipped: {total_skipped} (custom fixtures)")
    print(f"Output:  {FIXTURES_OUT.resolve()}")


if __name__ == "__main__":
    main()
