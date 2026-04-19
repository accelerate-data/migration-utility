"""Technology dispatch for setup-ddl source extraction."""

from __future__ import annotations

from pathlib import Path


def run_db_extraction(technology: str, staging_dir: Path, db_name: str, schemas: list[str]) -> None:
    if technology == "sql_server":
        from shared.sqlserver_extract import run_sqlserver_extraction

        run_sqlserver_extraction(staging_dir, db_name, schemas)
    elif technology == "oracle":
        from shared.oracle_extract import run_oracle_extraction

        run_oracle_extraction(staging_dir, schemas)
    else:
        raise ValueError(f"setup-ddl extract is not supported for technology '{technology}'")
