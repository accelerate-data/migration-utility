# Init ad-migration Prereqs

`/init-ad-migration` establishes environment/tool readiness for the chosen source and target technologies without collecting runtime connection details.

## Decisions

- Ask for source and target technologies during init. Persist sandbox as a separate runtime role initialized from source, but do not ask a separate sandbox question.
- Record common startup readiness and role-scoped startup readiness in `manifest.json` `init_handoff`.
- Keep repo-wide environment behavior in `.envrc`.
- Write machine-specific, non-secret overrides discovered during init to a local `.env` file in the project root.
- `.envrc` must load `.env` when present so local overrides apply to later command runs.

## Required init behavior

- Init does not ask for source, target, or sandbox connection details. Those are collected later by `/setup-ddl`, `/setup-target`, and `/setup-sandbox`.
- SQL Server init discovers the effective ODBC driver for the local machine. If a suitable driver is found, write `MSSQL_DRIVER` to `.env`; otherwise tell the user exactly what to add.
- Discovery writes only machine-specific overrides to `.env`; it does not commit laptop-specific paths or driver choices to shared repo files.
- The command output must distinguish common startup readiness, source runtime readiness, and target runtime readiness.

## Why this matters

Agents need a stable way to tell whether `/init-ad-migration` has prepared the local toolchain for each runtime role family and where local machine overrides are expected to live before later setup stages collect connection details.
