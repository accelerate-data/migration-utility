# Init ad-migration Prereqs

`/init-ad-migration` separates local tool startup readiness from live database connection readiness so users can scaffold the project before source or target connections are configured.

## Decisions

- Treat MCP readiness as two layers:
  - tool startup readiness for local binaries and launch checks
  - live connection readiness for credential-backed source access
- Record pending live-connection prerequisites explicitly as `false` in `manifest.json` `init_handoff`; do not omit them.
- Keep repo-wide environment behavior in `.envrc`.
- Write machine-specific, non-secret overrides discovered during init to a local `.env` file in the project root.
- `.envrc` must load `.env` when present so local overrides apply to later command runs.

## Required init behavior

- Missing source credentials do not mark an MCP server as failed. The command reports startup checks separately from connection-pending checks.
- SQL Server init discovers the effective ODBC driver for the local machine. If a suitable driver is found, write `MSSQL_DRIVER` to `.env`; otherwise tell the user exactly what to add.
- Oracle init discovers the SQLcl binary path for the local machine. If found, write `SQLCL_BIN` to `.env`; otherwise tell the user exactly what to add.
- Discovery writes only machine-specific overrides to `.env`; it does not commit laptop-specific paths or driver choices to shared repo files.
- The command output must distinguish:
  - startup check passed
  - startup check failed
  - live connection not configured yet

## Why this matters

Agents need a stable way to tell whether `/init-ad-migration` has prepared the local toolchain, whether live extraction is still blocked only on credentials, and where local machine overrides are expected to live.
