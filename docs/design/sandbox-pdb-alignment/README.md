# Sandbox PDB Alignment

Align the Oracle sandbox to use a Pluggable Database (PDB) instead of a user/schema, making it symmetric with the SQL Server sandbox which uses a database.

## Decision

The sandbox container is a **database-level** concept on both technologies:

| Operation | SQL Server | Oracle |
|---|---|---|
| Create sandbox | `CREATE DATABASE __test_xxx` | `CREATE PLUGGABLE DATABASE __test_xxx ...` |
| Create schemas | `CREATE SCHEMA [bronze]` | `CREATE USER bronze IDENTIFIED BY ... CONTAINER = CURRENT` |
| Clone objects | Per-schema from source DB | Per-schema from source PDB |
| Connect | `DATABASE=__test_xxx` in connection string | Service name or `ALTER SESSION SET CONTAINER` |
| Teardown | `DROP DATABASE __test_xxx` | `DROP PLUGGABLE DATABASE __test_xxx INCLUDING DATAFILES` |

The `SandboxBackend` interface (`sandbox_up(schemas)`, `sandbox_down`, etc.) remains unchanged. Callers are unaware of whether the container is a SQL Server database or an Oracle PDB.

## Why

The current Oracle sandbox creates a single user/schema. This breaks when:

- A procedure references multiple schemas (e.g. `SELECT ... FROM BRONZE.raw_sales` writing into `SILVER.fact_sales`)
- Views or procedures contain hardcoded schema-qualified names that don't resolve in the sandbox user's namespace
- The migration pipeline needs bronze, silver/int, and gold/mart schemas to coexist in the sandbox

A PDB gives full namespace isolation with multiple schemas inside, exactly like a SQL Server database.

## Current state (user-based sandbox)

- `_create_sandbox_schema` creates `CREATE USER "__test_xxx"` in the source PDB
- All objects are cloned into that single user's namespace
- Cross-schema references in procedure/view DDL break silently
- `sandbox_down` does `DROP USER "__test_xxx" CASCADE`

## Target state (PDB-based sandbox)

### PDB lifecycle

```sql
-- Create (CDB admin connection required)
CREATE PLUGGABLE DATABASE __test_xxx
  ADMIN USER pdb_admin IDENTIFIED BY <temp>
  FILE_NAME_CONVERT = ('/opt/oracle/oradata/FREE/pdbseed/', '/opt/oracle/oradata/FREE/__test_xxx/');
ALTER PLUGGABLE DATABASE __test_xxx OPEN;

-- Teardown
ALTER PLUGGABLE DATABASE __test_xxx CLOSE IMMEDIATE;
DROP PLUGGABLE DATABASE __test_xxx INCLUDING DATAFILES;
```

### Schema creation inside the PDB

Once connected to the sandbox PDB, create one user per source schema:

```sql
CREATE USER "BRONZE" IDENTIFIED BY <temp>;
GRANT CONNECT, RESOURCE, UNLIMITED TABLESPACE TO "BRONZE";

CREATE USER "SILVER" IDENTIFIED BY <temp>;
GRANT CONNECT, RESOURCE, UNLIMITED TABLESPACE TO "SILVER";
```

Then clone objects per-schema, same as today but targeting the correct user within the PDB.

### Connection routing

Two options for connecting to the sandbox PDB:

1. **Dynamic service registration** -- after `ALTER PLUGGABLE DATABASE ... OPEN`, Oracle auto-registers a service. Connect via `localhost:1521/__test_xxx`.
2. **Container switching** -- connect to CDB root, then `ALTER SESSION SET CONTAINER = __test_xxx`. Requires `SET CONTAINER` privilege.

Option 1 is simpler and matches how `_connect()` already works (DSN-based). The sandbox PDB name becomes the service name.

### Privilege requirements

| Privilege | Current (user sandbox) | PDB sandbox |
|---|---|---|
| Connection target | Source PDB | CDB root (for PDB lifecycle) + sandbox PDB (for DDL/DML) |
| Admin role | SYSDBA on source PDB | SYSDBA on CDB or `CREATE PLUGGABLE DATABASE` privilege |
| Grants needed | `CREATE SESSION`, `CREATE TABLE`, etc. | Same, but issued per-user inside the PDB |

The admin connection in `manifest.json` (`runtime.sandbox`) must point to the CDB root or have `CREATE PLUGGABLE DATABASE` privilege. This is a change from today where the sandbox connection targets the source PDB directly.

### Docker dev setup (Oracle Free)

Oracle Free 23ai container exposes CDB (`FREE`) and a default PDB (`FREEPDB1`). The `sys` user connected to CDB root (`localhost:1521/FREE`) can create PDBs. The `pdbseed` template PDB exists by default.

Connection for PDB lifecycle: `sys/password@localhost:1521/FREE as SYSDBA`
Connection for sandbox DDL: `sys/password@localhost:1521/__test_xxx as SYSDBA` (after PDB is open)

### Manifest changes

```json
{
  "runtime": {
    "source": {
      "technology": "oracle",
      "connection": {
        "host": "localhost",
        "port": "1521",
        "service": "FREEPDB1",
        "schema": "SH",
        "user": "sys",
        "password_env": "ORACLE_PWD"
      }
    },
    "sandbox": {
      "technology": "oracle",
      "connection": {
        "host": "localhost",
        "port": "1521",
        "service": "FREE",
        "user": "sys",
        "password_env": "ORACLE_PWD"
      }
    }
  }
}
```

The sandbox connection's `service` points to the CDB root (`FREE`) instead of the source PDB. The sandbox PDB name is generated at runtime, not configured.

## Implementation sequence

1. Add `_create_sandbox_pdb` / `_drop_sandbox_pdb` to `oracle_services.py` (CDB-level DDL)
2. Change `_connect()` to accept a PDB name and build the DSN dynamically
3. Update `_sandbox_clone_into` to: create PDB, open it, create users per schema, clone objects per-schema
4. Update `sandbox_down` to close and drop the PDB
5. Update `from_env` to read `sandbox.connection.service` as the CDB service
6. Update integration test fixtures (`materialize.sh`) to work with CDB connection
7. Update `.envrc` to add CDB connection variables if needed

## Risks

- **PDB creation speed** -- creating a PDB from `pdbseed` takes 5-15 seconds vs. near-instant for `CREATE USER`. Acceptable for test harness but worth measuring.
- **Datafile cleanup** -- `INCLUDING DATAFILES` in the drop should clean up, but if the process crashes mid-test, orphaned PDB datafiles accumulate. A periodic cleanup job or test teardown fixture is needed.
- **CDB access in CI** -- GitHub Actions Oracle containers may not expose CDB root. Needs validation.
