# Sandbox PDB Alignment — Implementation Plan

## Stream 1: Oracle PDB lifecycle (no callers change)

### Step 1.1: Add PDB create/drop to `oracle_services.py`

Add two methods to `_OracleSandboxCore`:

- `_create_sandbox_pdb(self, sandbox_name: str) -> None` — connects to CDB root, creates PDB from `pdbseed`, opens it
- `_drop_sandbox_pdb(self, sandbox_name: str) -> None` — closes and drops PDB including datafiles

```sql
-- _create_sandbox_pdb
CREATE PLUGGABLE DATABASE "<sandbox_name>"
  ADMIN USER pdb_admin IDENTIFIED BY "<temp>"
  FILE_NAME_CONVERT = ('<pdbseed_path>/', '<datafiles_path>/<sandbox_name>/');
ALTER PLUGGABLE DATABASE "<sandbox_name>" OPEN;

-- _drop_sandbox_pdb
ALTER PLUGGABLE DATABASE "<sandbox_name>" CLOSE IMMEDIATE;
DROP PLUGGABLE DATABASE "<sandbox_name>" INCLUDING DATAFILES;
```

The CDB connection uses the existing `self.host`, `self.port`, `self.password`, `self.admin_user` but targets the CDB service (e.g. `FREE`) instead of a PDB service. Store `self.cdb_service` separately from `self.source_service`.

### Step 1.2: Add `_connect_sandbox` context manager

New method: `_connect_sandbox(self, sandbox_name: str) -> Generator[Connection]`

After `_create_sandbox_pdb` opens the PDB, Oracle auto-registers a service with the PDB name. Connect via `{host}:{port}/{sandbox_name}` as SYSDBA.

This replaces the current `_connect()` for sandbox operations. The existing `_connect()` becomes `_connect_cdb()` (CDB root, for PDB lifecycle only). `_connect_source()` stays unchanged.

### Step 1.3: Update `from_env` manifest parsing

Current `runtime.sandbox.connection.service` points to the source PDB. Change it to point to the CDB root service. Add validation that the sandbox service is a CDB (or document the requirement).

```python
# Current
self.service = sandbox_role.connection.service  # "FREEPDB1"

# New
self.cdb_service = sandbox_role.connection.service  # "FREE" (CDB root)
```

Source connection stays the same — `runtime.source.connection.service` still points to the source PDB.

### Step 1.4: Update `_validate_oracle_sandbox_name`

PDB names have a 30-char limit in Oracle (128 in 23ai long identifiers, but PDB names use the short limit). The current `__test_<12hex>` = 19 chars fits. No regex change needed, but add a comment documenting the PDB name limit.

**Tests:** Unit tests for `_create_sandbox_pdb`, `_drop_sandbox_pdb`, `_connect_sandbox`. Mock `oracledb.connect` and verify DDL statements.

---

## Stream 2: Multi-schema cloning inside PDB

### Step 2.1: Change `_create_sandbox_schema` to create users inside PDB

Currently creates a user in the source PDB. Change to create users inside the sandbox PDB via `_connect_sandbox`. The method signature stays the same but the connection target changes.

For multi-schema: call `_create_sandbox_schema` once per schema in the `schemas` list, creating a user per schema inside the sandbox PDB.

```python
# Current (single schema)
_create_sandbox_schema(cursor, sandbox_schema="__test_xxx")

# New (per source schema, inside sandbox PDB)
with self._connect_sandbox(sandbox_name) as conn:
    cursor = conn.cursor()
    for schema in schemas:
        _create_user_in_pdb(cursor, schema)  # CREATE USER "BRONZE" ...
```

### Step 2.2: Update `_clone_tables`, `_clone_views`, `_clone_procedures`

Current signatures take `(source_cursor, sandbox_cursor, sandbox_schema, source_schema)` where `sandbox_schema` is the `__test_xxx` user name. Change `sandbox_schema` to the actual schema name being cloned (e.g. `BRONZE`), since objects now live under their original schema name inside the sandbox PDB.

The clone methods iterate objects `WHERE OWNER = source_schema` and create them under the matching sandbox schema. The source→sandbox schema mapping is identity (same names).

### Step 2.3: Update `_sandbox_clone_into` in `oracle_lifecycle.py`

Current flow:

```text
create sandbox schema → clone tables/views/procs (single schema)
```

New flow:

```text
create sandbox PDB
  → for each schema in schemas:
      create user in PDB
      clone tables/views/procs from source schema to PDB user
```

The `sandbox_db` parameter (currently the sandbox schema name) becomes the sandbox PDB name. Inside the PDB, schemas use their original names.

### Step 2.4: Update `sandbox_down`

Replace `DROP USER ... CASCADE` with `_drop_sandbox_pdb`. One operation cleans up everything — all users and objects inside the PDB are destroyed.

**Tests:** Integration tests that create a sandbox PDB with multiple schemas, verify objects exist in each schema, then tear down.

---

## Stream 3: Update fixture seeding and execution for PDB

### Step 3.1: Update `seed_fixtures` connection

Currently connects as the sandbox user. Now needs to connect to the sandbox PDB and insert into the correct schema. The INSERT statements already use `"{sandbox_schema}"."{table}"` syntax — just need the connection to target the sandbox PDB instead of the source PDB.

### Step 3.2: Update `ensure_view_tables`

Same change — connect to sandbox PDB. The source cursor still connects to the source PDB to check `ALL_VIEWS`.

### Step 3.3: Update `execute_scenario` and `execute_select`

The connection changes from source-PDB-as-sandbox-user to sandbox-PDB-as-admin. The `BEGIN "{schema}"."{procedure}"; END;` call uses the real schema name (e.g. `SILVER`) instead of `__test_xxx`.

### Step 3.4: Update `compare_two_sql`

Same connection change. The SQL strings should now reference real schema names (`BRONZE.table`, `SILVER.table`) which resolve inside the sandbox PDB.

**Tests:** Update all execution/comparison integration tests to use PDB-based sandbox. Verify fixture seeding works across schemas.

---

## Stream 4: Align SQL Server sandbox for multi-schema parity

### Step 4.1: Update `sandbox_up` to accept schema mapping

Currently `sandbox_up(schemas=["MigrationTest"])` clones everything from `MigrationTest`. For multi-schema support, the caller passes multiple schemas and the sandbox creates all of them:

```python
sandbox_up(schemas=["bronze", "silver"])
```

This already works today — `_create_schemas` and the clone methods accept a list. No code change needed, just document and test the multi-schema path.

### Step 4.2: Add integration tests for multi-schema SQL Server sandbox

Test that `sandbox_up(schemas=["bronze", "silver"])` creates both schemas, clones objects from each, and procedures that reference both schemas work correctly.

---

## Stream 5: Environment and CI

### Step 5.1: Update `.envrc` and `.env.example`

Add CDB connection variables:

```bash
export ORACLE_CDB_SERVICE=FREE          # CDB root service (for PDB lifecycle)
# ORACLE_SERVICE stays as FREEPDB1      # Source PDB service
```

### Step 5.2: Update `manifest.json` schema

Document that `runtime.sandbox.connection.service` should point to CDB root for Oracle.

### Step 5.3: Validate Docker Oracle Free setup

Verify that Oracle Free 23ai container allows PDB creation:

```bash
# Connect as SYSDBA to CDB root
python -c "
import oracledb
conn = oracledb.connect(user='sys', password='...', dsn='localhost:1521/FREE', mode=oracledb.AUTH_MODE_SYSDBA)
cursor = conn.cursor()
cursor.execute('SELECT NAME, OPEN_MODE FROM V\$PDBS')
print(cursor.fetchall())
"
```

### Step 5.4: Update integration test fixtures

Update `tests/integration/oracle/fixtures/materialize.sh` to connect to source PDB (not CDB) for fixture setup. Update `runtime_helpers.py` Oracle helpers to distinguish CDB vs PDB connections.

---

## Execution order

```text
Stream 1 (PDB lifecycle)     ████████░░░░░░░░░░░░  — foundation, no callers change
Stream 2 (multi-schema clone) ░░░░░░░░████████░░░░  — depends on Stream 1
Stream 3 (fixtures/execution)  ░░░░░░░░░░░░████████  — depends on Stream 2
Stream 4 (SQL Server parity)   ░░████░░░░░░░░░░░░░░  — independent, parallel with 1-2
Stream 5 (env/CI)              ░░░░████░░░░░░████░░  — early setup + late validation
```

Streams 1-3 are sequential (each depends on the prior). Stream 4 is independent. Stream 5 has early work (env vars) and late work (CI validation).

## Estimated scope

| Stream | Files modified | New files | Tests |
|---|---|---|---|
| 1 — PDB lifecycle | `oracle_services.py` | — | Unit: mock PDB DDL |
| 2 — Multi-schema clone | `oracle_services.py`, `oracle_lifecycle.py` | — | Unit: mock multi-schema clone |
| 3 — Fixtures/execution | `oracle_fixtures.py`, `oracle_execution.py`, `oracle_comparison.py` | — | Integration: PDB-based sandbox |
| 4 — SQL Server parity | — | — | Integration: multi-schema sandbox |
| 5 — Environment | `.envrc`, `.env.example`, `runtime_helpers.py`, `materialize.sh` | — | Smoke test |
