#!/usr/bin/env bash

set -euo pipefail

CONTAINER_NAME="${ORACLE_CONTAINER:-oracle-test}"
ORACLE_PWD="${ORACLE_PWD:-P@ssw0rd123}"
ORACLE_PORT="${ORACLE_PORT:-1521}"
ORACLE_PDB="FREEPDB1"
IMAGE="container-registry.oracle.com/database/free:latest"
SCHEMA_REPO="https://github.com/oracle-samples/db-sample-schemas.git"
TMPDIR_BASE="${TMPDIR:-/tmp}"
CLONE_DIR="$TMPDIR_BASE/oracle-sample-schemas"

if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: docker is required."
  exit 1
fi

# ── Step 1: Create or start the container ────────────────────────────

if docker inspect "$CONTAINER_NAME" >/dev/null 2>&1; then
  if [[ "$(docker inspect -f '{{.State.Running}}' "$CONTAINER_NAME")" == "true" ]]; then
    echo "Container '$CONTAINER_NAME' is already running."
  else
    echo "Starting existing container '$CONTAINER_NAME' ..."
    docker start "$CONTAINER_NAME"
  fi
else
  echo "Pulling Oracle Free image (first time may take a few minutes) ..."
  docker pull "$IMAGE"

  echo "Creating container '$CONTAINER_NAME' on port $ORACLE_PORT ..."
  docker run --name "$CONTAINER_NAME" \
    -e ORACLE_PWD="$ORACLE_PWD" \
    -p "$ORACLE_PORT":1521 \
    -v oracle-test-data:/opt/oracle/oradata \
    -d "$IMAGE"

  docker update --restart unless-stopped "$CONTAINER_NAME"
fi

# ── Step 2: Wait for database ready ─────────────────────────────────

echo "Waiting for Oracle database to be ready (first start takes 5-10 min) ..."
TIMEOUT=600
ELAPSED=0
until docker logs "$CONTAINER_NAME" 2>&1 | grep -q "DATABASE IS READY TO USE"; do
  if (( ELAPSED >= TIMEOUT )); then
    echo "ERROR: timed out after ${TIMEOUT}s waiting for Oracle to start."
    echo "Check: docker logs $CONTAINER_NAME"
    exit 1
  fi
  sleep 5
  ELAPSED=$((ELAPSED + 5))
done
echo "Oracle database is ready (${ELAPSED}s)."

# ── Step 3: Clone sample schemas ────────────────────────────────────

if [[ -d "$CLONE_DIR" ]]; then
  echo "Reusing existing clone at $CLONE_DIR"
else
  echo "Cloning Oracle sample schemas ..."
  git clone --depth 1 "$SCHEMA_REPO" "$CLONE_DIR"
fi

# ── Helper: create user ──────────────────────────────────────────────

create_user() {
  local user="$1"
  local password="$2"
  shift 2
  local grants=("$@")

  echo "Creating $user user ..."
  docker exec -i "$CONTAINER_NAME" sqlplus -s /nolog <<SQL
CONNECT sys/"$ORACLE_PWD"@$ORACLE_PDB as sysdba
SET FEEDBACK OFF
SET HEADING OFF

BEGIN
  EXECUTE IMMEDIATE 'DROP USER $user CASCADE';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -1918 THEN RAISE; END IF;
END;
/

CREATE USER $user IDENTIFIED BY $password
  DEFAULT TABLESPACE users
  TEMPORARY TABLESPACE temp
  QUOTA UNLIMITED ON users;

$(printf "GRANT %s TO $user;\n" "${grants[@]}")

EXIT;
SQL
}

# ── Step 4: Load SH schema ──────────────────────────────────────────

create_user sh sh \
  "CONNECT, RESOURCE" \
  "CREATE SESSION, CREATE TABLE, CREATE SEQUENCE" \
  "CREATE VIEW, CREATE PROCEDURE, CREATE TRIGGER" \
  "CREATE MATERIALIZED VIEW, CREATE DIMENSION" \
  "CREATE TYPE, CREATE SYNONYM" \
  "ALTER SESSION" \
  "EXECUTE ON SYS.DBMS_STATS"

echo "Copying SH schema files into container ..."
docker cp "$CLONE_DIR/sales_history" "$CONTAINER_NAME":/opt/oracle/

SQLCL_BIN="/opt/oracle/product/26ai/dbhomeFree/sqlcl/bin/sql"

echo "Installing SH schema (create tables) ..."
docker exec -i "$CONTAINER_NAME" sqlplus -s /nolog <<OUTER
CONNECT sh/sh@$ORACLE_PDB
SET DEFINE OFF

@/opt/oracle/sales_history/sh_create.sql

EXIT;
OUTER

echo "Installing SH schema (populate data via SQLcl) ..."
docker exec -w /opt/oracle/sales_history -i "$CONTAINER_NAME" \
  "$SQLCL_BIN" -s sh/sh@"$ORACLE_PDB" <<'OUTER'
SET DEFINE OFF

@/opt/oracle/sales_history/sh_populate.sql

EXIT;
OUTER

echo "Fixing CUSTOMERS and SUPPLEMENTARY_DEMOGRAPHICS (CSV quoting issue) ..."
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
uv run --with oracledb python "$SCRIPT_DIR/load-oracle-csv.py" \
  "localhost:$ORACLE_PORT/$ORACLE_PDB" \
  "$CLONE_DIR/sales_history"

# ── Step 5: Verify ──────────────────────────────────────────────────

echo ""
echo "=== Verification: SH ==="
docker exec -i "$CONTAINER_NAME" sqlplus -s /nolog <<SQL
CONNECT sh/sh@$ORACLE_PDB
SET PAGESIZE 100
SET LINESIZE 80

PROMPT --- SH Tables ---
SELECT table_name FROM user_tables ORDER BY table_name;

PROMPT --- SH Row Counts ---
SELECT 'SALES' AS tbl, COUNT(*) AS cnt FROM sales
UNION ALL
SELECT 'COSTS', COUNT(*) FROM costs
UNION ALL
SELECT 'CUSTOMERS', COUNT(*) FROM customers
UNION ALL
SELECT 'PRODUCTS', COUNT(*) FROM products
UNION ALL
SELECT 'CHANNELS', COUNT(*) FROM channels
UNION ALL
SELECT 'PROMOTIONS', COUNT(*) FROM promotions
UNION ALL
SELECT 'TIMES', COUNT(*) FROM times
UNION ALL
SELECT 'COUNTRIES', COUNT(*) FROM countries
UNION ALL
SELECT 'SUPPLEMENTARY_DEMOGRAPHICS', COUNT(*) FROM supplementary_demographics;

EXIT;
SQL

echo ""
echo "=== Oracle setup complete ==="
echo "SH  → sqlplus sh/sh@localhost:$ORACLE_PORT/$ORACLE_PDB"
echo ""
echo "Python:"
echo "  oracledb.connect(user='sh', password='sh', dsn='localhost:$ORACLE_PORT/$ORACLE_PDB')"
