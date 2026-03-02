#!/usr/bin/env bash

set -euo pipefail

CONTAINER_NAME="${CONTAINER_NAME:-aw-sql}"
SA_PASSWORD="${SA_PASSWORD:-${MSSQL_SA_PASSWORD:-}}"
SQL_SERVER_HOST="${SQL_SERVER_HOST:-localhost}"
SQL_SERVER_PORT="${SQL_SERVER_PORT:-1433}"
BACKUP_DIR="${BACKUP_DIR:-/tmp/sql-backups}"
KEEP_BACKUPS="${KEEP_BACKUPS:-0}"

CONTAINER_BACKUP_DIR="/var/opt/mssql/backup"
SQLCMD_ARGS=(-S "${SQL_SERVER_HOST},${SQL_SERVER_PORT}" -U sa -P "$SA_PASSWORD" -C -W -h -1 -s "|")

AW_DB_NAME="AdventureWorks2022"
AW_BAK_NAME="AdventureWorks2022.bak"
AW_BAK_URL="https://github.com/microsoft/sql-server-samples/releases/download/adventureworks/AdventureWorks2022.bak"

ADW_DB_NAME="AdventureWorksDW2022"
ADW_BAK_NAME="AdventureWorksDW2022.bak"
ADW_BAK_URL="https://github.com/microsoft/sql-server-samples/releases/download/adventureworks/AdventureWorksDW2022.bak"

WWI_DB_NAME="WideWorldImportersDW"
WWI_BAK_NAME="WideWorldImportersDW-Full.bak"
WWI_BAK_URL="https://github.com/microsoft/sql-server-samples/releases/download/wide-world-importers-v1.0/WideWorldImportersDW-Full.bak"

WWI_OLTP_DB_NAME="WideWorldImporters"
WWI_OLTP_BAK_NAME="WideWorldImporters-Full.bak"
WWI_OLTP_BAK_URL="https://github.com/microsoft/sql-server-samples/releases/download/wide-world-importers-v1.0/WideWorldImporters-Full.bak"

DOWNLOADED_FILES=()

if [[ -z "$SA_PASSWORD" ]]; then
  echo "ERROR: set SA_PASSWORD (or MSSQL_SA_PASSWORD) before running."
  echo "Example: SA_PASSWORD='YourStrong!Passw0rd' ./scripts/restore-dw-samples.sh"
  exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: docker is required."
  exit 1
fi

if ! docker inspect "$CONTAINER_NAME" >/dev/null 2>&1; then
  echo "ERROR: container '$CONTAINER_NAME' does not exist."
  exit 1
fi

if [[ "$(docker inspect -f '{{.State.Running}}' "$CONTAINER_NAME")" != "true" ]]; then
  echo "ERROR: container '$CONTAINER_NAME' is not running."
  echo "Run: docker start $CONTAINER_NAME"
  exit 1
fi

if docker exec "$CONTAINER_NAME" test -x /opt/mssql-tools18/bin/sqlcmd; then
  SQLCMD_BIN="/opt/mssql-tools18/bin/sqlcmd"
elif docker exec "$CONTAINER_NAME" test -x /opt/mssql-tools/bin/sqlcmd; then
  SQLCMD_BIN="/opt/mssql-tools/bin/sqlcmd"
else
  echo "ERROR: sqlcmd not found in container '$CONTAINER_NAME'."
  exit 1
fi

cleanup_local_backups() {
  if [[ "$KEEP_BACKUPS" == "1" ]]; then
    return
  fi
  for file in "${DOWNLOADED_FILES[@]+"${DOWNLOADED_FILES[@]}"}"; do
    rm -f "$file"
  done
}

trap cleanup_local_backups EXIT

run_sql() {
  local query="$1"
  docker exec "$CONTAINER_NAME" "$SQLCMD_BIN" "${SQLCMD_ARGS[@]}" -Q "$query"
}

db_exists() {
  local db_name="$1"
  local result
  result="$(run_sql "SET NOCOUNT ON; SELECT COUNT(1) FROM sys.databases WHERE name = N'$db_name';" | tr -d '\r' | xargs)"
  [[ "$result" == "1" ]]
}

ensure_backup_present() {
  local file_name="$1"
  local file_url="$2"
  local local_path="$BACKUP_DIR/$file_name"

  mkdir -p "$BACKUP_DIR"
  echo "Downloading $file_name to $local_path ..."
  curl -fL "$file_url" -o "$local_path"
  DOWNLOADED_FILES+=("$local_path")

  docker exec "$CONTAINER_NAME" mkdir -p "$CONTAINER_BACKUP_DIR"
  docker cp "$local_path" "$CONTAINER_NAME:$CONTAINER_BACKUP_DIR/$file_name"
}

restore_database_from_backup() {
  local db_name="$1"
  local bak_name="$2"
  local filelist_output
  local move_clauses=()
  local move_sql=""
  local row_count=0

  echo "Reading logical file names for $db_name ..."
  filelist_output="$(run_sql "RESTORE FILELISTONLY FROM DISK = N'$CONTAINER_BACKUP_DIR/$bak_name';" | tr -d '\r')"

  while IFS='|' read -r logical_name physical_name _rest; do
    logical_name="$(echo "$logical_name" | sed 's/^[[:space:]]*//; s/[[:space:]]*$//')"
    physical_name="$(echo "$physical_name" | sed 's/^[[:space:]]*//; s/[[:space:]]*$//')"

    if [[ -z "$logical_name" || -z "$physical_name" ]]; then
      continue
    fi

    local target_file
    target_file="$(basename "${physical_name//\\//}")"
    if [[ -z "$target_file" || "$target_file" == "." ]]; then
      row_count=$((row_count + 1))
      target_file="${db_name}_${row_count}.dat"
    fi

    move_clauses+=("MOVE N'$logical_name' TO N'/var/opt/mssql/data/$target_file'")
  done <<< "$filelist_output"

  if [[ ${#move_clauses[@]} -eq 0 ]]; then
    echo "ERROR: could not parse logical file names from backup $bak_name"
    exit 1
  fi

  for clause in "${move_clauses[@]}"; do
    if [[ -n "$move_sql" ]]; then
      move_sql+=", "
    fi
    move_sql+="$clause"
  done

  echo "Restoring $db_name ..."
  run_sql "RESTORE DATABASE [$db_name]
           FROM DISK = N'$CONTAINER_BACKUP_DIR/$bak_name'
           WITH $move_sql, STATS=10;"
}

restore_if_missing() {
  local db_name="$1"
  local bak_name="$2"
  local bak_url="$3"

  if db_exists "$db_name"; then
    echo "Skipping $db_name (already exists)."
    return
  fi

  ensure_backup_present "$bak_name" "$bak_url"
  restore_database_from_backup "$db_name" "$bak_name"
}

echo "Checking sample databases in container '$CONTAINER_NAME' ..."
restore_if_missing "$AW_DB_NAME" "$AW_BAK_NAME" "$AW_BAK_URL"
restore_if_missing "$ADW_DB_NAME" "$ADW_BAK_NAME" "$ADW_BAK_URL"
restore_if_missing "$WWI_DB_NAME" "$WWI_BAK_NAME" "$WWI_BAK_URL"
restore_if_missing "$WWI_OLTP_DB_NAME" "$WWI_OLTP_BAK_NAME" "$WWI_OLTP_BAK_URL"

echo "Verifying restored databases ..."
run_sql "SET NOCOUNT ON; SELECT name FROM sys.databases WHERE name IN (N'$AW_DB_NAME', N'$ADW_DB_NAME', N'$WWI_DB_NAME', N'$WWI_OLTP_DB_NAME') ORDER BY name;"

if [[ "$KEEP_BACKUPS" == "1" ]]; then
  echo "Local backup files kept in $BACKUP_DIR"
else
  echo "Local backup files cleaned from $BACKUP_DIR"
fi

echo "Done."
