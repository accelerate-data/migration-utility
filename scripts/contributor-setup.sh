#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./scripts/contributor-setup.sh        # fix mode
  ./scripts/contributor-setup.sh show   # non-mutating status
EOF
}

mode="fix"
if [[ $# -gt 1 ]]; then
  usage >&2
  exit 1
fi
if [[ $# -eq 1 ]]; then
  if [[ "$1" != "show" ]]; then
    usage >&2
    exit 1
  fi
  mode="show"
fi

script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"

json_string() {
  python3 -c 'import json, sys; print(json.dumps(sys.argv[1]))' "$1"
}

json_check() {
  local status="$1"
  local message="$2"
  local next_action="${3:-}"
  CHECK_STATUS="$status" CHECK_MESSAGE="$message" CHECK_NEXT_ACTION="$next_action" python3 - <<'PY'
import json
import os

payload = {
    "status": os.environ["CHECK_STATUS"],
    "message": os.environ["CHECK_MESSAGE"],
}
next_action = os.environ.get("CHECK_NEXT_ACTION", "")
if next_action:
    payload["next_action"] = next_action
print(json.dumps(payload))
PY
}

print_step() {
  local name="$1"
  local payload_json="$2"
  STEP_NAME="$name" STEP_PAYLOAD="$payload_json" python3 - <<'PY'
import json
import os

payload = json.loads(os.environ["STEP_PAYLOAD"])
line = f"{os.environ['STEP_NAME']}: {payload['status']} - {payload['message']}"
print(line)
PY
}

array_json_from_lines() {
  local lines="${1:-}"
  LINES="$lines" python3 - <<'PY'
import json
import os

items = [line for line in os.environ.get("LINES", "").splitlines() if line]
print(json.dumps(items))
PY
}

append_line() {
  local current="${1:-}"
  local line="$2"
  if [[ -z "$current" ]]; then
    printf '%s' "$line"
  else
    printf '%s\n%s' "$current" "$line"
  fi
}

run_in_dir() {
  local dir="$1"
  shift
  (
    cd "$dir"
    "$@"
  )
}

check_command() {
  local command_name="$1"
  shift
  if ! command -v "$command_name" >/dev/null 2>&1; then
    return 1
  fi
  "$command_name" "$@" >/dev/null 2>&1
}

platform_check=""
required_tools_check=""
optional_tools_check=""
repo_bootstrap_check=""
docker_check=""
sql_server_check=""
oracle_check=""

overall_status="ready"
manual_actions=""
working_backends=""

base_required_tools=(git python3 uv node npm direnv docker markdownlint)
optional_tools=(gh)

platform_name="$(uname)"
case "$platform_name" in
  Darwin|Linux)
    platform_check="$(json_check "ok" "Supported platform detected: $platform_name.")"
    ;;
  *)
    platform_check="$(json_check "blocked" "Windows and unsupported Unix variants are not supported by this contributor setup flow." "Use macOS or Linux/Unix-like for maintainer bootstrap.")"
    overall_status="blocked"
    ;;
esac
print_step "platform" "$platform_check"

missing_required_tools=""
for tool in "${base_required_tools[@]}"; do
  if ! check_command "$tool" "--version"; then
    missing_required_tools="$(append_line "$missing_required_tools" "$tool")"
  fi
done
if [[ -z "$missing_required_tools" ]]; then
  required_tools_check="$(json_check "ok" "Required machine-level tools are installed.")"
else
  required_tools_check="$(json_check "blocked" "Missing required machine-level tools: $(echo "$missing_required_tools" | tr '\n' ',' | sed 's/,/, /g; s/, $//')." "Install the missing required tools, then rerun ./scripts/contributor-setup.sh.")"
  overall_status="blocked"
fi
print_step "required_tools" "$required_tools_check"

missing_optional_tools=""
for tool in "${optional_tools[@]}"; do
  if ! check_command "$tool" "--version"; then
    missing_optional_tools="$(append_line "$missing_optional_tools" "$tool")"
  fi
done
if [[ -z "$missing_optional_tools" ]]; then
  optional_tools_check="$(json_check "ok" "Optional machine-level tools are installed.")"
else
  optional_tools_check="$(json_check "manual_action" "Missing optional tools: $(echo "$missing_optional_tools" | tr '\n' ',' | sed 's/,/, /g; s/, $//')." "Install optional tools only if you need their related workflows.")"
fi
print_step "optional_tools" "$optional_tools_check"

if [[ "$overall_status" == "blocked" ]]; then
  repo_bootstrap_check="$(json_check "skipped" "Repo bootstrap skipped because prerequisite checks are blocked.")"
  docker_check="$(json_check "skipped" "Docker checks skipped because prerequisite checks are blocked.")"
  sql_server_check="$(json_check "skipped" "SQL Server checks skipped because prerequisite checks are blocked.")"
  oracle_check="$(json_check "skipped" "Oracle checks skipped because prerequisite checks are blocked.")"
else
  if [[ "$mode" == "fix" ]]; then
    bootstrap_failures=""
    if ! run_in_dir "$repo_root/lib" uv sync --extra dev; then
      bootstrap_failures="$(append_line "$bootstrap_failures" "lib environment sync failed")"
    fi
    if ! run_in_dir "$repo_root/mcp/ddl" uv sync; then
      bootstrap_failures="$(append_line "$bootstrap_failures" "mcp/ddl environment sync failed")"
    fi
    if ! run_in_dir "$repo_root/tests/evals" npm install --no-audit --no-fund; then
      bootstrap_failures="$(append_line "$bootstrap_failures" "eval dependencies install failed")"
    fi

    if [[ -z "$bootstrap_failures" ]]; then
      repo_bootstrap_check="$(json_check "fixed" "Repo-local contributor bootstrap completed.")"
    else
      repo_bootstrap_check="$(json_check "manual_action" "Repo-local bootstrap did not complete cleanly: $(echo "$bootstrap_failures" | tr '\n' '; ' | sed 's/; $//')." "Repair the listed repo-local bootstrap failures, then rerun ./scripts/contributor-setup.sh.")"
      overall_status="partially_ready"
      manual_actions="$(append_line "$manual_actions" "Repair repo-local bootstrap failures and rerun ./scripts/contributor-setup.sh.")"
    fi
  else
    repo_bootstrap_check="$(json_check "ok" "Show mode does not mutate repo-local environments.")"
  fi
  print_step "repo_bootstrap" "$repo_bootstrap_check"

  if ! check_command docker "--version"; then
    docker_check="$(json_check "blocked" "Docker is not installed or not runnable." "Install Docker and ensure the binary is on PATH, then rerun ./scripts/contributor-setup.sh.")"
    overall_status="blocked"
    sql_server_check="$(json_check "skipped" "SQL Server checks skipped because Docker is unavailable.")"
    oracle_check="$(json_check "skipped" "Oracle checks skipped because Docker is unavailable.")"
  elif ! docker info >/dev/null 2>&1; then
    docker_check="$(json_check "blocked" "Docker daemon is not reachable." "Start Docker Desktop or the Docker daemon, then rerun ./scripts/contributor-setup.sh.")"
    overall_status="blocked"
    sql_server_check="$(json_check "skipped" "SQL Server checks skipped because Docker is unavailable.")"
    oracle_check="$(json_check "skipped" "Oracle checks skipped because Docker is unavailable.")"
  else
    docker_check="$(json_check "ok" "Docker is installed and the daemon is reachable.")"

    sql_tool_ok=true
    if ! check_command toolbox "--version"; then
      sql_tool_ok=false
    fi
    if ! $sql_tool_ok; then
      sql_server_check="$(json_check "manual_action" "SQL Server maintainer path is missing toolbox." "Install toolbox and rerun ./scripts/contributor-setup.sh.")"
    elif ! docker start sql-test >/dev/null 2>&1; then
      sql_server_check="$(json_check "manual_action" "SQL Server contributor container could not be started." "Run the SQL Server Docker setup from docs/reference/setup-docker/README.md, then rerun ./scripts/contributor-setup.sh.")"
    elif ! docker inspect --format '{{.State.Running}}' sql-test >/dev/null 2>&1; then
      sql_server_check="$(json_check "manual_action" "SQL Server contributor container is not healthy." "Check the sql-test container and rerun ./scripts/contributor-setup.sh.")"
    elif ! docker exec sql-test /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'P@ssw0rd123' -C -d KimballFixture -Q "SELECT 1" >/dev/null 2>&1; then
      sql_server_check="$(json_check "manual_action" "SQL Server contributor smoke check failed." "Repair the sql-test container image or credentials and rerun ./scripts/contributor-setup.sh.")"
    else
      sql_server_check="$(json_check "ok" "SQL Server maintainer path is working.")"
      working_backends="$(append_line "$working_backends" "sql_server")"
    fi

    java_ok=true
    java_version_output=""
    if ! command -v java >/dev/null 2>&1; then
      java_ok=false
    else
      java_version_output="$(java -version 2>&1 | head -n 1 || true)"
      java_major="$(JAVA_VERSION_OUTPUT="$java_version_output" python3 - <<'PY'
import os
import re

text = os.environ.get("JAVA_VERSION_OUTPUT", "")
match = re.search(r'"(\d+)(?:\.\d+)?', text)
if not match:
    print("")
else:
    print(match.group(1))
PY
)"
      if [[ -z "$java_major" || "$java_major" -lt 11 ]]; then
        java_ok=false
      fi
    fi

    if ! check_command sql "--version"; then
      oracle_check="$(json_check "manual_action" "Oracle maintainer path is missing SQLcl." "Install SQLcl and rerun ./scripts/contributor-setup.sh.")"
    elif ! $java_ok; then
      oracle_check="$(json_check "manual_action" "Oracle maintainer path requires Java 11 or newer." "Install Java 11+ and rerun ./scripts/contributor-setup.sh.")"
    elif ! docker start oracle-test >/dev/null 2>&1; then
      oracle_check="$(json_check "manual_action" "Oracle contributor container could not be started." "Run the Oracle Docker setup from docs/reference/setup-docker/README.md, then rerun ./scripts/contributor-setup.sh.")"
    elif ! docker inspect --format '{{.State.Running}}' oracle-test >/dev/null 2>&1; then
      oracle_check="$(json_check "manual_action" "Oracle contributor container is not healthy." "Check the oracle-test container and rerun ./scripts/contributor-setup.sh.")"
    elif ! docker exec oracle-test bash -c "echo 'SELECT 1 FROM dual;' | sqlplus -S kimball/kimball@FREEPDB1" >/dev/null 2>&1; then
      oracle_check="$(json_check "manual_action" "Oracle contributor smoke check failed." "Repair the oracle-test container image or credentials and rerun ./scripts/contributor-setup.sh.")"
    else
      oracle_check="$(json_check "ok" "Oracle maintainer path is working.")"
      working_backends="$(append_line "$working_backends" "oracle")"
    fi
  fi
  print_step "docker" "$docker_check"
  print_step "sql_server" "$sql_server_check"
  print_step "oracle" "$oracle_check"
fi

if [[ -z "$sql_server_check" ]]; then
  sql_server_check="$(json_check "skipped" "SQL Server checks were not reached.")"
fi
if [[ -z "$oracle_check" ]]; then
  oracle_check="$(json_check "skipped" "Oracle checks were not reached.")"
fi
if [[ -z "$repo_bootstrap_check" ]]; then
  repo_bootstrap_check="$(json_check "skipped" "Repo bootstrap was not reached.")"
fi
if [[ -z "$docker_check" ]]; then
  docker_check="$(json_check "skipped" "Docker checks were not reached.")"
fi

working_backends_json="$(array_json_from_lines "$working_backends")"
manual_actions_json="$(array_json_from_lines "$manual_actions")"

if [[ "$overall_status" != "blocked" ]]; then
  backend_count="$(WORKING_BACKENDS_JSON="$working_backends_json" python3 - <<'PY'
import json
import os

print(len(json.loads(os.environ["WORKING_BACKENDS_JSON"])))
PY
)"
  if [[ "$backend_count" -eq 0 ]]; then
    overall_status="blocked"
  fi
fi

summary_message="Maintainer readiness requires Docker plus at least one working backend."
case "$overall_status" in
  ready)
    summary_message="Contributor environment is maintainer ready."
    ;;
  partially_ready)
    summary_message="Contributor environment is usable but still needs manual follow-up."
    ;;
  blocked)
    summary_message="Contributor environment is blocked."
    ;;
esac

printf 'summary: %s\n' "$summary_message"

MODE="$mode" \
OVERALL_STATUS="$overall_status" \
SUMMARY_MESSAGE="$summary_message" \
WORKING_BACKENDS_JSON="$working_backends_json" \
MANUAL_ACTIONS_JSON="$manual_actions_json" \
PLATFORM_CHECK_JSON="$platform_check" \
REQUIRED_TOOLS_CHECK_JSON="$required_tools_check" \
OPTIONAL_TOOLS_CHECK_JSON="$optional_tools_check" \
REPO_BOOTSTRAP_CHECK_JSON="$repo_bootstrap_check" \
DOCKER_CHECK_JSON="$docker_check" \
SQL_SERVER_CHECK_JSON="$sql_server_check" \
ORACLE_CHECK_JSON="$oracle_check" \
python3 - <<'PY'
import json
import os

payload = {
    "mode": os.environ["MODE"],
    "status": os.environ["OVERALL_STATUS"],
    "summary": {
        "message": os.environ["SUMMARY_MESSAGE"],
        "working_backends": json.loads(os.environ["WORKING_BACKENDS_JSON"]),
        "manual_actions": json.loads(os.environ["MANUAL_ACTIONS_JSON"]),
    },
    "checks": {
        "platform": json.loads(os.environ["PLATFORM_CHECK_JSON"]),
        "required_tools": json.loads(os.environ["REQUIRED_TOOLS_CHECK_JSON"]),
        "optional_tools": json.loads(os.environ["OPTIONAL_TOOLS_CHECK_JSON"]),
        "repo_bootstrap": json.loads(os.environ["REPO_BOOTSTRAP_CHECK_JSON"]),
        "docker": json.loads(os.environ["DOCKER_CHECK_JSON"]),
        "sql_server": json.loads(os.environ["SQL_SERVER_CHECK_JSON"]),
        "oracle": json.loads(os.environ["ORACLE_CHECK_JSON"]),
    },
}
print(json.dumps(payload))
PY

if [[ "$overall_status" == "ready" ]]; then
  exit 0
fi
exit 1
