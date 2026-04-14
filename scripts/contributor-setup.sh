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

json_escape() {
  local value="${1//\\/\\\\}"
  value="${value//\"/\\\"}"
  value="${value//$'\n'/\\n}"
  value="${value//$'\r'/\\r}"
  value="${value//$'\t'/\\t}"
  printf '%s' "$value"
}

json_check() {
  local status="$1"
  local message="$2"
  local next_action="${3:-}"
  local payload
  payload="{\"status\":\"$(json_escape "$status")\",\"message\":\"$(json_escape "$message")\""
  if [[ -n "$next_action" ]]; then
    payload="${payload},\"next_action\":\"$(json_escape "$next_action")\""
  fi
  payload="${payload}}"
  printf '%s\n' "$payload"
}

print_step() {
  local name="$1"
  local payload_json="$2"
  local status message
  status="$(printf '%s' "$payload_json" | sed -n 's/.*"status":"\([^"]*\)".*/\1/p')"
  message="$(printf '%s' "$payload_json" | sed -n 's/.*"message":"\([^"]*\)".*/\1/p')"
  printf '%s: %s - %s\n' "$name" "$status" "$message"
}

array_json() {
  local first=true
  local item=""
  printf '['
  for item in "$@"; do
    if [[ "$first" == true ]]; then
      first=false
    else
      printf ','
    fi
    printf '"%s"' "$(json_escape "$item")"
  done
  printf ']'
}

join_items() {
  local separator="$1"
  shift
  local item=""
  local joined=""
  for item in "$@"; do
    [[ -z "$item" ]] && continue
    if [[ -z "$joined" ]]; then
      joined="$item"
    else
      joined="${joined}${separator}${item}"
    fi
  done
  printf '%s' "$joined"
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
manual_actions=()
working_backends=()
backend_result_status=""

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

missing_required_tools=()
for tool in "${base_required_tools[@]}"; do
  if ! check_command "$tool" "--version"; then
    missing_required_tools+=("$tool")
  fi
done
if [[ ${#missing_required_tools[@]} -eq 0 ]]; then
  required_tools_check="$(json_check "ok" "Required machine-level tools are installed.")"
else
  required_tools_check="$(json_check "blocked" "Missing required machine-level tools: $(join_items ", " "${missing_required_tools[@]}")." "Install the missing required tools, then rerun ./scripts/contributor-setup.sh.")"
  overall_status="blocked"
fi
print_step "required_tools" "$required_tools_check"

missing_optional_tools=()
for tool in "${optional_tools[@]}"; do
  if ! check_command "$tool" "--version"; then
    missing_optional_tools+=("$tool")
  fi
done
if [[ ${#missing_optional_tools[@]} -eq 0 ]]; then
  optional_tools_check="$(json_check "ok" "Optional machine-level tools are installed.")"
else
  optional_tools_check="$(json_check "manual_action" "Missing optional tools: $(join_items ", " "${missing_optional_tools[@]}")." "Install optional tools only if you need their related workflows.")"
fi
print_step "optional_tools" "$optional_tools_check"

backend_check() {
  local backend_name="$1"
  local container_name="$2"
  local setup_doc="$3"
  local smoke_failure_message="$4"
  shift 4
  local smoke_cmd=("$@")
  local container_running=""

  local result_status=""
  local result_json=""
  container_running="$(docker inspect --format '{{.State.Running}}' "$container_name" 2>/dev/null || true)"

  if [[ "$mode" == "fix" ]]; then
    if [[ "$container_running" != "true" ]] && ! docker start "$container_name" >/dev/null 2>&1; then
      result_status="manual_action"
      result_json="$(json_check "manual_action" "$backend_name contributor container could not be started." "Run the $backend_name Docker setup from $setup_doc, then rerun ./scripts/contributor-setup.sh.")"
      printf '%s\t%s\n' "$result_status" "$result_json"
      return
    fi
  elif [[ "$container_running" != "true" ]]; then
    result_status="manual_action"
    result_json="$(json_check "manual_action" "$backend_name contributor container is not running in show mode." "Start $container_name or rerun ./scripts/contributor-setup.sh in fix mode.")"
    printf '%s\t%s\n' "$result_status" "$result_json"
    return
  fi

  container_running="$(docker inspect --format '{{.State.Running}}' "$container_name" 2>/dev/null || true)"

  if [[ "$container_running" != "true" ]]; then
    result_status="manual_action"
    result_json="$(json_check "manual_action" "$backend_name contributor container is not healthy." "Check the $container_name container and rerun ./scripts/contributor-setup.sh.")"
  elif ! docker exec "$container_name" "${smoke_cmd[@]}" >/dev/null 2>&1; then
    result_status="manual_action"
    result_json="$(json_check "manual_action" "$smoke_failure_message" "Repair the $container_name container image or credentials and rerun ./scripts/contributor-setup.sh.")"
  else
    result_status="ok"
    result_json="$(json_check "ok" "$backend_name maintainer path is working.")"
  fi

  printf '%s\t%s\n' "$result_status" "$result_json"
}

if [[ "$overall_status" == "blocked" ]]; then
  repo_bootstrap_check="$(json_check "skipped" "Repo bootstrap skipped because prerequisite checks are blocked.")"
  docker_check="$(json_check "skipped" "Docker checks skipped because prerequisite checks are blocked.")"
  sql_server_check="$(json_check "skipped" "SQL Server checks skipped because prerequisite checks are blocked.")"
  oracle_check="$(json_check "skipped" "Oracle checks skipped because prerequisite checks are blocked.")"
else
  if [[ "$mode" == "fix" ]]; then
    bootstrap_failures=()
    if ! run_in_dir "$repo_root/lib" uv sync --extra dev; then
      bootstrap_failures+=("lib environment sync failed")
    fi
    if ! run_in_dir "$repo_root/mcp/ddl" uv sync; then
      bootstrap_failures+=("mcp/ddl environment sync failed")
    fi
    if ! run_in_dir "$repo_root/tests/evals" npm install --no-audit --no-fund; then
      bootstrap_failures+=("eval dependencies install failed")
    fi

    if [[ ${#bootstrap_failures[@]} -eq 0 ]]; then
      repo_bootstrap_check="$(json_check "fixed" "Repo-local contributor bootstrap completed.")"
    else
      repo_bootstrap_check="$(json_check "manual_action" "Repo-local bootstrap did not complete cleanly: $(join_items "; " "${bootstrap_failures[@]}")." "Repair the listed repo-local bootstrap failures, then rerun ./scripts/contributor-setup.sh.")"
      overall_status="partially_ready"
      manual_actions+=("Repair repo-local bootstrap failures and rerun ./scripts/contributor-setup.sh.")
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
    backend_result_status="manual_action"
    if ! check_command toolbox "--version"; then
      sql_tool_ok=false
    fi
    if ! $sql_tool_ok; then
      sql_server_check="$(json_check "manual_action" "SQL Server maintainer path is missing toolbox." "Install toolbox and rerun ./scripts/contributor-setup.sh.")"
    else
      IFS=$'\t' read -r backend_result_status sql_server_check < <(
        backend_check \
          "SQL Server" \
          "sql-test" \
          "docs/reference/setup-docker/README.md" \
          "SQL Server contributor smoke check failed." \
          /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'P@ssw0rd123' -C -d KimballFixture -Q "SELECT 1"
      )
    fi
    if [[ "$backend_result_status" == "ok" ]]; then
      working_backends+=("sql_server")
    fi

    java_ok=true
    java_version_output=""
    backend_result_status="manual_action"
    if ! command -v java >/dev/null 2>&1; then
      java_ok=false
    else
      java_version_output="$(java -version 2>&1 | head -n 1 || true)"
      java_major="$(printf '%s' "$java_version_output" | sed -nE 's/.*"([0-9]+)(\.[0-9]+)?(.+)?".*/\1/p')"
      if [[ -z "$java_major" || "$java_major" -lt 11 ]]; then
        java_ok=false
      fi
    fi

    if ! check_command sql "--version"; then
      oracle_check="$(json_check "manual_action" "Oracle maintainer path is missing SQLcl." "Install SQLcl and rerun ./scripts/contributor-setup.sh.")"
    elif ! $java_ok; then
      oracle_check="$(json_check "manual_action" "Oracle maintainer path requires Java 11 or newer." "Install Java 11+ and rerun ./scripts/contributor-setup.sh.")"
    else
      IFS=$'\t' read -r backend_result_status oracle_check < <(
        backend_check \
          "Oracle" \
          "oracle-test" \
          "docs/reference/setup-docker/README.md" \
          "Oracle contributor smoke check failed." \
          bash -c "echo 'SELECT 1 FROM dual;' | sqlplus -S kimball/kimball@FREEPDB1"
      )
    fi
    if [[ "$backend_result_status" == "ok" ]]; then
      working_backends+=("oracle")
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

working_backends_json="$(array_json "${working_backends[@]}")"
manual_actions_json="$(array_json "${manual_actions[@]}")"

if [[ "$overall_status" != "blocked" ]]; then
  if (( ${#working_backends[@]} == 0 )); then
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
