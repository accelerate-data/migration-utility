#!/usr/bin/env bash
set -euo pipefail

IMAGE="${DBT_DOCKER_IMAGE:-ghcr.io/dbt-labs/dbt-core:1.11.0}"
DOCKER_BIN="${DOCKER_BIN:-docker}"
DOCKER_PLATFORM="${DBT_DOCKER_PLATFORM:-linux/amd64}"
CONTAINER_NAME="${DBT_DOCKER_CONTAINER_NAME:-dbt-cli}"

if ! command -v "${DOCKER_BIN}" >/dev/null 2>&1; then
  echo "Docker CLI not found: ${DOCKER_BIN}" >&2
  exit 1
fi
if ! "${DOCKER_BIN}" info >/dev/null 2>&1; then
  echo "Docker daemon is not reachable. Start Docker Desktop and retry." >&2
  exit 1
fi

if "${DOCKER_BIN}" container inspect "${CONTAINER_NAME}" >/dev/null 2>&1; then
  if [[ "$("${DOCKER_BIN}" inspect -f '{{.State.Running}}' "${CONTAINER_NAME}")" == "true" ]]; then
    echo "Container name '${CONTAINER_NAME}' is already running." >&2
    echo "Set DBT_DOCKER_CONTAINER_NAME to a different value and retry." >&2
    exit 1
  fi
  "${DOCKER_BIN}" rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
fi

PWD_ABS="$(pwd)"
PROJECT_DIR_RAW="${DBT_PROJECT_DIR:-.}"
PROFILES_DIR_RAW="${DBT_PROFILES_DIR:-.dbt}"

if [[ "${PROJECT_DIR_RAW}" = /* ]]; then
  PROJECT_DIR_ABS="${PROJECT_DIR_RAW}"
else
  PROJECT_DIR_ABS="${PWD_ABS}/${PROJECT_DIR_RAW}"
fi

if [[ "${PROFILES_DIR_RAW}" = /* ]]; then
  PROFILES_DIR_ABS="${PROFILES_DIR_RAW}"
else
  PROFILES_DIR_ABS="${PWD_ABS}/${PROFILES_DIR_RAW}"
fi

if [[ ! -d "${PROJECT_DIR_ABS}" ]]; then
  echo "DBT_PROJECT_DIR does not exist: ${PROJECT_DIR_ABS}" >&2
  exit 1
fi
if [[ ! -d "${PROFILES_DIR_ABS}" ]]; then
  echo "DBT_PROFILES_DIR does not exist: ${PROFILES_DIR_ABS}" >&2
  exit 1
fi

args=(
  run --rm -i
  --name "${CONTAINER_NAME}"
  --platform "${DOCKER_PLATFORM}"
  -u "$(id -u):$(id -g)"
  -v "${PWD_ABS}:${PWD_ABS}"
  -w "${PWD_ABS}"
  -v "${PROJECT_DIR_ABS}:${PROJECT_DIR_ABS}"
  -v "${PROFILES_DIR_ABS}:${PROFILES_DIR_ABS}"
  -e "DBT_PROJECT_DIR=${PROJECT_DIR_ABS}"
  -e "DBT_PROFILES_DIR=${PROFILES_DIR_ABS}"
  "${IMAGE}"
)

exec "${DOCKER_BIN}" "${args[@]}" "$@"
