# dbt on Docker

This repo uses Docker-backed dbt execution through the wrapper script:

- `agent-sources/workspace/.claude/bin/dbt-docker.sh`

`dbt-mcp` runs on host (`uvx dbt-mcp`). dbt command execution is delegated to Docker via
`DBT_PATH=.claude/bin/dbt-docker.sh` in `.claude/settings.local`.

## One-Time Setup

- Step 1: Install and start Docker Desktop.
- Step 2: Pull dbt image:

```bash
docker pull ghcr.io/dbt-labs/dbt-core:1.11.0
```

If you are on Apple Silicon and get `no matching manifest`, pull amd64 explicitly:

```bash
docker pull --platform linux/amd64 ghcr.io/dbt-labs/dbt-core:1.11.0
```

- Step 3: Verify image:

```bash
docker images ghcr.io/dbt-labs/dbt-core:1.11.0
```

## Required Environment Variables

Set these before running dbt commands:

- `DBT_PROJECT_DIR` (directory containing `dbt_project.yml`)
- `DBT_PROFILES_DIR` (directory containing `profiles.yml`)

Example:

```bash
cd /Users/hbanerjee/src/migration-utility
export DBT_PROJECT_DIR=/absolute/path/to/dbt-project
export DBT_PROFILES_DIR=/absolute/path/to/.dbt
```

## Run dbt Commands via Wrapper

```bash
./agent-sources/workspace/.claude/bin/dbt-docker.sh --version
./agent-sources/workspace/.claude/bin/dbt-docker.sh parse
./agent-sources/workspace/.claude/bin/dbt-docker.sh compile
```

## Wrapper Defaults

- Image: `ghcr.io/dbt-labs/dbt-core:1.11.0`
- Platform: `linux/amd64` (for Apple Silicon compatibility)
- Container name: `dbt-cli`

Override examples:

```bash
export DBT_DOCKER_IMAGE=ghcr.io/dbt-labs/dbt-core:1.11.0
export DBT_DOCKER_PLATFORM=linux/amd64
export DBT_DOCKER_CONTAINER_NAME=my-dbt-cli
```

Container name behavior:

- Wrapper removes stale stopped containers with that name before `docker run`.
- If a container with that name is already running, wrapper exits with a clear error.

## Troubleshooting

Docker unavailable:

```bash
docker info
```

If this fails, start Docker Desktop.

Missing project/profile files:

- Ensure `${DBT_PROJECT_DIR}/dbt_project.yml` exists.
- Ensure `${DBT_PROFILES_DIR}/profiles.yml` exists.
