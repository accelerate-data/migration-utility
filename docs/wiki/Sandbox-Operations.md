# Sandbox Operations

Sandbox operations cover the lifecycle of the temporary execution environment used for ground-truth capture and proof-backed SQL validation.

## Commands

| Command | Purpose |
|---|---|
| `ad-migration setup-sandbox` | Provision the active sandbox and persist it in `manifest.json` |
| `ad-migration teardown-sandbox` | Drop the active sandbox and clear its persisted runtime state |

## setup-sandbox

`ad-migration setup-sandbox` provisions the active sandbox from the configured `runtime.sandbox` role in `manifest.json`.

### What it requires

- `manifest.json` with `runtime.sandbox`
- extracted schemas in `extraction.schemas`
- sandbox environment variables for the configured technology

### What it does

1. Loads `runtime.sandbox` from `manifest.json`
2. Validates the required `SANDBOX_*` environment variables
3. Writes the current connection details into `manifest.json`
4. Asks for confirmation unless `--yes` is used
5. Clones the extracted schemas into a new sandbox
6. Writes the active sandbox name back to `manifest.json`

### Output

The command reports the active sandbox name plus the number of tables, views, and procedures cloned.

## teardown-sandbox

`ad-migration teardown-sandbox` drops the active sandbox that was previously persisted in `manifest.json`.

### When to run

Run teardown after test generation and refactor work that depend on the sandbox are complete.

### What it does

1. Reads the active sandbox name from `manifest.json`
2. Asks for confirmation unless `--yes` is used
3. Drops the sandbox
4. Clears sandbox metadata from `manifest.json` on success

### Error handling

| Situation | Behavior |
|---|---|
| No sandbox name in `manifest.json` | Stops and tells the user to run `setup-sandbox` first |
| Drop succeeds | Clears the manifest sandbox state and reports success |
| Drop fails | Surfaces `SANDBOX_DOWN_FAILED` details and exits with an error |

## Connection variables

Use the technology-specific reference for source, target, and sandbox variables:

- [[SQL Server Connection Variables]]
- [[Oracle Connection Variables]]

## Related pages

- [[Stage 4 Sandbox Setup]] -- project-setup walkthrough for sandbox creation
- [[Stage 3 Test Generation]] -- where the sandbox is used first
- [[Git Workflow]] -- worktree and branch cleanup lives there, not here
