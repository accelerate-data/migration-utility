---
paths:
  - "app/src-tauri/**"
---

# Rust Backend

Tauri v2 backend in `app/src-tauri/`. Keep command modules grouped by concern under `src/commands/`.

## Command Conventions

Every `#[tauri::command]` function must:

- Log `info!` on entry with key params: `info!("workspace_create: name={}", name)`
- Log `error!` on failure: `error!("workspace_create: failed: {}", e)`
- Use `debug!` for intermediate steps
- Never log secrets (API keys, tokens) or PII column values

Canonical logging requirements (levels, redaction, correlation IDs) are in `.claude/rules/logging-policy.md`.

## Error Types

Use `thiserror` for all error types. Commands return `Result<T, CommandError>` where `CommandError`
derives `serde::Serialize` so Tauri serialises it to the frontend as a typed error, not a raw string.
Map external errors at boundaries with `map_err(CommandError::from)` where possible; use custom
message mapping only when adding user-facing context.

## Database Rules

- Do not hardcode source-related SQL in Rust command files.
- All source-related SQL must live under `app/src-tauri/sql/source/<source_type>/` (organized by query intent) and be loaded only via `resolve_source_query(...)`.
- Define foreign keys with `ON DELETE CASCADE` for app table relationships so parent deletes clean up dependent rows.
- Exception: usage/log snapshot tables must not have foreign keys to mutable entities; they must preserve point-in-time records and remain unaffected by parent-row deletes.
- Never use `INSERT OR REPLACE` on parent rows that are referenced by `ON DELETE CASCADE` children. In SQLite, `REPLACE` is delete+insert and will trigger cascades (for example, unexpectedly clearing `selected_tables` during refresh). Use `INSERT ... ON CONFLICT (...) DO UPDATE` instead.
- Use parameterized SQL (`?1`, `?2`, `params![...]`) for SQLite writes; never build SQL by interpolating user input.
- Wrap multi-table write flows in a transaction and commit once; rollback on error via normal `?` propagation.
- For destructive reset flows, clear child tables before parent tables to satisfy foreign keys.
- For live SQL Server tests, keep them `#[ignore]` and run explicitly with env vars documented in `docs/reference/setup-docker/README.md`.
- Unit tests should default to `db::open_in_memory()`; do not depend on the user's local app DB.

## Testing

Inline `#[cfg(test)]` tests in the same file as the command where practical. Use
`crate::db::open_in_memory()` to create a migrated in-memory SQLite connection:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;

    #[test]
    fn creates_workspace_and_retrieves_it() {
        let conn = db::open_in_memory().expect("in-memory db");
        // ... exercise command logic directly against conn
    }
}
```

Run tests with:

```bash
cargo test --manifest-path app/src-tauri/Cargo.toml     # all Rust tests
cargo test --manifest-path app/src-tauri/Cargo.toml db  # module filter
```
