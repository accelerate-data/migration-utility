use std::{path::Path, sync::Mutex};

use rusqlite::Connection;
use thiserror::Error;

use crate::types::{AppPhase, AppPhaseState, AppSettings};

#[derive(Debug, Error)]
pub enum DbError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub struct DbState(pub Mutex<Connection>);

impl DbState {
    /// Acquire the database connection, mapping a poisoned mutex to a recoverable error.
    pub fn conn(&self) -> Result<std::sync::MutexGuard<'_, Connection>, String> {
        self.0
            .lock()
            .map_err(|e| format!("DB lock poisoned: {e}"))
    }
}

const APP_PHASE_KEY: &str = "app_phase";

const MIGRATIONS: &[(i64, &str)] = &[
    (1, include_str!("../migrations/001_initial_schema.sql")),
    (12, include_str!("../migrations/012_drop_legacy_tables.sql")),
];

pub fn open(path: &Path) -> Result<Connection, DbError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    log::info!("db::open: opening database at {}", path.display());
    let conn = Connection::open(path)?;
    conn.execute_batch("PRAGMA foreign_keys = ON;")?;
    run_migrations(&conn)?;
    Ok(conn)
}

#[cfg(test)]
pub(crate) fn open_in_memory() -> Result<Connection, DbError> {
    let conn = Connection::open_in_memory()?;
    conn.execute_batch("PRAGMA foreign_keys = ON;")?;
    run_migrations(&conn)?;
    Ok(conn)
}

fn run_migrations(conn: &Connection) -> Result<(), DbError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS schema_version (
           version    INTEGER PRIMARY KEY,
           applied_at TEXT NOT NULL
         );",
    )?;

    for (version, sql) in MIGRATIONS {
        let already_applied: bool = conn.query_row(
            "SELECT COUNT(*) > 0 FROM schema_version WHERE version = ?1",
            [version],
            |row| row.get(0),
        )?;

        if !already_applied {
            log::info!("db: applying migration {}", version);
            let tx = conn.unchecked_transaction()?;
            tx.execute_batch(sql)?;
            tx.execute(
                "INSERT INTO schema_version(version, applied_at) VALUES (?1, datetime('now'))",
                [version],
            )?;
            tx.commit()?;
        }
    }
    Ok(())
}

/// Read the persisted app settings from the settings table.
/// Returns defaults if no row exists yet.
pub fn read_settings(conn: &Connection) -> Result<AppSettings, String> {
    let mut stmt = conn
        .prepare("SELECT value FROM settings WHERE key = ?1")
        .map_err(|e| e.to_string())?;

    let result: Result<String, _> = stmt.query_row(["app_settings"], |row| row.get(0));

    match result {
        Ok(json) => serde_json::from_str(&json).map_err(|e| e.to_string()),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(AppSettings::default()),
        Err(e) => Err(e.to_string()),
    }
}

/// Write app settings to the settings table.
pub fn write_settings(conn: &Connection, settings: &AppSettings) -> Result<(), String> {
    let json = serde_json::to_string(settings).map_err(|e| e.to_string())?;
    conn.execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES (?1, ?2)",
        ["app_settings", &json],
    )
    .map_err(|e| e.to_string())?;
    Ok(())
}

fn read_settings_value(conn: &Connection, key: &str) -> Result<Option<String>, String> {
    conn.query_row("SELECT value FROM settings WHERE key = ?1", [key], |row| {
        row.get(0)
    })
    .map(Some)
    .or_else(|err| match err {
        rusqlite::Error::QueryReturnedNoRows => Ok(None),
        _ => Err(err),
    })
    .map_err(|e| e.to_string())
}

fn write_settings_value(conn: &Connection, key: &str, value: &str) -> Result<(), String> {
    conn.execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES (?1, ?2)",
        [key, value],
    )
    .map_err(|e| e.to_string())?;
    Ok(())
}

pub fn read_app_phase(conn: &Connection) -> Result<Option<AppPhase>, String> {
    let raw = read_settings_value(conn, APP_PHASE_KEY)?;
    let phase = raw.and_then(|v| AppPhase::from_str(v.as_str()));
    Ok(phase)
}

pub fn write_app_phase(conn: &Connection, phase: AppPhase) -> Result<(), String> {
    write_settings_value(conn, APP_PHASE_KEY, phase.as_str())
}

fn read_phase_facts(conn: &Connection) -> Result<AppPhaseState, String> {
    let settings = read_settings(conn)?;
    let has_github_auth = settings
        .github_oauth_token
        .as_deref()
        .is_some_and(|v| !v.trim().is_empty());
    let has_anthropic_key = settings
        .anthropic_api_key
        .as_deref()
        .is_some_and(|v| !v.trim().is_empty());
    let has_project: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM projects LIMIT 1)",
            [],
            |row| row.get(0),
        )
        .map_err(|e| e.to_string())?;

    Ok(AppPhaseState {
        app_phase: AppPhase::SetupRequired,
        has_github_auth,
        has_anthropic_key,
        has_project,
    })
}

pub fn read_current_app_phase_state(conn: &Connection) -> Result<AppPhaseState, String> {
    let mut state = read_phase_facts(conn)?;
    state.app_phase = read_app_phase(conn)?.unwrap_or(AppPhase::SetupRequired);
    Ok(state)
}

/// Return the current app phase state from persisted DB value.
///
/// Phase transitions are driven by explicit writes via `app_set_phase`.
pub fn reconcile_and_persist_app_phase(conn: &Connection) -> Result<AppPhaseState, String> {
    read_current_app_phase_state(conn)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_memory() -> Connection {
        let conn = Connection::open_in_memory().expect("in-memory db");
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        run_migrations(&conn).expect("migrations failed");
        conn
    }

    #[test]
    fn fresh_db_has_all_tables() {
        let conn = open_memory();
        let expected = ["schema_version", "settings", "projects"];
        for table in expected {
            let count: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
                    [table],
                    |row| row.get(0),
                )
                .expect("query failed");
            assert_eq!(count, 1, "table '{table}' missing");
        }
    }

    #[test]
    fn migrations_are_idempotent() {
        let conn = Connection::open_in_memory().expect("in-memory db");
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        run_migrations(&conn).expect("first run failed");
        run_migrations(&conn).expect("second run failed");
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM schema_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 2, "schema_version should have exactly 2 rows after migrations 1 and 12");
    }

    #[test]
    fn reconcile_phase_defaults_to_setup_required_when_no_phase_persisted() {
        let conn = open_memory();
        let state = reconcile_and_persist_app_phase(&conn).unwrap();
        assert_eq!(state.app_phase, AppPhase::SetupRequired);
        assert!(!state.has_project);
    }

    #[test]
    fn reconcile_phase_returns_persisted_phase() {
        let conn = open_memory();
        write_app_phase(&conn, AppPhase::Configured).unwrap();
        let state = reconcile_and_persist_app_phase(&conn).unwrap();
        assert_eq!(state.app_phase, AppPhase::Configured);
    }
}
