use std::{path::Path, sync::Mutex};

use rusqlite::{params, Connection};
use thiserror::Error;

use crate::types::{AppSettings, CommandError, Project};

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
    pub fn conn(&self) -> Result<std::sync::MutexGuard<'_, Connection>, CommandError> {
        self.0
            .lock()
            .map_err(|e| CommandError::Database(format!("DB lock poisoned: {e}")))
    }
}

const MIGRATIONS: &[(i64, &str)] = &[
    (1, include_str!("../migrations/001_initial_schema.sql")),
    (2, include_str!("../migrations/002_ensure_tables.sql")),
    (3, include_str!("../migrations/003_add_project_port.sql")),
    (4, include_str!("../migrations/004_technology_schema.sql")),
];

pub fn open(path: &Path) -> Result<Connection, DbError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    log::info!("db::open: opening database at {}", path.display());
    let conn = Connection::open(path)?;
    conn.execute_batch("PRAGMA foreign_keys = ON;")?;
    run_migrations(&conn)?;
    repair_schema(&conn)?;
    Ok(conn)
}

#[cfg(test)]
pub(crate) fn open_in_memory() -> Result<Connection, DbError> {
    let conn = Connection::open_in_memory()?;
    conn.execute_batch("PRAGMA foreign_keys = ON;")?;
    run_migrations(&conn)?;
    repair_schema(&conn)?;
    Ok(conn)
}

/// Best-effort column repairs run after migrations on every startup.
///
/// Handles edge cases where a migration was skipped (e.g. schema_version was
/// already marked applied on a DB that predates the column, or the migration
/// ran against a different DB file). No-ops silently when columns already exist.
/// Keep this function until all pre-004 developer databases have been updated.
fn repair_schema(conn: &Connection) -> Result<(), DbError> {
    // projects.technology — added by migration 004; may be absent on older DB files.
    let has_technology: bool = conn.query_row(
        "SELECT COUNT(*) > 0 FROM pragma_table_info('projects') WHERE name='technology'",
        [],
        |row| row.get(0),
    )?;
    if !has_technology {
        log::warn!("repair_schema: projects.technology missing — adding column (DEFAULT 'sql_server')");
        conn.execute_batch(
            "ALTER TABLE projects ADD COLUMN technology TEXT NOT NULL DEFAULT 'sql_server';",
        )?;
    }
    Ok(())
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
pub fn read_settings(conn: &Connection) -> Result<AppSettings, CommandError> {
    let mut stmt = conn
        .prepare("SELECT value FROM settings WHERE key = ?1")?;

    let result: Result<String, _> = stmt.query_row(["app_settings"], |row| row.get(0));

    match result {
        Ok(json) => serde_json::from_str(&json)
            .map_err(|e| CommandError::Database(format!("settings JSON parse error: {e}"))),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(AppSettings::default()),
        Err(e) => Err(CommandError::from(e)),
    }
}

/// Write app settings to the settings table.
pub fn write_settings(conn: &Connection, settings: &AppSettings) -> Result<(), CommandError> {
    let json = serde_json::to_string(settings)
        .map_err(|e| CommandError::Database(format!("settings JSON serialize error: {e}")))?;
    conn.execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES (?1, ?2)",
        ["app_settings", &json],
    )?;
    Ok(())
}

// ── Project repository helpers ───────────────────────────────────────────────

fn row_to_project(row: &rusqlite::Row<'_>) -> rusqlite::Result<Project> {
    Ok(Project {
        id: row.get(0)?,
        slug: row.get(1)?,
        name: row.get(2)?,
        technology: row.get(3)?,
        created_at: row.get(4)?,
    })
}

const PROJECT_COLUMNS: &str = "id, slug, name, technology, created_at";

pub fn insert_project(conn: &Connection, project: &Project) -> Result<(), CommandError> {
    conn.execute(
        "INSERT INTO projects(id, slug, name, technology, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![project.id, project.slug, project.name, project.technology, project.created_at],
    )
    .map_err(|e| {
        log::error!("[db::insert_project] insert failed: {e}");
        CommandError::from(e)
    })?;
    Ok(())
}

pub fn get_project(conn: &Connection, id: &str) -> Result<Project, CommandError> {
    conn.query_row(
        &format!("SELECT {PROJECT_COLUMNS} FROM projects WHERE id = ?1"),
        params![id],
        row_to_project,
    )
    .map_err(|e| match e {
        rusqlite::Error::QueryReturnedNoRows => CommandError::NotFound(format!("project {id}")),
        other => CommandError::from(other),
    })
}

pub fn list_projects(conn: &Connection) -> Result<Vec<Project>, CommandError> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {PROJECT_COLUMNS} FROM projects ORDER BY created_at DESC"
    ))?;
    let projects = stmt
        .query_map([], row_to_project)?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(projects)
}

pub fn delete_project(conn: &Connection, id: &str) -> Result<(), CommandError> {
    let rows = conn.execute("DELETE FROM projects WHERE id = ?1", params![id])?;
    if rows == 0 {
        return Err(CommandError::NotFound(format!("project {id}")));
    }
    Ok(())
}

pub fn project_exists(conn: &Connection, id: &str) -> Result<bool, CommandError> {
    let exists: bool = conn.query_row(
        "SELECT COUNT(*) > 0 FROM projects WHERE id = ?1",
        params![id],
        |row| row.get(0),
    )?;
    Ok(exists)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_memory() -> Connection {
        let conn = Connection::open_in_memory().expect("in-memory db");
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        run_migrations(&conn).expect("migrations failed");
        repair_schema(&conn).expect("repair_schema failed");
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
    fn project_repository_roundtrip() {
        let conn = open_memory();
        let project = Project {
            id: "test-1".into(),
            slug: "my-project".into(),
            name: "My Project".into(),
            technology: "sql_server".into(),
            created_at: "2026-01-01T00:00:00Z".into(),
        };
        insert_project(&conn, &project).unwrap();

        let fetched = get_project(&conn, "test-1").unwrap();
        assert_eq!(fetched.name, "My Project");
        assert_eq!(fetched.technology, "sql_server");

        let all = list_projects(&conn).unwrap();
        assert_eq!(all.len(), 1);

        assert!(project_exists(&conn, "test-1").unwrap());
        assert!(!project_exists(&conn, "nonexistent").unwrap());

        delete_project(&conn, "test-1").unwrap();
        assert!(list_projects(&conn).unwrap().is_empty());

        // NotFound on missing
        assert!(get_project(&conn, "test-1").is_err());
        assert!(delete_project(&conn, "test-1").is_err());
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
        assert_eq!(count, MIGRATIONS.len() as i64, "schema_version should have exactly one row per migration");
    }

}
