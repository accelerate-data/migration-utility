use rusqlite::params;
use tauri::State;
use uuid::Uuid;

use crate::db::DbState;
use crate::types::{CommandError, Project};

#[tauri::command]
pub fn project_create(
    state: State<'_, DbState>,
    name: String,
    sa_password: String,
) -> Result<Project, CommandError> {
    log::info!("[project_create] name={}", name);
    let conn = state.conn().map_err(|e| {
        log::error!("[project_create] DB lock: {}", e);
        CommandError::Database(e)
    })?;

    let id = Uuid::new_v4().to_string();
    let slug = slugify(&name, &conn)?;
    let created_at = chrono::Utc::now().to_rfc3339();

    conn.execute(
        "INSERT INTO projects(id, slug, name, sa_password, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![id, slug, name, sa_password, created_at],
    )?;

    log::info!("[project_create] created id={} slug={}", id, slug);
    Ok(Project { id, slug, name, created_at })
}

#[tauri::command]
pub fn project_list(state: State<'_, DbState>) -> Result<Vec<Project>, CommandError> {
    log::info!("[project_list]");
    let conn = state.conn().map_err(|e| {
        log::error!("[project_list] DB lock: {}", e);
        CommandError::Database(e)
    })?;

    let mut stmt = conn.prepare(
        "SELECT id, slug, name, created_at FROM projects ORDER BY created_at DESC",
    )?;
    let projects = stmt
        .query_map([], |row| {
            Ok(Project {
                id: row.get(0)?,
                slug: row.get(1)?,
                name: row.get(2)?,
                created_at: row.get(3)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;

    Ok(projects)
}

#[tauri::command]
pub fn project_get(
    state: State<'_, DbState>,
    id: String,
) -> Result<Project, CommandError> {
    log::info!("[project_get] id={}", id);
    let conn = state.conn().map_err(|e| {
        log::error!("[project_get] DB lock: {}", e);
        CommandError::Database(e)
    })?;

    conn.query_row(
        "SELECT id, slug, name, created_at FROM projects WHERE id = ?1",
        params![id],
        |row| {
            Ok(Project {
                id: row.get(0)?,
                slug: row.get(1)?,
                name: row.get(2)?,
                created_at: row.get(3)?,
            })
        },
    )
    .map_err(|e| match e {
        rusqlite::Error::QueryReturnedNoRows => CommandError::NotFound(format!("project {id}")),
        other => CommandError::from(other),
    })
}

#[tauri::command]
pub fn project_delete(
    state: State<'_, DbState>,
    id: String,
) -> Result<(), CommandError> {
    log::info!("[project_delete] id={}", id);
    let conn = state.conn().map_err(|e| {
        log::error!("[project_delete] DB lock: {}", e);
        CommandError::Database(e)
    })?;

    let rows = conn.execute("DELETE FROM projects WHERE id = ?1", params![id])?;
    if rows == 0 {
        return Err(CommandError::NotFound(format!("project {id}")));
    }
    log::info!("[project_delete] deleted id={}", id);
    Ok(())
}

/// Generate a kebab-case slug from name, appending a short suffix on collision.
fn slugify(name: &str, conn: &rusqlite::Connection) -> Result<String, CommandError> {
    let base = name
        .to_lowercase()
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '-' })
        .collect::<String>()
        .split('-')
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("-");

    let exists: bool = conn.query_row(
        "SELECT COUNT(*) > 0 FROM projects WHERE slug = ?1",
        params![base],
        |row| row.get(0),
    )?;

    if !exists {
        return Ok(base);
    }

    // Append short hash suffix on collision
    let suffix = &Uuid::new_v4().to_string()[..6];
    Ok(format!("{base}-{suffix}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;

    #[test]
    fn project_create_and_list_roundtrip() {
        let conn = db::open_in_memory().unwrap();
        conn.execute(
            "INSERT INTO projects(id, slug, name, sa_password, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
            params!["proj-1", "my-project", "My Project", "secret", "2026-01-01T00:00:00Z"],
        )
        .unwrap();

        let mut stmt = conn
            .prepare("SELECT id, slug, name, created_at FROM projects")
            .unwrap();
        let projects: Vec<Project> = stmt
            .query_map([], |row| {
                Ok(Project {
                    id: row.get(0)?,
                    slug: row.get(1)?,
                    name: row.get(2)?,
                    created_at: row.get(3)?,
                })
            })
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();

        assert_eq!(projects.len(), 1);
        assert_eq!(projects[0].name, "My Project");
        assert_eq!(projects[0].slug, "my-project");
    }

    #[test]
    fn delete_project_cascades_to_agent_runs() {
        let conn = db::open_in_memory().unwrap();
        conn.execute(
            "INSERT INTO projects(id, slug, name, sa_password, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
            params!["p1", "proj", "Proj", "pw", "2026-01-01T00:00:00Z"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO agent_runs(project_id, run_id, action, submitted_ts, status) VALUES (?1, ?2, ?3, ?4, ?5)",
            params!["p1", "run-uuid-1", "scoping-agent", "2026-01-01T00:00:00Z", "success"],
        )
        .unwrap();

        conn.execute("DELETE FROM projects WHERE id = ?1", ["p1"]).unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM agent_runs", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }
}
