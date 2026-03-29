use rusqlite::params;
use tauri::State;
use uuid::Uuid;

use crate::db::DbState;
use crate::types::{CommandError, Project};

#[tauri::command]
pub fn project_create(
    state: State<'_, DbState>,
    name: String,
    technology: String,
) -> Result<Project, CommandError> {
    log::info!("[project_create] name={} technology={}", name, technology);
    let conn = state.conn().inspect_err(|e| {
        log::error!("[project_create] DB lock: {}", e);
    })?;

    let id = Uuid::new_v4().to_string();
    let slug = slugify(&name, &conn)?;
    let created_at = chrono::Utc::now().to_rfc3339();

    conn.execute(
        "INSERT INTO projects(id, slug, name, technology, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![id, slug, name, technology, created_at],
    )
    .map_err(|e| {
        log::error!("[project_create] insert failed: {}", e);
        CommandError::from(e)
    })?;

    log::info!("[project_create] created id={} slug={}", id, slug);
    Ok(Project { id, slug, name, technology, created_at })
}

#[tauri::command]
pub fn project_list(state: State<'_, DbState>) -> Result<Vec<Project>, CommandError> {
    log::info!("[project_list]");
    let conn = state.conn().inspect_err(|e| {
        log::error!("[project_list] DB lock: {}", e);
    })?;

    let mut stmt = conn.prepare(
        "SELECT id, slug, name, technology, created_at FROM projects ORDER BY created_at DESC",
    )?;
    let projects = stmt
        .query_map([], |row| {
            Ok(Project {
                id: row.get(0)?,
                slug: row.get(1)?,
                name: row.get(2)?,
                technology: row.get(3)?,
                created_at: row.get(4)?,
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
    let conn = state.conn().inspect_err(|e| {
        log::error!("[project_get] DB lock: {}", e);
    })?;

    conn.query_row(
        "SELECT id, slug, name, technology, created_at FROM projects WHERE id = ?1",
        params![id],
        |row| {
            Ok(Project {
                id: row.get(0)?,
                slug: row.get(1)?,
                name: row.get(2)?,
                technology: row.get(3)?,
                created_at: row.get(4)?,
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
    let conn = state.conn().inspect_err(|e| {
        log::error!("[project_delete] DB lock: {}", e);
    })?;

    let rows = conn.execute("DELETE FROM projects WHERE id = ?1", params![id])
        .map_err(|e| {
            log::error!("[project_delete] delete failed id={}: {}", id, e);
            CommandError::from(e)
        })?;
    if rows == 0 {
        return Err(CommandError::NotFound(format!("project {id}")));
    }
    log::info!("[project_delete] deleted id={}", id);
    Ok(())
}

#[tauri::command]
pub fn project_set_active(
    state: State<'_, DbState>,
    id: String,
) -> Result<(), CommandError> {
    log::info!("[project_set_active] id={}", id);
    let conn = state.conn().inspect_err(|e| {
        log::error!("[project_set_active] DB lock: {}", e);
    })?;

    // Verify project exists
    let exists: bool = conn.query_row(
        "SELECT COUNT(*) > 0 FROM projects WHERE id = ?1",
        params![id],
        |row| row.get(0),
    )?;
    if !exists {
        return Err(CommandError::NotFound(format!("project {id}")));
    }

    let mut settings = crate::db::read_settings(&conn)?;
    settings.active_project_id = Some(id.clone());
    crate::db::write_settings(&conn, &settings)?;
    log::info!("[project_set_active] active project set to id={}", id);
    Ok(())
}

#[tauri::command]
pub fn project_get_active(
    state: State<'_, DbState>,
) -> Result<Option<Project>, CommandError> {
    log::info!("[project_get_active]");
    let conn = state.conn().inspect_err(|e| {
        log::error!("[project_get_active] DB lock: {}", e);
    })?;

    let settings = crate::db::read_settings(&conn)?;
    let Some(id) = settings.active_project_id else {
        return Ok(None);
    };

    match conn.query_row(
        "SELECT id, slug, name, technology, created_at FROM projects WHERE id = ?1",
        params![id],
        |row| {
            Ok(Project {
                id: row.get(0)?,
                slug: row.get(1)?,
                name: row.get(2)?,
                technology: row.get(3)?,
                created_at: row.get(4)?,
            })
        },
    ) {
        Ok(p) => Ok(Some(p)),
        Err(rusqlite::Error::QueryReturnedNoRows) => {
            // active_project_id points to a deleted project — clear it
            let mut s = crate::db::read_settings(&conn)?;
            s.active_project_id = None;
            crate::db::write_settings(&conn, &s)?;
            Ok(None)
        }
        Err(e) => Err(CommandError::from(e)),
    }
}

/// Generate a kebab-case slug from name, appending a short suffix on collision.
pub(crate) fn slugify(name: &str, conn: &rusqlite::Connection) -> Result<String, CommandError> {
    let base = name
        .to_lowercase()
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '-' })
        .collect::<String>()
        .split('-')
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("-");

    if base.is_empty() {
        return Err(CommandError::Validation(
            "Project name must contain at least one alphanumeric character".into(),
        ));
    }

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
            "INSERT INTO projects(id, slug, name, technology, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
            params!["proj-1", "my-project", "My Project", "sql_server", "2026-01-01T00:00:00Z"],
        )
        .unwrap();

        let mut stmt = conn
            .prepare("SELECT id, slug, name, technology, created_at FROM projects")
            .unwrap();
        let projects: Vec<Project> = stmt
            .query_map([], |row| {
                Ok(Project {
                    id: row.get(0)?,
                    slug: row.get(1)?,
                    name: row.get(2)?,
                    technology: row.get(3)?,
                    created_at: row.get(4)?,
                })
            })
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();

        assert_eq!(projects.len(), 1);
        assert_eq!(projects[0].name, "My Project");
        assert_eq!(projects[0].slug, "my-project");
        assert_eq!(projects[0].technology, "sql_server");
    }

}
