use rusqlite::params;
use tauri::State;
use uuid::Uuid;

use crate::db::{self, DbState};
use crate::types::{CommandError, Project};

#[tauri::command]
pub fn project_list(state: State<'_, DbState>) -> Result<Vec<Project>, CommandError> {
    log::info!("[project_list]");
    let conn = state.conn().inspect_err(|e| {
        log::error!("[project_list] DB lock: {}", e);
    })?;
    db::list_projects(&conn)
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
    db::get_project(&conn, &id)
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

    db::delete_project(&conn, &id).inspect_err(|_| {
        log::error!("[project_delete] delete failed id={}", id);
    })?;

    // Clear active_project_id if the deleted project was active
    let mut settings = db::read_settings(&conn)?;
    if settings.active_project_id.as_deref() == Some(&id) {
        log::info!("[project_delete] clearing active_project_id (was deleted project id={})", id);
        settings.active_project_id = None;
        db::write_settings(&conn, &settings)?;
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

    if !db::project_exists(&conn, &id)? {
        return Err(CommandError::NotFound(format!("project {id}")));
    }

    let mut settings = db::read_settings(&conn)?;
    settings.active_project_id = Some(id.clone());
    db::write_settings(&conn, &settings)?;
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

    let settings = db::read_settings(&conn)?;
    let Some(id) = settings.active_project_id else {
        return Ok(None);
    };

    match db::get_project(&conn, &id) {
        Ok(p) => Ok(Some(p)),
        Err(CommandError::NotFound(_)) => {
            // active_project_id points to a deleted project — clear it
            log::warn!("[project_get_active] stale active_project_id={} references non-existent project — clearing", id);
            let mut s = db::read_settings(&conn)?;
            s.active_project_id = None;
            db::write_settings(&conn, &s)?;
            Ok(None)
        }
        Err(e) => Err(e),
    }
}

/// Convert a project name to a kebab-case slug (pure, no DB access).
fn slug_base(name: &str) -> Result<String, CommandError> {
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
    Ok(base)
}

/// Generate a kebab-case slug from name, appending a short suffix on collision.
pub(crate) fn slugify(name: &str, conn: &rusqlite::Connection) -> Result<String, CommandError> {
    let base = slug_base(name)?;

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

#[tauri::command]
pub fn project_slug_preview(name: String) -> Result<String, CommandError> {
    log::info!("[project_slug_preview] name={}", name);
    slug_base(&name)
}

#[cfg(test)]
mod tests {
    use crate::db;
    use crate::types::Project;

    #[test]
    fn project_create_and_list_roundtrip() {
        let conn = db::open_in_memory().unwrap();
        let project = Project {
            id: "proj-1".into(),
            slug: "my-project".into(),
            name: "My Project".into(),
            technology: "sql_server".into(),
            created_at: "2026-01-01T00:00:00Z".into(),
        };
        db::insert_project(&conn, &project).unwrap();

        let projects = db::list_projects(&conn).unwrap();
        assert_eq!(projects.len(), 1);
        assert_eq!(projects[0].name, "My Project");
        assert_eq!(projects[0].slug, "my-project");
        assert_eq!(projects[0].technology, "sql_server");
    }

}
