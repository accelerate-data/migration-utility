use tauri::State;

use crate::db::DbState;
use crate::types::AppSettingsPublic;

#[tauri::command]
pub fn get_settings(state: State<'_, DbState>) -> Result<AppSettingsPublic, String> {
    log::info!("[get_settings]");
    let conn = state.conn().map_err(|e| {
        log::error!("[get_settings] Failed to acquire DB lock: {}", e);
        e
    })?;
    crate::db::read_settings(&conn).map(AppSettingsPublic::from)
}

#[tauri::command]
pub fn save_repo_settings(
    state: State<'_, DbState>,
    full_name: String,
    clone_url: String,
    local_path: String,
) -> Result<(), String> {
    log::info!(
        "[save_repo_settings] repo={} local_path={}",
        full_name,
        local_path
    );
    let conn = state.conn().map_err(|e| {
        log::error!("[save_repo_settings] Failed to acquire DB lock: {}", e);
        e
    })?;
    let mut settings = crate::db::read_settings(&conn).map_err(|e| {
        log::error!("[save_repo_settings] read_settings failed: {}", e);
        e
    })?;
    settings.migration_repo_full_name = Some(full_name);
    settings.migration_repo_clone_url = Some(clone_url);
    settings.local_clone_path = Some(local_path);
    crate::db::write_settings(&conn, &settings).map_err(|e| {
        log::error!("[save_repo_settings] write_settings failed: {}", e);
        e
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::db;
    use crate::types::AppSettings;

    #[test]
    fn log_level_roundtrip_persists_and_deserializes() {
        let conn = db::open_in_memory().unwrap();
        let settings = AppSettings {
            log_level: Some("debug".to_string()),
            ..AppSettings::default()
        };
        db::write_settings(&conn, &settings).unwrap();
        let read = db::read_settings(&conn).unwrap();
        assert_eq!(read.log_level.as_deref(), Some("debug"));
    }
}
