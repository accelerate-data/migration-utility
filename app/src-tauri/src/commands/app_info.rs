use tauri::{AppHandle, Manager, State};

use crate::db::DbState;
use crate::types::CommandError;

/// Set the global log level for Rust backend and also persist it to AppSettings.
#[tauri::command]
pub fn set_log_level(state: State<'_, DbState>, level: String) -> Result<(), CommandError> {
    log::info!("[set_log_level] level={}", level);
    crate::logging::set_log_level(&level);
    let conn = state.conn().inspect_err(|e| {
        log::error!("[set_log_level] DB lock: {e}");
    })?;
    let mut settings = crate::db::read_settings(&conn)?;
    settings.log_level = Some(level);
    crate::db::write_settings(&conn, &settings)?;
    Ok(())
}

/// Return the absolute path to the app log file for display in settings.
#[tauri::command]
pub fn get_log_file_path(app: AppHandle) -> Result<String, CommandError> {
    log::info!("[get_log_file_path]");
    crate::logging::get_log_file_path(&app)
        .map_err(CommandError::Io)
}

/// Return the app local data directory path (where the SQLite database and workspace live).
#[tauri::command]
pub fn get_data_dir_path(app: AppHandle) -> Result<String, CommandError> {
    log::info!("[get_data_dir_path]");
    app.path()
        .app_data_dir()
        .map_err(|e| CommandError::Io(format!("cannot resolve app data dir: {e}")))?
        .to_str()
        .map(|s: &str| s.to_string())
        .ok_or_else(|| CommandError::Io("Data dir path contains invalid UTF-8".into()))
}


#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    // log::max_level() is a global AtomicUsize — serialize tests that mutate it.
    static LOG_LEVEL_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn set_log_level_sets_debug() {
        let _g = LOG_LEVEL_LOCK.lock().unwrap();
        crate::logging::set_log_level("debug");
        assert_eq!(log::max_level(), log::LevelFilter::Debug);
    }

    #[test]
    fn set_log_level_defaults_to_info_for_unknown() {
        let _g = LOG_LEVEL_LOCK.lock().unwrap();
        crate::logging::set_log_level("not-a-level");
        assert_eq!(log::max_level(), log::LevelFilter::Info);
    }
}
