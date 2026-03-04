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
