mod commands;
mod db;
mod logging;
mod types;

use std::sync::Mutex;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(logging::build_log_plugin().build())
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_dialog::init())
        .setup(|app| {
            logging::truncate_log_file(app.handle());
            use tauri::Manager;
            let db_path = app
                .path()
                .app_data_dir()
                .expect("no app data dir")
                .join("migration-utility.db");
            let conn = db::open(&db_path).map_err(|e| {
                log::error!("db::open failed: {e}");
                e
            })?;
            // Restore persisted log level before moving conn into DbState.
            match db::read_settings(&conn) {
                Ok(settings) => {
                    if let Some(ref level) = settings.log_level {
                        logging::set_log_level(level);
                    }
                }
                Err(e) => {
                    log::warn!("startup: failed to read settings for log level restore: {}", e);
                }
            }
            app.manage(db::DbState(Mutex::new(conn)));
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            commands::app_info::set_log_level,
            commands::app_info::get_log_file_path,
            commands::app_info::get_data_dir_path,
            commands::settings::get_settings,
            commands::github_auth::github_start_device_flow,
            commands::github_auth::github_poll_for_token,
            commands::github_auth::github_get_user,
            commands::github_auth::github_logout,
            commands::github_auth::github_list_repos,
            commands::project::project_create,
            commands::project::project_list,
            commands::project::project_get,
            commands::project::project_delete,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
