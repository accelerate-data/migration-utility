mod agent_sources;
mod commands;
mod db;
mod logging;
mod source_sql;
mod types;

use std::path::PathBuf;
use std::sync::Mutex;

/// Canonical local data directory for the app (DB, workspace, etc.).
/// Resolved once at startup and made available as managed state so every
/// module derives its paths from a single source of truth.
///
/// macOS: ~/Library/Application Support/<bundle-id>/
/// Windows: %LOCALAPPDATA%\<bundle-id>\
/// Linux: ~/.local/share/<bundle-id>/
pub struct DataDir(pub PathBuf);

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(logging::build_log_plugin().build())
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_dialog::init())
        .setup(|app| {
            logging::truncate_log_file(app.handle());
            use tauri::Manager;
            let data_dir = app
                .path()
                .app_local_data_dir()
                .map_err(|e| format!("cannot resolve local data dir: {e}"))?;
            let db_path = data_dir.join("migration-utility.db");
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
            app.manage(DataDir(data_dir.clone()));
            agent_sources::deploy_on_startup(app.handle(), &data_dir).map_err(|e| {
                log::error!("agent_sources deploy failed on startup: {e}");
                e
            })?;
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            commands::app_info::set_log_level,
            commands::app_info::get_log_file_path,
            commands::app_info::get_data_dir_path,
            commands::usage::usage_get_summary,
            commands::usage::usage_list_runs,
            commands::usage::usage_get_run_detail,
            commands::settings::get_settings,
            commands::settings::save_anthropic_api_key,
            commands::settings::save_agent_settings,
            commands::settings::list_models,
            commands::settings::test_api_key,
            commands::settings::app_hydrate_phase,
            commands::workspace::workspace_apply_start,
            commands::workspace::workspace_apply_status,
            commands::workspace::workspace_get,
            commands::workspace::workspace_test_source_connection,
            commands::workspace::workspace_discover_source_databases,
            commands::workspace::workspace_reset_state,
            commands::github_auth::github_start_device_flow,
            commands::github_auth::github_poll_for_token,
            commands::github_auth::github_get_user,
            commands::github_auth::github_logout,
            commands::github_auth::github_list_repos,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
