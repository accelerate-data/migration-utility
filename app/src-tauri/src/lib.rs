mod agent_sources;
mod commands;
mod db;
mod logging;
mod source_sql;
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
            agent_sources::deploy_on_startup(app.handle()).map_err(|e| {
                log::error!("agent_sources deploy failed on startup: {e}");
                e
            })?;
            app.manage(commands::agent::SidecarManager::default());
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            commands::agent::monitor_launch_agent,
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
            commands::settings::app_set_phase,
            commands::settings::app_set_phase_flags,
            commands::workspace::workspace_create,
            commands::workspace::workspace_apply_and_clone,
            commands::workspace::workspace_apply_start,
            commands::workspace::workspace_apply_status,
            commands::workspace::workspace_get,
            commands::workspace::workspace_test_source_connection,
            commands::workspace::workspace_discover_source_databases,
            commands::workspace::workspace_reset_state,
            commands::fabric::fabric_upsert_items,
            commands::fabric::fabric_upsert_schemas,
            commands::fabric::fabric_upsert_tables,
            commands::fabric::fabric_upsert_procedures,
            commands::fabric::fabric_upsert_pipeline_activities,
            commands::migration::migration_save_selected_tables,
            commands::migration::migration_save_table_artifact,
            commands::migration::migration_save_candidacy,
            commands::migration::migration_override_candidacy,
            commands::migration::migration_list_candidacy,
            commands::migration::migration_list_scope_inventory,
            commands::migration::migration_add_tables_to_selection,
            commands::migration::migration_set_table_selected,
            commands::migration::migration_reset_selected_tables,
            commands::migration::migration_analyze_table_details,
            commands::migration::migration_save_table_config,
            commands::migration::migration_get_table_config,
            commands::migration::migration_approve_table_config,
            commands::migration::migration_reconcile_scope_state,
            commands::migration::migration_validate_relationship,
            commands::plan::plan_serialize,
            commands::github_auth::github_start_device_flow,
            commands::github_auth::github_poll_for_token,
            commands::github_auth::github_get_user,
            commands::github_auth::github_logout,
            commands::github_auth::github_list_repos,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
