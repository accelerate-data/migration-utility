use tauri_plugin_log::{Target, TargetKind};

const LOG_FILE_NAME: &str = "app";

/// Build the log plugin with dual targets: log file + stderr.
///
/// The plugin level is set to Debug so that debug messages are not silently
/// dropped by the plugin's own filter before reaching the file/stderr targets.
/// Runtime log level is controlled by `set_log_level` via `log::set_max_level`.
/// External crates are capped at Warn to avoid noise.
pub fn build_log_plugin() -> tauri_plugin_log::Builder {
    tauri_plugin_log::Builder::new()
        .targets([
            Target::new(TargetKind::LogDir {
                file_name: Some(LOG_FILE_NAME.into()),
            }),
            Target::new(TargetKind::Stderr),
        ])
        .level(log::LevelFilter::Debug)
        // Cap external crates at Info — only our code gets debug.
        .level_for("tiberius", log::LevelFilter::Info)
        .level_for("tokio", log::LevelFilter::Info)
        .level_for("reqwest", log::LevelFilter::Info)
        .level_for("hyper", log::LevelFilter::Info)
        .level_for("native_tls", log::LevelFilter::Info)
        .max_file_size(50_000_000)
}

/// Truncate the log file at session start so each run gets a clean slate.
pub fn truncate_log_file(app: &tauri::AppHandle) {
    use tauri::Manager;
    if let Ok(log_dir) = app.path().app_log_dir() {
        let log_file = log_dir.join(format!("{}.log", LOG_FILE_NAME));
        if let Err(e) = std::fs::create_dir_all(&log_dir) {
            log::warn!(
                "Failed to create log directory {}: {}",
                log_dir.display(),
                e
            );
            return;
        }
        if let Err(e) = std::fs::write(&log_file, "") {
            log::warn!("Failed to reset log file {}: {}", log_file.display(), e);
        }
    }
}

/// Set the global Rust log level at runtime (survives until next call or restart).
pub fn set_log_level(level: &str) {
    let filter = match level.to_lowercase().as_str() {
        "error" => log::LevelFilter::Error,
        "warn" => log::LevelFilter::Warn,
        "info" => log::LevelFilter::Info,
        "debug" => log::LevelFilter::Debug,
        _ => log::LevelFilter::Info,
    };
    log::info!("Log level set to {}", filter);
    log::set_max_level(filter);
}

/// Return the absolute path to the app log file.
pub fn get_log_file_path(app: &tauri::AppHandle) -> Result<String, String> {
    use tauri::Manager;
    let log_dir = app.path().app_log_dir().map_err(|e| e.to_string())?;
    let log_file = log_dir.join(format!("{}.log", LOG_FILE_NAME));
    log_file
        .to_str()
        .map(|s| s.to_string())
        .ok_or_else(|| "Log file path contains invalid UTF-8".to_string())
}
