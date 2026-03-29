use std::path::Path;

use tauri::State;

use crate::commands::process::run_cmd;
use crate::db::DbState;
use crate::types::{AppSettingsPublic, CommandError};

/// Managed .gitignore seeded into freshly cloned migration repos.
/// Critically does NOT exclude *.dacpac — those are tracked via Git LFS.
const GITIGNORE_TEMPLATE: &str = include_str!("../../resources/.gitignore-template");

/// FDE-facing README seeded into freshly cloned migration repos.
const README_TEMPLATE: &str = include_str!("../../resources/README-template.md");

#[tauri::command]
pub fn get_settings(state: State<'_, DbState>) -> Result<AppSettingsPublic, CommandError> {
    log::info!("[get_settings]");
    let conn = state.conn().inspect_err(|e| {
        log::error!("[get_settings] Failed to acquire DB lock: {}", e);
    })?;
    crate::db::read_settings(&conn).map(AppSettingsPublic::from)
}

/// Save migration repo settings and clone the repo into `{parent_folder}/{repo_name}` if not
/// already present. Saves the derived clone path as `local_clone_path` in settings.
#[tauri::command]
pub fn save_repo_settings(
    state: State<'_, DbState>,
    full_name: String,
    clone_url: String,
    parent_folder: String,
) -> Result<(), CommandError> {
    // Extract repo short name from "org/repo-name".
    let repo_name = full_name
        .split('/')
        .next_back()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| CommandError::Validation("Invalid repository name".into()))?
        .to_string();

    let parent = parent_folder.trim_end_matches('/');
    let clone_path = format!("{parent}/{repo_name}");

    log::info!(
        "[save_repo_settings] repo={} parent={} clone_path={}",
        full_name,
        parent_folder,
        clone_path
    );

    let conn = state.conn().inspect_err(|e| {
        log::error!("[save_repo_settings] Failed to acquire DB lock: {}", e);
    })?;

    let settings = crate::db::read_settings(&conn).inspect_err(|e| {
        log::error!("[save_repo_settings] read_settings failed: {}", e);
    })?;

    // Clone if not already present; skip if .git already exists.
    if !Path::new(&clone_path).join(".git").exists() {
        std::fs::create_dir_all(parent).map_err(|e| {
            log::error!("[save_repo_settings] create_dir_all '{}' failed: {}", parent, e);
            CommandError::Io(e.to_string())
        })?;

        // Inject token into the HTTPS clone URL. GitHub accepts the token directly
        // as the username (with an empty password) for HTTPS git operations.
        let auth_url = if let Some(ref tok) = settings.github_oauth_token {
            clone_url.replacen("https://", &format!("https://{}@", tok), 1)
        } else {
            clone_url.clone()
        };

        log::info!("[save_repo_settings] cloning into {}", clone_path);
        run_cmd("git", &["clone", &auth_url, &clone_path], None, &[("GIT_TERMINAL_PROMPT", "0")]).inspect_err(|e| {
            // Redact token from error message before logging (git may echo the remote URL in stderr).
            let safe_msg = if let Some(ref tok) = settings.github_oauth_token {
                e.to_string().replace(tok.as_str(), "<token>")
            } else {
                e.to_string()
            };
            log::error!("[save_repo_settings] git clone failed: {}", safe_msg);
        })?;

    } else {
        log::info!("[save_repo_settings] repo already cloned at {}", clone_path);
    }

    // Seed managed files — runs whether the repo was just cloned or already existed.
    // .gitignore: always overwrite so the managed version (no *.dacpac) wins.
    // README.md: only write if absent so FDE edits are preserved.
    if Path::new(&clone_path).join(".git").exists() {
        let cwd = clone_path.as_str();
        if let Err(e) = std::fs::write(Path::new(&clone_path).join(".gitignore"), GITIGNORE_TEMPLATE) {
            log::warn!("[save_repo_settings] failed to write .gitignore (non-fatal): {e}");
        } else {
            let _ = run_cmd("git", &["add", ".gitignore"], Some(cwd), &[]);
        }
        let readme_path = Path::new(&clone_path).join("README.md");
        if !readme_path.exists() {
            if let Err(e) = std::fs::write(&readme_path, README_TEMPLATE) {
                log::warn!("[save_repo_settings] failed to write README.md (non-fatal): {e}");
            } else {
                let _ = run_cmd("git", &["add", "README.md"], Some(cwd), &[]);
            }
        }
        // Commit + push anything staged (non-fatal).
        let has_staged = run_cmd("git", &["diff", "--quiet", "--cached"], Some(cwd), &[]).is_err();
        if has_staged {
            let _ = run_cmd(
                "git",
                &[
                    "-c", "user.name=Migration Utility",
                    "-c", "user.email=migration@vibedata.com",
                    "commit", "-m", "chore: seed managed .gitignore and README",
                ],
                Some(cwd),
                &[],
            );
            if let Err(e) = run_cmd("git", &["push"], Some(cwd), &[("GIT_TERMINAL_PROMPT", "0")]) {
                log::warn!("[save_repo_settings] failed to push seeded files (non-fatal): {e}");
            } else {
                log::info!("[save_repo_settings] seeded .gitignore and README, pushed");
            }
        } else {
            log::info!("[save_repo_settings] seeded files already up to date, nothing to commit");
        }
    }

    // Persist settings with the derived clone path.
    let mut updated = settings;
    updated.migration_repo_full_name = Some(full_name);
    updated.migration_repo_clone_url = Some(clone_url);
    updated.local_clone_path = Some(clone_path);

    crate::db::write_settings(&conn, &updated).inspect_err(|e| {
        log::error!("[save_repo_settings] write_settings failed: {}", e);
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
