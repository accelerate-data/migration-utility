use std::path::Path;

use rusqlite::params;

use tauri::{Emitter, State};
use uuid::Uuid;

use crate::commands::ddl::{check_ddl_stale, compute_file_sha256, dacpac_db_name, extract_ddl_from_dacpac, extract_ddl_from_zip};
use crate::commands::git_ops::{check_dotnet_runtime, git_commit_and_push, DotnetStatus};
use crate::commands::process::{run_cmd, run_cmd_async};
use crate::commands::project::slugify;
use crate::db::DbState;
use crate::types::{CommandError, InitStep, InitStepEvent, InitStepStatus, Project};

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Validate that settings contain a local clone path and GitHub token, and that the
/// clone path is an existing git directory. Returns `(local_clone_path, token)`.
fn validate_create_settings(
    state: &State<'_, DbState>,
) -> Result<(String, String), CommandError> {
    let (lcp, tok) = {
        let conn = state.conn().inspect_err(|e| {
            log::error!("[validate_create_settings] DB lock: {e}");
        })?;
        let s = crate::db::read_settings(&conn)?;
        let lcp = s.local_clone_path.ok_or_else(|| {
            CommandError::Validation("Local clone path not configured in Settings".into())
        })?;
        let tok = s.github_oauth_token.ok_or_else(|| {
            CommandError::Validation("GitHub authentication required".into())
        })?;
        (lcp, tok)
        // conn guard drops here — before filesystem checks
    };

    if !Path::new(&lcp).exists() {
        return Err(CommandError::Validation(format!(
            "Migration repository not found at '{lcp}'. Go to Settings → Connections and click Save & Clone first."
        )));
    }
    if !Path::new(&lcp).join(".git").exists() {
        return Err(CommandError::Validation(format!(
            "'{lcp}' is not a git repository. Go to Settings → Connections and click Save & Clone first."
        )));
    }

    Ok((lcp, tok))
}

/// Execute steps 3-8 of project creation: create dirs, copy source, compute sha256,
/// write metadata, extract DDL, git LFS + commit/push. On failure the caller is
/// responsible for cleaning up `slug_dir`.
fn setup_project_artifacts(
    slug_dir: &Path,
    source_path: &str,
    project: &Project,
    db_name: &str,
    extraction_datetime: &str,
    local_clone_path: &str,
) -> Result<(), CommandError> {
    let source_dir = slug_dir.join("artifacts").join("source");
    let ddl_dir = slug_dir.join("artifacts").join("ddl");

    // 3. Create directory structure.
    std::fs::create_dir_all(&source_dir)?;
    std::fs::create_dir_all(&ddl_dir)?;
    log::debug!("[setup_project_artifacts] created artifact dirs under {}", slug_dir.display());

    // 4. Copy source binary to artifacts/source/.
    let src = Path::new(source_path);
    let source_filename = src.file_name().ok_or_else(|| {
        CommandError::Validation(format!("Invalid source path: {source_path}"))
    })?;
    let source_dest = source_dir.join(source_filename);
    std::fs::copy(src, &source_dest)
        .map_err(|e| CommandError::Io(format!("Failed to copy source file: {e}")))?;
    log::debug!("[setup_project_artifacts] copied source to {}", source_dest.display());

    // 5. Compute source SHA-256 for metadata and future DDL consistency checks.
    let source_sha256 = compute_file_sha256(&source_dest)?;

    // 6. Write metadata.json into artifacts/source/.
    let metadata = serde_json::json!({
        "id": project.id,
        "slug": project.slug,
        "name": project.name,
        "technology": project.technology,
        "createdAt": project.created_at,
        "dbName": db_name,
        "extractionDatetime": extraction_datetime,
        "sourceFilename": source_filename.to_string_lossy(),
        "sourceSha256": source_sha256,
    });
    std::fs::write(
        source_dir.join("metadata.json"),
        serde_json::to_string_pretty(&metadata)
            .map_err(|e| CommandError::Io(format!("JSON serialize error: {e}")))?,
    )?;
    log::debug!("[setup_project_artifacts] wrote metadata.json");

    // 7. Extract DDL from source binary.
    let ext = src.extension().and_then(|e| e.to_str()).unwrap_or("").to_lowercase();
    if ext == "dacpac" {
        extract_ddl_from_dacpac(&source_dest, &ddl_dir)?;
    } else {
        extract_ddl_from_zip(&source_dest, &ddl_dir)?;
    }
    log::info!("[setup_project_artifacts] DDL extraction complete slug={}", project.slug);

    // 8. Enable Git LFS, track source file type, git add → commit → push.
    run_cmd("git", &["lfs", "install"], None, &[])
        .map_err(|e| {
            if e.to_string().contains("is not a git command") || e.to_string().contains("not found") {
                CommandError::Validation(
                    "Git LFS is not installed. Install it and retry:\n\
                     • macOS:   brew install git-lfs\n\
                     • Linux:   apt install git-lfs  (or equivalent)\n\
                     • Windows: winget install Git.LFS\n\
                     Then run: git lfs install".into(),
                )
            } else {
                e
            }
        })?;

    let lfs_pattern = if ext == "dacpac" { "*.dacpac" } else { "artifacts/source/*.zip" };
    run_cmd("git", &["lfs", "track", lfs_pattern], Some(local_clone_path), &[])?;

    // --force because the repo's .gitignore may contain *.dacpac.
    git_commit_and_push(
        local_clone_path,
        &format!("feat: add project {}", project.slug),
        &[".gitattributes", &project.slug],
        true,
    )?;
    log::info!("[setup_project_artifacts] pushed project {} to repo", project.slug);

    Ok(())
}

/// Remove the legacy `artifacts/dacpac/` directory if `artifacts/source/` already
/// exists (project was migrated to the new layout). Stages, commits, and pushes the
/// removal.
fn cleanup_legacy_dacpac_dir(
    slug: &str,
    slug_dir: &Path,
    source_dir: &Path,
    local_clone_path: &str,
) {
    let old_dacpac_dir = slug_dir.join("artifacts").join("dacpac");
    if !source_dir.join("metadata.json").exists() || !old_dacpac_dir.exists() {
        return;
    }

    log::info!("[cleanup_legacy_dacpac_dir] removing stale artifacts/dacpac/ for slug={slug}");
    if let Err(e) = std::fs::remove_dir_all(&old_dacpac_dir) {
        log::warn!("[cleanup_legacy_dacpac_dir] could not remove artifacts/dacpac/: {e}");
    } else {
        // Stage and commit the removal.
        let _ = run_cmd("git", &["rm", "-rf", "--cached",
            &format!("{slug}/artifacts/dacpac")], Some(local_clone_path), &[]);
        let _ = git_commit_and_push(
            local_clone_path,
            &format!("chore: remove legacy artifacts/dacpac/ for {slug}"),
            &[slug],
            false,
        );
    }
}

/// Inject the GitHub OAuth token into an HTTPS clone URL so that git operations
/// against private repos succeed. Returns the original URL if no token is provided.
fn inject_token_into_url(url: &str, token: Option<&str>) -> String {
    match token {
        Some(tok) if !tok.is_empty() => url.replacen("https://", &format!("https://{}@", tok), 1),
        _ => url.to_string(),
    }
}

/// Perform the git pull (or clone) step during startup, emitting events. Returns
/// `Ok(())` on success or if the clone path / URL are not configured (warning only).
async fn startup_git_pull(
    app: &tauri::AppHandle,
    local_clone_path: &Option<String>,
    clone_url: &Option<String>,
    github_token: Option<&str>,
) -> Result<(), CommandError> {
    emit_step(app, InitStep::GitPull, InitStepStatus::Running, None);
    match (local_clone_path, clone_url) {
        (None, _) => {
            log::warn!("[startup_git_pull] local_clone_path not configured — skipping git pull");
            emit_step(app, InitStep::GitPull, InitStepStatus::Warning {
                warnings: vec!["Git repo not configured in Settings → Connections. Skipping sync.".into()],
            }, None);
        }
        (Some(lcp), Some(url)) => {
            match git_pull_or_clone(lcp, url, github_token).await {
                Err(ref e) => {
                    let msg = e.to_string();
                    log::error!("[startup_git_pull] GitPull failed: {msg}");
                    emit_step(app, InitStep::GitPull, InitStepStatus::Error { message: msg.clone() }, None);
                    return Err(CommandError::External(msg));
                }
                Ok(()) => {
                    emit_step(app, InitStep::GitPull, InitStepStatus::Ok, None);
                }
            }
        }
        (Some(lcp), None) if Path::new(lcp).join(".git").exists() => {
            // No clone URL but local .git exists — pull only.
            match git_pull_or_clone(lcp, "", github_token).await {
                Err(ref e) => {
                    let msg = e.to_string();
                    log::error!("[startup_git_pull] GitPull failed: {msg}");
                    emit_step(app, InitStep::GitPull, InitStepStatus::Error { message: msg.clone() }, None);
                    return Err(CommandError::External(msg));
                }
                Ok(()) => {
                    emit_step(app, InitStep::GitPull, InitStepStatus::Ok, None);
                }
            }
        }
        (Some(_), None) => {
            log::warn!("[startup_git_pull] clone_url not configured — skipping git pull");
            emit_step(app, InitStep::GitPull, InitStepStatus::Warning {
                warnings: vec!["Clone URL not configured — using existing local files.".into()],
            }, None);
        }
    }
    Ok(())
}

/// Shared git pull-or-clone + LFS pull helper. Returns `Ok(())` on success.
/// Callers are responsible for emitting step events and error mapping.
async fn git_pull_or_clone(
    local_clone_path: &str,
    clone_url: &str,
    github_token: Option<&str>,
) -> Result<(), CommandError> {
    if Path::new(local_clone_path).join(".git").exists() {
        run_cmd_async("git", &["pull"], Some(local_clone_path), &[("GIT_TERMINAL_PROMPT", "0")]).await?;
    } else {
        let auth_url = inject_token_into_url(clone_url, github_token);
        run_cmd_async("git", &["clone", &auth_url, local_clone_path], None, &[("GIT_TERMINAL_PROMPT", "0")]).await?;
    }
    if let Err(e) = run_cmd_async("git", &["lfs", "pull"], Some(local_clone_path), &[("GIT_TERMINAL_PROMPT", "0")]).await {
        log::warn!("[git_pull_or_clone] git lfs pull failed (non-fatal): {e}");
    }
    Ok(())
}

fn emit_step(app: &tauri::AppHandle, step: InitStep, status: InitStepStatus, project_id: Option<String>) {
    let event = InitStepEvent {
        step,
        status,
        project_id,
    };
    if let Err(e) = app.emit("project:init:step", event) {
        log::warn!("[emit_step] failed to emit: {e}");
    }
}

/// Prepare a Project struct (id, slug, timestamps) without inserting into the DB.
/// Validates the technology string against the `Technology` enum.
/// Requires a connection only for slug collision checks.
fn prepare_project(
    conn: &rusqlite::Connection,
    name: &str,
    technology: &str,
) -> Result<Project, CommandError> {
    let tech = crate::types::Technology::validate(technology)?;
    let id = Uuid::new_v4().to_string();
    let slug = slugify(name, conn)?;
    let created_at = chrono::Utc::now().to_rfc3339();
    Ok(Project { id, slug, name: name.to_string(), technology: tech.to_string(), created_at })
}


// ── project_create_full ────────────────────────────────────────────────────────

/// Create a project: insert DB row, copy source binary to artifacts/source/ (LFS),
/// extract DDL to artifacts/ddl/, commit and push. No Docker or SA password required.
#[allow(clippy::too_many_arguments)]
#[tauri::command]
pub fn project_create_full(
    state: State<'_, DbState>,
    name: String,
    technology: String,
    source_path: String,
    db_name: String,
    extraction_datetime: String,
) -> Result<Project, CommandError> {
    log::info!("[project_create_full] name={} technology={} db_name={}", name, technology, db_name);

    // 1. Validate settings — fail fast with no side effects.
    let (local_clone_path, _token) = validate_create_settings(&state)?;

    // 2. Prepare project identity (id, slug) — checks slug collisions but does NOT insert yet.
    let project = {
        let conn = state.conn().inspect_err(|e| {
            log::error!("[project_create_full] DB lock: {e}");
        })?;
        prepare_project(&conn, &name, &technology)?
    };
    log::debug!("[project_create_full] prepared id={} slug={}", project.id, project.slug);

    // 3–8. Execute external steps. On failure, clean up local dir only (no DB row to rollback).
    let slug_dir = Path::new(&local_clone_path).join(&project.slug);

    if let Err(e) = setup_project_artifacts(&slug_dir, &source_path, &project, &db_name, &extraction_datetime, &local_clone_path) {
        log::error!("[project_create_full] external steps failed: {e}");
        // Clean up local dir only — no DB row was inserted.
        if slug_dir.exists() {
            if let Err(rm_e) = std::fs::remove_dir_all(&slug_dir) {
                log::warn!("[project_create_full] cleanup dir failed (non-fatal): {rm_e}");
            }
        }
        return Err(e);
    }

    // 9. Insert DB row now that external steps succeeded — crash-safe ordering.
    {
        let conn = state.conn().inspect_err(|e| {
            log::error!("[project_create_full] DB lock: {e}");
        })?;
        crate::db::insert_project(&conn, &project)?;
        log::debug!("[project_create_full] row inserted id={} slug={}", project.id, project.slug);
    }

    // 10. Set as active project.
    {
        let conn = state.conn()?;
        let mut settings = crate::db::read_settings(&conn)?;
        settings.active_project_id = Some(project.id.clone());
        crate::db::write_settings(&conn, &settings)?;
    }
    log::info!("[project_create_full] done id={} slug={}", project.id, project.slug);
    Ok(project)
}

// ── project_init ──────────────────────────────────────────────────────────────

/// Canonical initialization orchestrator. Emits `project:init:step` events per step.
/// Steps: GitPull → DdlCheck (→ DdlExtract if stale or missing).
#[tauri::command]
pub async fn project_init(
    app: tauri::AppHandle,
    state: State<'_, DbState>,
    id: String,
) -> Result<(), CommandError> {
    log::info!("[project_init] id={}", id);

    let (slug, technology, local_clone_path, clone_url, github_token) = {
        let conn = state.conn().inspect_err(|e| {
            log::error!("[project_init] DB lock: {e}");
        })?;
        let (slug, technology) = conn
            .query_row(
                "SELECT slug, technology FROM projects WHERE id = ?1",
                params![id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .map_err(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => CommandError::NotFound(format!("project {id}")),
                other => CommandError::from(other),
            })?;
        let settings = crate::db::read_settings(&conn)?;
        let lcp = settings.local_clone_path.ok_or_else(|| {
            CommandError::Validation("Local clone path not configured in Settings".into())
        })?;
        let url = settings.migration_repo_clone_url.ok_or_else(|| {
            CommandError::Validation("Migration repository not configured in Settings".into())
        })?;
        (slug, technology, lcp, url, settings.github_oauth_token)
    };

    // ── Step 0: .NET runtime check ────────────────────────────────────────────
    emit_step(&app, InitStep::DotnetCheck, InitStepStatus::Running, Some(id.clone()));
    match check_dotnet_runtime(technology == "sql_server") {
        DotnetStatus::Ok(_ver) => {
            emit_step(&app, InitStep::DotnetCheck, InitStepStatus::Ok, Some(id.clone()));
        }
        DotnetStatus::Warning(msg) => {
            emit_step(&app, InitStep::DotnetCheck, InitStepStatus::Warning {
                warnings: vec![msg],
            }, Some(id.clone()));
        }
        DotnetStatus::Error(msg) => {
            emit_step(&app, InitStep::DotnetCheck, InitStepStatus::Error { message: msg.clone() }, Some(id.clone()));
            return Err(CommandError::External(msg));
        }
    }

    // ── Step 1: GitPull ───────────────────────────────────────────────────────
    emit_step(&app, InitStep::GitPull, InitStepStatus::Running, Some(id.clone()));
    match git_pull_or_clone(&local_clone_path, &clone_url, github_token.as_deref()).await {
        Err(ref e) => {
            let msg = e.to_string();
            log::error!("[project_init] GitPull failed: {msg}");
            emit_step(&app, InitStep::GitPull, InitStepStatus::Error { message: msg.clone() }, Some(id.clone()));
            return Err(CommandError::External(msg));
        }
        Ok(()) => {
            emit_step(&app, InitStep::GitPull, InitStepStatus::Ok, Some(id.clone()));
        }
    }

    // ── Steps 2-3: DDL check and optional re-extraction ───────────────────────
    run_project_ddl_steps(&app, &id, &slug, &technology, &local_clone_path).await
}

/// Run DdlCheck (→ DdlExtract if stale or missing) for a single project.
/// Called from `project_init` and `app_startup_sync`.
async fn run_project_ddl_steps(
    app: &tauri::AppHandle,
    id: &str,
    slug: &str,
    technology: &str,
    local_clone_path: &str,
) -> Result<(), CommandError> {
    let pid = Some(id.to_string());
    let slug_dir = Path::new(local_clone_path).join(slug);
    let source_dir = slug_dir.join("artifacts").join("source");
    let ddl_dir = slug_dir.join("artifacts").join("ddl");

    // ── Cleanup: remove legacy artifacts/dacpac/ if source/ already exists ───
    cleanup_legacy_dacpac_dir(slug, &slug_dir, &source_dir, local_clone_path);

    // ── Step 2: DdlCheck ─────────────────────────────────────────────────────
    emit_step(app, InitStep::DdlCheck, InitStepStatus::Running, pid.clone());

    // Read metadata.json to get source_filename and source_sha256.
    let metadata_path = source_dir.join("metadata.json");
    let ddl_stale = check_ddl_stale(&metadata_path, &source_dir, &ddl_dir);

    match &ddl_stale {
        Ok(false) => {
            log::info!("[run_project_ddl_steps] DDL is current slug={slug}");
            emit_step(app, InitStep::DdlCheck, InitStepStatus::Ok, pid.clone());
            emit_step(app, InitStep::DdlExtract, InitStepStatus::Ok, pid);
            return Ok(());
        }
        Ok(true) => {
            log::info!("[run_project_ddl_steps] DDL is stale or missing, re-extracting slug={slug}");
            emit_step(app, InitStep::DdlCheck, InitStepStatus::Warning {
                warnings: vec!["DDL files are stale or missing — re-extracting.".into()],
            }, pid.clone());
        }
        Err(ref e) => {
            let msg = e.to_string();
            if !metadata_path.exists() {
                // Check for legacy projects: DacPac stored at artifacts/dacpac/ (old path).
                let old_dacpac_dir = slug_dir.join("artifacts").join("dacpac");
                if old_dacpac_dir.exists() && technology == "sql_server" {
                    log::info!("[run_project_ddl_steps] legacy dacpac project detected slug={slug} — migrating");
                    emit_step(app, InitStep::DdlCheck, InitStepStatus::Warning {
                        warnings: vec!["Legacy project — migrating from old DacPac location.".into()],
                    }, pid.clone());
                    emit_step(app, InitStep::DdlExtract, InitStepStatus::Running, pid.clone());
                    match migrate_legacy_dacpac(slug, &slug_dir, &source_dir, &ddl_dir, local_clone_path) {
                        Ok(()) => {
                            log::info!("[run_project_ddl_steps] legacy migration complete slug={slug}");
                            emit_step(app, InitStep::DdlExtract, InitStepStatus::Ok, pid);
                            return Ok(());
                        }
                        Err(e) => {
                            let msg = e.to_string();
                            log::error!("[run_project_ddl_steps] legacy migration failed slug={slug}: {msg}");
                            emit_step(app, InitStep::DdlExtract, InitStepStatus::Error { message: msg.clone() }, pid);
                            return Err(CommandError::External(msg));
                        }
                    }
                }
                // No legacy DacPac found — skip gracefully.
                log::warn!("[run_project_ddl_steps] metadata.json absent and no legacy DacPac for slug={slug} — skipping");
                emit_step(app, InitStep::DdlCheck, InitStepStatus::Warning {
                    warnings: vec!["Project was created before DDL extraction. Re-create the project to enable DDL sync.".into()],
                }, pid.clone());
                emit_step(app, InitStep::DdlExtract, InitStepStatus::Warning {
                    warnings: vec!["Skipped — no source file available.".into()],
                }, pid);
                return Ok(());
            }
            log::error!("[run_project_ddl_steps] DdlCheck failed slug={slug}: {msg}");
            emit_step(app, InitStep::DdlCheck, InitStepStatus::Error { message: msg.clone() }, pid.clone());
            return Err(CommandError::External(msg));
        }
    }

    // ── Step 3: DdlExtract ───────────────────────────────────────────────────
    emit_step(app, InitStep::DdlExtract, InitStepStatus::Running, pid.clone());

    let extract_result = (|| -> Result<(), CommandError> {
        // Find the source file.
        let metadata: serde_json::Value = {
            let content = std::fs::read_to_string(&metadata_path)
                .map_err(|e| CommandError::Io(format!("Cannot read metadata.json: {e}")))?;
            serde_json::from_str(&content)
                .map_err(|e| CommandError::Io(format!("Cannot parse metadata.json: {e}")))?
        };
        let source_filename = metadata["sourceFilename"].as_str().ok_or_else(|| {
            CommandError::Validation("metadata.json missing sourceFilename".into())
        })?;
        let source_path = source_dir.join(source_filename);
        if !source_path.exists() {
            return Err(CommandError::Io(format!(
                "Source file '{}' not found — git lfs pull may be needed", source_path.display()
            )));
        }

        let ext = source_path.extension().and_then(|e| e.to_str()).unwrap_or("").to_lowercase();
        if ext == "dacpac" || technology == "sql_server" {
            extract_ddl_from_dacpac(&source_path, &ddl_dir)
        } else {
            extract_ddl_from_zip(&source_path, &ddl_dir)
        }?;

        // Commit and push updated DDL.
        git_commit_and_push(
            local_clone_path,
            &format!("chore: refresh DDL for {slug}"),
            &[slug],
            false,
        )?;
        Ok(())
    })();

    match extract_result {
        Ok(()) => {
            emit_step(app, InitStep::DdlExtract, InitStepStatus::Ok, pid.clone());
            log::info!("[run_project_ddl_steps] DDL extraction complete slug={slug}");
            Ok(())
        }
        Err(ref e) => {
            let msg = e.to_string();
            log::error!("[run_project_ddl_steps] DdlExtract failed slug={slug}: {msg}");
            emit_step(app, InitStep::DdlExtract, InitStepStatus::Error { message: msg.clone() }, pid);
            Err(CommandError::External(msg))
        }
    }
}

/// Migrate a legacy project whose DacPac lives at `artifacts/dacpac/` to the new layout:
/// move DacPac → `artifacts/source/`, remove old `artifacts/dacpac/`, write new `metadata.json`,
/// extract DDL to `artifacts/ddl/`, then commit and push.
fn migrate_legacy_dacpac(
    slug: &str,
    slug_dir: &Path,
    source_dir: &Path,
    ddl_dir: &Path,
    local_clone_path: &str,
) -> Result<(), CommandError> {
    let old_dacpac_dir = slug_dir.join("artifacts").join("dacpac");
    let old_metadata_path = old_dacpac_dir.join("metadata.json");

    // Find the .dacpac file in the old directory.
    let dacpac_path = std::fs::read_dir(&old_dacpac_dir)
        .map_err(|e| CommandError::Io(format!("Cannot read legacy dacpac dir: {e}")))?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| p.extension().and_then(|e| e.to_str()) == Some("dacpac"))
        .ok_or_else(|| CommandError::Validation(
            format!("No .dacpac file found in legacy directory {}", old_dacpac_dir.display()),
        ))?;

    // Read old metadata for field preservation.
    let old_meta: serde_json::Value = if old_metadata_path.exists() {
        let s = std::fs::read_to_string(&old_metadata_path)
            .map_err(|e| CommandError::Io(format!("Cannot read legacy metadata.json: {e}")))?;
        serde_json::from_str(&s)
            .map_err(|e| CommandError::Io(format!("Cannot parse legacy metadata.json: {e}")))?
    } else {
        serde_json::json!({})
    };

    std::fs::create_dir_all(source_dir)?;
    std::fs::create_dir_all(ddl_dir)?;

    let filename = dacpac_path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| CommandError::Validation("Invalid DacPac filename".into()))?;

    let source_dest = source_dir.join(filename);
    std::fs::copy(&dacpac_path, &source_dest)
        .map_err(|e| CommandError::Io(format!("Failed to move DacPac to artifacts/source/: {e}")))?;
    log::debug!("[migrate_legacy_dacpac] copied DacPac to {}", source_dest.display());

    let sha256 = compute_file_sha256(&source_dest)?;

    // Remove old artifacts/dacpac/ directory now that the file is safely in source/.
    std::fs::remove_dir_all(&old_dacpac_dir)
        .map_err(|e| CommandError::Io(format!("Failed to remove legacy artifacts/dacpac/: {e}")))?;
    log::debug!("[migrate_legacy_dacpac] removed legacy artifacts/dacpac/ slug={slug}");

    // Write new metadata.json compatible with check_ddl_stale.
    std::fs::write(
        source_dir.join("metadata.json"),
        serde_json::to_string_pretty(&serde_json::json!({
            "id":                  old_meta.get("id").and_then(|v| v.as_str()).unwrap_or(""),
            "slug":                slug,
            "name":                old_meta.get("name").and_then(|v| v.as_str()).unwrap_or(""),
            "technology":          "sql_server",
            "createdAt":           old_meta.get("createdAt").and_then(|v| v.as_str()).unwrap_or(""),
            "dbName":              old_meta.get("dbName").and_then(|v| v.as_str()).unwrap_or(""),
            "extractionDatetime":  old_meta.get("extractionDatetime").and_then(|v| v.as_str()).unwrap_or(""),
            "sourceFilename":      filename,
            "sourceSha256":        sha256,
        }))
        .map_err(|e| CommandError::Io(format!("JSON serialize error: {e}")))?,
    )?;
    log::debug!("[migrate_legacy_dacpac] wrote new metadata.json slug={slug}");

    extract_ddl_from_dacpac(&source_dest, ddl_dir)?;
    log::info!("[migrate_legacy_dacpac] DDL extracted slug={slug}");

    // Commit and push the migrated files.
    run_cmd("git", &["lfs", "install"], None, &[])?;
    run_cmd("git", &["lfs", "track", "*.dacpac"], Some(local_clone_path), &[])?;
    git_commit_and_push(
        local_clone_path,
        &format!("migrate: {slug} — DDL extraction from legacy DacPac"),
        &[".gitattributes", slug],
        true,
    )?;
    log::info!("[migrate_legacy_dacpac] committed and pushed slug={slug}");

    Ok(())
}

/// Startup sync: git pull once (global), then DDL check for each project in parallel.
#[tauri::command]
pub async fn app_startup_sync(
    app: tauri::AppHandle,
    state: State<'_, DbState>,
) -> Result<(), CommandError> {
    log::info!("[app_startup_sync] starting multi-project startup sync");

    struct ProjectRow {
        id: String,
        slug: String,
        technology: String,
    }

    let (rows, local_clone_path, clone_url, github_token) = {
        let conn = state.conn().inspect_err(|e| {
            log::error!("[app_startup_sync] DB lock: {e}");
        })?;
        let settings = crate::db::read_settings(&conn).inspect_err(|e| {
            log::error!("[app_startup_sync] read_settings failed: {e}");
        })?;
        let mut stmt = conn
            .prepare("SELECT id, slug, technology FROM projects ORDER BY created_at")
            .map_err(CommandError::from)?;
        let rows: Vec<ProjectRow> = stmt
            .query_map([], |row| {
                Ok(ProjectRow {
                    id: row.get(0)?,
                    slug: row.get(1)?,
                    technology: row.get(2)?,
                })
            })
            .map_err(CommandError::from)?
            .collect::<Result<_, rusqlite::Error>>()
            .map_err(CommandError::from)?;
        (rows, settings.local_clone_path, settings.migration_repo_clone_url, settings.github_oauth_token)
    };

    if rows.is_empty() {
        log::info!("[app_startup_sync] no projects configured, nothing to sync");
        return Ok(());
    }

    log::info!("[app_startup_sync] syncing {} project(s)", rows.len());

    // ── Step 0: .NET runtime check (global, once) ─────────────────────────────
    let has_sql_server = rows.iter().any(|r| r.technology == "sql_server");
    emit_step(&app, InitStep::DotnetCheck, InitStepStatus::Running, None);
    match check_dotnet_runtime(has_sql_server) {
        DotnetStatus::Ok(ver) => {
            log::info!("[app_startup_sync] dotnet runtime ok: {ver}");
            emit_step(&app, InitStep::DotnetCheck, InitStepStatus::Ok, None);
        }
        DotnetStatus::Warning(msg) => {
            log::warn!("[app_startup_sync] {msg}");
            emit_step(&app, InitStep::DotnetCheck, InitStepStatus::Warning {
                warnings: vec![msg],
            }, None);
        }
        DotnetStatus::Error(msg) => {
            log::error!("[app_startup_sync] {msg}");
            emit_step(&app, InitStep::DotnetCheck, InitStepStatus::Error { message: msg.clone() }, None);
            return Err(CommandError::External(msg));
        }
    }

    // ── Step 1: GitPull (global, once) ────────────────────────────────────────
    startup_git_pull(&app, &local_clone_path, &clone_url, github_token.as_deref()).await?;

    // ── Steps 2-3: DDL check per project, in parallel ─────────────────────────
    let Some(lcp) = local_clone_path else {
        log::warn!("[app_startup_sync] local_clone_path not configured — skipping DDL steps");
        return Ok(());
    };
    if lcp.is_empty() {
        log::warn!("[app_startup_sync] local_clone_path is empty — skipping DDL steps");
        return Ok(());
    }
    let mut join_set = tokio::task::JoinSet::new();
    for row in rows {
        let app_clone = app.clone();
        let lcp_clone = lcp.clone();
        join_set.spawn(async move {
            run_project_ddl_steps(&app_clone, &row.id, &row.slug, &row.technology, &lcp_clone).await
        });
    }

    let mut errors: Vec<String> = Vec::new();
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                log::error!("[app_startup_sync] project DDL steps failed: {e}");
                errors.push(e.to_string());
            }
            Err(e) => {
                log::error!("[app_startup_sync] task panicked: {e}");
                errors.push(format!("task panicked: {e}"));
            }
        }
    }

    if errors.is_empty() {
        log::info!("[app_startup_sync] all projects synced successfully");
        Ok(())
    } else {
        Err(CommandError::External(errors.join("; ")))
    }
}

// ── project_delete_full ───────────────────────────────────────────────────────

/// Fully delete a project: remove local dir, git cleanup, and DB row removal.
/// All external operations are best-effort; the DB row is always removed.
#[tauri::command]
pub fn project_delete_full(
    state: State<'_, DbState>,
    id: String,
) -> Result<(), CommandError> {
    log::info!("[project_delete_full] id={}", id);

    let (slug, local_clone_path, has_token) = {
        let conn = state.conn().inspect_err(|e| {
            log::error!("[project_delete_full] DB lock: {e}");
        })?;
        let slug: String = conn
            .query_row(
                "SELECT slug FROM projects WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .map_err(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => CommandError::NotFound(format!("project {id}")),
                other => CommandError::from(other),
            })?;
        let s = crate::db::read_settings(&conn)?;
        (slug, s.local_clone_path, s.github_oauth_token.is_some())
    };

    // Step 1: Delete local project directory (best-effort).
    if let Some(ref lcp) = local_clone_path {
        let slug_dir = Path::new(lcp).join(&slug);
        if slug_dir.exists() {
            if let Err(e) = std::fs::remove_dir_all(&slug_dir) {
                log::warn!("[project_delete_full] remove local dir {} (non-fatal): {e}", slug_dir.display());
            } else {
                log::debug!("[project_delete_full] removed local dir {}", slug_dir.display());
            }
        }
    }

    // Step 2: Git rm + commit + push (best-effort).
    if let Some(ref lcp) = local_clone_path {
        if has_token {
            if let Err(e) = run_cmd("git", &["rm", "-r", "--ignore-unmatch", &slug], Some(lcp), &[("GIT_TERMINAL_PROMPT", "0")]) {
                log::warn!("[project_delete_full] git rm (non-fatal): {e}");
            }
            // git rm already stages; pass empty paths so git_commit_and_push only checks + commits + pushes.
            if let Err(e) = git_commit_and_push(
                lcp,
                &format!("chore: remove project {slug}"),
                &[],
                false,
            ) {
                log::warn!("[project_delete_full] git commit/push (non-fatal): {e}");
            }
        }
    }

    // Step 3: Delete DB row and clear active_project_id.
    {
        let conn = state.conn()?;
        conn.execute("DELETE FROM projects WHERE id = ?1", params![id])
            .map_err(|e| {
                log::error!("[project_delete_full] DB delete failed: {e}");
                CommandError::from(e)
            })?;
        let mut s = crate::db::read_settings(&conn)?;
        if s.active_project_id.as_deref() == Some(&id) {
            s.active_project_id = None;
            crate::db::write_settings(&conn, &s)?;
            log::debug!("[project_delete_full] cleared active_project_id");
        }
    }
    log::info!("[project_delete_full] deleted project id={} slug={}", id, slug);
    Ok(())
}

// ── project_reset_local ───────────────────────────────────────────────────────

/// Reset local state for a project: delete the local slug dir so the next
/// `project_init` re-syncs from git. No Docker operations needed.
#[tauri::command]
pub fn project_reset_local(
    state: State<'_, DbState>,
    id: String,
) -> Result<(), CommandError> {
    log::info!("[project_reset_local] id={}", id);

    let (slug, local_clone_path) = {
        let conn = state.conn().inspect_err(|e| {
            log::error!("[project_reset_local] DB lock: {e}");
        })?;
        let slug: String = conn
            .query_row(
                "SELECT slug FROM projects WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .map_err(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => CommandError::NotFound(format!("project {id}")),
                other => CommandError::from(other),
            })?;
        let s = crate::db::read_settings(&conn)?;
        let lcp = s.local_clone_path.ok_or_else(|| {
            CommandError::Validation("Local clone path not configured".into())
        })?;
        (slug, lcp)
    };

    let slug_dir = Path::new(&local_clone_path).join(&slug);
    if slug_dir.exists() {
        std::fs::remove_dir_all(&slug_dir).map_err(|e| {
            CommandError::Io(format!("Failed to remove local dir {}: {e}", slug_dir.display()))
        })?;
        log::debug!("[project_reset_local] removed local dir {}", slug_dir.display());
    }

    log::info!("[project_reset_local] local state cleared for id={} slug={}", id, slug);
    Ok(())
}

// ── project_detect_databases ──────────────────────────────────────────────────

/// Parse the DacPac file and return the source database name.
/// No Docker or sqlpackage is needed — the DacPac is a ZIP containing DacMetadata.xml.
#[tauri::command]
pub fn project_detect_databases(dacpac_path: String) -> Result<Vec<String>, CommandError> {
    log::info!("[project_detect_databases] dacpac_path={}", dacpac_path);
    let db_name = dacpac_db_name(&dacpac_path)?;
    log::info!("[project_detect_databases] detected db_name={db_name}");
    Ok(vec![db_name])
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use crate::types::AppSettings;

    /// Helper: prepare + insert a project in tests.
    fn create_test_project(conn: &rusqlite::Connection, name: &str, technology: &str) -> Project {
        let p = prepare_project(conn, name, technology).unwrap();
        db::insert_project(conn, &p).unwrap();
        p
    }

    #[test]
    fn insert_project_roundtrip() {
        let conn = db::open_in_memory().unwrap();
        let project = create_test_project(&conn, "Test Project", "sql_server");
        assert_eq!(project.name, "Test Project");
        assert_eq!(project.slug, "test-project");
        assert_eq!(project.technology, "sql_server");
        assert!(!project.id.is_empty());
    }

    #[test]
    fn insert_project_slug_collision() {
        let conn = db::open_in_memory().unwrap();
        let p1 = create_test_project(&conn, "My Project", "sql_server");
        let p2 = create_test_project(&conn, "My Project", "fabric_warehouse");
        assert_eq!(p1.slug, "my-project");
        assert_ne!(p1.slug, p2.slug, "collision must produce unique slug");
    }

    #[test]
    fn insert_project_technology_variants() {
        let conn = db::open_in_memory().unwrap();
        for tech in &["sql_server", "fabric_warehouse", "fabric_lakehouse", "snowflake"] {
            let p = create_test_project(&conn, tech, tech);
            assert_eq!(p.technology, *tech);
        }
    }

    #[test]
    fn prepare_project_does_not_insert() {
        let conn = db::open_in_memory().unwrap();
        let p = prepare_project(&conn, "Ghost", "sql_server").unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM projects WHERE id = ?1", params![p.id], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0, "prepare_project must not insert a DB row");
    }

    #[test]
    fn project_sets_active() {
        let conn = db::open_in_memory().unwrap();
        let p = create_test_project(&conn, "Acme", "sql_server");
        let mut s = AppSettings::default();
        s.active_project_id = Some(p.id.clone());
        db::write_settings(&conn, &s).unwrap();
        let read = db::read_settings(&conn).unwrap();
        assert_eq!(read.active_project_id.as_deref(), Some(p.id.as_str()));
    }

    #[test]
    fn project_delete_clears_active_when_matches() {
        let conn = db::open_in_memory().unwrap();
        let p = create_test_project(&conn, "Alpha", "sql_server");
        let mut s = AppSettings::default();
        s.active_project_id = Some(p.id.clone());
        db::write_settings(&conn, &s).unwrap();

        conn.execute("DELETE FROM projects WHERE id = ?1", params![p.id]).unwrap();
        let mut s2 = db::read_settings(&conn).unwrap();
        if s2.active_project_id.as_deref() == Some(&p.id) {
            s2.active_project_id = None;
            db::write_settings(&conn, &s2).unwrap();
        }
        let after = db::read_settings(&conn).unwrap();
        assert!(after.active_project_id.is_none());
    }

    #[test]
    fn reset_does_not_affect_db_row() {
        let conn = db::open_in_memory().unwrap();
        let p = create_test_project(&conn, "Beta", "snowflake");
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM projects WHERE id = ?1", params![p.id], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn migration_004_adds_technology_drops_sa_password_and_port() {
        let conn = db::open_in_memory().unwrap();
        // Verify technology column exists.
        let has_technology: bool = conn.query_row(
            "SELECT COUNT(*) > 0 FROM pragma_table_info('projects') WHERE name='technology'",
            [],
            |row| row.get(0),
        ).unwrap();
        assert!(has_technology, "technology column must exist after migration 004");

        // Verify sa_password and port are gone.
        let has_sa_password: bool = conn.query_row(
            "SELECT COUNT(*) > 0 FROM pragma_table_info('projects') WHERE name='sa_password'",
            [],
            |row| row.get(0),
        ).unwrap();
        assert!(!has_sa_password, "sa_password column must be removed by migration 004");

        let has_port: bool = conn.query_row(
            "SELECT COUNT(*) > 0 FROM pragma_table_info('projects') WHERE name='port'",
            [],
            |row| row.get(0),
        ).unwrap();
        assert!(!has_port, "port column must be removed by migration 004");
    }

}
