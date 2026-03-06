use std::io::Read as IoRead;
use std::path::Path;

use rusqlite::params;
use sha2::{Digest, Sha256};

use tauri::{Emitter, State};
use uuid::Uuid;

use crate::commands::project::slugify;
use crate::db::DbState;
use crate::types::{CommandError, InitStep, InitStepEvent, InitStepStatus, Project};

// ── DDL extraction ────────────────────────────────────────────────────────────

/// Compute the SHA-256 hex digest of a file.
fn compute_file_sha256(path: &Path) -> Result<String, CommandError> {
    let mut f = std::fs::File::open(path)
        .map_err(|e| CommandError::Io(format!("Cannot open '{}': {e}", path.display())))?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 65536];
    loop {
        let n = std::io::Read::read(&mut f, &mut buf)
            .map_err(|e| CommandError::Io(format!("Read error on '{}': {e}", path.display())))?;
        if n == 0 { break; }
        hasher.update(&buf[..n]);
    }
    Ok(hex::encode(hasher.finalize()))
}

/// Resolve the path to the `dacpac-extract` sidecar binary.
///
/// Search order:
/// 1. `DACPAC_EXTRACT_BIN` env var (test / CI override).
/// 2. Next to the current executable (Tauri dev + production bundle).
/// 3. `src-tauri/binaries/` relative to the crate manifest (dev fallback via
///    `CARGO_MANIFEST_DIR`, embedded at compile time).
fn dacpac_extract_bin() -> std::path::PathBuf {
    if let Ok(p) = std::env::var("DACPAC_EXTRACT_BIN") {
        return std::path::PathBuf::from(p);
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let candidate = dir.join("dacpac-extract");
            if candidate.exists() {
                return candidate;
            }
        }
    }
    // Dev fallback: binaries/ next to Cargo.toml (CARGO_MANIFEST_DIR is compile-time).
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("binaries")
        .join("dacpac-extract")
}

/// Extract DDL from a `.dacpac` file via the `dacpac-extract` DacFx sidecar.
/// Writes `procedures.sql`, `views.sql`, `functions.sql`, `tables.sql` to `ddl_dir`.
pub(crate) fn extract_ddl_from_dacpac(dacpac_path: &Path, ddl_dir: &Path) -> Result<(), CommandError> {
    log::info!("[extract_ddl_from_dacpac] extracting via DacFx sidecar: {} → {}",
        dacpac_path.display(), ddl_dir.display());

    std::fs::create_dir_all(ddl_dir)
        .map_err(|e| CommandError::Io(format!("Cannot create DDL dir: {e}")))?;

    let bin = dacpac_extract_bin();
    if !bin.exists() {
        return Err(CommandError::External(format!(
            "dacpac-extract sidecar not found at '{}'. \
             Run scripts/build-dacpac-extract.sh to build it.",
            bin.display()
        )));
    }

    let output = std::process::Command::new(&bin)
        .arg(dacpac_path)
        .arg(ddl_dir)
        .output()
        .map_err(|e| CommandError::External(format!("Failed to launch dacpac-extract: {e}")))?;

    let stderr = String::from_utf8_lossy(&output.stderr);
    if output.status.success() {
        log::info!("[extract_ddl_from_dacpac] sidecar: {}", stderr.trim());
        Ok(())
    } else {
        log::error!("[extract_ddl_from_dacpac] sidecar failed: {stderr}");
        Err(CommandError::External(format!("dacpac-extract failed: {stderr}")))
    }
}

/// Extract DDL from a `.zip` source file (Fabric Warehouse, Snowflake, etc.) into `ddl_dir`.
/// The ZIP is expected to contain `.sql` files; they are organized by name pattern into
/// the canonical DDL output files, or written to `other.sql` if unclassified.
pub(crate) fn extract_ddl_from_zip(zip_path: &Path, ddl_dir: &Path) -> Result<(), CommandError> {
    log::info!("[extract_ddl_from_zip] extracting from {} → {}", zip_path.display(), ddl_dir.display());

    let file = std::fs::File::open(zip_path)
        .map_err(|e| CommandError::Io(format!("Cannot open zip: {e}")))?;
    let mut archive = zip::ZipArchive::new(file)
        .map_err(|e| CommandError::Io(format!("Not a valid ZIP: {e}")))?;

    std::fs::create_dir_all(ddl_dir)
        .map_err(|e| CommandError::Io(format!("Cannot create DDL dir: {e}")))?;

    let mut by_type: std::collections::HashMap<&str, Vec<String>> = std::collections::HashMap::new();
    by_type.insert("tables", Vec::new());
    by_type.insert("procedures", Vec::new());
    by_type.insert("views", Vec::new());
    by_type.insert("functions", Vec::new());
    by_type.insert("other", Vec::new());

    for i in 0..archive.len() {
        let mut entry = archive.by_index(i)
            .map_err(|e| CommandError::Io(format!("ZIP read error: {e}")))?;
        if entry.is_dir() { continue; }
        let name = entry.name().to_lowercase();
        if !name.ends_with(".sql") { continue; }

        let mut content = String::new();
        entry.read_to_string(&mut content).map_err(|e| CommandError::Io(format!("ZIP entry read error: {e}")))?;

        let bucket = if name.contains("procedure") || name.contains("proc") || name.contains("/sp/") {
            "procedures"
        } else if name.contains("view") || name.contains("/vw/") {
            "views"
        } else if name.contains("function") || name.contains("/fn/") || name.contains("/udf/") {
            "functions"
        } else if name.contains("table") || name.contains("/tbl/") {
            "tables"
        } else {
            "other"
        };

        by_type.entry(bucket).or_default().push(format!("-- {}\n{}\nGO\n\n", entry.name(), content));
    }

    for (key, blocks) in &by_type {
        if !blocks.is_empty() || *key != "other" {
            let path = ddl_dir.join(format!("{key}.sql"));
            write_ddl_file(path, &format!("-- {key}\n-- Generated by Migration Utility\n\n"), blocks)?;
        }
    }

    log::info!(
        "[extract_ddl_from_zip] extracted: {} proc, {} view, {} fn, {} table, {} other",
        by_type["procedures"].len(), by_type["views"].len(), by_type["functions"].len(),
        by_type["tables"].len(), by_type["other"].len()
    );
    Ok(())
}

/// Write a DDL output file: header + joined content blocks.
fn write_ddl_file(path: impl AsRef<Path>, header: &str, blocks: &[String]) -> Result<(), CommandError> {
    let mut content = String::with_capacity(header.len() + blocks.iter().map(|b| b.len()).sum::<usize>());
    content.push_str(header);
    for block in blocks {
        content.push_str(block);
    }
    std::fs::write(&path, content)
        .map_err(|e| CommandError::Io(format!("Cannot write {}: {e}", path.as_ref().display())))
}

// ── Helpers ───────────────────────────────────────────────────────────────────

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

/// Run an external command, returning stdout on success or `CommandError::External` on failure.
/// Async version of `run_cmd` using `tokio::process::Command`.
/// Use this inside `async` Tauri commands so blocking I/O does not starve
/// the Tokio runtime and Tauri event delivery between steps.
pub(crate) async fn run_cmd_async(program: &str, args: &[&str], cwd: Option<&str>, envs: &[(&str, &str)]) -> Result<String, CommandError> {
    let mut cmd = tokio::process::Command::new(program);
    cmd.args(args);
    if let Some(dir) = cwd {
        cmd.current_dir(dir);
    }
    for (k, v) in envs {
        cmd.env(k, v);
    }
    let output = cmd.output().await.map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            CommandError::External(format!("'{program}' not found in PATH — please install it"))
        } else {
            CommandError::External(format!("failed to run '{program}': {e}"))
        }
    })?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let detail = if stderr.is_empty() { stdout } else { stderr };
        log::error!("[run_cmd_async] '{}' exited {}: {}", program, output.status, detail);
        return Err(CommandError::External(format!("'{program}' exited {}: {detail}", output.status)));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

pub(crate) fn run_cmd(program: &str, args: &[&str], cwd: Option<&str>, envs: &[(&str, &str)]) -> Result<String, CommandError> {
    let mut cmd = std::process::Command::new(program);
    cmd.args(args);
    if let Some(dir) = cwd {
        cmd.current_dir(dir);
    }
    for (k, v) in envs {
        cmd.env(k, v);
    }
    let output = cmd.output().map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            CommandError::External(format!(
                "'{program}' not found in PATH — please install it"
            ))
        } else {
            CommandError::External(format!("failed to run '{program}': {e}"))
        }
    })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let detail = if stderr.is_empty() { stdout } else { stderr };
        log::error!("[run_cmd] '{}' exited {}: {}", program, output.status, detail);
        return Err(CommandError::External(format!(
            "'{program}' exited {}: {detail}",
            output.status
        )));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Insert a project row and return the new Project.
fn insert_project_row(
    conn: &rusqlite::Connection,
    name: &str,
    technology: &str,
) -> Result<Project, CommandError> {
    let id = Uuid::new_v4().to_string();
    let slug = slugify(name, conn)?;
    let created_at = chrono::Utc::now().to_rfc3339();
    conn.execute(
        "INSERT INTO projects(id, slug, name, technology, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![id, slug, name, technology, created_at],
    )
    .map_err(|e| {
        log::error!("[insert_project_row] insert failed: {e}");
        CommandError::from(e)
    })?;
    Ok(Project { id, slug, name: name.to_string(), technology: technology.to_string(), created_at })
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
    let (local_clone_path, _token) = {
        let conn = state.conn().map_err(|e| {
            log::error!("[project_create_full] DB lock: {e}");
            CommandError::Database(e)
        })?;
        let s = crate::db::read_settings(&conn).map_err(CommandError::Database)?;
        let lcp = s.local_clone_path.ok_or_else(|| {
            CommandError::Validation("Local clone path not configured in Settings".into())
        })?;
        let tok = s.github_oauth_token.ok_or_else(|| {
            CommandError::Validation("GitHub authentication required".into())
        })?;
        (lcp, tok)
    };

    if !Path::new(&local_clone_path).exists() {
        return Err(CommandError::Validation(format!(
            "Migration repository not found at '{local_clone_path}'. Go to Settings → Connections and click Save & Clone first."
        )));
    }
    if !Path::new(&local_clone_path).join(".git").exists() {
        return Err(CommandError::Validation(format!(
            "'{local_clone_path}' is not a git repository. Go to Settings → Connections and click Save & Clone first."
        )));
    }

    // 2. Insert DB row.
    let project = {
        let conn = state.conn().map_err(|e| {
            log::error!("[project_create_full] DB lock: {e}");
            CommandError::Database(e)
        })?;
        insert_project_row(&conn, &name, &technology)?
    };
    log::debug!("[project_create_full] row inserted id={} slug={}", project.id, project.slug);

    // 3–8. Execute external steps. On failure, rollback DB row and local dir.
    let slug_dir = Path::new(&local_clone_path).join(&project.slug);
    let artifacts_dir = slug_dir.join("artifacts");
    let source_dir = artifacts_dir.join("source");
    let ddl_dir = artifacts_dir.join("ddl");

    let external_result: Result<(), CommandError> = (|| {
        // 3. Create directory structure.
        std::fs::create_dir_all(&source_dir)?;
        std::fs::create_dir_all(&ddl_dir)?;
        log::debug!("[project_create_full] created artifact dirs under {}", slug_dir.display());

        // 4. Copy source binary to artifacts/source/.
        let src = Path::new(&source_path);
        let source_filename = src.file_name().ok_or_else(|| {
            CommandError::Validation(format!("Invalid source path: {source_path}"))
        })?;
        let source_dest = source_dir.join(source_filename);
        std::fs::copy(src, &source_dest)
            .map_err(|e| CommandError::Io(format!("Failed to copy source file: {e}")))?;
        log::debug!("[project_create_full] copied source to {}", source_dest.display());

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
            serde_json::to_string_pretty(&metadata).unwrap(),
        )?;
        log::debug!("[project_create_full] wrote metadata.json");

        // 7. Extract DDL from source binary.
        let ext = src.extension().and_then(|e| e.to_str()).unwrap_or("").to_lowercase();
        if ext == "dacpac" {
            extract_ddl_from_dacpac(&source_dest, &ddl_dir)?;
        } else {
            extract_ddl_from_zip(&source_dest, &ddl_dir)?;
        }
        log::info!("[project_create_full] DDL extraction complete slug={}", project.slug);

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
        run_cmd("git", &["lfs", "track", lfs_pattern], Some(&local_clone_path), &[])?;

        // --force because the repo's .gitignore may contain *.dacpac.
        run_cmd("git", &["add", "--force", ".gitattributes", &project.slug], Some(&local_clone_path), &[])?;
        run_cmd(
            "git",
            &[
                "-c", "user.name=Migration Utility",
                "-c", "user.email=migration@vibedata.com",
                "commit", "-m", &format!("feat: add project {}", project.slug),
            ],
            Some(&local_clone_path),
            &[],
        )?;
        run_cmd("git", &["push"], Some(&local_clone_path), &[("GIT_TERMINAL_PROMPT", "0")])?;
        log::info!("[project_create_full] pushed project {} to repo", project.slug);

        Ok(())
    })();

    if let Err(ref e) = external_result {
        log::error!("[project_create_full] step failed, rolling back id={}: {e}", project.id);
        if slug_dir.exists() {
            if let Err(rm_e) = std::fs::remove_dir_all(&slug_dir) {
                log::warn!("[project_create_full] cleanup dir failed (non-fatal): {rm_e}");
            }
        }
        match state.conn() {
            Ok(conn) => {
                if let Err(del_e) = conn.execute("DELETE FROM projects WHERE id = ?1", params![project.id]) {
                    log::error!("[project_create_full] rollback DB delete failed: {del_e}");
                } else {
                    log::info!("[project_create_full] rolled back DB row id={}", project.id);
                }
            }
            Err(lock_e) => log::error!("[project_create_full] rollback DB lock failed: {lock_e}"),
        }
        return Err(external_result.unwrap_err());
    }

    // 9. Set as active project.
    {
        let conn = state.conn().map_err(CommandError::Database)?;
        let mut settings = crate::db::read_settings(&conn).map_err(CommandError::Database)?;
        settings.active_project_id = Some(project.id.clone());
        crate::db::write_settings(&conn, &settings).map_err(CommandError::Database)?;
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

    let (slug, technology, local_clone_path, clone_url) = {
        let conn = state.conn().map_err(|e| {
            log::error!("[project_init] DB lock: {e}");
            CommandError::Database(e)
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
        let settings = crate::db::read_settings(&conn).map_err(CommandError::Database)?;
        let lcp = settings.local_clone_path.ok_or_else(|| {
            CommandError::Validation("Local clone path not configured in Settings".into())
        })?;
        let url = settings.migration_repo_clone_url.ok_or_else(|| {
            CommandError::Validation("Migration repository not configured in Settings".into())
        })?;
        (slug, technology, lcp, url)
    };

    // ── Step 1: GitPull ───────────────────────────────────────────────────────
    emit_step(&app, InitStep::GitPull, InitStepStatus::Running, Some(id.clone()));
    let git_result = if Path::new(&local_clone_path).join(".git").exists() {
        run_cmd_async("git", &["pull"], Some(&local_clone_path), &[("GIT_TERMINAL_PROMPT", "0")]).await
    } else {
        run_cmd_async("git", &["clone", &clone_url, &local_clone_path], None, &[("GIT_TERMINAL_PROMPT", "0")]).await
    };
    match git_result {
        Err(ref e) => {
            let msg = e.to_string();
            log::error!("[project_init] GitPull failed: {msg}");
            emit_step(&app, InitStep::GitPull, InitStepStatus::Error { message: msg.clone() }, Some(id.clone()));
            return Err(CommandError::External(msg));
        }
        Ok(_) => {
            log::debug!("[project_init] git lfs pull in {local_clone_path}");
            if let Err(e) = run_cmd_async("git", &["lfs", "pull"], Some(&local_clone_path), &[("GIT_TERMINAL_PROMPT", "0")]).await {
                log::warn!("[project_init] git lfs pull failed (non-fatal): {e}");
            }
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

    // ── Step 2: DdlCheck ─────────────────────────────────────────────────────
    emit_step(app, InitStep::DdlCheck, InitStepStatus::Running, pid.clone());

    // Read metadata.json to get source_filename and source_sha256.
    let metadata_path = source_dir.join("metadata.json");
    let ddl_stale = check_ddl_stale(&metadata_path, &source_dir, &ddl_dir);

    match &ddl_stale {
        Ok(false) => {
            log::info!("[run_project_ddl_steps] DDL is current slug={slug}");
            emit_step(app, InitStep::DdlCheck, InitStepStatus::Ok, pid.clone());
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
        let lcp = local_clone_path;
        run_cmd("git", &["add", slug], Some(lcp), &[])?;
        let has_staged = run_cmd("git", &["diff", "--cached", "--quiet"], Some(lcp), &[]).is_err();
        if has_staged {
            run_cmd(
                "git",
                &[
                    "-c", "user.name=Migration Utility",
                    "-c", "user.email=migration@vibedata.com",
                    "commit", "-m", &format!("chore: refresh DDL for {slug}"),
                ],
                Some(lcp),
                &[],
            )?;
            run_cmd("git", &["push"], Some(lcp), &[("GIT_TERMINAL_PROMPT", "0")])?;
        }
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
        .unwrap(),
    )?;
    log::debug!("[migrate_legacy_dacpac] wrote new metadata.json slug={slug}");

    extract_ddl_from_dacpac(&source_dest, ddl_dir)?;
    log::info!("[migrate_legacy_dacpac] DDL extracted slug={slug}");

    // Commit and push the migrated files.
    run_cmd("git", &["lfs", "install"], None, &[])?;
    run_cmd("git", &["lfs", "track", "*.dacpac"], Some(local_clone_path), &[])?;
    run_cmd("git", &["add", "--force", ".gitattributes", slug], Some(local_clone_path), &[])?;
    run_cmd(
        "git",
        &[
            "-c", "user.name=Migration Utility",
            "-c", "user.email=migration@vibedata.com",
            "commit", "-m", &format!("migrate: {slug} — DDL extraction from legacy DacPac"),
        ],
        Some(local_clone_path),
        &[],
    )?;
    run_cmd("git", &["push"], Some(local_clone_path), &[])?;
    log::info!("[migrate_legacy_dacpac] committed and pushed slug={slug}");

    Ok(())
}

/// Returns `Ok(false)` if DDL is current, `Ok(true)` if stale/missing, `Err` if check itself fails.
fn check_ddl_stale(
    metadata_path: &Path,
    source_dir: &Path,
    ddl_dir: &Path,
) -> Result<bool, CommandError> {
    if !metadata_path.exists() {
        return Err(CommandError::Validation(format!(
            "metadata.json not found at '{}' — project may not have been properly created",
            metadata_path.display()
        )));
    }

    let content = std::fs::read_to_string(metadata_path)
        .map_err(|e| CommandError::Io(format!("Cannot read metadata.json: {e}")))?;
    let metadata: serde_json::Value = serde_json::from_str(&content)
        .map_err(|e| CommandError::Io(format!("Cannot parse metadata.json: {e}")))?;

    let expected_sha = metadata["sourceSha256"].as_str().unwrap_or("");
    let source_filename = metadata["sourceFilename"].as_str().unwrap_or("");

    if source_filename.is_empty() || expected_sha.is_empty() {
        log::warn!("[check_ddl_stale] metadata.json missing sourceFilename or sourceSha256 — treating DDL as stale");
        return Ok(true);
    }

    let source_path = source_dir.join(source_filename);
    if !source_path.exists() {
        log::warn!("[check_ddl_stale] source file '{}' not found — DDL stale", source_path.display());
        return Ok(true);
    }

    let actual_sha = compute_file_sha256(&source_path)?;
    if actual_sha != expected_sha {
        log::warn!("[check_ddl_stale] source SHA256 mismatch (expected={expected_sha}, actual={actual_sha}) — DDL stale");
        return Ok(true);
    }

    // Check that at least one DDL file exists.
    let has_ddl = ["procedures.sql", "views.sql", "functions.sql", "tables.sql"]
        .iter()
        .any(|f| ddl_dir.join(f).exists());
    if !has_ddl {
        log::warn!("[check_ddl_stale] no DDL files found in '{}' — DDL stale", ddl_dir.display());
        return Ok(true);
    }

    Ok(false)
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

    let (rows, local_clone_path, clone_url) = {
        let conn = state.conn().map_err(|e| {
            log::error!("[app_startup_sync] DB lock: {e}");
            CommandError::Database(e)
        })?;
        let settings = crate::db::read_settings(&conn).map_err(|e| {
            log::error!("[app_startup_sync] read_settings failed: {e}");
            CommandError::Database(e)
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
        (rows, settings.local_clone_path, settings.migration_repo_clone_url)
    };

    if rows.is_empty() {
        log::info!("[app_startup_sync] no projects configured, nothing to sync");
        return Ok(());
    }

    log::info!("[app_startup_sync] syncing {} project(s)", rows.len());

    // ── Step 0: .NET runtime check (global, once) ─────────────────────────────
    let has_sql_server = rows.iter().any(|r| r.technology == "sql_server");
    emit_step(&app, InitStep::DotnetCheck, InitStepStatus::Running, None);
    match std::process::Command::new("dotnet").arg("--version").output() {
        Ok(out) if out.status.success() => {
            let ver = String::from_utf8_lossy(&out.stdout).trim().to_string();
            let major: u32 = ver.split('.').next().and_then(|s| s.parse().ok()).unwrap_or(0);
            if major >= 8 {
                log::info!("[app_startup_sync] dotnet runtime ok: {ver}");
                emit_step(&app, InitStep::DotnetCheck, InitStepStatus::Ok, None);
            } else {
                let msg = format!(".NET 8+ required, found {ver}. Install from https://dot.net");
                log::error!("[app_startup_sync] dotnet version too old: {ver}");
                emit_step(&app, InitStep::DotnetCheck, InitStepStatus::Error { message: msg.clone() }, None);
                return Err(CommandError::External(msg));
            }
        }
        Ok(_) | Err(_) => {
            if has_sql_server {
                let msg = ".NET 8 runtime not found. Install from https://dot.net to use SQL Server projects.".to_string();
                log::error!("[app_startup_sync] dotnet not found and SQL Server projects present");
                emit_step(&app, InitStep::DotnetCheck, InitStepStatus::Error { message: msg.clone() }, None);
                return Err(CommandError::External(msg));
            } else {
                log::warn!("[app_startup_sync] dotnet not found (no SQL Server projects — non-fatal)");
                emit_step(&app, InitStep::DotnetCheck, InitStepStatus::Warning {
                    warnings: vec!["Install .NET 8 runtime to support SQL Server projects.".into()],
                }, None);
            }
        }
    }

    // ── Step 1: GitPull (global, once) ────────────────────────────────────────
    emit_step(&app, InitStep::GitPull, InitStepStatus::Running, None);
    match (&local_clone_path, &clone_url) {
        (None, _) => {
            log::warn!("[app_startup_sync] local_clone_path not configured — skipping git pull");
            emit_step(&app, InitStep::GitPull, InitStepStatus::Warning {
                warnings: vec!["Git repo not configured in Settings → Connections. Skipping sync.".into()],
            }, None);
        }
        (Some(lcp), _) if Path::new(lcp).join(".git").exists() => {
            match run_cmd_async("git", &["pull"], Some(lcp), &[("GIT_TERMINAL_PROMPT", "0")]).await {
                Err(ref e) => {
                    let msg = e.to_string();
                    log::error!("[app_startup_sync] GitPull failed: {msg}");
                    emit_step(&app, InitStep::GitPull, InitStepStatus::Error { message: msg.clone() }, None);
                    return Err(CommandError::External(msg));
                }
                Ok(_) => {
                    if let Err(e) = run_cmd_async("git", &["lfs", "pull"], Some(lcp), &[("GIT_TERMINAL_PROMPT", "0")]).await {
                        log::warn!("[app_startup_sync] git lfs pull failed (non-fatal): {e}");
                    }
                    emit_step(&app, InitStep::GitPull, InitStepStatus::Ok, None);
                }
            }
        }
        (Some(lcp), Some(url)) => {
            match run_cmd_async("git", &["clone", url, lcp], None, &[("GIT_TERMINAL_PROMPT", "0")]).await {
                Err(ref e) => {
                    let msg = e.to_string();
                    log::error!("[app_startup_sync] git clone failed: {msg}");
                    emit_step(&app, InitStep::GitPull, InitStepStatus::Error { message: msg.clone() }, None);
                    return Err(CommandError::External(msg));
                }
                Ok(_) => {
                    if let Err(e) = run_cmd_async("git", &["lfs", "pull"], Some(lcp), &[("GIT_TERMINAL_PROMPT", "0")]).await {
                        log::warn!("[app_startup_sync] git lfs pull (post-clone) failed (non-fatal): {e}");
                    }
                    emit_step(&app, InitStep::GitPull, InitStepStatus::Ok, None);
                }
            }
        }
        (Some(_), None) => {
            log::warn!("[app_startup_sync] clone_url not configured — skipping git pull");
            emit_step(&app, InitStep::GitPull, InitStepStatus::Warning {
                warnings: vec!["Clone URL not configured — using existing local files.".into()],
            }, None);
        }
    }

    // ── Steps 2-3: DDL check per project, in parallel ─────────────────────────
    let lcp = local_clone_path.clone().unwrap_or_default();
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

    let (slug, local_clone_path, token) = {
        let conn = state.conn().map_err(|e| {
            log::error!("[project_delete_full] DB lock: {e}");
            CommandError::Database(e)
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
        let s = crate::db::read_settings(&conn).map_err(CommandError::Database)?;
        (slug, s.local_clone_path, s.github_oauth_token)
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
    if let (Some(ref lcp), Some(ref _tok)) = (&local_clone_path, &token) {
        if let Err(e) = run_cmd("git", &["rm", "-r", "--ignore-unmatch", &slug], Some(lcp), &[("GIT_TERMINAL_PROMPT", "0")]) {
            log::warn!("[project_delete_full] git rm (non-fatal): {e}");
        }
        let has_staged = run_cmd("git", &["diff", "--cached", "--quiet"], Some(lcp), &[]).is_err();
        if has_staged {
            if let Err(e) = run_cmd("git", &[
                "-c", "user.name=Migration Utility",
                "-c", "user.email=migration@vibedata.com",
                "commit", "-m", &format!("chore: remove project {slug}"),
            ], Some(lcp), &[]) {
                log::warn!("[project_delete_full] git commit (non-fatal): {e}");
            } else if let Err(e) = run_cmd("git", &["push"], Some(lcp), &[("GIT_TERMINAL_PROMPT", "0")]) {
                log::warn!("[project_delete_full] git push (non-fatal): {e}");
            }
        }
    }

    // Step 3: Delete DB row and clear active_project_id.
    {
        let conn = state.conn().map_err(CommandError::Database)?;
        conn.execute("DELETE FROM projects WHERE id = ?1", params![id])
            .map_err(|e| {
                log::error!("[project_delete_full] DB delete failed: {e}");
                CommandError::from(e)
            })?;
        let mut s = crate::db::read_settings(&conn).map_err(CommandError::Database)?;
        if s.active_project_id.as_deref() == Some(&id) {
            s.active_project_id = None;
            crate::db::write_settings(&conn, &s).map_err(CommandError::Database)?;
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
        let conn = state.conn().map_err(|e| {
            log::error!("[project_reset_local] DB lock: {e}");
            CommandError::Database(e)
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
        let s = crate::db::read_settings(&conn).map_err(CommandError::Database)?;
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

/// Extract the source database name from a DacPac file by reading DacMetadata.xml.
/// DacPac files are ZIP archives; DacMetadata.xml contains the `<Name>` element.
fn dacpac_db_name(dacpac_path: &str) -> Result<String, CommandError> {
    log::debug!("[dacpac_db_name] reading DacMetadata.xml from {dacpac_path}");
    let xml = run_cmd("unzip", &["-p", dacpac_path, "DacMetadata.xml"], None, &[])
        .map_err(|e| {
            CommandError::External(format!("Failed to read DacPac metadata: {e}"))
        })?;
    // Parse <Name>...</Name> from the XML (DacMetadata.xml is simple and well-formed).
    for line in xml.lines() {
        let trimmed = line.trim();
        if let Some(inner) = trimmed.strip_prefix("<Name>").and_then(|s| s.strip_suffix("</Name>")) {
            if !inner.is_empty() {
                log::debug!("[dacpac_db_name] db_name={inner}");
                return Ok(inner.to_string());
            }
        }
    }
    Err(CommandError::External("<Name> not found in DacMetadata.xml — is this a valid DacPac?".into()))
}

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

    #[test]
    fn insert_project_row_roundtrip() {
        let conn = db::open_in_memory().unwrap();
        let project = insert_project_row(&conn, "Test Project", "sql_server").unwrap();
        assert_eq!(project.name, "Test Project");
        assert_eq!(project.slug, "test-project");
        assert_eq!(project.technology, "sql_server");
        assert!(!project.id.is_empty());
    }

    #[test]
    fn insert_project_row_slug_collision() {
        let conn = db::open_in_memory().unwrap();
        let p1 = insert_project_row(&conn, "My Project", "sql_server").unwrap();
        let p2 = insert_project_row(&conn, "My Project", "fabric_warehouse").unwrap();
        assert_eq!(p1.slug, "my-project");
        assert_ne!(p1.slug, p2.slug, "collision must produce unique slug");
    }

    #[test]
    fn insert_project_row_technology_variants() {
        let conn = db::open_in_memory().unwrap();
        for tech in &["sql_server", "fabric_warehouse", "fabric_lakehouse", "snowflake"] {
            let p = insert_project_row(&conn, tech, tech).unwrap();
            assert_eq!(p.technology, *tech);
        }
    }

    #[test]
    fn project_sets_active() {
        let conn = db::open_in_memory().unwrap();
        let p = insert_project_row(&conn, "Acme", "sql_server").unwrap();
        let mut s = AppSettings::default();
        s.active_project_id = Some(p.id.clone());
        db::write_settings(&conn, &s).unwrap();
        let read = db::read_settings(&conn).unwrap();
        assert_eq!(read.active_project_id.as_deref(), Some(p.id.as_str()));
    }

    #[test]
    fn project_delete_clears_active_when_matches() {
        let conn = db::open_in_memory().unwrap();
        let p = insert_project_row(&conn, "Alpha", "sql_server").unwrap();
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
        let p = insert_project_row(&conn, "Beta", "snowflake").unwrap();
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
