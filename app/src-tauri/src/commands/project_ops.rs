use std::path::Path;

use rusqlite::params;

/// sqlpackage publish profile embedded from `resources/exclusions.publish.xml`.
/// Edit that file to add/remove exclusions, then rebuild to pick up the change.
const PUBLISH_PROFILE_XML: &str = include_str!("../../resources/exclusions.publish.xml");
use tauri::{Emitter, State};
use uuid::Uuid;

use crate::commands::project::slugify;
use crate::db::DbState;
use crate::types::{CommandError, InitStep, InitStepEvent, InitStepStatus, Project};

// ── Helpers ───────────────────────────────────────────────────────────────────

fn emit_step(app: &tauri::AppHandle, step: InitStep, status: InitStepStatus) {
    let event = InitStepEvent {
        step,
        status,
    };
    if let Err(e) = app.emit("project:init:step", event) {
        log::warn!("[emit_step] failed to emit: {e}");
    }
}

/// Run an external command, returning stdout on success or `CommandError::External` on failure.
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

/// Insert a project row and return the new Project. Extracts logic shared with project_create.
fn insert_project_row(
    conn: &rusqlite::Connection,
    name: &str,
    sa_password: &str,
) -> Result<Project, CommandError> {
    let id = Uuid::new_v4().to_string();
    let slug = slugify(name, conn)?;
    let created_at = chrono::Utc::now().to_rfc3339();
    conn.execute(
        "INSERT INTO projects(id, slug, name, sa_password, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![id, slug, name, sa_password, created_at],
    )
    .map_err(|e| {
        log::error!("[insert_project_row] insert failed: {e}");
        CommandError::from(e)
    })?;
    Ok(Project { id, slug, name: name.to_string(), created_at })
}

/// Docker container name for a project slug.
fn container_name(slug: &str) -> String {
    format!("migration-{slug}")
}

/// Named Docker volume for a project.
fn volume_name(slug: &str) -> String {
    format!("migration-{slug}-data")
}


// ── project_create_full (VU-403 absorbed into VU-404) ─────────────────────────

/// Create a project: insert DB row, scaffold repo structure, push DacPac, create GH secret,
/// and set as active. Caller must subsequently invoke `project_init` to start the SQL container.
#[tauri::command]
pub fn project_create_full(
    state: State<'_, DbState>,
    name: String,
    sa_password: String,
    dacpac_path: String,
    sql_server_version: String,
    customer: String,
    system: String,
    db_name: String,
    extraction_datetime: String,
) -> Result<Project, CommandError> {
    log::info!("[project_create_full] name={} db_name={}", name, db_name);

    // 1. Validate settings before touching the DB — fail fast with no side effects.
    let (local_clone_path, repo_full_name, token) = {
        let conn = state.conn().map_err(|e| {
            log::error!("[project_create_full] DB lock: {e}");
            CommandError::Database(e)
        })?;
        let s = crate::db::read_settings(&conn).map_err(CommandError::Database)?;
        let lcp = s.local_clone_path.ok_or_else(|| {
            CommandError::Validation("Local clone path not configured in Settings".into())
        })?;
        let repo = s.migration_repo_full_name.ok_or_else(|| {
            CommandError::Validation("Migration repository not configured in Settings".into())
        })?;
        let tok = s.github_oauth_token.ok_or_else(|| {
            CommandError::Validation("GitHub authentication required".into())
        })?;
        (lcp, repo, tok)
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

    // 2. Insert DB row (needed for slug + id used by subsequent steps).
    let project = {
        let conn = state.conn().map_err(|e| {
            log::error!("[project_create_full] DB lock: {e}");
            CommandError::Database(e)
        })?;
        insert_project_row(&conn, &name, &sa_password)?
    };
    log::debug!("[project_create_full] row inserted id={} slug={}", project.id, project.slug);

    // 3–9. Execute external steps. On any failure, rollback the DB row and clean up.
    let slug_dir = Path::new(&local_clone_path).join(&project.slug);
    let external_result: Result<(), CommandError> = (|| {
        let slug_dir_str = slug_dir.to_string_lossy().to_string();

        // 3. Create project directory.
        std::fs::create_dir_all(&slug_dir)?;
        log::debug!("[project_create_full] created dir {slug_dir_str}");

        // 4. Write metadata.json.
        let metadata = serde_json::json!({
            "id": project.id,
            "slug": project.slug,
            "name": project.name,
            "createdAt": project.created_at,
            "sqlServerVersion": sql_server_version,
            "customer": customer,
            "system": system,
            "dbName": db_name,
            "extractionDatetime": extraction_datetime,
        });
        std::fs::write(
            slug_dir.join("metadata.json"),
            serde_json::to_string_pretty(&metadata).unwrap(),
        )?;
        log::debug!("[project_create_full] wrote metadata.json");

        // 5. Enable Git LFS globally — does not need a repo, run without cwd.
        run_cmd("git", &["lfs", "install"], None, &[])
            .map_err(|e| {
                if e.to_string().contains("is not a git command") || e.to_string().contains("not found") {
                    CommandError::Validation(
                        "Git LFS is not installed. Install it and retry:\n\
                         • macOS:   brew install git-lfs\n\
                         • Linux:   apt install git-lfs  (or equivalent)\n\
                         • Windows: winget install Git.LFS\n\
                         Then run: git lfs install".into()
                    )
                } else {
                    e
                }
            })?;

        // 6. Copy DacPac into project dir.
        let dacpac_src = Path::new(&dacpac_path);
        let dacpac_filename = dacpac_src.file_name().ok_or_else(|| {
            CommandError::Validation(format!("Invalid DacPac path: {dacpac_path}"))
        })?;
        let dacpac_dest = slug_dir.join(dacpac_filename);
        std::fs::copy(dacpac_src, &dacpac_dest).map_err(|e| {
            CommandError::Io(format!("Failed to copy DacPac: {e}"))
        })?;
        log::debug!("[project_create_full] copied DacPac to {}", dacpac_dest.display());

        // 7. Track *.dacpac with LFS.
        run_cmd("git", &["lfs", "track", "*.dacpac"], Some(&local_clone_path), &[])?;

        // 8. Git add → commit → push.
        run_cmd("git", &["add", &project.slug], Some(&local_clone_path), &[])?;
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
        let auth_header = format!("Authorization: Bearer {token}");
        run_cmd(
            "git",
            &["-c", &format!("http.extraheader={auth_header}"), "push"],
            Some(&local_clone_path),
            &[("GIT_TERMINAL_PROMPT", "0")],
        )?;
        log::info!("[project_create_full] pushed project {} to repo", project.slug);

        // 9. Create GitHub secret SA_PASSWORD_{SLUG_UPPER}.
        let secret_name = format!("SA_PASSWORD_{}", project.slug.replace('-', "_").to_uppercase());
        run_cmd(
            "gh",
            &["secret", "set", &secret_name, "--repo", &repo_full_name, "--body", &sa_password],
            None,
            &[("GITHUB_TOKEN", &token)],
        )?;
        log::info!("[project_create_full] created GH secret {secret_name}");

        Ok(())
    })();

    if let Err(ref e) = external_result {
        log::error!("[project_create_full] step failed, rolling back id={}: {e}", project.id);
        // Remove local directory if it was created.
        if slug_dir.exists() {
            if let Err(rm_e) = std::fs::remove_dir_all(&slug_dir) {
                log::warn!("[project_create_full] cleanup dir failed (non-fatal): {rm_e}");
            }
        }
        // Delete DB row so the project does not appear in the list.
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

    // 10. Set as active project.
    {
        let conn = state.conn().map_err(CommandError::Database)?;
        let mut settings = crate::db::read_settings(&conn).map_err(CommandError::Database)?;
        settings.active_project_id = Some(project.id.clone());
        crate::db::write_settings(&conn, &settings).map_err(CommandError::Database)?;
    }
    log::info!("[project_create_full] done id={} slug={}", project.id, project.slug);
    Ok(project)
}

// ── project_init (VU-404 shared orchestrator) ─────────────────────────────────

/// Canonical initialization orchestrator. Emits `project:init:step` events per step.
/// Safe to call from startup, after create, on project switch, and after reset.
#[tauri::command]
pub async fn project_init(
    app: tauri::AppHandle,
    state: State<'_, DbState>,
    id: String,
) -> Result<(), CommandError> {
    log::info!("[project_init] id={}", id);

    // Load project + settings before any async work.
    let (slug, sa_password, local_clone_path, clone_url) = {
        let conn = state.conn().map_err(|e| {
            log::error!("[project_init] DB lock: {e}");
            CommandError::Database(e)
        })?;

        let (slug, sa_password) = conn
            .query_row(
                "SELECT slug, sa_password FROM projects WHERE id = ?1",
                params![id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .map_err(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => {
                    CommandError::NotFound(format!("project {id}"))
                }
                other => CommandError::from(other),
            })?;

        let settings = crate::db::read_settings(&conn).map_err(CommandError::Database)?;
        let lcp = settings.local_clone_path.ok_or_else(|| {
            CommandError::Validation("Local clone path not configured in Settings".into())
        })?;
        let url = settings.migration_repo_clone_url.ok_or_else(|| {
            CommandError::Validation("Migration repository not configured in Settings".into())
        })?;
        (slug, sa_password, lcp, url)
    };

    let container = container_name(&slug);
    let volume = volume_name(&slug);

    // ── Step 1: GitPull ───────────────────────────────────────────────────────
    emit_step(&app, InitStep::GitPull, InitStepStatus::Running);
    let git_result = if Path::new(&local_clone_path).join(".git").exists() {
        log::debug!("[project_init] git pull in {local_clone_path}");
        run_cmd("git", &["pull"], Some(&local_clone_path), &[("GIT_TERMINAL_PROMPT", "0")])
    } else {
        log::debug!("[project_init] git clone {clone_url} into {local_clone_path}");
        run_cmd("git", &["clone", &clone_url, &local_clone_path], None, &[("GIT_TERMINAL_PROMPT", "0")])
    };
    match git_result {
        Ok(_) => emit_step(&app, InitStep::GitPull, InitStepStatus::Ok),
        Err(ref e) => {
            let msg = e.to_string();
            log::error!("[project_init] GitPull failed: {msg}");
            emit_step(&app, InitStep::GitPull, InitStepStatus::Error { message: msg.clone() });
            return Err(CommandError::External(msg));
        }
    }

    // ── Step 2: DockerCheck ───────────────────────────────────────────────────
    emit_step(&app, InitStep::DockerCheck, InitStepStatus::Running);
    match run_cmd("docker", &["info"], None, &[]) {
        Ok(_) => emit_step(&app, InitStep::DockerCheck, InitStepStatus::Ok),
        Err(ref e) => {
            let msg = format!("Docker is not running or not installed — please start Docker Desktop. Detail: {e}");
            log::error!("[project_init] DockerCheck failed: {msg}");
            emit_step(&app, InitStep::DockerCheck, InitStepStatus::Error { message: msg.clone() });
            return Err(CommandError::External(msg));
        }
    }

    // ── Step 3: StartContainer ────────────────────────────────────────────────
    emit_step(&app, InitStep::StartContainer, InitStepStatus::Running);

    // Check if container already exists and is running.
    let ps_out = run_cmd(
        "docker",
        &["ps", "-a", "--filter", &format!("name={container}"), "--format", "{{.Status}}"],
        None,
        &[],
    )
    .unwrap_or_default();

    let container_result = if ps_out.to_lowercase().starts_with("up") {
        log::debug!("[project_init] container {container} already running");
        Ok(())
    } else if ps_out.is_empty() {
        // Container doesn't exist — create and start.
        log::debug!("[project_init] creating container {container}");
        run_cmd(
            "docker",
            &[
                "run", "-d",
                "--platform", "linux/amd64",
                "--name", &container,
                "-e", "ACCEPT_EULA=Y",
                "-e", &format!("SA_PASSWORD={sa_password}"),
                "-e", "MSSQL_PID=Developer",
                "-p", "1433:1433",
                "-v", &format!("{volume}:/var/opt/mssql"),
                "mcr.microsoft.com/mssql/server:2022-latest",
            ],
            None,
            &[],
        )
        .map(|_| ())
    } else {
        // Container exists but stopped — start it.
        log::debug!("[project_init] starting stopped container {container}");
        run_cmd("docker", &["start", &container], None, &[]).map(|_| ())
    };

    match container_result {
        Ok(_) => emit_step(&app, InitStep::StartContainer, InitStepStatus::Ok),
        Err(ref e) => {
            let msg = e.to_string();
            log::error!("[project_init] StartContainer failed: {msg}");
            emit_step(&app, InitStep::StartContainer, InitStepStatus::Error { message: msg.clone() });
            return Err(CommandError::External(msg));
        }
    }

    // ── Step 4: RestoreDacpac ─────────────────────────────────────────────────
    emit_step(&app, InitStep::RestoreDacpac, InitStepStatus::Running);

    let slug_dir = Path::new(&local_clone_path).join(&slug);
    let dacpac_path = find_dacpac(&slug_dir);

    let restore_result = match dacpac_path {
        None => Err(CommandError::External(format!(
            "No .dacpac file found in {}", slug_dir.display()
        ))),
        Some(ref dacpac) => {
            // DB name is embedded in the connection string — /TargetDatabaseName
            // cannot be used alongside /TargetConnectionString.
            let db_name = slug.replace('-', "_");
            let conn_str = format!(
                "Server=localhost,1433;Database={db_name};User Id=sa;Password={sa_password};TrustServerCertificate=True"
            );

            // Write the embedded publish profile to a temp file so sqlpackage can read it.
            // Edit `resources/exclusions.publish.xml` to adjust exclusions, then rebuild.
            let profile_path = std::env::temp_dir().join("migration-utility-exclusions.publish.xml");
            if let Err(e) = std::fs::write(&profile_path, PUBLISH_PROFILE_XML) {
                log::warn!("[project_init] failed to write publish profile (will run without it): {e}");
            }

            let source_arg = format!("/SourceFile:{}", dacpac.display());
            let conn_arg = format!("/TargetConnectionString:{conn_str}");
            let profile_arg = format!("/pr:{}", profile_path.display());
            let mut sqlpackage_args: Vec<&str> = vec![
                "/Action:Publish",
                &source_arg,
                &conn_arg,
            ];
            if profile_path.exists() {
                sqlpackage_args.push(&profile_arg);
            }
            log::debug!("[project_init] RestoreDacpac db_name={db_name} profile={}", profile_path.display());

            run_cmd("sqlpackage", &sqlpackage_args, None, &[]).map(|_| ())
        }
    };

    match restore_result {
        Ok(_) => emit_step(&app, InitStep::RestoreDacpac, InitStepStatus::Ok),
        Err(ref e) => {
            let msg = e.to_string();
            log::error!("[project_init] RestoreDacpac failed: {msg}");
            emit_step(&app, InitStep::RestoreDacpac, InitStepStatus::Error { message: msg.clone() });
            return Err(CommandError::External(msg));
        }
    }

    // ── Step 5: VerifyDb ──────────────────────────────────────────────────────
    emit_step(&app, InitStep::VerifyDb, InitStepStatus::Running);

    let verify_result = wait_for_sql_server("localhost", 1433, &sa_password, 120).await;
    match verify_result {
        Ok(_) => {
            emit_step(&app, InitStep::VerifyDb, InitStepStatus::Ok);
            log::info!("[project_init] initialization complete for id={}", id);
            Ok(())
        }
        Err(ref e) => {
            let msg = e.to_string();
            log::error!("[project_init] VerifyDb failed: {msg}");
            emit_step(&app, InitStep::VerifyDb, InitStepStatus::Error { message: msg.clone() });
            Err(CommandError::External(msg))
        }
    }
}

/// Find the first .dacpac file in a directory.
fn find_dacpac(dir: &Path) -> Option<std::path::PathBuf> {
    std::fs::read_dir(dir).ok()?.flatten().find_map(|entry| {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("dacpac") {
            Some(path)
        } else {
            None
        }
    })
}

/// Poll SQL Server until it accepts a TDS connection or the timeout expires.
/// SQL Server can take 30–60 s to initialise after the container starts.
async fn wait_for_sql_server(
    host: &str,
    port: u16,
    sa_password: &str,
    timeout_secs: u64,
) -> Result<(), CommandError> {
    use tokio_util::compat::TokioAsyncWriteCompatExt;

    let addr = format!("{host}:{port}");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
    let mut attempt = 0u32;

    loop {
        attempt += 1;
        log::debug!("[wait_for_sql_server] attempt={} addr={}", attempt, addr);

        let try_connect = async {
            let tcp = tokio::net::TcpStream::connect(&addr).await?;
            tcp.set_nodelay(true).ok();
            let mut config = tiberius::Config::new();
            config.host(host);
            config.port(port);
            config.authentication(tiberius::AuthMethod::sql_server("sa", sa_password));
            config.trust_cert();
            tiberius::Client::connect(config, tcp.compat_write()).await.map(|_| ())
        };

        match try_connect.await {
            Ok(_) => {
                log::debug!("[wait_for_sql_server] ready after {} attempt(s)", attempt);
                return Ok(());
            }
            Err(e) => {
                if std::time::Instant::now() >= deadline {
                    log::error!("[wait_for_sql_server] timed out after {} attempt(s): {e}", attempt);
                    return Err(CommandError::External(format!(
                        "SQL Server did not become ready within {timeout_secs}s: {e}"
                    )));
                }
                log::debug!("[wait_for_sql_server] not ready yet (attempt {attempt}): {e} — retrying in 5s");
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        }
    }
}

// ── project_delete_full (VU-405) ──────────────────────────────────────────────

/// Fully delete a project: Docker teardown, local dir removal, git cleanup,
/// GH secret deletion, and DB row removal.
///
/// All external operations (Docker, git, GitHub) are best-effort — failures are
/// logged as warnings but never block the DB cleanup. The DB row is always removed.
#[tauri::command]
pub fn project_delete_full(
    state: State<'_, DbState>,
    id: String,
) -> Result<(), CommandError> {
    log::info!("[project_delete_full] id={}", id);

    // Load project slug (required) and settings (optional — may not be configured
    // for projects created before settings were set up).
    let (slug, local_clone_path, repo_full_name, token) = {
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
        // Settings are optional — missing values simply skip the corresponding cleanup step.
        (slug, s.local_clone_path, s.migration_repo_full_name, s.github_oauth_token)
    };

    let container = container_name(&slug);
    let volume = volume_name(&slug);

    // Step 1: Stop + remove container and volume (best-effort).
    for args in [
        vec!["stop", container.as_str()],
        vec!["rm", "-v", container.as_str()],
        vec!["volume", "rm", volume.as_str()],
    ] {
        if let Err(e) = run_cmd("docker", &args, None, &[]) {
            log::warn!("[project_delete_full] docker {} (non-fatal): {e}", args[0]);
        }
    }

    // Step 2: Delete local project directory (best-effort).
    if let Some(ref lcp) = local_clone_path {
        let slug_dir = Path::new(lcp).join(&slug);
        if slug_dir.exists() {
            if let Err(e) = std::fs::remove_dir_all(&slug_dir) {
                log::warn!("[project_delete_full] remove local dir {} (non-fatal): {e}", slug_dir.display());
            } else {
                log::debug!("[project_delete_full] removed local dir {}", slug_dir.display());
            }
        }
    } else {
        log::debug!("[project_delete_full] local_clone_path not configured, skipping local dir removal");
    }

    // Step 3: Git rm + commit + push (best-effort — repo may not exist or be set up).
    if let (Some(ref lcp), Some(ref tok)) = (&local_clone_path, &token) {
        let auth_header = format!("Authorization: Bearer {tok}");
        let push_args = ["-c", &format!("http.extraheader={auth_header}"), "push"];
        let git_steps: &[(&[&str], &str)] = &[
            (&["rm", "-r", "--ignore-unmatch", &slug], "git rm"),
            (&["-c", "user.name=Migration Utility", "-c", "user.email=migration@vibedata.com",
               "commit", "-m", &format!("chore: remove project {slug}")], "git commit"),
            (&push_args, "git push"),
        ];
        for (args, label) in git_steps {
            if let Err(e) = run_cmd("git", args, Some(lcp), &[("GIT_TERMINAL_PROMPT", "0")]) {
                log::warn!("[project_delete_full] {} (non-fatal): {e}", label);
            }
        }
        log::debug!("[project_delete_full] git cleanup attempted for {slug}");
    } else {
        log::debug!("[project_delete_full] git not configured, skipping repo cleanup");
    }

    // Step 4: Delete GitHub secret (best-effort).
    if let (Some(ref repo), Some(ref tok)) = (&repo_full_name, &token) {
        let secret_name = format!("SA_PASSWORD_{}", slug.replace('-', "_").to_uppercase());
        if let Err(e) = run_cmd("gh", &["secret", "delete", &secret_name, "--repo", repo],
                                None, &[("GITHUB_TOKEN", tok)]) {
            log::warn!("[project_delete_full] delete GH secret {secret_name} (non-fatal): {e}");
        } else {
            log::debug!("[project_delete_full] deleted GH secret {secret_name}");
        }
    } else {
        log::debug!("[project_delete_full] GitHub not configured, skipping secret deletion");
    }

    // Step 5: Delete DB row and clear active_project_id — this must succeed.
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

// ── project_reset_local (VU-408) ──────────────────────────────────────────────

/// Reset local state for a project: stop/remove container, delete local slug dir.
/// After this returns successfully, call `project_init` to reinitialize.
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
                rusqlite::Error::QueryReturnedNoRows => {
                    CommandError::NotFound(format!("project {id}"))
                }
                other => CommandError::from(other),
            })?;
        let s = crate::db::read_settings(&conn).map_err(CommandError::Database)?;
        let lcp = s.local_clone_path.ok_or_else(|| {
            CommandError::Validation("Local clone path not configured".into())
        })?;
        (slug, lcp)
    };

    let container = container_name(&slug);
    let volume = volume_name(&slug);

    // Step 1: Stop + remove container and data volume (best-effort).
    for args in [
        vec!["stop", container.as_str()],
        vec!["rm", "-v", container.as_str()],
        vec!["volume", "rm", volume.as_str()],
    ] {
        if let Err(e) = run_cmd("docker", &args, None, &[]) {
            log::warn!("[project_reset_local] docker {} (non-fatal): {e}", args[0]);
        }
    }

    // Step 2: Delete the project's local directory (it will be restored by git pull in project_init).
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
        let project = insert_project_row(&conn, "Test Project", "secret").unwrap();
        assert_eq!(project.name, "Test Project");
        assert_eq!(project.slug, "test-project");
        assert!(!project.id.is_empty());
    }

    #[test]
    fn insert_project_row_slug_collision() {
        let conn = db::open_in_memory().unwrap();
        let p1 = insert_project_row(&conn, "My Project", "secret").unwrap();
        let p2 = insert_project_row(&conn, "My Project", "secret").unwrap();
        assert_eq!(p1.slug, "my-project");
        assert_ne!(p1.slug, p2.slug, "collision must produce unique slug");
    }

    #[test]
    fn project_create_full_sets_active_project() {
        let conn = db::open_in_memory().unwrap();
        let p = insert_project_row(&conn, "Acme", "pw").unwrap();
        // Simulate setting active
        let mut s = AppSettings::default();
        s.active_project_id = Some(p.id.clone());
        db::write_settings(&conn, &s).unwrap();
        let read = db::read_settings(&conn).unwrap();
        assert_eq!(read.active_project_id.as_deref(), Some(p.id.as_str()));
    }

    #[test]
    fn project_delete_clears_active_when_matches() {
        let conn = db::open_in_memory().unwrap();
        let p = insert_project_row(&conn, "Alpha", "pw").unwrap();
        let mut s = AppSettings::default();
        s.active_project_id = Some(p.id.clone());
        db::write_settings(&conn, &s).unwrap();

        // Simulate delete + clear active
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
        let p = insert_project_row(&conn, "Beta", "pw").unwrap();
        // reset_local only touches filesystem/docker — DB row stays
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM projects WHERE id = ?1", params![p.id], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }
}
