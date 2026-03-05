use std::io::{Read as IoRead, Write as IoWrite};
use std::path::Path;

use rusqlite::params;
use sha2::{Digest, Sha256};

/// sqlpackage publish profile embedded from `resources/exclusions.publish.xml`.
/// Edit that file to add/remove exclusions, then rebuild to pick up the change.
const PUBLISH_PROFILE_XML: &str = include_str!("../../resources/exclusions.publish.xml");
use tauri::{Emitter, State};
use uuid::Uuid;

use crate::commands::project::slugify;
use crate::db::DbState;
use crate::types::{CommandError, InitStep, InitStepEvent, InitStepStatus, Project};

// ── DacPac full-text stripping ────────────────────────────────────────────────

/// Strip full-text objects from a `.dacpac` file (which is a ZIP), recalculate
/// the SHA-256 of the modified `model.xml`, update `Origin.xml`, and write a
/// new `.dacpac` to a temp path.
///
/// Stripped elements:
/// - `SqlFullTextIndex`, `SqlFullTextCatalog` — FT schema objects
/// - Any other element whose body contains FTS predicates (`FREETEXTTABLE`,
///   `CONTAINSTABLE`, `FREETEXT(`, `CONTAINS(`) — e.g. stored procedures that
///   would fail to compile without the FT index present
///
/// Validates the file is a `.dacpac` (contains `DacMetadata.xml`, not a
/// `.bacpac`).  Returns the original path unchanged if nothing was stripped.
fn strip_dacpac_fulltext(dacpac_path: &Path) -> Result<std::path::PathBuf, CommandError> {
    use zip::write::SimpleFileOptions;

    // Validate extension.
    if dacpac_path.extension().and_then(|e| e.to_str()) != Some("dacpac") {
        return Err(CommandError::Validation(format!(
            "'{}' is not a .dacpac file", dacpac_path.display()
        )));
    }

    let file = std::fs::File::open(dacpac_path).map_err(|e| {
        CommandError::Io(format!("Cannot open dacpac '{}': {e}", dacpac_path.display()))
    })?;
    let mut archive = zip::ZipArchive::new(file).map_err(|e| {
        CommandError::Io(format!("Not a valid dacpac ZIP: {e}"))
    })?;

    // Validate it is a dacpac (has DacMetadata.xml) not a bacpac.
    if archive.by_name("DacMetadata.xml").is_err() {
        return Err(CommandError::Validation(
            "File does not appear to be a .dacpac (missing DacMetadata.xml). \
             A .bacpac (data export) is not supported — please provide a schema-only .dacpac."
                .into(),
        ));
    }

    // Read model.xml.
    let model_xml = {
        let mut entry = archive.by_name("model.xml").map_err(|_| {
            CommandError::Io("dacpac does not contain model.xml".into())
        })?;
        let mut buf = String::new();
        entry.read_to_string(&mut buf).map_err(|e| CommandError::Io(e.to_string()))?;
        buf
    };

    // Strip full-text elements using line-based depth tracking.
    // Each element block looks like:
    //   <Element Type="SqlFullTextIndex" ...>
    //     ... (possibly nested <Element> tags)
    //   </Element>
    let stripped = strip_fulltext_elements(&model_xml);

    if stripped == model_xml {
        log::debug!("[strip_dacpac_fulltext] no full-text elements found, skipping copy");
        return Ok(dacpac_path.to_path_buf());
    }

    let removed = model_xml.len() - stripped.len();
    log::info!(
        "[strip_dacpac_fulltext] stripped {}B of full-text XML from model.xml",
        removed
    );

    // Compute new SHA-256 of stripped model.xml.
    let new_hash = {
        let mut hasher = Sha256::new();
        hasher.update(stripped.as_bytes());
        hex::encode(hasher.finalize()).to_uppercase()
    };

    // Read and patch Origin.xml (replace the model.xml checksum line).
    let origin_xml = {
        let mut entry = archive.by_name("Origin.xml").map_err(|_| {
            CommandError::Io("dacpac does not contain Origin.xml".into())
        })?;
        let mut buf = String::new();
        entry.read_to_string(&mut buf).map_err(|e| CommandError::Io(e.to_string()))?;
        buf
    };

    // Origin.xml checksum line: <Checksum Uri="/model.xml">HEXHEX...</Checksum>
    let patched_origin = patch_origin_checksum(&origin_xml, &new_hash);

    // Build new ZIP to a temp file, copying every entry except model.xml / Origin.xml.
    let tmp_path = std::env::temp_dir().join("migration-utility-stripped.dacpac");
    {
        let out_file = std::fs::File::create(&tmp_path).map_err(|e| {
            CommandError::Io(format!("Cannot create temp dacpac: {e}"))
        })?;
        let mut writer = zip::ZipWriter::new(out_file);
        let opts = SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated);

        for i in 0..archive.len() {
            let mut entry = archive.by_index(i).map_err(|e| CommandError::Io(e.to_string()))?;
            let name = entry.name().to_string();
            if name == "model.xml" {
                writer.start_file(&name, opts).map_err(|e| CommandError::Io(e.to_string()))?;
                writer.write_all(stripped.as_bytes()).map_err(|e| CommandError::Io(e.to_string()))?;
            } else if name == "Origin.xml" {
                writer.start_file(&name, opts).map_err(|e| CommandError::Io(e.to_string()))?;
                writer.write_all(patched_origin.as_bytes()).map_err(|e| CommandError::Io(e.to_string()))?;
            } else {
                let mut buf = Vec::new();
                entry.read_to_end(&mut buf).map_err(|e| CommandError::Io(e.to_string()))?;
                writer.start_file(&name, opts).map_err(|e| CommandError::Io(e.to_string()))?;
                writer.write_all(&buf).map_err(|e| CommandError::Io(e.to_string()))?;
            }
        }
        writer.finish().map_err(|e| CommandError::Io(e.to_string()))?;
    }

    log::info!("[strip_dacpac_fulltext] wrote stripped dacpac to {}", tmp_path.display());
    Ok(tmp_path)
}

/// Two-pass strip of full-text related elements from model.xml.
///
/// Pass 1: Collect all top-level `<Element>` blocks as (header_line, body_lines) pairs,
///         plus any leading non-element lines (XML declaration, root tag, etc.).
/// Pass 2: Drop any block where:
///   - Type attribute is `SqlFullTextIndex` or `SqlFullTextCatalog`
///   - OR: the block body contains FTS-only predicates (FREETEXTTABLE, CONTAINSTABLE,
///     FREETEXT() — these fail to compile when no FT index exists)
///
/// Returns the reassembled XML string.
fn strip_fulltext_elements(xml: &str) -> String {
    // FT object types to strip unconditionally.
    const FT_TYPES: &[&str] = &["SqlFullTextIndex", "SqlFullTextCatalog"];
    // FTS predicate substrings (case-insensitive) that indicate an element uses FTS.
    // FREETEXTTABLE / CONTAINSTABLE are FTS-only functions; FREETEXT( / CONTAINS( are predicates.
    const FT_PREDICATES: &[&str] = &["FREETEXTTABLE", "CONTAINSTABLE", "FREETEXT(", "CONTAINS("];

    // ── Pass 1: split into segments ──────────────────────────────────────────
    // A segment is either:
    //   Preamble(lines) — everything before the first top-level <Element>
    //   Block(lines)    — a complete top-level <Element>...</Element> block
    enum Seg { Preamble(Vec<String>), Block(Vec<String>) }

    let mut segments: Vec<Seg> = Vec::new();
    let mut preamble: Vec<String> = Vec::new();
    let mut block: Vec<String> = Vec::new();
    let mut depth: i32 = 0;
    let mut in_block = false;

    for line in xml.lines() {
        let trimmed = line.trim();
        if !in_block {
            if trimmed.starts_with("<Element") {
                in_block = true;
                depth = if trimmed.ends_with("/>") { 0 } else { 1 };
                block.push(line.to_string());
                if depth == 0 {
                    // Self-closing single-line element.
                    segments.push(Seg::Block(std::mem::take(&mut block)));
                    in_block = false;
                }
            } else {
                preamble.push(line.to_string());
            }
        } else {
            block.push(line.to_string());
            if trimmed.starts_with("<Element") && !trimmed.ends_with("/>") {
                depth += 1;
            } else if trimmed == "</Element>" {
                depth -= 1;
                if depth == 0 {
                    if !preamble.is_empty() {
                        segments.push(Seg::Preamble(std::mem::take(&mut preamble)));
                    }
                    segments.push(Seg::Block(std::mem::take(&mut block)));
                    in_block = false;
                }
            }
        }
    }
    // Flush any trailing preamble (closing root tag, etc.).
    if !preamble.is_empty() {
        segments.push(Seg::Preamble(preamble));
    }

    // ── Pass 2: filter and reassemble ────────────────────────────────────────
    let mut out = String::with_capacity(xml.len());
    let mut stripped_count = 0usize;

    for seg in segments {
        match seg {
            Seg::Preamble(lines) => {
                for l in lines { out.push_str(&l); out.push('\n'); }
            }
            Seg::Block(lines) => {
                let header = lines.first().map(|s| s.as_str()).unwrap_or("");
                // Check if this block is an FT type.
                let is_ft_type = FT_TYPES.iter().any(|t| header.contains(&format!("\"{t}\"")));
                if is_ft_type {
                    stripped_count += 1;
                    log::debug!("[strip_fulltext_elements] stripping FT type: {}", header.trim());
                    continue;
                }
                // Check if the body uses FTS predicates (case-insensitive).
                let body_upper = lines.join("\n").to_uppercase();
                let uses_fts = FT_PREDICATES.iter().any(|p| body_upper.contains(&p.to_uppercase()));
                if uses_fts {
                    stripped_count += 1;
                    log::debug!("[strip_fulltext_elements] stripping FTS-using element: {}", header.trim());
                    continue;
                }
                for l in &lines { out.push_str(l); out.push('\n'); }
            }
        }
    }

    if stripped_count > 0 {
        log::info!("[strip_fulltext_elements] stripped {} element(s) containing full-text references", stripped_count);
    }
    out
}

/// Replace the SHA-256 hex value in the `<Checksum Uri="/model.xml">` line of Origin.xml.
fn patch_origin_checksum(origin: &str, new_hex: &str) -> String {
    let mut out = String::with_capacity(origin.len());
    for line in origin.lines() {
        if line.contains("Uri=\"/model.xml\"") {
            // Replace everything between > and </Checksum>.
            if let (Some(open), Some(close)) = (line.find('>'), line.find("</Checksum>")) {
                let before = &line[..=open];
                let after = &line[close..];
                out.push_str(&format!("{before}{new_hex}{after}\n"));
                continue;
            }
        }
        out.push_str(line);
        out.push('\n');
    }
    out
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

/// Insert a project row and return the new Project. Extracts logic shared with project_create.
/// Bind to port 0 to let the OS pick a free ephemeral port, then release it.
/// There is a brief TOCTOU window but it is acceptable for local dev containers.
fn find_free_port() -> Result<u16, CommandError> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")
        .map_err(|e| CommandError::Io(format!("Failed to find a free port: {e}")))?;
    Ok(listener.local_addr().unwrap().port())
}

fn insert_project_row(
    conn: &rusqlite::Connection,
    name: &str,
    sa_password: &str,
    port: u16,
) -> Result<Project, CommandError> {
    let id = Uuid::new_v4().to_string();
    let slug = slugify(name, conn)?;
    let created_at = chrono::Utc::now().to_rfc3339();
    conn.execute(
        "INSERT INTO projects(id, slug, name, sa_password, created_at, port) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![id, slug, name, sa_password, created_at, port],
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
        let port = find_free_port()?;
        let conn = state.conn().map_err(|e| {
            log::error!("[project_create_full] DB lock: {e}");
            CommandError::Database(e)
        })?;
        insert_project_row(&conn, &name, &sa_password, port)?
    };
    log::debug!("[project_create_full] row inserted id={} slug={}", project.id, project.slug);

    // 3–9. Execute external steps. On any failure, rollback the DB row and clean up.
    let slug_dir = Path::new(&local_clone_path).join(&project.slug);
    let dacpac_dir = slug_dir.join("artifacts").join("dacpac");
    let external_result: Result<(), CommandError> = (|| {
        // 3. Create artifacts/dacpac/ directory structure.
        std::fs::create_dir_all(&dacpac_dir)?;
        log::debug!("[project_create_full] created dir {}", dacpac_dir.display());

        // 4. Write metadata.json into artifacts/dacpac/.
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
            dacpac_dir.join("metadata.json"),
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

        // 6. Copy DacPac into artifacts/dacpac/.
        let dacpac_src = Path::new(&dacpac_path);
        let dacpac_filename = dacpac_src.file_name().ok_or_else(|| {
            CommandError::Validation(format!("Invalid DacPac path: {dacpac_path}"))
        })?;
        let dacpac_dest = dacpac_dir.join(dacpac_filename);
        std::fs::copy(dacpac_src, &dacpac_dest).map_err(|e| {
            CommandError::Io(format!("Failed to copy DacPac: {e}"))
        })?;
        log::debug!("[project_create_full] copied DacPac to {}", dacpac_dest.display());

        // 7. Track *.dacpac with LFS.
        // lfs track writes to .gitattributes at the repo root — stage it so the
        // remote also knows to use LFS for .dacpac files.
        run_cmd("git", &["lfs", "track", "*.dacpac"], Some(&local_clone_path), &[])?;

        // 8. Git add → commit → push.
        // Stage .gitattributes (updated by lfs track) plus the full project dir.
        // --force is required because the repo's .gitignore contains *.dacpac
        // (an SSDT convention); we need to override it for our LFS-tracked file.
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
        // Credentials are embedded in the remote URL from Settings → Apply clone.
        run_cmd("git", &["push"], Some(&local_clone_path), &[("GIT_TERMINAL_PROMPT", "0")])?;
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
    let (slug, sa_password, port, local_clone_path, clone_url) = {
        let conn = state.conn().map_err(|e| {
            log::error!("[project_init] DB lock: {e}");
            CommandError::Database(e)
        })?;

        let (slug, sa_password, port) = conn
            .query_row(
                "SELECT slug, sa_password, port FROM projects WHERE id = ?1",
                params![id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, u16>(2)?)),
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
        (slug, sa_password, port, lcp, url)
    };

    // ── Step 1: GitPull ───────────────────────────────────────────────────────
    emit_step(&app, InitStep::GitPull, InitStepStatus::Running, Some(id.clone()));
    let git_result = if Path::new(&local_clone_path).join(".git").exists() {
        log::debug!("[project_init] git pull in {local_clone_path}");
        run_cmd_async("git", &["pull"], Some(&local_clone_path), &[("GIT_TERMINAL_PROMPT", "0")]).await
    } else {
        log::debug!("[project_init] git clone {clone_url} into {local_clone_path}");
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
            // Pull LFS objects (e.g. .dacpac) after the tree is synced.
            // git lfs pull is a no-op if LFS is not installed or there are no pointers.
            log::debug!("[project_init] git lfs pull in {local_clone_path}");
            if let Err(e) = run_cmd_async("git", &["lfs", "pull"], Some(&local_clone_path), &[("GIT_TERMINAL_PROMPT", "0")]).await {
                log::warn!("[project_init] git lfs pull failed (non-fatal): {e}");
            }
            emit_step(&app, InitStep::GitPull, InitStepStatus::Ok, Some(id.clone()));
        }
    }

    // ── Step 2: DockerCheck ───────────────────────────────────────────────────
    emit_step(&app, InitStep::DockerCheck, InitStepStatus::Running, Some(id.clone()));
    match run_cmd_async("docker", &["info"], None, &[]).await {
        Ok(_) => emit_step(&app, InitStep::DockerCheck, InitStepStatus::Ok, Some(id.clone())),
        Err(ref e) => {
            let msg = format!("Docker is not running or not installed — please start Docker Desktop. Detail: {e}");
            log::error!("[project_init] DockerCheck failed: {msg}");
            emit_step(&app, InitStep::DockerCheck, InitStepStatus::Error { message: msg.clone() }, Some(id.clone()));
            return Err(CommandError::External(msg));
        }
    }

    // ── Steps 3-5: per-project (stop others first on single-project switch) ───
    run_project_local_steps(&app, &id, &slug, &sa_password, port, &local_clone_path, true).await
}

/// Run steps 3-5 (StartContainer → RestoreDacpac → VerifyDb) for a single project.
///
/// Called both from `project_init` (single-project switch, `stop_others = true`) and
/// from `app_startup_sync` (parallel startup, `stop_others = false`).
async fn run_project_local_steps(
    app: &tauri::AppHandle,
    id: &str,
    slug: &str,
    sa_password: &str,
    port: u16,
    local_clone_path: &str,
    stop_others: bool,
) -> Result<(), CommandError> {
    let container = container_name(slug);
    let volume = volume_name(slug);
    let pid = Some(id.to_string());

    // ── Step 3: StartContainer ────────────────────────────────────────────────
    emit_step(app, InitStep::StartContainer, InitStepStatus::Running, pid.clone());

    // Stop any OTHER running migration-* containers (only for single-project switch).
    if stop_others {
        if let Ok(running) = run_cmd_async(
            "docker",
            &["ps", "--filter", "name=migration-", "--format", "{{.Names}}"],
            None,
            &[],
        ).await {
            for name in running.lines().map(str::trim).filter(|n| !n.is_empty() && *n != container.as_str()) {
                log::info!("[run_project_local_steps] stopping inactive container {name}");
                if let Err(e) = run_cmd_async("docker", &["stop", name], None, &[]).await {
                    log::warn!("[run_project_local_steps] failed to stop container {name} (non-fatal): {e}");
                }
            }
        }
    }

    // Use `docker inspect` for exact container state (ps --filter does substring matching).
    let inspect_state = run_cmd_async(
        "docker",
        &["inspect", "--format", "{{.State.Status}}", &container],
        None,
        &[],
    )
    .await
    .unwrap_or_default();
    log::debug!("[run_project_local_steps] container {container} inspect state='{inspect_state}'");

    if inspect_state != "running" {
        if inspect_state.is_empty() {
            // Container doesn't exist — create it.
            log::info!("[run_project_local_steps] creating container {container}");
            run_cmd_async(
                "docker",
                &[
                    "run", "-d",
                    "--platform", "linux/amd64",
                    "--name", &container,
                    "-e", "ACCEPT_EULA=Y",
                    "-e", &format!("SA_PASSWORD={sa_password}"),
                    "-e", "MSSQL_PID=Developer",
                    "-p", &format!("{port}:1433"),
                    "-v", &format!("{volume}:/var/opt/mssql"),
                    "mcr.microsoft.com/mssql/server:2022-latest",
                ],
                None,
                &[],
            )
            .await
            .map_err(|e| {
                let logs = std::process::Command::new("docker")
                    .args(["logs", "--tail", "20", &container])
                    .output()
                    .map(|o| String::from_utf8_lossy(&o.stderr).trim().to_string())
                    .unwrap_or_default();
                let detail = if logs.is_empty() { e.to_string() } else { format!("{e}\nContainer logs:\n{logs}") };
                log::error!("[run_project_local_steps] docker run failed: {detail}");
                CommandError::External(detail)
            })?;
        } else {
            // Container exists but not running — start it.
            log::info!("[run_project_local_steps] starting stopped container {container} (was: {inspect_state})");
            run_cmd_async("docker", &["start", &container], None, &[]).await.map_err(|e| {
                log::error!("[run_project_local_steps] docker start failed: {e}");
                e
            })?;
        }

        // Brief pause then verify container is actually running.
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let post_state = run_cmd_async(
            "docker",
            &["inspect", "--format", "{{.State.Status}}", &container],
            None,
            &[],
        )
        .await
        .unwrap_or_default();
        log::info!("[run_project_local_steps] container {container} post-start state='{post_state}'");

        if post_state != "running" {
            let logs = run_cmd_async("docker", &["logs", "--tail", "30", &container], None, &[])
                .await
                .unwrap_or_default();
            let msg = format!("Container '{container}' failed to start (state: {post_state}).\nDocker logs:\n{logs}");
            log::error!("[run_project_local_steps] StartContainer verify failed: {msg}");
            emit_step(app, InitStep::StartContainer, InitStepStatus::Error { message: msg.clone() }, pid.clone());
            return Err(CommandError::External(msg));
        }
    } else {
        log::debug!("[run_project_local_steps] container {container} already running");
    }

    emit_step(app, InitStep::StartContainer, InitStepStatus::Ok, pid.clone());

    // ── Step 4: RestoreDacpac ─────────────────────────────────────────────────
    emit_step(app, InitStep::RestoreDacpac, InitStepStatus::Running, pid.clone());

    let slug_dir = Path::new(local_clone_path).join(slug);
    let dacpac_dir = slug_dir.join("artifacts").join("dacpac");
    let dacpac_path = find_dacpac(&dacpac_dir);

    let restore_result = match dacpac_path {
        None => Err(CommandError::External(format!(
            "No .dacpac file found in {}", dacpac_dir.display()
        ))),
        Some(ref dacpac) => {
            // Guard: detect an unhydrated Git LFS pointer (small text file beginning with
            // "version https://git-lfs.github.com/spec/v1"). sqlpackage fails with
            // "File contains corrupted data" when fed a pointer instead of the actual binary.
            if is_lfs_pointer(dacpac) {
                log::warn!("[run_project_local_steps] dacpac is an LFS pointer — attempting git lfs pull slug={slug}");
                match run_cmd_async(
                    "git",
                    &["lfs", "pull", "--include", &format!("{}/artifacts/dacpac/", slug)],
                    Some(local_clone_path),
                    &[("GIT_TERMINAL_PROMPT", "0")],
                )
                .await
                {
                    Ok(_) => log::info!("[run_project_local_steps] git lfs pull succeeded slug={slug}"),
                    Err(ref e) => {
                        let msg = format!(
                            "The .dacpac file is an unhydrated Git LFS pointer and 'git lfs pull' failed: {e}. \
                             Ensure Git LFS is installed ('git lfs install') and the repo has LFS access."
                        );
                        log::error!("[run_project_local_steps] {msg}");
                        emit_step(app, InitStep::RestoreDacpac, InitStepStatus::Error { message: msg.clone() }, pid.clone());
                        return Err(CommandError::External(msg));
                    }
                }
                // Re-check: if still a pointer after pull, fail with a clear message.
                if is_lfs_pointer(dacpac) {
                    let msg = format!(
                        "The .dacpac file at '{}' is still an LFS pointer after 'git lfs pull'. \
                         The binary object may not be available in LFS storage.",
                        dacpac.display()
                    );
                    log::error!("[run_project_local_steps] {msg}");
                    emit_step(app, InitStep::RestoreDacpac, InitStepStatus::Error { message: msg.clone() }, pid.clone());
                    return Err(CommandError::External(msg));
                }
            }

            let effective_dacpac = match strip_dacpac_fulltext(dacpac) {
                Ok(p) => p,
                Err(e) => {
                    log::warn!("[run_project_local_steps] dacpac strip failed (using original): {e}");
                    dacpac.clone()
                }
            };

            let db_name = slug.replace('-', "_");
            let conn_str = format!(
                "Server=localhost,{port};Database={db_name};User Id=sa;Password={sa_password};TrustServerCertificate=True"
            );

            // Write the embedded publish profile to a per-project temp path to avoid
            // collisions when multiple projects run in parallel.
            let profile_path = std::env::temp_dir()
                .join(format!("migration-utility-exclusions-{slug}.publish.xml"));
            if let Err(e) = std::fs::write(&profile_path, PUBLISH_PROFILE_XML) {
                log::warn!("[run_project_local_steps] failed to write publish profile (will run without it): {e}");
            }

            let source_arg = format!("/SourceFile:{}", effective_dacpac.display());
            let conn_arg = format!("/TargetConnectionString:{conn_str}");
            let profile_arg = format!("/pr:{}", profile_path.display());
            let mut sqlpackage_args: Vec<&str> = vec![
                "/Action:Publish",
                &source_arg,
                &conn_arg,
                "/p:TreatVerificationErrorsAsWarnings=true",
            ];
            if profile_path.exists() {
                sqlpackage_args.push(&profile_arg);
            }
            log::debug!("[run_project_local_steps] RestoreDacpac slug={slug} db={db_name}");

            log::info!("[run_project_local_steps] waiting for SQL Server to be ready (up to 300s) slug={slug}");
            let wait_result = wait_for_sql_server("localhost", port, sa_password, 300, Some(&container)).await
                .map_err(|e| CommandError::External(format!("SQL Server did not become ready in time: {e}")));

            match wait_result {
                Err(e) => Err(e),
                Ok(_) => run_cmd_async("sqlpackage", &sqlpackage_args, None, &[]).await.map(|out| out),
            }
        }
    };

    match restore_result {
        Ok(ref output) => {
            let warnings: Vec<String> = output
                .lines()
                .filter(|l| {
                    let u = l.to_uppercase();
                    u.contains("WARNING") || u.contains("*** WARNING") || u.contains("SQL7")
                })
                .map(|l| l.trim().to_string())
                .collect();
            if warnings.is_empty() {
                emit_step(app, InitStep::RestoreDacpac, InitStepStatus::Ok, pid.clone());
            } else {
                log::warn!("[run_project_local_steps] RestoreDacpac completed with {} warning(s) slug={slug}", warnings.len());
                emit_step(app, InitStep::RestoreDacpac, InitStepStatus::Warning { warnings }, pid.clone());
            }
        }
        Err(ref e) => {
            let msg = e.to_string();
            log::error!("[run_project_local_steps] RestoreDacpac failed slug={slug}: {msg}");
            emit_step(app, InitStep::RestoreDacpac, InitStepStatus::Error { message: msg.clone() }, pid.clone());
            return Err(CommandError::External(msg));
        }
    }

    // ── Step 5: VerifyDb ──────────────────────────────────────────────────────
    emit_step(app, InitStep::VerifyDb, InitStepStatus::Running, pid.clone());

    let verify_result = wait_for_sql_server("localhost", port, sa_password, 10, Some(&container)).await;
    match verify_result {
        Ok(_) => {
            emit_step(app, InitStep::VerifyDb, InitStepStatus::Ok, pid.clone());
            log::info!("[run_project_local_steps] initialization complete slug={slug}");
            Ok(())
        }
        Err(ref e) => {
            let msg = e.to_string();
            log::error!("[run_project_local_steps] VerifyDb failed slug={slug}: {msg}");
            emit_step(app, InitStep::VerifyDb, InitStepStatus::Error { message: msg.clone() }, pid);
            Err(CommandError::External(msg))
        }
    }
}

/// Startup sync: git pull + docker check once, then initialize all projects in parallel.
///
/// Call this on app startup instead of `project_init` to avoid redundant git/docker work
/// when multiple projects exist. Each project's container/dacpac steps run concurrently
/// on their own Tokio tasks.
#[tauri::command]
pub async fn app_startup_sync(
    app: tauri::AppHandle,
    state: State<'_, DbState>,
) -> Result<(), CommandError> {
    log::info!("[app_startup_sync] starting multi-project startup sync");

    // Read all project data + settings under the mutex before spawning async tasks.
    struct ProjectRow {
        id: String,
        slug: String,
        sa_password: String,
        port: u16,
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
            .prepare("SELECT id, slug, sa_password, port FROM projects ORDER BY created_at")
            .map_err(CommandError::from)?;
        let rows: Vec<ProjectRow> = stmt
            .query_map([], |row| {
                Ok(ProjectRow {
                    id: row.get(0)?,
                    slug: row.get(1)?,
                    sa_password: row.get(2)?,
                    port: row.get(3)?,
                })
            })
            .map_err(CommandError::from)?
            .collect::<Result<_, rusqlite::Error>>()
            .map_err(CommandError::from)?;

        // Git settings are optional — projects may have been created without a repo
        // configured, or the dacpacs already exist on disk from a previous sync.
        (rows, settings.local_clone_path, settings.migration_repo_clone_url)
    };

    if rows.is_empty() {
        log::info!("[app_startup_sync] no projects configured, nothing to sync");
        return Ok(());
    }

    log::info!("[app_startup_sync] syncing {} project(s)", rows.len());

    // ── Step 1: GitPull (global, once) ────────────────────────────────────────
    emit_step(&app, InitStep::GitPull, InitStepStatus::Running, None);
    match (&local_clone_path, &clone_url) {
        (None, _) => {
            log::warn!("[app_startup_sync] local_clone_path not configured — skipping git pull");
            emit_step(&app, InitStep::GitPull, InitStepStatus::Warning {
                warnings: vec!["Git repo not configured in Settings → Connections. Skipping sync.".into()]
            }, None);
        }
        (Some(lcp), _) if Path::new(lcp).join(".git").exists() => {
            // Repo already cloned — pull latest.
            log::debug!("[app_startup_sync] git pull in {lcp}");
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
            // No .git yet — clone.
            log::debug!("[app_startup_sync] git clone {url} into {lcp}");
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
        (Some(_lcp), None) => {
            // Local dir exists but no clone URL — skip pull, use existing files.
            log::warn!("[app_startup_sync] clone_url not configured — skipping git pull, using local files");
            emit_step(&app, InitStep::GitPull, InitStepStatus::Warning {
                warnings: vec!["Clone URL not configured — using existing local files.".into()]
            }, None);
        }
    }

    // ── Step 2: DockerCheck (global, once) ────────────────────────────────────
    emit_step(&app, InitStep::DockerCheck, InitStepStatus::Running, None);
    match run_cmd_async("docker", &["info"], None, &[]).await {
        Ok(_) => emit_step(&app, InitStep::DockerCheck, InitStepStatus::Ok, None),
        Err(ref e) => {
            let msg = format!("Docker is not running or not installed — please start Docker Desktop. Detail: {e}");
            log::error!("[app_startup_sync] DockerCheck failed: {msg}");
            emit_step(&app, InitStep::DockerCheck, InitStepStatus::Error { message: msg.clone() }, None);
            return Err(CommandError::External(msg));
        }
    }

    // ── Steps 3-5: Per-project, in parallel ───────────────────────────────────
    // Use an empty string for local_clone_path when not configured; run_project_local_steps
    // will emit a RestoreDacpac error if no .dacpac is found at that path.
    let lcp_or_empty = local_clone_path.clone().unwrap_or_default();
    let mut join_set = tokio::task::JoinSet::new();
    for row in rows {
        let app_clone = app.clone();
        let lcp = lcp_or_empty.clone();
        join_set.spawn(async move {
            run_project_local_steps(
                &app_clone,
                &row.id,
                &row.slug,
                &row.sa_password,
                row.port,
                &lcp,
                false, // startup: start all containers, don't stop siblings
            )
            .await
        });
    }

    let mut errors: Vec<String> = Vec::new();
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                log::error!("[app_startup_sync] project local steps failed: {e}");
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

/// Find the first .dacpac file in a directory.
/// Returns true if the file looks like an unhydrated Git LFS pointer.
/// LFS pointers are plain-text files starting with "version https://git-lfs.github.com/spec/v1".
fn is_lfs_pointer(path: &Path) -> bool {
    let Ok(mut f) = std::fs::File::open(path) else { return false };
    let mut buf = [0u8; 43];
    let n = std::io::Read::read(&mut f, &mut buf).unwrap_or(0);
    buf[..n].starts_with(b"version https://git-lfs.github.com/spec/v1")
}

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
/// Pass `container` to detect early container exits (e.g. bad SA password) and fail fast.
async fn wait_for_sql_server(
    host: &str,
    port: u16,
    sa_password: &str,
    timeout_secs: u64,
    container: Option<&str>,
) -> Result<(), CommandError> {
    use tokio_util::compat::TokioAsyncWriteCompatExt;

    let addr = format!("{host}:{port}");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
    let mut attempt = 0u32;

    loop {
        attempt += 1;
        let elapsed = std::time::Instant::now().duration_since(deadline - std::time::Duration::from_secs(timeout_secs));
        log::debug!("[wait_for_sql_server] attempt={} elapsed={}s addr={}", attempt, elapsed.as_secs(), addr);

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

                // Check if the container has exited — if so, fail fast with its logs.
                if let Some(ctr) = container {
                    let state = run_cmd_async(
                        "docker",
                        &["inspect", "--format", "{{.State.Status}}", ctr],
                        None,
                        &[],
                    )
                    .await
                    .unwrap_or_default();
                    if state != "running" && !state.is_empty() {
                        let raw_logs = run_cmd_async("docker", &["logs", "--tail", "50", ctr], None, &[])
                            .await
                            .unwrap_or_default();
                        log::error!("[wait_for_sql_server] container exited. Raw logs:\n{raw_logs}");

                        // Extract only lines containing "ERROR:" for a concise user message.
                        let error_lines: Vec<&str> = raw_logs
                            .lines()
                            .filter(|l| l.contains("ERROR:"))
                            .collect();
                        let detail = if error_lines.is_empty() {
                            format!("Container exited (state: {state}). Check docker logs for '{ctr}'.")
                        } else {
                            // Strip the SQL Server log prefix (timestamp + spidNNs) to show just the message.
                            let cleaned: Vec<String> = error_lines
                                .iter()
                                .map(|l| {
                                    // Format: "2026-03-05 03:46:04.68 spid52s     ERROR: ..."
                                    // Split on "ERROR:" and take the part after it.
                                    l.splitn(2, "ERROR:").nth(1).map(|s| s.trim().to_string())
                                        .unwrap_or_else(|| l.trim().to_string())
                                })
                                .collect();
                            cleaned.join("\n")
                        };
                        return Err(CommandError::External(detail));
                    }
                }

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
    if let (Some(ref lcp), Some(ref _tok)) = (&local_clone_path, &token) {
        // Credentials are embedded in the remote URL from Settings → Apply clone.
        let git_steps: &[(&[&str], &str)] = &[
            (&["rm", "-r", "--ignore-unmatch", &slug], "git rm"),
            (&["-c", "user.name=Migration Utility", "-c", "user.email=migration@vibedata.com",
               "commit", "-m", &format!("chore: remove project {slug}")], "git commit"),
            (&["push"], "git push"),
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
        let project = insert_project_row(&conn, "Test Project", "secret", 1434).unwrap();
        assert_eq!(project.name, "Test Project");
        assert_eq!(project.slug, "test-project");
        assert!(!project.id.is_empty());
    }

    #[test]
    fn insert_project_row_slug_collision() {
        let conn = db::open_in_memory().unwrap();
        let p1 = insert_project_row(&conn, "My Project", "secret", 1434).unwrap();
        let p2 = insert_project_row(&conn, "My Project", "secret", 1434).unwrap();
        assert_eq!(p1.slug, "my-project");
        assert_ne!(p1.slug, p2.slug, "collision must produce unique slug");
    }

    #[test]
    fn project_create_full_sets_active_project() {
        let conn = db::open_in_memory().unwrap();
        let p = insert_project_row(&conn, "Acme", "pw", 1434).unwrap();
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
        let p = insert_project_row(&conn, "Alpha", "pw", 1434).unwrap();
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
        let p = insert_project_row(&conn, "Beta", "pw", 1435).unwrap();
        // reset_local only touches filesystem/docker — DB row stays
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM projects WHERE id = ?1", params![p.id], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }
}
