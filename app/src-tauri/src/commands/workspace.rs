use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    process::Command,
    sync::{
        atomic::{AtomicBool, Ordering},
        LazyLock, Mutex,
    },
};

use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use tauri::{AppHandle, Emitter, State};
use tiberius::{AuthMethod, Client, Config, EncryptionLevel};
use tokio::net::TcpStream;
use tokio::runtime::Builder as RuntimeBuilder;
use tokio_util::compat::TokioAsyncWriteCompatExt;
use uuid::Uuid;

use crate::db::DbState;
use crate::source_sql::{resolve_source_query, should_log_source_sql, SourceQuery};
use crate::types::{CommandError, WarehouseProcedure, WarehouseSchema, WarehouseTable, Workspace};

static WORKSPACE_APPLY_RUNNING: AtomicBool = AtomicBool::new(false);
static WORKSPACE_APPLY_JOBS: LazyLock<Mutex<HashMap<String, WorkspaceApplyJobStatus>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateWorkspaceArgs {
    pub name: String,
    pub migration_repo_name: Option<String>,
    pub migration_repo_path: String,
    pub fabric_url: Option<String>,
    pub fabric_service_principal_id: Option<String>,
    pub fabric_service_principal_secret: Option<String>,
    pub source_type: Option<String>,
    pub source_server: Option<String>,
    pub source_database: Option<String>,
    pub source_port: Option<i64>,
    pub source_authentication_mode: Option<String>,
    pub source_username: Option<String>,
    pub source_password: Option<String>,
    pub source_encrypt: Option<bool>,
    pub source_trust_server_certificate: Option<bool>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApplyWorkspaceArgs {
    pub name: String,
    pub migration_repo_name: String,
    pub migration_repo_path: String,
    pub fabric_url: Option<String>,
    pub fabric_service_principal_id: Option<String>,
    pub fabric_service_principal_secret: Option<String>,
    pub source_type: Option<String>,
    pub source_server: Option<String>,
    pub source_database: Option<String>,
    pub source_port: Option<i64>,
    pub source_authentication_mode: Option<String>,
    pub source_username: Option<String>,
    pub source_password: Option<String>,
    pub source_encrypt: Option<bool>,
    pub source_trust_server_certificate: Option<bool>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TestSourceConnectionArgs {
    pub source_type: String,
    pub source_server: String,
    pub source_port: i64,
    pub source_authentication_mode: String,
    pub source_username: String,
    pub source_password: String,
    pub source_encrypt: bool,
    pub source_trust_server_certificate: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscoverSourceDatabasesArgs {
    pub source_type: String,
    pub source_server: String,
    pub source_port: i64,
    pub source_authentication_mode: String,
    pub source_username: String,
    pub source_password: String,
    pub source_encrypt: bool,
    pub source_trust_server_certificate: bool,
}

fn clear_workspace_state(conn: &Connection) -> Result<(), CommandError> {
    let tx = conn.unchecked_transaction().map_err(|e| {
        log::error!("workspace_reset_state: failed to begin transaction: {e}");
        CommandError::from(e)
    })?;

    // Root delete; dependent workspace rows are removed by FK ON DELETE CASCADE.
    tx.execute("DELETE FROM workspaces", []).map_err(|e| {
        log::error!("workspace_reset_state: failed to clear workspaces: {e}");
        CommandError::from(e)
    })?;

    tx.commit().map_err(|e| {
        log::error!("workspace_reset_state: failed to commit: {e}");
        CommandError::from(e)
    })?;
    Ok(())
}

fn clear_migration_repo_contents(repo_path: &str) -> Result<(), CommandError> {
    let trimmed = repo_path.trim();
    if trimmed.is_empty() {
        return Ok(());
    }

    let path = Path::new(trimmed);
    if !path.exists() {
        std::fs::create_dir_all(path).map_err(|e| {
            log::error!(
                "workspace_reset_state: failed to recreate missing migration repo path {}: {e}",
                path.display()
            );
            CommandError::from(e)
        })?;
        return Ok(());
    }

    if !path.is_dir() {
        return Err(CommandError::Io(format!(
            "Migration repo path is not a directory: {}",
            path.display()
        )));
    }

    for entry in std::fs::read_dir(path).map_err(|e| {
        log::error!(
            "workspace_reset_state: failed to read migration repo path {}: {e}",
            path.display()
        );
        CommandError::from(e)
    })? {
        let entry = entry.map_err(CommandError::from)?;
        let entry_path = entry.path();
        let file_type = entry.file_type().map_err(CommandError::from)?;
        if file_type.is_dir() {
            std::fs::remove_dir_all(&entry_path).map_err(|e| {
                log::error!(
                    "workspace_reset_state: failed to remove directory {}: {e}",
                    entry_path.display()
                );
                CommandError::from(e)
            })?;
        } else {
            std::fs::remove_file(&entry_path).map_err(|e| {
                log::error!(
                    "workspace_reset_state: failed to remove file {}: {e}",
                    entry_path.display()
                );
                CommandError::from(e)
            })?;
        }
    }

    Ok(())
}

fn validate_source_type(source_type: &Option<String>) -> Result<(), CommandError> {
    match source_type.as_deref() {
        Some("sql_server") | Some("fabric_warehouse") | None => Ok(()),
        Some(_) => Err(CommandError::Io(
            "Unsupported source_type. Expected sql_server or fabric_warehouse".to_string(),
        )),
    }
}

fn normalize_repo_full_name(remote_url: &str) -> Option<String> {
    let trimmed = remote_url.trim().trim_end_matches('/');
    let without_git = trimmed.strip_suffix(".git").unwrap_or(trimmed);

    if let Some(after_github) = without_git.split("github.com/").nth(1) {
        return Some(after_github.trim_start_matches('/').to_lowercase());
    }

    if let Some(after_github) = without_git.split("github.com:").nth(1) {
        return Some(after_github.trim_start_matches('/').to_lowercase());
    }

    None
}

fn ensure_existing_repo_matches(repo_name: &str, repo_path: &str) -> Result<(), CommandError> {
    let output = Command::new("git")
        .args(["-C", repo_path, "config", "--get", "remote.origin.url"])
        .output()
        .map_err(CommandError::from)?;
    if !output.status.success() {
        return Err(CommandError::Io(format!(
            "Existing git repo at {repo_path} has no origin remote. Choose an empty directory or the repo path for {repo_name}."
        )));
    }

    let remote_url = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let actual = normalize_repo_full_name(&remote_url);
    let expected = repo_name.to_lowercase();
    match actual {
        Some(found) if found == expected => Ok(()),
        Some(found) => Err(CommandError::Io(format!(
            "Selected repo is {repo_name}, but existing path contains {found}. Choose a matching repo path or empty directory."
        ))),
        None => Err(CommandError::Io(format!(
            "Could not parse origin remote '{remote_url}'. Choose an empty directory or matching repo path for {repo_name}."
        ))),
    }
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkspaceApplyProgressEvent {
    stage: &'static str,
    percent: u8,
    message: String,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceApplyJobStatus {
    pub job_id: String,
    pub state: String,
    pub is_alive: bool,
    pub stage: Option<String>,
    pub percent: u8,
    pub message: Option<String>,
    pub error: Option<String>,
}

#[derive(Clone)]
struct SourceConnectionConfig {
    source_type: String,
    source_server: String,
    source_database: String,
    source_port: u16,
    source_username: String,
    source_password: String,
    source_encrypt: bool,
    source_trust_server_certificate: bool,
}

struct SqlServerInventory {
    container_id_local: Option<i64>,
    schemas: Vec<WarehouseSchema>,
    tables: Vec<WarehouseTable>,
    procedures: Vec<WarehouseProcedure>,
    columns: Vec<TableColumn>,
    indexes_constraints: Vec<IndexConstraint>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct TableColumn {
    schema_name: String,
    table_name: String,
    object_id_local: i64,
    column_name: String,
    column_id: i32,
    data_type: String,
    is_nullable: bool,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct IndexConstraint {
    schema_name: String,
    table_name: String,
    object_id_local: i64,
    index_name: String,
    index_type: String,
    is_unique: bool,
    is_primary_key: bool,
    is_unique_constraint: bool,
    columns_json: Option<String>,
}

fn emit_apply_progress(
    app: &AppHandle,
    job_id: &str,
    stage: &'static str,
    percent: u8,
    message: impl Into<String>,
) {
    let message = message.into();
    set_job_status(
        job_id,
        "running",
        Some(stage),
        percent,
        Some(message.clone()),
        None,
    );
    let payload = WorkspaceApplyProgressEvent {
        stage,
        percent,
        message,
    };
    if let Err(e) = app.emit("workspace-apply-progress", payload) {
        log::warn!("workspace_apply_and_clone: failed to emit progress event: {e}");
    }
}

fn set_job_status(
    job_id: &str,
    state: &str,
    stage: Option<&str>,
    percent: u8,
    message: Option<String>,
    error: Option<String>,
) {
    let mut jobs = WORKSPACE_APPLY_JOBS.lock().unwrap();
    jobs.insert(
        job_id.to_string(),
        WorkspaceApplyJobStatus {
            job_id: job_id.to_string(),
            state: state.to_string(),
            is_alive: state == "running",
            stage: stage.map(|s| s.to_string()),
            percent,
            message,
            error,
        },
    );
}

fn require_sql_server_source(
    args: &ApplyWorkspaceArgs,
) -> Result<SourceConnectionConfig, CommandError> {
    if args.source_type.as_deref() != Some("sql_server") {
        return Err(CommandError::Io(
            "Apply currently supports sql_server source type only".to_string(),
        ));
    }
    let source_server = args
        .source_server
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| CommandError::Io("Source server is required".to_string()))?
        .to_string();
    let source_database = args
        .source_database
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| CommandError::Io("Source database is required".to_string()))?
        .to_string();
    let source_port = validate_source_port(
        args.source_port
            .ok_or_else(|| CommandError::Io("Source port is required".to_string()))?,
    )?;
    let source_username = args
        .source_username
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| CommandError::Io("Source username is required".to_string()))?
        .to_string();
    let source_password = args
        .source_password
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| CommandError::Io("Source password is required".to_string()))?
        .to_string();

    Ok(SourceConnectionConfig {
        source_type: "sql_server".to_string(),
        source_server,
        source_database,
        source_port,
        source_username,
        source_password,
        source_encrypt: args.source_encrypt.unwrap_or(true),
        source_trust_server_certificate: args.source_trust_server_certificate.unwrap_or(false),
    })
}

fn build_tiberius_config(cfg: &SourceConnectionConfig) -> Config {
    let mut config = Config::new();
    config.host(cfg.source_server.trim());
    config.port(cfg.source_port);
    config.database(cfg.source_database.trim());
    config.authentication(AuthMethod::sql_server(
        cfg.source_username.trim(),
        cfg.source_password.trim(),
    ));
    config.encryption(if cfg.source_encrypt {
        EncryptionLevel::Required
    } else {
        EncryptionLevel::Off
    });
    if cfg.source_trust_server_certificate {
        config.trust_cert();
    }
    config
}

fn fetch_sql_server_inventory(
    cfg: &SourceConnectionConfig,
    app: &AppHandle,
    job_id: &str,
) -> Result<SqlServerInventory, CommandError> {
    let runtime = RuntimeBuilder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .map_err(|e| CommandError::Io(format!("Failed to create async runtime: {e}")))?;

    let config = build_tiberius_config(cfg);
    runtime.block_on(async move {
        let tcp = TcpStream::connect(config.get_addr()).await.map_err(|e| {
            log::error!("workspace_apply_and_clone: failed to connect tcp: {e}");
            CommandError::Io(format!("Could not connect to source endpoint: {e}"))
        })?;
        tcp.set_nodelay(true).map_err(|e| {
            log::error!("workspace_apply_and_clone: failed to set nodelay: {e}");
            CommandError::Io(format!("Could not configure socket: {e}"))
        })?;

        let mut client = Client::connect(config, tcp.compat_write())
            .await
            .map_err(|e| {
                let _ = e;
                log::error!("workspace_apply_and_clone: failed to authenticate");
                CommandError::Io("Connection test failed".to_string())
            })?;

        let container_id_query =
            resolve_source_query(&cfg.source_type, SourceQuery::DiscoverContainerId)?;
        if should_log_source_sql() {
            log::debug!(
                "workspace_apply_and_clone: executing query={} source_type={} sql={}",
                SourceQuery::DiscoverContainerId.name(),
                cfg.source_type,
                container_id_query.trim()
            );
        }
        let container_rows = client
            .simple_query(container_id_query)
            .await
            .map_err(|e| {
                log::error!("workspace_apply_and_clone: container id query failed: {e}");
                CommandError::Io(format!("Container discovery failed: {e}"))
            })?
            .into_first_result()
            .await
            .map_err(|e| {
                log::error!("workspace_apply_and_clone: container id result parse failed: {e}");
                CommandError::Io(format!("Container discovery failed: {e}"))
            })?;
        let container_id_local = container_rows.first().and_then(|row| row.get::<i64, _>(0));

        emit_apply_progress(
            app,
            job_id,
            "importing_schemas",
            50,
            "Importing source schemas...",
        );
        let schemas_query = resolve_source_query(&cfg.source_type, SourceQuery::DiscoverSchemas)?;
        if should_log_source_sql() {
            log::debug!(
                "workspace_apply_and_clone: executing query={} source_type={} sql={}",
                SourceQuery::DiscoverSchemas.name(),
                cfg.source_type,
                schemas_query.trim()
            );
        }
        let schema_rows = client
            .simple_query(schemas_query)
            .await
            .map_err(|e| {
                log::error!("workspace_apply_and_clone: schema query failed: {e}");
                CommandError::Io(format!("Schema discovery failed: {e}"))
            })?
            .into_first_result()
            .await
            .map_err(|e| {
                log::error!("workspace_apply_and_clone: schema result parse failed: {e}");
                CommandError::Io(format!("Schema discovery failed: {e}"))
            })?;

        let mut schemas: Vec<WarehouseSchema> = Vec::with_capacity(schema_rows.len());
        for row in schema_rows {
            let schema_name = row
                .get::<&str, _>(1)
                .ok_or_else(|| {
                    CommandError::Io("Schema discovery returned invalid data".to_string())
                })?
                .to_string();
            schemas.push(WarehouseSchema {
                warehouse_item_id: String::new(),
                schema_name,
                schema_id_local: row.get::<i64, _>(0),
            });
        }
        log::info!(
            "workspace_apply_and_clone: discovered {} schemas",
            schemas.len()
        );

        emit_apply_progress(
            app,
            job_id,
            "importing_tables",
            65,
            "Importing source tables...",
        );
        let tables_query = resolve_source_query(&cfg.source_type, SourceQuery::DiscoverTables)?;
        if should_log_source_sql() {
            log::debug!(
                "workspace_apply_and_clone: executing query={} source_type={} sql={}",
                SourceQuery::DiscoverTables.name(),
                cfg.source_type,
                tables_query.trim()
            );
        }
        let table_rows = client
            .simple_query(tables_query)
            .await
            .map_err(|e| {
                log::error!("workspace_apply_and_clone: table query failed: {e}");
                CommandError::Io(format!("Table discovery failed: {e}"))
            })?
            .into_first_result()
            .await
            .map_err(|e| {
                log::error!("workspace_apply_and_clone: table result parse failed: {e}");
                CommandError::Io(format!("Table discovery failed: {e}"))
            })?;

        let mut tables: Vec<WarehouseTable> = Vec::with_capacity(table_rows.len());
        for row in table_rows {
            let schema_name = row
                .get::<&str, _>(0)
                .ok_or_else(|| {
                    CommandError::Io("Table discovery returned invalid schema".to_string())
                })?
                .to_string();
            let table_name = row
                .get::<&str, _>(1)
                .ok_or_else(|| {
                    CommandError::Io("Table discovery returned invalid table".to_string())
                })?
                .to_string();
            tables.push(WarehouseTable {
                warehouse_item_id: String::new(),
                schema_name,
                table_name,
                object_id_local: row.get::<i64, _>(2),
                row_count: row.get::<i64, _>(3),
            });
        }
        log::info!(
            "workspace_apply_and_clone: discovered {} tables",
            tables.len()
        );

        emit_apply_progress(
            app,
            job_id,
            "importing_procedures",
            80,
            "Importing source procedures...",
        );
        let procedures_query =
            resolve_source_query(&cfg.source_type, SourceQuery::DiscoverProcedures)?;
        if should_log_source_sql() {
            log::debug!(
                "workspace_apply_and_clone: executing query={} source_type={} sql={}",
                SourceQuery::DiscoverProcedures.name(),
                cfg.source_type,
                procedures_query.trim()
            );
        }
        let procedure_rows = client
            .simple_query(procedures_query)
            .await
            .map_err(|e| {
                log::error!("workspace_apply_and_clone: procedure query failed: {e}");
                CommandError::Io(format!("Procedure discovery failed: {e}"))
            })?
            .into_first_result()
            .await
            .map_err(|e| {
                log::error!("workspace_apply_and_clone: procedure result parse failed: {e}");
                CommandError::Io(format!("Procedure discovery failed: {e}"))
            })?;

        let mut procedures: Vec<WarehouseProcedure> = Vec::with_capacity(procedure_rows.len());
        for row in procedure_rows {
            let schema_name = row
                .get::<&str, _>(0)
                .ok_or_else(|| {
                    CommandError::Io("Procedure discovery returned invalid schema".to_string())
                })?
                .to_string();
            let procedure_name = row
                .get::<&str, _>(1)
                .ok_or_else(|| {
                    CommandError::Io("Procedure discovery returned invalid procedure".to_string())
                })?
                .to_string();
            procedures.push(WarehouseProcedure {
                warehouse_item_id: String::new(),
                schema_name,
                procedure_name,
                object_id_local: row.get::<i64, _>(2),
                sql_body: row.get::<&str, _>(3).map(|v| v.to_string()),
            });
        }
        log::info!(
            "workspace_apply_and_clone: discovered {} procedures",
            procedures.len()
        );

        emit_apply_progress(
            app,
            job_id,
            "importing_columns",
            90,
            "Importing table columns...",
        );
        let columns_query = resolve_source_query(&cfg.source_type, SourceQuery::DiscoverColumns)?;
        if should_log_source_sql() {
            log::debug!(
                "workspace_apply_and_clone: executing query={} source_type={} sql={}",
                SourceQuery::DiscoverColumns.name(),
                cfg.source_type,
                columns_query.trim()
            );
        }
        let column_rows = client
            .simple_query(columns_query)
            .await
            .map_err(|e| {
                log::error!("workspace_apply_and_clone: column query failed: {e}");
                CommandError::Io(format!("Column discovery failed: {e}"))
            })?
            .into_first_result()
            .await
            .map_err(|e| {
                log::error!("workspace_apply_and_clone: column result parse failed: {e}");
                CommandError::Io(format!("Column discovery failed: {e}"))
            })?;

        let mut columns: Vec<TableColumn> = Vec::with_capacity(column_rows.len());
        for row in column_rows {
            let schema_name = row
                .get::<&str, _>(0)
                .ok_or_else(|| {
                    CommandError::Io("Column discovery returned invalid schema".to_string())
                })?
                .to_string();
            let table_name = row
                .get::<&str, _>(1)
                .ok_or_else(|| {
                    CommandError::Io("Column discovery returned invalid table".to_string())
                })?
                .to_string();
            let object_id_local = row.get::<i64, _>(2).ok_or_else(|| {
                CommandError::Io("Column discovery returned invalid object_id".to_string())
            })?;
            let column_name = row
                .get::<&str, _>(3)
                .ok_or_else(|| {
                    CommandError::Io("Column discovery returned invalid column name".to_string())
                })?
                .to_string();
            let column_id = row.get::<i32, _>(4).ok_or_else(|| {
                CommandError::Io("Column discovery returned invalid column_id".to_string())
            })?;
            let data_type = row
                .get::<&str, _>(5)
                .ok_or_else(|| {
                    CommandError::Io("Column discovery returned invalid data_type".to_string())
                })?
                .to_string();
            let is_nullable = row.get::<bool, _>(6).ok_or_else(|| {
                CommandError::Io("Column discovery returned invalid is_nullable".to_string())
            })?;

            columns.push(TableColumn {
                schema_name,
                table_name,
                object_id_local,
                column_name,
                column_id,
                data_type,
                is_nullable,
            });
        }
        log::info!(
            "workspace_apply_and_clone: discovered {} columns",
            columns.len()
        );

        emit_apply_progress(
            app,
            job_id,
            "importing_indexes",
            95,
            "Importing indexes and constraints...",
        );
        let indexes_query = resolve_source_query(&cfg.source_type, SourceQuery::DiscoverIndexesConstraints)?;
        if should_log_source_sql() {
            log::debug!(
                "workspace_apply_and_clone: executing query={} source_type={} sql={}",
                SourceQuery::DiscoverIndexesConstraints.name(),
                cfg.source_type,
                indexes_query.trim()
            );
        }
        let index_rows = client
            .simple_query(indexes_query)
            .await
            .map_err(|e| {
                log::error!("workspace_apply_and_clone: index query failed: {e}");
                CommandError::Io(format!("Index discovery failed: {e}"))
            })?
            .into_first_result()
            .await
            .map_err(|e| {
                log::error!("workspace_apply_and_clone: index result parse failed: {e}");
                CommandError::Io(format!("Index discovery failed: {e}"))
            })?;

        let mut indexes_constraints: Vec<IndexConstraint> = Vec::with_capacity(index_rows.len());
        for row in index_rows {
            let schema_name = row
                .get::<&str, _>(0)
                .ok_or_else(|| {
                    CommandError::Io("Index discovery returned invalid schema".to_string())
                })?
                .to_string();
            let table_name = row
                .get::<&str, _>(1)
                .ok_or_else(|| {
                    CommandError::Io("Index discovery returned invalid table".to_string())
                })?
                .to_string();
            let object_id_local = row.get::<i64, _>(2).ok_or_else(|| {
                CommandError::Io("Index discovery returned invalid object_id".to_string())
            })?;
            let index_name = row
                .get::<&str, _>(3)
                .ok_or_else(|| {
                    CommandError::Io("Index discovery returned invalid index name".to_string())
                })?
                .to_string();
            let index_type = row
                .get::<&str, _>(4)
                .ok_or_else(|| {
                    CommandError::Io("Index discovery returned invalid index type".to_string())
                })?
                .to_string();
            let is_unique = row.get::<bool, _>(5).ok_or_else(|| {
                CommandError::Io("Index discovery returned invalid is_unique".to_string())
            })?;
            let is_primary_key = row.get::<bool, _>(6).ok_or_else(|| {
                CommandError::Io("Index discovery returned invalid is_primary_key".to_string())
            })?;
            let is_unique_constraint = row.get::<bool, _>(7).ok_or_else(|| {
                CommandError::Io("Index discovery returned invalid is_unique_constraint".to_string())
            })?;
            let columns_json = row.get::<&str, _>(8).map(|v| v.to_string());

            indexes_constraints.push(IndexConstraint {
                schema_name,
                table_name,
                object_id_local,
                index_name,
                index_type,
                is_unique,
                is_primary_key,
                is_unique_constraint,
                columns_json,
            });
        }
        log::info!(
            "workspace_apply_and_clone: discovered {} indexes/constraints",
            indexes_constraints.len()
        );

        log::info!(
            "workspace_apply_and_clone: discovered {} schemas, {} tables, {} procedures, {} columns, {} indexes/constraints",
            schemas.len(),
            tables.len(),
            procedures.len(),
            columns.len(),
            indexes_constraints.len()
        );

        Ok(SqlServerInventory {
            container_id_local,
            schemas,
            tables,
            procedures,
            columns,
            indexes_constraints,
        })
    })
}

fn upsert_workspace(
    conn: &Connection,
    args: &ApplyWorkspaceArgs,
    repo_name: &str,
    repo_path: &str,
) -> Result<Workspace, CommandError> {
    let existing: Option<(String, String)> = conn
        .query_row(
            "SELECT id, created_at FROM workspaces ORDER BY created_at DESC LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(CommandError::from)?;

    let workspace = if let Some((id, created_at)) = existing {
        conn.execute(
            "UPDATE workspaces SET
                display_name=?1,
                migration_repo_name=?2,
                migration_repo_path=?3,
                fabric_url=?4,
                fabric_service_principal_id=?5,
                fabric_service_principal_secret=?6,
                source_type=?7,
                source_server=?8,
                source_database=?9,
                source_port=?10,
                source_authentication_mode=?11,
                source_username=?12,
                source_password=?13,
                source_encrypt=?14,
                source_trust_server_certificate=?15
             WHERE id=?16",
            params![
                args.name,
                repo_name,
                repo_path,
                args.fabric_url,
                args.fabric_service_principal_id,
                args.fabric_service_principal_secret,
                args.source_type,
                args.source_server,
                args.source_database,
                args.source_port,
                args.source_authentication_mode,
                args.source_username,
                args.source_password,
                args.source_encrypt,
                args.source_trust_server_certificate,
                id
            ],
        )
        .map_err(|e| {
            log::error!("workspace_apply_and_clone: failed to update workspace: {e}");
            CommandError::from(e)
        })?;

        Workspace {
            id,
            display_name: args.name.clone(),
            migration_repo_name: Some(repo_name.to_string()),
            migration_repo_path: repo_path.to_string(),
            fabric_url: args.fabric_url.clone(),
            fabric_service_principal_id: args.fabric_service_principal_id.clone(),
            fabric_service_principal_secret: args.fabric_service_principal_secret.clone(),
            source_type: args.source_type.clone(),
            source_server: args.source_server.clone(),
            source_database: args.source_database.clone(),
            source_port: args.source_port,
            source_authentication_mode: args.source_authentication_mode.clone(),
            source_username: args.source_username.clone(),
            source_password: args.source_password.clone(),
            source_encrypt: args.source_encrypt,
            source_trust_server_certificate: args.source_trust_server_certificate,
            created_at,
        }
    } else {
        let id = Uuid::new_v4().to_string();
        let created_at = Utc::now().to_rfc3339();
        conn.execute(
            "INSERT INTO workspaces(
                id, display_name, migration_repo_name, migration_repo_path, fabric_url,
                fabric_service_principal_id, fabric_service_principal_secret, source_type,
                source_server, source_database, source_port, source_authentication_mode,
                source_username, source_password, source_encrypt, source_trust_server_certificate,
                created_at
              ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
            params![
                id,
                args.name,
                repo_name,
                repo_path,
                args.fabric_url,
                args.fabric_service_principal_id,
                args.fabric_service_principal_secret,
                args.source_type,
                args.source_server,
                args.source_database,
                args.source_port,
                args.source_authentication_mode,
                args.source_username,
                args.source_password,
                args.source_encrypt,
                args.source_trust_server_certificate,
                created_at
            ],
        )
        .map_err(|e| {
            log::error!("workspace_apply_and_clone: failed to create workspace: {e}");
            CommandError::from(e)
        })?;

        Workspace {
            id,
            display_name: args.name.clone(),
            migration_repo_name: Some(repo_name.to_string()),
            migration_repo_path: repo_path.to_string(),
            fabric_url: args.fabric_url.clone(),
            fabric_service_principal_id: args.fabric_service_principal_id.clone(),
            fabric_service_principal_secret: args.fabric_service_principal_secret.clone(),
            source_type: args.source_type.clone(),
            source_server: args.source_server.clone(),
            source_database: args.source_database.clone(),
            source_port: args.source_port,
            source_authentication_mode: args.source_authentication_mode.clone(),
            source_username: args.source_username.clone(),
            source_password: args.source_password.clone(),
            source_encrypt: args.source_encrypt,
            source_trust_server_certificate: args.source_trust_server_certificate,
            created_at,
        }
    };

    Ok(workspace)
}

fn persist_sql_server_inventory(
    conn: &Connection,
    workspace_id: &str,
    source_cfg: &SourceConnectionConfig,
    inventory: &SqlServerInventory,
    app: Option<&AppHandle>,
    job_id: Option<&str>,
) -> Result<(), CommandError> {
    let tx = conn.unchecked_transaction().map_err(|e| {
        log::error!("workspace_apply_and_clone: failed to begin inventory transaction: {e}");
        CommandError::from(e)
    })?;

    let source_database = source_cfg.source_database.as_str();
    let source_item_id = format!("source-db-{workspace_id}");

    tx.execute(
        "INSERT INTO items(id, workspace_id, display_name, description, folder_id, item_type, connection_string, collation_type)
         VALUES (?1, ?2, ?3, ?4, NULL, 'Warehouse', NULL, NULL)
         ON CONFLICT(id) DO UPDATE SET
           workspace_id = excluded.workspace_id,
           display_name = excluded.display_name,
           description = excluded.description,
           folder_id = excluded.folder_id,
           item_type = excluded.item_type,
           connection_string = excluded.connection_string,
           collation_type = excluded.collation_type",
        params![
            source_item_id,
            workspace_id,
            source_database,
            Some(format!("SQL Server source database {source_database}"))
        ],
    )
    .map_err(|e| {
        log::error!("workspace_apply_and_clone: failed to upsert source item: {e}");
        CommandError::from(e)
    })?;

    let mut existing_keys: HashSet<(String, String)> = HashSet::new();
    {
        let mut existing_tables_stmt = tx
            .prepare(
                "SELECT schema_name, table_name
                 FROM warehouse_tables
                 WHERE warehouse_item_id = ?1",
            )
            .map_err(|e| {
                log::error!("workspace_apply_and_clone: failed to prepare existing table diff: {e}");
                CommandError::from(e)
            })?;
        let existing_rows = existing_tables_stmt
            .query_map(params![source_item_id.clone()], |row| {
                let schema_name: String = row.get(0)?;
                let table_name: String = row.get(1)?;
                Ok((schema_name, table_name))
            })
            .map_err(|e| {
                log::error!("workspace_apply_and_clone: failed to read existing table diff: {e}");
                CommandError::from(e)
            })?;
        for row in existing_rows {
            let (schema_name, table_name) = row.map_err(CommandError::from)?;
            existing_keys.insert((schema_name.to_lowercase(), table_name.to_lowercase()));
        }
    }

    let discovered_keys: HashSet<(String, String)> = inventory
        .tables
        .iter()
        .map(|t| (t.schema_name.to_lowercase(), t.table_name.to_lowercase()))
        .collect();
    let removed_keys: Vec<(String, String)> = existing_keys
        .difference(&discovered_keys)
        .cloned()
        .collect();
    tx.execute(
        "DELETE FROM warehouse_procedures WHERE warehouse_item_id=?1",
        params![source_item_id],
    )
    .map_err(|e| {
        log::error!("workspace_apply_and_clone: failed to clear source procedures: {e}");
        CommandError::from(e)
    })?;
    tx.execute(
        "DELETE FROM warehouse_schemas WHERE warehouse_item_id=?1",
        params![source_item_id],
    )
    .map_err(|e| {
        log::error!("workspace_apply_and_clone: failed to clear source schemas: {e}");
        CommandError::from(e)
    })?;

    let total_objects =
        inventory.schemas.len() + inventory.tables.len() + inventory.procedures.len() + inventory.columns.len() + inventory.indexes_constraints.len();
    let mut imported_objects = 0usize;

    for schema in &inventory.schemas {
        tx.execute(
            "INSERT OR REPLACE INTO warehouse_schemas(warehouse_item_id, schema_name, schema_id_local)
             VALUES (?1, ?2, ?3)",
            params![source_item_id, schema.schema_name, schema.schema_id_local],
        )
        .map_err(|e| {
            log::error!("workspace_apply_and_clone: failed to upsert schema: {e}");
            CommandError::from(e)
        })?;
        imported_objects += 1;
        maybe_emit_object_import_progress(app, job_id, imported_objects, total_objects);
    }
    for table in &inventory.tables {
        tx.execute(
            "INSERT OR REPLACE INTO warehouse_tables(warehouse_item_id, schema_name, table_name, object_id_local)
             VALUES (?1, ?2, ?3, ?4)",
            params![source_item_id, table.schema_name, table.table_name, table.object_id_local],
        )
        .map_err(|e| {
            log::error!("workspace_apply_and_clone: failed to upsert table: {e}");
            CommandError::from(e)
        })?;
        imported_objects += 1;
        maybe_emit_object_import_progress(app, job_id, imported_objects, total_objects);
    }
    for (schema_name_lc, table_name_lc) in removed_keys {
        tx.execute(
            "DELETE FROM warehouse_tables
             WHERE warehouse_item_id = ?1
               AND LOWER(schema_name) = ?2
               AND LOWER(table_name) = ?3",
            params![source_item_id, schema_name_lc, table_name_lc],
        )
        .map_err(|e| {
            log::error!("workspace_apply_and_clone: failed to remove stale source table: {e}");
            CommandError::from(e)
        })?;
    }
    for procedure in &inventory.procedures {
        tx.execute(
            "INSERT OR REPLACE INTO warehouse_procedures(warehouse_item_id, schema_name, procedure_name, object_id_local, sql_body)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                source_item_id,
                procedure.schema_name,
                procedure.procedure_name,
                procedure.object_id_local,
                procedure.sql_body.as_deref()
            ],
        )
        .map_err(|e| {
            log::error!("workspace_apply_and_clone: failed to upsert procedure: {e}");
            CommandError::from(e)
        })?;
        imported_objects += 1;
        maybe_emit_object_import_progress(app, job_id, imported_objects, total_objects);
    }

    persist_sql_server_canonical_model(&tx, workspace_id, source_cfg, inventory)?;

    tx.commit().map_err(|e| {
        log::error!("workspace_apply_and_clone: failed to commit source inventory: {e}");
        CommandError::from(e)
    })?;
    Ok(())
}

fn canonical_source_external_id(cfg: &SourceConnectionConfig) -> String {
    format!(
        "sql_server://{}:{}/{}",
        cfg.source_server.trim().to_lowercase(),
        cfg.source_port,
        cfg.source_database.trim().to_lowercase()
    )
}

fn source_row_id_for_workspace(workspace_id: &str) -> String {
    format!("source-{workspace_id}")
}

fn persist_sql_server_canonical_model(
    tx: &rusqlite::Transaction<'_>,
    workspace_id: &str,
    source_cfg: &SourceConnectionConfig,
    inventory: &SqlServerInventory,
) -> Result<(), CommandError> {
    let source_id = source_row_id_for_workspace(workspace_id);
    let source_external_id = canonical_source_external_id(source_cfg);
    let container_id = format!("container-{workspace_id}-sqlserver-db");
    let container_external_id = inventory
        .container_id_local
        .map(|v| v.to_string())
        .ok_or_else(|| {
            CommandError::Io(
                "SQL Server container discovery did not return database_id (DB_ID())".to_string(),
            )
        })?;

    tx.execute(
        "DELETE FROM sources WHERE workspace_id = ?1",
        params![workspace_id],
    )
    .map_err(CommandError::from)?;

    tx.execute(
        "INSERT INTO sources(
            id, workspace_id, source_type, external_source_id, display_name,
            source_server, source_database, source_port, source_authentication_mode
         ) VALUES (?1, ?2, 'sql_server', ?3, ?4, ?5, ?6, ?7, 'sql_password')",
        params![
            source_id,
            workspace_id,
            source_external_id,
            source_cfg.source_database,
            source_cfg.source_server,
            source_cfg.source_database,
            i64::from(source_cfg.source_port),
        ],
    )
    .map_err(CommandError::from)?;

    tx.execute(
        "INSERT INTO containers(id, source_id, container_type, external_container_id, container_name)
         VALUES (?1, ?2, 'database', ?3, ?4)",
        params![
            container_id,
            source_id,
            container_external_id,
            source_cfg.source_database
        ],
    )
    .map_err(CommandError::from)?;

    for schema in &inventory.schemas {
        let namespace_id = format!(
            "namespace-{workspace_id}-{}",
            schema.schema_name.to_lowercase()
        );
        tx.execute(
            "INSERT INTO namespaces(id, container_id, namespace_name, external_namespace_id)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                namespace_id,
                container_id,
                schema.schema_name,
                schema.schema_id_local.map(|v| v.to_string())
            ],
        )
        .map_err(CommandError::from)?;
    }

    for table in &inventory.tables {
        let namespace_id = format!(
            "namespace-{workspace_id}-{}",
            table.schema_name.to_lowercase()
        );
        let object_id = format!(
            "object-{workspace_id}-table-{}-{}",
            table.schema_name.to_lowercase(),
            table.table_name.to_lowercase()
        );
        tx.execute(
            "INSERT INTO data_objects(id, namespace_id, object_name, object_type, external_object_id, sql_body)
             VALUES (?1, ?2, ?3, 'table', ?4, NULL)",
            params![
                object_id,
                namespace_id,
                table.table_name,
                table.object_id_local.map(|v| v.to_string())
            ],
        )
        .map_err(CommandError::from)?;

        if let Some(row_count) = table.row_count {
            let partition_id = format!(
                "partition-{workspace_id}-{}-{}-1",
                table.schema_name.to_lowercase(),
                table.table_name.to_lowercase()
            );
            tx.execute(
                "INSERT INTO sqlserver_partitions(id, data_object_id, partition_number, row_count)
                 VALUES (?1, ?2, ?3, ?4)",
                params![partition_id, object_id, 1_i64, row_count],
            )
            .map_err(CommandError::from)?;
        }
    }

    for column in &inventory.columns {
        let object_id = format!(
            "object-{workspace_id}-table-{}-{}",
            column.schema_name.to_lowercase(),
            column.table_name.to_lowercase()
        );
        let column_id = format!(
            "column-{workspace_id}-{}-{}-{}",
            column.schema_name.to_lowercase(),
            column.table_name.to_lowercase(),
            column.column_name.to_lowercase()
        );
        tx.execute(
            "INSERT INTO sqlserver_object_columns(id, data_object_id, column_name, column_id, data_type, is_nullable)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                column_id,
                object_id,
                column.column_name,
                column.column_id,
                column.data_type,
                if column.is_nullable { 1 } else { 0 }
            ],
        )
        .map_err(CommandError::from)?;
    }

    for index_constraint in &inventory.indexes_constraints {
        let object_id = format!(
            "object-{workspace_id}-table-{}-{}",
            index_constraint.schema_name.to_lowercase(),
            index_constraint.table_name.to_lowercase()
        );
        let constraint_index_id = format!(
            "constraint-index-{workspace_id}-{}-{}-{}",
            index_constraint.schema_name.to_lowercase(),
            index_constraint.table_name.to_lowercase(),
            index_constraint.index_name.to_lowercase()
        );
        
        // Determine constraint type based on flags
        let constraint_type = if index_constraint.is_primary_key {
            "PRIMARY KEY"
        } else if index_constraint.is_unique_constraint {
            "UNIQUE"
        } else {
            "INDEX"
        };

        tx.execute(
            "INSERT INTO sqlserver_constraints_indexes(id, data_object_id, constraint_name, index_name, constraint_type, definition_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                constraint_index_id,
                object_id,
                if index_constraint.is_primary_key || index_constraint.is_unique_constraint {
                    Some(index_constraint.index_name.as_str())
                } else {
                    None::<&str>
                },
                Some(index_constraint.index_name.as_str()),
                constraint_type,
                index_constraint.columns_json.as_deref()
            ],
        )
        .map_err(CommandError::from)?;
    }

    for procedure in &inventory.procedures {
        let namespace_id = format!(
            "namespace-{workspace_id}-{}",
            procedure.schema_name.to_lowercase()
        );
        let object_id = format!(
            "object-{workspace_id}-procedure-{}-{}",
            procedure.schema_name.to_lowercase(),
            procedure.procedure_name.to_lowercase()
        );
        tx.execute(
            "INSERT INTO data_objects(id, namespace_id, object_name, object_type, external_object_id, sql_body)
             VALUES (?1, ?2, ?3, 'procedure', ?4, ?5)",
            params![
                object_id,
                namespace_id,
                procedure.procedure_name,
                procedure.object_id_local.map(|v| v.to_string()),
                procedure.sql_body.as_deref()
            ],
        )
        .map_err(CommandError::from)?;
    }

    Ok(())
}

fn maybe_emit_object_import_progress(
    app: Option<&AppHandle>,
    job_id: Option<&str>,
    imported_objects: usize,
    total_objects: usize,
) {
    let Some(app) = app else {
        return;
    };
    let Some(job_id) = job_id else {
        return;
    };
    if total_objects == 0 {
        emit_apply_progress(
            app,
            job_id,
            "importing_source_metadata",
            99,
            "Importing source metadata... 0/0",
        );
        return;
    }

    if imported_objects == 1
        || imported_objects == total_objects
        || imported_objects.is_multiple_of(25)
    {
        let ratio = imported_objects as f64 / total_objects as f64;
        let percent = (95.0 + (ratio * 4.0)).round() as u8;
        emit_apply_progress(
            app,
            job_id,
            "importing_source_metadata",
            percent.min(99),
            format!("Importing source metadata... {imported_objects}/{total_objects}"),
        );
    }
}

#[tauri::command]
pub fn workspace_create(
    args: CreateWorkspaceArgs,
    state: State<DbState>,
) -> Result<Workspace, CommandError> {
    log::info!("workspace_create: name={}", args.name);
    validate_source_type(&args.source_type)?;
    if !Path::new(&args.migration_repo_path).is_dir() {
        log::error!(
            "workspace_create: migration_repo_path does not exist: {}",
            args.migration_repo_path
        );
        return Err(CommandError::Io(format!(
            "Migration repo path does not exist or is not a directory: {}",
            args.migration_repo_path
        )));
    }
    let conn = state.0.lock().unwrap();
    let id = Uuid::new_v4().to_string();
    let created_at = Utc::now().to_rfc3339();
    conn.execute(
        "INSERT INTO workspaces(
            id, display_name, migration_repo_name, migration_repo_path, fabric_url,
            fabric_service_principal_id, fabric_service_principal_secret, source_type,
            source_server, source_database, source_port, source_authentication_mode,
            source_username, source_password, source_encrypt, source_trust_server_certificate,
            created_at
          ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
        params![
            id,
            args.name,
            args.migration_repo_name,
            args.migration_repo_path,
            args.fabric_url,
            args.fabric_service_principal_id,
            args.fabric_service_principal_secret,
            args.source_type,
            args.source_server,
            args.source_database,
            args.source_port,
            args.source_authentication_mode,
            args.source_username,
            args.source_password,
            args.source_encrypt,
            args.source_trust_server_certificate,
            created_at
        ],
    )
    .map_err(|e| {
        log::error!("workspace_create: failed: {e}");
        CommandError::from(e)
    })?;
    Ok(Workspace {
        id,
        display_name: args.name,
        migration_repo_name: args.migration_repo_name,
        migration_repo_path: args.migration_repo_path,
        fabric_url: args.fabric_url,
        fabric_service_principal_id: args.fabric_service_principal_id,
        fabric_service_principal_secret: args.fabric_service_principal_secret,
        source_type: args.source_type,
        source_server: args.source_server,
        source_database: args.source_database,
        source_port: args.source_port,
        source_authentication_mode: args.source_authentication_mode,
        source_username: args.source_username,
        source_password: args.source_password,
        source_encrypt: args.source_encrypt,
        source_trust_server_certificate: args.source_trust_server_certificate,
        created_at,
    })
}

#[tauri::command]
pub fn workspace_apply_and_clone(
    args: ApplyWorkspaceArgs,
    state: State<DbState>,
    app: AppHandle,
) -> Result<Workspace, CommandError> {
    let conn = state.0.lock().unwrap();
    run_workspace_apply_with_conn(args, &conn, &app, "direct-apply")
}

fn run_workspace_apply_with_conn(
    args: ApplyWorkspaceArgs,
    conn: &Connection,
    app: &AppHandle,
    job_id: &str,
) -> Result<Workspace, CommandError> {
    log::info!(
        "workspace_apply_and_clone: name={} repo={}",
        args.name,
        args.migration_repo_name
    );

    let repo_name = args.migration_repo_name.trim().to_string();
    let repo_path = args.migration_repo_path.trim().to_string();
    if repo_name.is_empty() {
        return Err(CommandError::Io(
            "Migration repo name is required".to_string(),
        ));
    }
    if !repo_name.contains('/') {
        return Err(CommandError::Io(
            "Migration repo name must be in owner/repo format".to_string(),
        ));
    }
    if repo_path.is_empty() {
        return Err(CommandError::Io(
            "Migration repo path is required".to_string(),
        ));
    }
    validate_source_type(&args.source_type)?;
    let source_cfg = require_sql_server_source(&args)?;

    let token = {
        let settings = crate::db::read_settings(conn).map_err(CommandError::Io)?;
        settings
            .github_oauth_token
            .ok_or_else(|| CommandError::Io("GitHub is not connected".to_string()))?
    };

    emit_apply_progress(
        app,
        job_id,
        "validating_source_access",
        15,
        "Validating source connectivity and access...",
    );
    let inventory = fetch_sql_server_inventory(&source_cfg, app, job_id)?;

    emit_apply_progress(
        app,
        job_id,
        "verifying_repo",
        35,
        "Verifying migration repository...",
    );
    if let Err(e) = clone_repo_if_needed(&repo_name, &repo_path, &token) {
        log::error!("workspace_apply_and_clone: failed: {}", e);
        return Err(e);
    }

    emit_apply_progress(
        app,
        job_id,
        "persisting_workspace",
        90,
        "Persisting source settings...",
    );
    let workspace = upsert_workspace(conn, &args, &repo_name, &repo_path)?;
    emit_apply_progress(
        app,
        job_id,
        "importing_source_metadata",
        95,
        "Writing source metadata to local workspace...",
    );
    persist_sql_server_inventory(
        conn,
        &workspace.id,
        &source_cfg,
        &inventory,
        Some(app),
        Some(job_id),
    )?;
    crate::db::write_scope_finalized(conn, false).map_err(CommandError::Io)?;
    crate::db::write_plan_finalized(conn, false).map_err(CommandError::Io)?;
    let _ = crate::db::reconcile_and_persist_app_phase(conn).map_err(CommandError::Io)?;

    emit_apply_progress(app, job_id, "completed", 100, "Apply completed.");
    Ok(workspace)
}

#[tauri::command]
pub fn workspace_apply_start(
    args: ApplyWorkspaceArgs,
    app: AppHandle,
) -> Result<String, CommandError> {
    if WORKSPACE_APPLY_RUNNING
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return Err(CommandError::Io(
            "An apply operation is already running".to_string(),
        ));
    }

    let job_id = Uuid::new_v4().to_string();
    set_job_status(
        &job_id,
        "running",
        Some("starting"),
        5,
        Some("Starting apply...".to_string()),
        None,
    );

    let app_handle = app.clone();
    let job_id_for_thread = job_id.clone();
    std::thread::spawn(move || {
        let result = (|| -> Result<(), CommandError> {
            use tauri::Manager;
            let db_path = app_handle
                .path()
                .app_data_dir()
                .map_err(|e| CommandError::Io(e.to_string()))?
                .join("migration-utility.db");
            let conn = crate::db::open(&db_path).map_err(|e| CommandError::Io(e.to_string()))?;
            let _workspace =
                run_workspace_apply_with_conn(args, &conn, &app_handle, &job_id_for_thread)?;
            Ok(())
        })();

        match result {
            Ok(_) => set_job_status(
                &job_id_for_thread,
                "succeeded",
                Some("completed"),
                100,
                Some("Apply completed.".to_string()),
                None,
            ),
            Err(e) => {
                let message = e.to_string();
                set_job_status(
                    &job_id_for_thread,
                    "failed",
                    Some("failed"),
                    0,
                    Some("Apply failed.".to_string()),
                    Some(message),
                );
            }
        }
        WORKSPACE_APPLY_RUNNING.store(false, Ordering::SeqCst);
    });

    Ok(job_id)
}

#[tauri::command]
pub fn workspace_apply_status(job_id: String) -> Result<WorkspaceApplyJobStatus, CommandError> {
    let jobs = WORKSPACE_APPLY_JOBS.lock().unwrap();
    jobs.get(&job_id)
        .cloned()
        .ok_or_else(|| CommandError::NotFound(format!("Apply job not found: {job_id}")))
}

fn clone_repo_if_needed(repo_name: &str, repo_path: &str, token: &str) -> Result<(), CommandError> {
    let target = PathBuf::from(repo_path);
    let git_dir = target.join(".git");

    if git_dir.is_dir() {
        ensure_existing_repo_matches(repo_name, repo_path)?;
        log::info!(
            "workspace_apply_and_clone: repo already exists at {}",
            target.display()
        );
        return Ok(());
    }

    if target.exists() {
        if !target.is_dir() {
            return Err(CommandError::Io(format!(
                "Migration repo path is not a directory: {}",
                target.display()
            )));
        }
        let mut entries = std::fs::read_dir(&target).map_err(CommandError::from)?;
        if entries.next().is_some() {
            return Err(CommandError::Io(format!(
                "Migration repo path is not empty and not a git repo: {}",
                target.display()
            )));
        }
    } else if let Some(parent) = target.parent() {
        std::fs::create_dir_all(parent).map_err(CommandError::from)?;
    }

    let clone_url = format!(
        "https://x-access-token:{}@github.com/{}.git",
        token, repo_name
    );
    let output = Command::new("git")
        .args(["clone", "--depth", "1", &clone_url, repo_path])
        .output()
        .map_err(|e| {
            log::error!("workspace_apply_and_clone: failed to spawn git clone: {e}");
            CommandError::Git(e.to_string())
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let safe_stderr = redact_clone_error(&stderr, token, repo_name);
        log::error!(
            "workspace_apply_and_clone: git clone failed: {}",
            safe_stderr
        );
        return Err(CommandError::Git(if stderr.is_empty() {
            "git clone failed".to_string()
        } else {
            safe_stderr
        }));
    }

    Ok(())
}

fn redact_clone_error(stderr: &str, token: &str, repo_name: &str) -> String {
    if stderr.is_empty() {
        return String::new();
    }

    let raw_url = format!(
        "https://x-access-token:{}@github.com/{}.git",
        token, repo_name
    );
    let safe_url = format!("https://x-access-token:***@github.com/{}.git", repo_name);
    stderr.replace(token, "***").replace(&raw_url, &safe_url)
}

fn validate_source_port(port: i64) -> Result<u16, CommandError> {
    if !(1..=65535).contains(&port) {
        return Err(CommandError::Io(
            "Source port must be between 1 and 65535".to_string(),
        ));
    }
    Ok(port as u16)
}

#[tauri::command]
pub fn workspace_test_source_connection(
    args: TestSourceConnectionArgs,
) -> Result<String, CommandError> {
    log::info!(
        "workspace_test_source_connection: source_type={} server={} port={} auth_mode={}",
        args.source_type,
        args.source_server,
        args.source_port,
        args.source_authentication_mode
    );

    if args.source_server.trim().is_empty() {
        log::error!("workspace_test_source_connection: failed: source server is required");
        return Err(CommandError::Io("Source server is required".to_string()));
    }
    let source_port = validate_source_port(args.source_port).inspect_err(|_e| {
        log::error!(
            "workspace_test_source_connection: failed: invalid source port {}",
            args.source_port
        );
    })?;
    if args.source_username.trim().is_empty() {
        log::error!("workspace_test_source_connection: failed: source username is required");
        return Err(CommandError::Io("Source username is required".to_string()));
    }
    if args.source_password.trim().is_empty() {
        log::error!("workspace_test_source_connection: failed: source password is required");
        return Err(CommandError::Io("Source password is required".to_string()));
    }
    if args.source_type != "sql_server" && args.source_type != "fabric_warehouse" {
        log::error!("workspace_test_source_connection: failed: unsupported source type");
        return Err(CommandError::Io(
            "Unsupported source type. Expected sql_server or fabric_warehouse".to_string(),
        ));
    }

    let mut config = Config::new();
    config.host(args.source_server.trim());
    config.port(source_port);
    // Authenticate against master first so we can return a precise DB-access error.
    // Some SQL Server setups fail login when an unavailable DB is requested directly.
    config.database("master");
    config.authentication(AuthMethod::sql_server(
        args.source_username.trim(),
        args.source_password.trim(),
    ));
    config.encryption(if args.source_encrypt {
        EncryptionLevel::Required
    } else {
        EncryptionLevel::Off
    });
    if args.source_trust_server_certificate {
        config.trust_cert();
    }

    let runtime = RuntimeBuilder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .map_err(|e| CommandError::Io(format!("Failed to create async runtime: {e}")))?;

    runtime.block_on(async move {
        let tcp = TcpStream::connect(config.get_addr()).await.map_err(|e| {
            log::error!("workspace_test_source_connection: failed to connect tcp: {e}");
            CommandError::Io(format!("Could not connect to source endpoint: {e}"))
        })?;
        tcp.set_nodelay(true).map_err(|e| {
            log::error!("workspace_test_source_connection: failed to set nodelay: {e}");
            CommandError::Io(format!("Could not configure socket: {e}"))
        })?;

        let _client = Client::connect(config, tcp.compat_write())
            .await
            .map_err(|e| {
                let _ = e;
                log::error!("workspace_test_source_connection: failed to authenticate");
                CommandError::Io("Connection test failed".to_string())
            })?;

        Ok::<(), CommandError>(())
    })?;

    Ok("Connection successful".to_string())
}

#[tauri::command]
pub fn workspace_discover_source_databases(
    args: DiscoverSourceDatabasesArgs,
) -> Result<Vec<String>, CommandError> {
    log::info!(
        "workspace_discover_source_databases: source_type={} server={} port={} auth_mode={}",
        args.source_type,
        args.source_server,
        args.source_port,
        args.source_authentication_mode
    );

    if args.source_server.trim().is_empty() {
        log::error!("workspace_discover_source_databases: failed: source server is required");
        return Err(CommandError::Io("Source server is required".to_string()));
    }
    let source_port = validate_source_port(args.source_port).inspect_err(|_e| {
        log::error!(
            "workspace_discover_source_databases: failed: invalid source port {}",
            args.source_port
        );
    })?;
    if args.source_username.trim().is_empty() {
        log::error!("workspace_discover_source_databases: failed: source username is required");
        return Err(CommandError::Io("Source username is required".to_string()));
    }
    if args.source_password.trim().is_empty() {
        log::error!("workspace_discover_source_databases: failed: source password is required");
        return Err(CommandError::Io("Source password is required".to_string()));
    }
    if args.source_type != "sql_server" && args.source_type != "fabric_warehouse" {
        log::error!("workspace_discover_source_databases: failed: unsupported source type");
        return Err(CommandError::Io(
            "Unsupported source type. Expected sql_server or fabric_warehouse".to_string(),
        ));
    }

    let mut config = Config::new();
    config.host(args.source_server.trim());
    config.port(source_port);
    config.database("master");
    config.authentication(AuthMethod::sql_server(
        args.source_username.trim(),
        args.source_password.trim(),
    ));
    config.encryption(if args.source_encrypt {
        EncryptionLevel::Required
    } else {
        EncryptionLevel::Off
    });
    if args.source_trust_server_certificate {
        config.trust_cert();
    }

    let runtime = RuntimeBuilder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .map_err(|e| CommandError::Io(format!("Failed to create async runtime: {e}")))?;

    runtime.block_on(async move {
        let tcp = TcpStream::connect(config.get_addr()).await.map_err(|e| {
            log::error!("workspace_discover_source_databases: failed to connect tcp: {e}");
            CommandError::Io(format!("Could not connect to source endpoint: {e}"))
        })?;
        tcp.set_nodelay(true).map_err(|e| {
            log::error!("workspace_discover_source_databases: failed to set nodelay: {e}");
            CommandError::Io(format!("Could not configure socket: {e}"))
        })?;

        let mut client = Client::connect(config, tcp.compat_write())
            .await
            .map_err(|e| {
                let _ = e;
                log::error!("workspace_discover_source_databases: failed to authenticate");
                CommandError::Io("Database discovery failed".to_string())
            })?;

        let query = resolve_source_query(&args.source_type, SourceQuery::DiscoverDatabases)?;
        if should_log_source_sql() {
            log::debug!(
                "workspace_discover_source_databases: executing query={} source_type={} sql={}",
                SourceQuery::DiscoverDatabases.name(),
                args.source_type,
                query.trim()
            );
        }

        let rows = client
            .simple_query(query)
            .await
            .map_err(|e| {
                log::error!("workspace_discover_source_databases: query failed: {e}");
                CommandError::Io(format!("Database discovery failed: {e}"))
            })?
            .into_first_result()
            .await
            .map_err(|e| {
                log::error!("workspace_discover_source_databases: result parse failed: {e}");
                CommandError::Io(format!("Database discovery failed: {e}"))
            })?;

        let names: Vec<String> = rows
            .into_iter()
            .filter_map(|row| row.get::<&str, _>(0).map(|name| name.to_string()))
            .collect();

        Ok::<Vec<String>, CommandError>(names)
    })
}

#[tauri::command]
pub fn workspace_reset_state(state: State<DbState>) -> Result<(), CommandError> {
    log::info!("workspace_reset_state");
    let conn = state.0.lock().unwrap();
    let migration_repo_path: Option<String> = conn
        .query_row(
            "SELECT migration_repo_path FROM workspaces ORDER BY created_at DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(CommandError::from)?;

    if let Some(path) = migration_repo_path {
        clear_migration_repo_contents(&path)?;
    }

    clear_workspace_state(&conn)?;
    crate::db::write_scope_finalized(&conn, false).map_err(CommandError::Io)?;
    crate::db::write_plan_finalized(&conn, false).map_err(CommandError::Io)?;
    let _ = crate::db::reconcile_and_persist_app_phase(&conn).map_err(CommandError::Io)?;
    Ok(())
}

#[tauri::command]
pub fn workspace_get(state: State<DbState>) -> Result<Option<Workspace>, CommandError> {
    log::info!("workspace_get");
    let conn = state.0.lock().unwrap();
    let result = conn.query_row(
        "SELECT
            id, display_name, migration_repo_name, migration_repo_path, fabric_url,
            fabric_service_principal_id, fabric_service_principal_secret, source_type,
            source_server, source_database, source_port, source_authentication_mode,
            source_username, source_password, source_encrypt, source_trust_server_certificate,
            created_at
         FROM workspaces
         ORDER BY created_at DESC LIMIT 1",
        [],
        |row| {
            Ok(Workspace {
                id: row.get(0)?,
                display_name: row.get(1)?,
                migration_repo_name: row.get(2)?,
                migration_repo_path: row.get(3)?,
                fabric_url: row.get(4)?,
                fabric_service_principal_id: row.get(5)?,
                fabric_service_principal_secret: row.get(6)?,
                source_type: row.get(7)?,
                source_server: row.get(8)?,
                source_database: row.get(9)?,
                source_port: row.get(10)?,
                source_authentication_mode: row.get(11)?,
                source_username: row.get(12)?,
                source_password: row.get(13)?,
                source_encrypt: row.get(14)?,
                source_trust_server_certificate: row.get(15)?,
                created_at: row.get(16)?,
            })
        },
    );
    match result {
        Ok(w) => Ok(Some(w)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => {
            log::error!("workspace_get: failed: {e}");
            Err(CommandError::from(e))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use crate::source_sql::{resolve_source_query, SourceQuery};
    use std::fs;
    use tempfile::tempdir;
    use tiberius::{AuthMethod, Client, Config, EncryptionLevel};
    use tokio::net::TcpStream;
    use tokio::runtime::Builder as RuntimeBuilder;
    use tokio_util::compat::TokioAsyncWriteCompatExt;

    fn test_source_cfg(database: &str) -> SourceConnectionConfig {
        SourceConnectionConfig {
            source_type: "sql_server".to_string(),
            source_server: "localhost".to_string(),
            source_database: database.to_string(),
            source_port: 1433,
            source_username: "sa".to_string(),
            source_password: "secret".to_string(),
            source_encrypt: false,
            source_trust_server_certificate: true,
        }
    }

    #[test]
    fn create_and_get_workspace() {
        let conn = db::open_in_memory().unwrap();
        let id = Uuid::new_v4().to_string();
        conn.execute(
            "INSERT INTO workspaces(
                id, display_name, migration_repo_name, migration_repo_path, fabric_service_principal_id,
                fabric_service_principal_secret, source_type, source_server, source_database, source_port,
                source_authentication_mode, source_username, source_password, source_encrypt,
                source_trust_server_certificate, created_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
            rusqlite::params![
                id,
                "Test",
                Some("acme/repo"),
                std::env::temp_dir().to_str().unwrap(),
                Some("sp-id"),
                Some("sp-secret"),
                Some("sql_server"),
                Some("sql.local"),
                Some("warehouse"),
                Some(1433),
                Some("sql_password"),
                Some("sa"),
                Some("secret"),
                Some(true),
                Some(false),
                "2026-01-01T00:00:00Z"
            ],
        )
        .unwrap();
        let w: Workspace = conn
            .query_row(
                "SELECT
                    id, display_name, migration_repo_name, migration_repo_path, fabric_url,
                    fabric_service_principal_id, fabric_service_principal_secret, source_type,
                    source_server, source_database, source_port, source_authentication_mode,
                    source_username, source_password, source_encrypt, source_trust_server_certificate,
                    created_at
                 FROM workspaces WHERE id=?1",
                rusqlite::params![id],
                |row| {
                    Ok(Workspace {
                        id: row.get(0)?,
                        display_name: row.get(1)?,
                        migration_repo_name: row.get(2)?,
                        migration_repo_path: row.get(3)?,
                        fabric_url: row.get(4)?,
                        fabric_service_principal_id: row.get(5)?,
                        fabric_service_principal_secret: row.get(6)?,
                        source_type: row.get(7)?,
                        source_server: row.get(8)?,
                        source_database: row.get(9)?,
                        source_port: row.get(10)?,
                        source_authentication_mode: row.get(11)?,
                        source_username: row.get(12)?,
                        source_password: row.get(13)?,
                        source_encrypt: row.get(14)?,
                        source_trust_server_certificate: row.get(15)?,
                        created_at: row.get(16)?,
                    })
                },
            )
            .unwrap();
        assert_eq!(w.display_name, "Test");
        assert_eq!(w.migration_repo_name.as_deref(), Some("acme/repo"));
        assert_eq!(w.source_type.as_deref(), Some("sql_server"));
    }

    #[test]
    fn get_returns_none_when_empty() {
        let conn = db::open_in_memory().unwrap();
        let result: Option<Workspace> = conn
            .query_row(
                "SELECT
                    id, display_name, migration_repo_name, migration_repo_path, fabric_url,
                    fabric_service_principal_id, fabric_service_principal_secret, source_type,
                    source_server, source_database, source_port, source_authentication_mode,
                    source_username, source_password, source_encrypt, source_trust_server_certificate,
                    created_at
                 FROM workspaces LIMIT 1",
                [],
                |row| {
                    Ok(Workspace {
                        id: row.get(0)?,
                        display_name: row.get(1)?,
                        migration_repo_name: row.get(2)?,
                        migration_repo_path: row.get(3)?,
                        fabric_url: row.get(4)?,
                        fabric_service_principal_id: row.get(5)?,
                        fabric_service_principal_secret: row.get(6)?,
                        source_type: row.get(7)?,
                        source_server: row.get(8)?,
                        source_database: row.get(9)?,
                        source_port: row.get(10)?,
                        source_authentication_mode: row.get(11)?,
                        source_username: row.get(12)?,
                        source_password: row.get(13)?,
                        source_encrypt: row.get(14)?,
                        source_trust_server_certificate: row.get(15)?,
                        created_at: row.get(16)?,
                    })
                },
            )
            .optional()
            .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn normalize_repo_full_name_handles_common_remote_formats() {
        assert_eq!(
            normalize_repo_full_name("https://github.com/acme/data-platform.git").as_deref(),
            Some("acme/data-platform")
        );
        assert_eq!(
            normalize_repo_full_name("git@github.com:acme/data-platform.git").as_deref(),
            Some("acme/data-platform")
        );
        assert_eq!(
            normalize_repo_full_name(
                "https://x-access-token:abc@github.com/acme/data-platform.git"
            )
            .as_deref(),
            Some("acme/data-platform")
        );
    }

    #[test]
    fn redact_clone_error_masks_token_and_url() {
        let token = "gho_test_secret";
        let repo = "acme/data-platform";
        let stderr = format!(
            "fatal: could not read from remote repository https://x-access-token:{}@github.com/{}.git",
            token, repo
        );
        let redacted = redact_clone_error(&stderr, token, repo);
        assert!(!redacted.contains(token));
        assert!(redacted.contains("https://x-access-token:***@github.com/acme/data-platform.git"));
    }

    #[test]
    fn validate_source_port_enforces_bounds() {
        assert_eq!(validate_source_port(1).unwrap(), 1);
        assert_eq!(validate_source_port(65535).unwrap(), 65535);
        assert!(validate_source_port(0).is_err());
        assert!(validate_source_port(65536).is_err());
        assert!(validate_source_port(-1).is_err());
    }

    #[test]
    fn test_source_connection_validates_required_fields() {
        let args = TestSourceConnectionArgs {
            source_type: "sql_server".to_string(),
            source_server: "".to_string(),
            source_port: 1433,
            source_authentication_mode: "sql_password".to_string(),
            source_username: "sa".to_string(),
            source_password: "secret".to_string(),
            source_encrypt: true,
            source_trust_server_certificate: false,
        };
        assert!(workspace_test_source_connection(args).is_err());

        let args = TestSourceConnectionArgs {
            source_type: "bad".to_string(),
            source_server: "localhost".to_string(),
            source_port: 1433,
            source_authentication_mode: "sql_password".to_string(),
            source_username: "sa".to_string(),
            source_password: "secret".to_string(),
            source_encrypt: true,
            source_trust_server_certificate: false,
        };
        assert!(workspace_test_source_connection(args).is_err());

        let args = TestSourceConnectionArgs {
            source_type: "sql_server".to_string(),
            source_server: "localhost".to_string(),
            source_port: 65536,
            source_authentication_mode: "sql_password".to_string(),
            source_username: "sa".to_string(),
            source_password: "secret".to_string(),
            source_encrypt: true,
            source_trust_server_certificate: false,
        };
        assert!(workspace_test_source_connection(args).is_err());
    }

    #[test]
    fn discover_source_databases_validates_port_bounds() {
        let args = DiscoverSourceDatabasesArgs {
            source_type: "sql_server".to_string(),
            source_server: "localhost".to_string(),
            source_port: 0,
            source_authentication_mode: "sql_password".to_string(),
            source_username: "sa".to_string(),
            source_password: "secret".to_string(),
            source_encrypt: true,
            source_trust_server_certificate: false,
        };
        assert!(workspace_discover_source_databases(args).is_err());

        let args = DiscoverSourceDatabasesArgs {
            source_type: "sql_server".to_string(),
            source_server: "localhost".to_string(),
            source_port: 65536,
            source_authentication_mode: "sql_password".to_string(),
            source_username: "sa".to_string(),
            source_password: "secret".to_string(),
            source_encrypt: true,
            source_trust_server_certificate: false,
        };
        assert!(workspace_discover_source_databases(args).is_err());
    }

    #[test]
    fn clear_workspace_state_deletes_workspace_and_children() {
        let conn = db::open_in_memory().unwrap();
        conn.execute(
            "INSERT INTO workspaces(id, display_name, migration_repo_path, created_at) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params!["ws-1", "Workspace", "/tmp/repo", "2026-01-01T00:00:00Z"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO items(id, workspace_id, display_name, item_type) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params!["item-1", "ws-1", "Warehouse", "Warehouse"],
        )
        .unwrap();

        clear_workspace_state(&conn).unwrap();

        let workspaces: i64 = conn
            .query_row("SELECT COUNT(*) FROM workspaces", [], |row| row.get(0))
            .unwrap();
        let items: i64 = conn
            .query_row("SELECT COUNT(*) FROM items", [], |row| row.get(0))
            .unwrap();
        assert_eq!(workspaces, 0);
        assert_eq!(items, 0);
    }

    #[test]
    fn require_sql_server_source_rejects_missing_database() {
        let args = ApplyWorkspaceArgs {
            name: "Workspace".to_string(),
            migration_repo_name: "acme/repo".to_string(),
            migration_repo_path: "/tmp/repo".to_string(),
            fabric_url: None,
            fabric_service_principal_id: None,
            fabric_service_principal_secret: None,
            source_type: Some("sql_server".to_string()),
            source_server: Some("localhost".to_string()),
            source_database: None,
            source_port: Some(1433),
            source_authentication_mode: Some("sql_password".to_string()),
            source_username: Some("sa".to_string()),
            source_password: Some("secret".to_string()),
            source_encrypt: Some(true),
            source_trust_server_certificate: Some(false),
        };

        assert!(require_sql_server_source(&args).is_err());
    }

    #[test]
    fn persist_sql_server_inventory_replaces_existing_rows() {
        let conn = db::open_in_memory().unwrap();
        conn.execute(
            "INSERT INTO workspaces(id, display_name, migration_repo_path, created_at) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params!["ws-1", "Workspace", "/tmp/repo", "2026-01-01T00:00:00Z"],
        )
        .unwrap();

        let first = SqlServerInventory {
            container_id_local: Some(1),
            schemas: vec![WarehouseSchema {
                warehouse_item_id: String::new(),
                schema_name: "sales".to_string(),
                schema_id_local: Some(1),
            }],
            tables: vec![WarehouseTable {
                warehouse_item_id: String::new(),
                schema_name: "sales".to_string(),
                table_name: "orders".to_string(),
                object_id_local: Some(10),
                row_count: Some(123),
            }],
            procedures: vec![WarehouseProcedure {
                warehouse_item_id: String::new(),
                schema_name: "sales".to_string(),
                procedure_name: "sp_orders".to_string(),
                object_id_local: Some(100),
                sql_body: Some("SELECT 1".to_string()),
            }],
            columns: vec![],
            indexes_constraints: vec![],
        };
        let cfg = test_source_cfg("AdventureWorks");
        persist_sql_server_inventory(&conn, "ws-1", &cfg, &first, None, None).unwrap();
        let first_partition_rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM sqlserver_partitions", [], |row| row.get(0))
            .unwrap();
        let first_partition_row_count: i64 = conn
            .query_row(
                "SELECT COALESCE(SUM(row_count), 0) FROM sqlserver_partitions",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(first_partition_rows, 1);
        assert_eq!(first_partition_row_count, 123);

        let second = SqlServerInventory {
            container_id_local: Some(1),
            schemas: vec![WarehouseSchema {
                warehouse_item_id: String::new(),
                schema_name: "finance".to_string(),
                schema_id_local: Some(2),
            }],
            tables: vec![],
            procedures: vec![],
            columns: vec![],
            indexes_constraints: vec![],
        };
        persist_sql_server_inventory(&conn, "ws-1", &cfg, &second, None, None).unwrap();

        let schema_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM warehouse_schemas", [], |row| {
                row.get(0)
            })
            .unwrap();
        let table_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM warehouse_tables", [], |row| {
                row.get(0)
            })
            .unwrap();
        let procedure_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM warehouse_procedures", [], |row| {
                row.get(0)
            })
            .unwrap();
        let partition_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM sqlserver_partitions", [], |row| row.get(0))
            .unwrap();
        let schema_name: String = conn
            .query_row(
                "SELECT schema_name FROM warehouse_schemas LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(schema_count, 1);
        assert_eq!(table_count, 0);
        assert_eq!(procedure_count, 0);
        assert_eq!(partition_count, 0);
        assert_eq!(schema_name, "finance");
    }

    #[test]
    fn persist_sql_server_inventory_keeps_selected_tables_for_unchanged_rows() {
        let conn = db::open_in_memory().unwrap();
        conn.execute(
            "INSERT INTO workspaces(id, display_name, migration_repo_path, created_at) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params!["ws-1", "Workspace", "/tmp/repo", "2026-01-01T00:00:00Z"],
        )
        .unwrap();

        let first = SqlServerInventory {
            container_id_local: Some(1),
            schemas: vec![WarehouseSchema {
                warehouse_item_id: String::new(),
                schema_name: "dbo".to_string(),
                schema_id_local: Some(1),
            }],
            tables: vec![
                WarehouseTable {
                    warehouse_item_id: String::new(),
                    schema_name: "dbo".to_string(),
                    table_name: "dim_account".to_string(),
                    object_id_local: Some(10),
                    row_count: Some(1),
                },
                WarehouseTable {
                    warehouse_item_id: String::new(),
                    schema_name: "dbo".to_string(),
                    table_name: "dim_currency".to_string(),
                    object_id_local: Some(11),
                    row_count: Some(1),
                },
            ],
            procedures: vec![],
            columns: vec![],
            indexes_constraints: vec![],
        };
        let cfg = test_source_cfg("AdventureWorks");
        persist_sql_server_inventory(&conn, "ws-1", &cfg, &first, None, None).unwrap();

        conn.execute(
            "INSERT INTO selected_tables(id, workspace_id, warehouse_item_id, schema_name, table_name)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                "st-1",
                "ws-1",
                "source-db-ws-1",
                "dbo",
                "dim_account"
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO selected_tables(id, workspace_id, warehouse_item_id, schema_name, table_name)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                "st-2",
                "ws-1",
                "source-db-ws-1",
                "dbo",
                "dim_currency"
            ],
        )
        .unwrap();

        // Refresh with same tables should preserve selections.
        persist_sql_server_inventory(&conn, "ws-1", &cfg, &first, None, None).unwrap();

        let selected_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM selected_tables WHERE workspace_id = ?1",
                rusqlite::params!["ws-1"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(selected_count, 2);
    }

    #[test]
    fn persist_sql_server_inventory_requires_container_database_id() {
        let conn = db::open_in_memory().unwrap();
        conn.execute(
            "INSERT INTO workspaces(id, display_name, migration_repo_path, created_at) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params!["ws-1", "Workspace", "/tmp/repo", "2026-01-01T00:00:00Z"],
        )
        .unwrap();

        let cfg = test_source_cfg("AdventureWorks");
        let inventory = SqlServerInventory {
            container_id_local: None,
            schemas: vec![WarehouseSchema {
                warehouse_item_id: String::new(),
                schema_name: "sales".to_string(),
                schema_id_local: Some(1),
            }],
            tables: vec![],
            procedures: vec![],
            columns: vec![],
            indexes_constraints: vec![],
        };

        let err = persist_sql_server_inventory(&conn, "ws-1", &cfg, &inventory, None, None)
            .expect_err("expected missing DB_ID() to fail persistence");
        assert!(
            err.to_string().contains("database_id"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn clear_migration_repo_contents_removes_all_children_and_keeps_root() {
        let temp = tempdir().unwrap();
        let repo = temp.path().join("migration-repo");
        fs::create_dir_all(repo.join(".git")).unwrap();
        fs::create_dir_all(repo.join("models").join("silver")).unwrap();
        fs::write(
            repo.join(".git").join("config"),
            "[core]\nrepositoryformatversion = 0\n",
        )
        .unwrap();
        fs::write(repo.join("README.md"), "test").unwrap();
        fs::write(
            repo.join("models").join("silver").join("orders.sql"),
            "select 1",
        )
        .unwrap();

        clear_migration_repo_contents(repo.to_str().unwrap()).unwrap();

        assert!(repo.exists());
        assert!(repo.is_dir());
        let entries: Vec<_> = fs::read_dir(&repo).unwrap().collect();
        assert_eq!(entries.len(), 0);
    }

    #[test]
    fn clear_migration_repo_contents_creates_missing_folder() {
        let temp = tempdir().unwrap();
        let repo = temp.path().join("missing-repo");
        assert!(!repo.exists());

        clear_migration_repo_contents(repo.to_str().unwrap()).unwrap();

        assert!(repo.exists());
        assert!(repo.is_dir());
    }

    #[test]
    fn reset_flow_clears_sqlite_state_and_repo_contents() {
        let temp = tempdir().unwrap();
        let repo = temp.path().join("migration-repo");
        fs::create_dir_all(repo.join(".git")).unwrap();
        fs::write(repo.join(".git").join("config"), "gitdir").unwrap();
        fs::write(repo.join("README.md"), "content").unwrap();

        let conn = db::open_in_memory().unwrap();
        conn.execute(
            "INSERT INTO workspaces(id, display_name, migration_repo_path, created_at) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![
                "ws-reset",
                "Workspace",
                repo.to_str().unwrap(),
                "2026-01-01T00:00:00Z"
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO items(id, workspace_id, display_name, item_type) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![
                "source-db-ws-reset",
                "ws-reset",
                "WideWorldImportersDW",
                "Warehouse"
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO warehouse_schemas(warehouse_item_id, schema_name) VALUES (?1, ?2)",
            rusqlite::params!["source-db-ws-reset", "Sales"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO warehouse_tables(warehouse_item_id, schema_name, table_name) VALUES (?1, ?2, ?3)",
            rusqlite::params!["source-db-ws-reset", "Sales", "Orders"],
        )
        .unwrap();

        clear_migration_repo_contents(repo.to_str().unwrap()).unwrap();
        clear_workspace_state(&conn).unwrap();

        let workspaces: i64 = conn
            .query_row("SELECT COUNT(*) FROM workspaces", [], |row| row.get(0))
            .unwrap();
        let items: i64 = conn
            .query_row("SELECT COUNT(*) FROM items", [], |row| row.get(0))
            .unwrap();
        let schemas: i64 = conn
            .query_row("SELECT COUNT(*) FROM warehouse_schemas", [], |row| {
                row.get(0)
            })
            .unwrap();
        let tables: i64 = conn
            .query_row("SELECT COUNT(*) FROM warehouse_tables", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(workspaces, 0);
        assert_eq!(items, 0);
        assert_eq!(schemas, 0);
        assert_eq!(tables, 0);

        let entries: Vec<_> = fs::read_dir(&repo).unwrap().collect();
        assert!(repo.exists());
        assert_eq!(entries.len(), 0);
    }

    #[test]
    fn reset_clears_apply_lifecycle_state_via_fk_cascade() {
        let conn = db::open_in_memory().unwrap();
        conn.execute(
            "INSERT INTO workspaces(id, display_name, migration_repo_path, created_at) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params!["ws-cascade", "Workspace", "/tmp/repo", "2026-01-01T00:00:00Z"],
        )
        .unwrap();

        // Legacy mirror/state branch (scope/planning dependencies)
        conn.execute(
            "INSERT INTO items(id, workspace_id, display_name, item_type) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params!["item-1", "ws-cascade", "AdventureWorks", "Warehouse"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO warehouse_schemas(warehouse_item_id, schema_name) VALUES (?1, ?2)",
            rusqlite::params!["item-1", "dbo"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO warehouse_tables(warehouse_item_id, schema_name, table_name) VALUES (?1, ?2, ?3)",
            rusqlite::params!["item-1", "dbo", "Orders"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO selected_tables(id, workspace_id, warehouse_item_id, schema_name, table_name) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["st-1", "ws-cascade", "item-1", "dbo", "Orders"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO table_config(selected_table_id, table_type) VALUES (?1, ?2)",
            rusqlite::params!["st-1", "fact"],
        )
        .unwrap();

        // Canonical source branch (apply-discovered source metadata)
        conn.execute(
            "INSERT INTO sources(id, workspace_id, source_type, external_source_id, display_name) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["source-1", "ws-cascade", "sql_server", "sql_server://localhost:1433/db", "db"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO containers(id, source_id, container_type, external_container_id, container_name) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["container-1", "source-1", "database", "1", "db"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO namespaces(id, container_id, namespace_name, external_namespace_id) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params!["ns-1", "container-1", "dbo", "1"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO data_objects(id, namespace_id, object_name, object_type, external_object_id) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["obj-1", "ns-1", "Orders", "table", "42"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO sqlserver_object_columns(id, data_object_id, column_name, column_id) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params!["col-1", "obj-1", "OrderId", 1],
        )
        .unwrap();

        clear_workspace_state(&conn).unwrap();

        let tables = [
            "workspaces",
            "items",
            "warehouse_schemas",
            "warehouse_tables",
            "selected_tables",
            "table_config",
            "sources",
            "containers",
            "namespaces",
            "data_objects",
            "sqlserver_object_columns",
        ];
        for table in tables {
            let count: i64 = conn
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(count, 0, "expected table '{table}' to be empty after reset");
        }
    }

    #[test]
    #[ignore = "requires reachable SQL Server (e.g. Docker)"]
    fn sql_server_inventory_populates_sqlite_on_apply_path() {
        let host = std::env::var("MIGRATION_TEST_SQL_SERVER_HOST")
            .unwrap_or_else(|_| "127.0.0.1".to_string());
        let port: u16 = std::env::var("MIGRATION_TEST_SQL_SERVER_PORT")
            .ok()
            .and_then(|v| v.parse::<u16>().ok())
            .unwrap_or(1433);
        let username =
            std::env::var("MIGRATION_TEST_SQL_SERVER_USER").unwrap_or_else(|_| "sa".to_string());
        let password = std::env::var("MIGRATION_TEST_SQL_SERVER_PASSWORD")
            .unwrap_or_else(|_| "YourStrong!Passw0rd".to_string());
        let database = std::env::var("MIGRATION_TEST_SQL_SERVER_DATABASE")
            .unwrap_or_else(|_| "WideWorldImportersDW".to_string());

        let mut config = Config::new();
        config.host(&host);
        config.port(port);
        config.database(&database);
        config.authentication(AuthMethod::sql_server(&username, &password));
        config.encryption(EncryptionLevel::Off);
        config.trust_cert();

        let rt = RuntimeBuilder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .unwrap();

        let (container_id_local, schemas, tables, procedures) = rt.block_on(async {
            let tcp = TcpStream::connect(config.get_addr())
                .await
                .expect("failed to connect to SQL Server");
            tcp.set_nodelay(true).expect("failed to set TCP nodelay");
            let mut client = Client::connect(config, tcp.compat_write())
                .await
                .expect("failed to authenticate to SQL Server");

            let schemas_sql = resolve_source_query("sql_server", SourceQuery::DiscoverSchemas)
                .expect("missing schemas query");
            let tables_sql = resolve_source_query("sql_server", SourceQuery::DiscoverTables)
                .expect("missing tables query");
            let procedures_sql =
                resolve_source_query("sql_server", SourceQuery::DiscoverProcedures)
                    .expect("missing procedures query");
            let container_id_sql =
                resolve_source_query("sql_server", SourceQuery::DiscoverContainerId)
                    .expect("missing container id query");

            let container_rows = client
                .simple_query(container_id_sql)
                .await
                .expect("container id query failed")
                .into_first_result()
                .await
                .expect("container id result parse failed");
            let container_id_local = container_rows.first().and_then(|row| row.get::<i64, _>(0));

            let schema_rows = client
                .simple_query(schemas_sql)
                .await
                .expect("schema query failed")
                .into_first_result()
                .await
                .expect("schema result parse failed");
            let table_rows = client
                .simple_query(tables_sql)
                .await
                .expect("table query failed")
                .into_first_result()
                .await
                .expect("table result parse failed");
            let procedure_rows = client
                .simple_query(procedures_sql)
                .await
                .expect("procedure query failed")
                .into_first_result()
                .await
                .expect("procedure result parse failed");

            let schemas: Vec<WarehouseSchema> = schema_rows
                .into_iter()
                .filter_map(|row| {
                    row.get::<&str, _>(1).map(|schema_name| WarehouseSchema {
                        warehouse_item_id: String::new(),
                        schema_name: schema_name.to_string(),
                        schema_id_local: row.get::<i64, _>(0),
                    })
                })
                .collect();
            let tables: Vec<WarehouseTable> = table_rows
                .into_iter()
                .filter_map(|row| {
                    let schema_name = row.get::<&str, _>(0)?;
                    let table_name = row.get::<&str, _>(1)?;
                    Some(WarehouseTable {
                        warehouse_item_id: String::new(),
                        schema_name: schema_name.to_string(),
                        table_name: table_name.to_string(),
                        object_id_local: row.get::<i64, _>(2),
                        row_count: row.get::<i64, _>(3),
                    })
                })
                .collect();
            let procedures: Vec<WarehouseProcedure> = procedure_rows
                .into_iter()
                .filter_map(|row| {
                    let schema_name = row.get::<&str, _>(0)?;
                    let procedure_name = row.get::<&str, _>(1)?;
                    Some(WarehouseProcedure {
                        warehouse_item_id: String::new(),
                        schema_name: schema_name.to_string(),
                        procedure_name: procedure_name.to_string(),
                        object_id_local: row.get::<i64, _>(2),
                        sql_body: row.get::<&str, _>(3).map(|v| v.to_string()),
                    })
                })
                .collect();

            (container_id_local, schemas, tables, procedures)
        });

        assert!(
            container_id_local.is_some(),
            "expected DB_ID() to return current database id"
        );
        assert!(!schemas.is_empty(), "expected schemas from SQL Server");
        assert!(!tables.is_empty(), "expected tables from SQL Server");
        assert!(
            !procedures.is_empty(),
            "expected procedures from SQL Server"
        );

        let conn = db::open_in_memory().unwrap();
        conn.execute(
            "INSERT INTO workspaces(id, display_name, migration_repo_path, created_at) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params!["ws-live", "Workspace", "/tmp/repo", "2026-01-01T00:00:00Z"],
        )
        .unwrap();

        let cfg = test_source_cfg(&database);
        let inventory = SqlServerInventory {
            container_id_local,
            schemas,
            tables,
            procedures,
            columns: vec![],
            indexes_constraints: vec![],
        };
        persist_sql_server_inventory(&conn, "ws-live", &cfg, &inventory, None, None).unwrap();

        let items_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM items", [], |row| row.get(0))
            .unwrap();
        let schemas_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM warehouse_schemas", [], |row| {
                row.get(0)
            })
            .unwrap();
        let tables_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM warehouse_tables", [], |row| {
                row.get(0)
            })
            .unwrap();
        let procedures_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM warehouse_procedures", [], |row| {
                row.get(0)
            })
            .unwrap();
        let db_item_name: String = conn
            .query_row("SELECT display_name FROM items LIMIT 1", [], |row| {
                row.get(0)
            })
            .unwrap();
        let sources_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM sources", [], |row| row.get(0))
            .unwrap();
        let namespaces_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM namespaces", [], |row| row.get(0))
            .unwrap();
        let data_objects_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM data_objects", [], |row| row.get(0))
            .unwrap();
        let container_external_id: String = conn
            .query_row(
                "SELECT external_container_id FROM containers LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(items_count, 1);
        assert!(schemas_count > 0);
        assert!(tables_count > 0);
        assert!(procedures_count > 0);
        assert_eq!(db_item_name, database);
        assert_eq!(sources_count, 1);
        assert!(namespaces_count > 0);
        assert!(data_objects_count > 0);
        assert_eq!(
            container_external_id,
            container_id_local.unwrap().to_string()
        );
    }
}
