use std::{path::Path, sync::Mutex};

use rusqlite::Connection;
use thiserror::Error;

use crate::types::{AppPhase, AppPhaseState, AppSettings};

#[derive(Debug, Error)]
pub enum DbError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub struct DbState(pub Mutex<Connection>);

const APP_PHASE_KEY: &str = "app_phase";
const SCOPE_FINALIZED_KEY: &str = "scope_finalized";
const PLAN_FINALIZED_KEY: &str = "plan_finalized";

const MIGRATIONS: &[(i64, &str)] = &[
    (1, include_str!("../migrations/001_initial_schema.sql")),
    (2, include_str!("../migrations/002_add_fabric_url.sql")),
    (3, include_str!("../migrations/003_add_settings.sql")),
    (
        4,
        include_str!("../migrations/004_add_migration_repo_name.sql"),
    ),
    (
        5,
        include_str!("../migrations/005_add_fabric_credentials.sql"),
    ),
    (
        6,
        include_str!("../migrations/006_add_workspace_source_connection.sql"),
    ),
    (
        7,
        include_str!("../migrations/007_add_fk_delete_cascade.sql"),
    ),
    (
        8,
        include_str!("../migrations/008_add_canonical_source_model.sql"),
    ),
    (
        9,
        include_str!("../migrations/009_selected_tables_natural_key.sql"),
    ),
];

pub fn open(path: &Path) -> Result<Connection, DbError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    log::info!("db::open: opening database at {}", path.display());
    let conn = Connection::open(path)?;
    conn.execute_batch("PRAGMA foreign_keys = ON;")?;
    run_migrations(&conn)?;
    Ok(conn)
}

#[cfg(test)]
pub(crate) fn open_in_memory() -> Result<Connection, DbError> {
    let conn = Connection::open_in_memory()?;
    conn.execute_batch("PRAGMA foreign_keys = ON;")?;
    run_migrations(&conn)?;
    Ok(conn)
}

fn run_migrations(conn: &Connection) -> Result<(), DbError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS schema_version (
           version    INTEGER PRIMARY KEY,
           applied_at TEXT NOT NULL
         );",
    )?;

    for (version, sql) in MIGRATIONS {
        let already_applied: bool = conn.query_row(
            "SELECT COUNT(*) > 0 FROM schema_version WHERE version = ?1",
            [version],
            |row| row.get(0),
        )?;

        if !already_applied {
            log::info!("db: applying migration {}", version);
            let tx = conn.unchecked_transaction()?;
            tx.execute_batch(sql)?;
            tx.execute(
                "INSERT INTO schema_version(version, applied_at) VALUES (?1, datetime('now'))",
                [version],
            )?;
            tx.commit()?;
        }
    }
    Ok(())
}

/// Read the persisted app settings from the settings table.
/// Returns defaults if no row exists yet.
pub fn read_settings(conn: &Connection) -> Result<AppSettings, String> {
    let mut stmt = conn
        .prepare("SELECT value FROM settings WHERE key = ?1")
        .map_err(|e| e.to_string())?;

    let result: Result<String, _> = stmt.query_row(["app_settings"], |row| row.get(0));

    match result {
        Ok(json) => serde_json::from_str(&json).map_err(|e| e.to_string()),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(AppSettings::default()),
        Err(e) => Err(e.to_string()),
    }
}

/// Write app settings to the settings table.
pub fn write_settings(conn: &Connection, settings: &AppSettings) -> Result<(), String> {
    let json = serde_json::to_string(settings).map_err(|e| e.to_string())?;
    conn.execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES (?1, ?2)",
        ["app_settings", &json],
    )
    .map_err(|e| e.to_string())?;
    Ok(())
}

fn read_settings_value(conn: &Connection, key: &str) -> Result<Option<String>, String> {
    conn.query_row("SELECT value FROM settings WHERE key = ?1", [key], |row| {
        row.get(0)
    })
    .map(Some)
    .or_else(|err| match err {
        rusqlite::Error::QueryReturnedNoRows => Ok(None),
        _ => Err(err),
    })
    .map_err(|e| e.to_string())
}

fn write_settings_value(conn: &Connection, key: &str, value: &str) -> Result<(), String> {
    conn.execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES (?1, ?2)",
        [key, value],
    )
    .map_err(|e| e.to_string())?;
    Ok(())
}

fn read_bool_flag(conn: &Connection, key: &str) -> Result<bool, String> {
    let raw = read_settings_value(conn, key)?;
    Ok(matches!(
        raw.as_deref(),
        Some("1") | Some("true") | Some("TRUE")
    ))
}

fn write_bool_flag(conn: &Connection, key: &str, value: bool) -> Result<(), String> {
    write_settings_value(conn, key, if value { "1" } else { "0" })
}

pub fn read_scope_finalized(conn: &Connection) -> Result<bool, String> {
    read_bool_flag(conn, SCOPE_FINALIZED_KEY)
}

pub fn write_scope_finalized(conn: &Connection, finalized: bool) -> Result<(), String> {
    write_bool_flag(conn, SCOPE_FINALIZED_KEY, finalized)
}

pub fn read_plan_finalized(conn: &Connection) -> Result<bool, String> {
    read_bool_flag(conn, PLAN_FINALIZED_KEY)
}

pub fn write_plan_finalized(conn: &Connection, finalized: bool) -> Result<(), String> {
    write_bool_flag(conn, PLAN_FINALIZED_KEY, finalized)
}

pub fn read_app_phase(conn: &Connection) -> Result<Option<AppPhase>, String> {
    let raw = read_settings_value(conn, APP_PHASE_KEY)?;
    let phase = raw.and_then(|v| AppPhase::from_str(v.as_str()));
    Ok(phase)
}

pub fn write_app_phase(conn: &Connection, phase: AppPhase) -> Result<(), String> {
    write_settings_value(conn, APP_PHASE_KEY, phase.as_str())
}

fn read_phase_facts(conn: &Connection) -> Result<AppPhaseState, String> {
    let settings = read_settings(conn)?;
    let has_github_auth = settings
        .github_oauth_token
        .as_deref()
        .is_some_and(|v| !v.trim().is_empty());
    let has_anthropic_key = settings
        .anthropic_api_key
        .as_deref()
        .is_some_and(|v| !v.trim().is_empty());
    let is_source_applied: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM workspaces LIMIT 1)",
            [],
            |row| row.get(0),
        )
        .map_err(|e| e.to_string())?;
    let scope_finalized = read_scope_finalized(conn)?;
    let plan_finalized = read_plan_finalized(conn)?;

    Ok(AppPhaseState {
        app_phase: AppPhase::SetupRequired,
        has_github_auth,
        has_anthropic_key,
        is_source_applied,
        scope_finalized,
        plan_finalized,
    })
}

pub fn read_current_app_phase_state(conn: &Connection) -> Result<AppPhaseState, String> {
    let mut state = read_phase_facts(conn)?;
    state.app_phase = read_app_phase(conn)?.unwrap_or(AppPhase::SetupRequired);
    Ok(state)
}

pub fn reconcile_and_persist_app_phase(conn: &Connection) -> Result<AppPhaseState, String> {
    let persisted_phase = read_app_phase(conn)?;
    let mut state = read_phase_facts(conn)?;

    let reconciled =
        if !state.has_github_auth || !state.has_anthropic_key || !state.is_source_applied {
            AppPhase::SetupRequired
        } else if matches!(persisted_phase, Some(AppPhase::RunningLocked)) {
            AppPhase::RunningLocked
        } else if !state.scope_finalized {
            AppPhase::ScopeEditable
        } else if !state.plan_finalized {
            AppPhase::PlanEditable
        } else {
            AppPhase::ReadyToRun
        };

    if persisted_phase != Some(reconciled) {
        write_app_phase(conn, reconciled)?;
    }
    state.app_phase = reconciled;
    Ok(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::params;

    fn open_memory() -> Connection {
        let conn = Connection::open_in_memory().expect("in-memory db");
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        run_migrations(&conn).expect("migrations failed");
        conn
    }

    fn assert_pk_id(conn: &Connection, table: &str) {
        let pk_id_count: i64 = conn
            .query_row(
                &format!(
                    "SELECT COUNT(*) FROM pragma_table_info('{table}') WHERE name='id' AND pk=1"
                ),
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            pk_id_count, 1,
            "expected primary key column id on table '{table}'"
        );
    }

    fn assert_index_exists(conn: &Connection, index_name: &str, table: &str) {
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name=?1 AND tbl_name=?2",
                params![index_name, table],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "expected index '{index_name}' on '{table}'");
    }

    fn assert_fk_delete_cascade(
        conn: &Connection,
        child_table: &str,
        fk_from: &str,
        parent_table: &str,
        parent_to: &str,
    ) {
        let count: i64 = conn
            .query_row(
                &format!(
                    "SELECT COUNT(*) FROM pragma_foreign_key_list('{child_table}')
                     WHERE \"from\"=?1 AND \"table\"=?2 AND \"to\"=?3 AND on_delete='CASCADE'"
                ),
                params![fk_from, parent_table, parent_to],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            count, 1,
            "expected CASCADE FK on '{child_table}'.{fk_from} -> '{parent_table}'.{parent_to}"
        );
    }

    #[test]
    fn fresh_db_has_all_tables() {
        let conn = open_memory();
        let expected = [
            "schema_version",
            "workspaces",
            "items",
            "warehouse_schemas",
            "warehouse_tables",
            "warehouse_procedures",
            "pipeline_activities",
            "selected_tables",
            "table_artifacts",
            "candidacy",
            "table_config",
            "settings",
            "sources",
            "containers",
            "namespaces",
            "data_objects",
            "orchestration_items",
            "orchestration_activities",
            "activity_object_links",
            "sqlserver_object_columns",
            "sqlserver_constraints_indexes",
            "sqlserver_partitions",
            "sqlserver_procedure_parameters",
            "sqlserver_procedure_runtime_stats",
            "sqlserver_procedure_lineage",
            "sqlserver_table_ddl_snapshots",
        ];
        for table in expected {
            let count: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
                    [table],
                    |row| row.get(0),
                )
                .expect("query failed");
            assert_eq!(count, 1, "table '{table}' missing");
        }
    }

    #[test]
    fn migrations_are_idempotent() {
        let conn = Connection::open_in_memory().expect("in-memory db");
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        // Run twice — second run must be a no-op
        run_migrations(&conn).expect("first run failed");
        run_migrations(&conn).expect("second run failed");
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM schema_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 9, "schema_version should have exactly 9 rows");
    }

    #[test]
    fn migration_4_adds_migration_repo_name_to_legacy_workspaces() {
        let conn = Connection::open_in_memory().expect("in-memory db");
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();

        // Simulate a legacy DB that already ran migrations 1-3.
        conn.execute_batch(
            r#"
            CREATE TABLE schema_version (
              version    INTEGER PRIMARY KEY,
              applied_at TEXT NOT NULL
            );
            INSERT INTO schema_version(version, applied_at) VALUES (1, datetime('now'));
            INSERT INTO schema_version(version, applied_at) VALUES (2, datetime('now'));
            INSERT INTO schema_version(version, applied_at) VALUES (3, datetime('now'));
            "#,
        )
        .unwrap();
        conn.execute_batch(include_str!("../migrations/001_initial_schema.sql"))
            .unwrap();
        conn.execute_batch(include_str!("../migrations/002_add_fabric_url.sql"))
            .unwrap();
        conn.execute_batch(include_str!("../migrations/003_add_settings.sql"))
            .unwrap();

        run_migrations(&conn).expect("migrations failed");

        let column_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('workspaces') WHERE name = 'migration_repo_name'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            column_count, 1,
            "migration_repo_name column should be added"
        );

        let version_4_applied: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM schema_version WHERE version = 4",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(version_4_applied, 1, "migration 4 should be recorded");
    }

    #[test]
    fn migration_6_adds_workspace_source_columns() {
        let conn = open_memory();

        let expected = [
            "source_type",
            "source_server",
            "source_database",
            "source_port",
            "source_authentication_mode",
            "source_username",
            "source_password",
            "source_encrypt",
            "source_trust_server_certificate",
        ];

        for column in expected {
            let exists: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM pragma_table_info('workspaces') WHERE name=?1",
                    [column],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(exists, 1, "column '{column}' missing");
        }
    }

    #[test]
    fn workspace_migration_repo_name_roundtrip() {
        let conn = open_memory();
        conn.execute(
            "INSERT INTO workspaces(id, display_name, migration_repo_name, migration_repo_path, fabric_url, fabric_service_principal_id, fabric_service_principal_secret, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            rusqlite::params![
                "ws-1",
                "Migration Workspace",
                "acme/data-platform",
                "/tmp/repo",
                Option::<String>::None,
                Some("sp-id-123"),
                Some("sp-secret-123"),
                "2026-01-01T00:00:00Z"
            ],
        )
        .unwrap();

        let repo_name: Option<String> = conn
            .query_row(
                "SELECT migration_repo_name FROM workspaces WHERE id = ?1",
                ["ws-1"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(repo_name.as_deref(), Some("acme/data-platform"));

        let sp_id: Option<String> = conn
            .query_row(
                "SELECT fabric_service_principal_id FROM workspaces WHERE id = ?1",
                ["ws-1"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(sp_id.as_deref(), Some("sp-id-123"));
    }

    #[test]
    fn deleting_workspace_cascades_to_dependent_tables() {
        let conn = open_memory();
        conn.execute(
            "INSERT INTO workspaces(id, display_name, migration_repo_path, created_at) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params!["ws-1", "Migration Workspace", "/tmp/repo", "2026-01-01T00:00:00Z"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO items(id, workspace_id, display_name, item_type) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params!["item-1", "ws-1", "AdventureWorks", "Warehouse"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO warehouse_schemas(warehouse_item_id, schema_name, schema_id_local) VALUES (?1, ?2, ?3)",
            rusqlite::params!["item-1", "dbo", 1i64],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO warehouse_tables(warehouse_item_id, schema_name, table_name, object_id_local) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params!["item-1", "dbo", "Customers", 100i64],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO selected_tables(id, workspace_id, warehouse_item_id, schema_name, table_name) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["st-1", "ws-1", "item-1", "dbo", "Customers"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO table_config(selected_table_id, table_type) VALUES (?1, ?2)",
            rusqlite::params!["st-1", "fact"],
        )
        .unwrap();

        conn.execute("DELETE FROM workspaces WHERE id=?1", ["ws-1"])
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
        let selected: i64 = conn
            .query_row("SELECT COUNT(*) FROM selected_tables", [], |row| row.get(0))
            .unwrap();
        let config: i64 = conn
            .query_row("SELECT COUNT(*) FROM table_config", [], |row| row.get(0))
            .unwrap();

        assert_eq!(items, 0);
        assert_eq!(schemas, 0);
        assert_eq!(tables, 0);
        assert_eq!(selected, 0);
        assert_eq!(config, 0);
    }

    #[test]
    fn canonical_schema_contract_matches_database_design_doc() {
        let conn = open_memory();

        // Foundational canonical tables use id PK.
        for table in [
            "sources",
            "containers",
            "namespaces",
            "data_objects",
            "orchestration_items",
            "orchestration_activities",
            "activity_object_links",
        ] {
            assert_pk_id(&conn, table);
        }

        // Named unique/index contracts.
        assert_index_exists(&conn, "ux_sources_external", "sources");
        assert_index_exists(&conn, "ux_containers_external", "containers");
        assert_index_exists(&conn, "ix_containers_source_id", "containers");
        assert_index_exists(&conn, "ux_namespaces_natural", "namespaces");
        assert_index_exists(&conn, "ix_namespaces_container_id", "namespaces");
        assert_index_exists(&conn, "ux_data_objects_natural", "data_objects");
        assert_index_exists(&conn, "ix_data_objects_namespace_id", "data_objects");
        assert_index_exists(
            &conn,
            "ux_orchestration_items_external",
            "orchestration_items",
        );
        assert_index_exists(
            &conn,
            "ix_orchestration_items_source_id",
            "orchestration_items",
        );
        assert_index_exists(
            &conn,
            "ux_orchestration_activities_natural",
            "orchestration_activities",
        );
        assert_index_exists(
            &conn,
            "ix_orchestration_activities_item_id",
            "orchestration_activities",
        );
        assert_index_exists(&conn, "ux_activity_object_links", "activity_object_links");
        assert_index_exists(
            &conn,
            "ix_activity_object_links_activity_id",
            "activity_object_links",
        );
        assert_index_exists(
            &conn,
            "ix_activity_object_links_data_object_id",
            "activity_object_links",
        );

        // FK Delete Policy: cascade-by-default on foundational graph.
        assert_fk_delete_cascade(&conn, "sources", "workspace_id", "workspaces", "id");
        assert_fk_delete_cascade(&conn, "containers", "source_id", "sources", "id");
        assert_fk_delete_cascade(&conn, "namespaces", "container_id", "containers", "id");
        assert_fk_delete_cascade(&conn, "data_objects", "namespace_id", "namespaces", "id");
        assert_fk_delete_cascade(&conn, "orchestration_items", "source_id", "sources", "id");
        assert_fk_delete_cascade(
            &conn,
            "orchestration_activities",
            "orchestration_item_id",
            "orchestration_items",
            "id",
        );
        assert_fk_delete_cascade(
            &conn,
            "activity_object_links",
            "orchestration_activity_id",
            "orchestration_activities",
            "id",
        );
        assert_fk_delete_cascade(
            &conn,
            "activity_object_links",
            "data_object_id",
            "data_objects",
            "id",
        );

        // Extension tables should cascade from data_objects.
        for extension_table in [
            "sqlserver_object_columns",
            "sqlserver_constraints_indexes",
            "sqlserver_partitions",
            "sqlserver_procedure_parameters",
            "sqlserver_procedure_runtime_stats",
            "sqlserver_table_ddl_snapshots",
        ] {
            assert_fk_delete_cascade(
                &conn,
                extension_table,
                "data_object_id",
                "data_objects",
                "id",
            );
        }
        assert_fk_delete_cascade(
            &conn,
            "sqlserver_procedure_lineage",
            "procedure_data_object_id",
            "data_objects",
            "id",
        );
        assert_fk_delete_cascade(
            &conn,
            "sqlserver_procedure_lineage",
            "table_data_object_id",
            "data_objects",
            "id",
        );
    }

    #[test]
    fn selected_tables_has_natural_unique_index() {
        let conn = open_memory();
        assert_index_exists(&conn, "ux_selected_tables_natural", "selected_tables");
    }

    #[test]
    fn reconcile_phase_prefers_setup_when_prereqs_missing() {
        let conn = open_memory();
        write_app_phase(&conn, AppPhase::RunningLocked).unwrap();

        let state = reconcile_and_persist_app_phase(&conn).unwrap();
        assert_eq!(state.app_phase, AppPhase::SetupRequired);
    }

    #[test]
    fn reconcile_phase_returns_scope_editable_after_prereqs() {
        let conn = open_memory();
        let settings = AppSettings {
            anthropic_api_key: Some("sk-ant-test".to_string()),
            github_oauth_token: Some("gho_test".to_string()),
            github_user_login: None,
            github_user_avatar: None,
            github_user_email: None,
        };
        write_settings(&conn, &settings).unwrap();
        conn.execute(
            "INSERT INTO workspaces(id, display_name, migration_repo_path, created_at) VALUES (?1, ?2, ?3, ?4)",
            params!["ws-1", "ws", "/tmp/repo", "2026-01-01T00:00:00Z"],
        )
        .unwrap();

        let state = reconcile_and_persist_app_phase(&conn).unwrap();
        assert_eq!(state.app_phase, AppPhase::ScopeEditable);
    }

    #[test]
    fn reconcile_phase_returns_ready_when_scope_and_plan_finalized() {
        let conn = open_memory();
        let settings = AppSettings {
            anthropic_api_key: Some("sk-ant-test".to_string()),
            github_oauth_token: Some("gho_test".to_string()),
            github_user_login: None,
            github_user_avatar: None,
            github_user_email: None,
        };
        write_settings(&conn, &settings).unwrap();
        conn.execute(
            "INSERT INTO workspaces(id, display_name, migration_repo_path, created_at) VALUES (?1, ?2, ?3, ?4)",
            params!["ws-1", "ws", "/tmp/repo", "2026-01-01T00:00:00Z"],
        )
        .unwrap();
        write_scope_finalized(&conn, true).unwrap();
        write_plan_finalized(&conn, true).unwrap();

        let state = reconcile_and_persist_app_phase(&conn).unwrap();
        assert_eq!(state.app_phase, AppPhase::ReadyToRun);
    }

    #[test]
    fn reconcile_phase_keeps_running_locked_when_prereqs_hold() {
        let conn = open_memory();
        let settings = AppSettings {
            anthropic_api_key: Some("sk-ant-test".to_string()),
            github_oauth_token: Some("gho_test".to_string()),
            github_user_login: None,
            github_user_avatar: None,
            github_user_email: None,
        };
        write_settings(&conn, &settings).unwrap();
        conn.execute(
            "INSERT INTO workspaces(id, display_name, migration_repo_path, created_at) VALUES (?1, ?2, ?3, ?4)",
            params!["ws-1", "ws", "/tmp/repo", "2026-01-01T00:00:00Z"],
        )
        .unwrap();
        write_app_phase(&conn, AppPhase::RunningLocked).unwrap();

        let state = reconcile_and_persist_app_phase(&conn).unwrap();
        assert_eq!(state.app_phase, AppPhase::RunningLocked);
    }
}
