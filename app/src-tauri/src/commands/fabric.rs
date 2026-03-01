use rusqlite::params;
use tauri::State;

use crate::db::DbState;
use crate::types::{
    CommandError, Item, PipelineActivity, WarehouseProcedure, WarehouseSchema, WarehouseTable,
};

#[tauri::command]
pub fn fabric_upsert_items(
    workspace_id: String,
    items: Vec<Item>,
    state: State<DbState>,
) -> Result<(), CommandError> {
    log::info!(
        "fabric_upsert_items: workspace_id={} count={}",
        workspace_id,
        items.len()
    );
    let conn = state.0.lock().unwrap();
    let tx = conn.unchecked_transaction().map_err(|e| {
        log::error!("fabric_upsert_items: failed to begin transaction: {e}");
        CommandError::from(e)
    })?;
    for item in &items {
        log::debug!("fabric_upsert_items: upserting item id={}", item.id);
        tx.execute(
            "INSERT OR REPLACE INTO items(id, workspace_id, display_name, description, folder_id, item_type, connection_string, collation_type)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                item.id,
                item.workspace_id,
                item.display_name,
                item.description,
                item.folder_id,
                item.item_type,
                item.connection_string,
                item.collation_type,
            ],
        )
        .map_err(|e| {
            log::error!("fabric_upsert_items: failed to upsert item {}: {e}", item.id);
            CommandError::from(e)
        })?;
    }
    tx.commit().map_err(|e| {
        log::error!("fabric_upsert_items: failed to commit: {e}");
        CommandError::from(e)
    })?;
    Ok(())
}

#[tauri::command]
pub fn fabric_upsert_schemas(
    items: Vec<WarehouseSchema>,
    state: State<DbState>,
) -> Result<(), CommandError> {
    log::info!("fabric_upsert_schemas: count={}", items.len());
    let conn = state.0.lock().unwrap();
    let tx = conn.unchecked_transaction().map_err(|e| {
        log::error!("fabric_upsert_schemas: failed to begin transaction: {e}");
        CommandError::from(e)
    })?;
    for schema in &items {
        log::debug!(
            "fabric_upsert_schemas: upserting schema warehouse_item_id={} schema_name={}",
            schema.warehouse_item_id,
            schema.schema_name
        );
        tx.execute(
            "INSERT OR REPLACE INTO warehouse_schemas(warehouse_item_id, schema_name, schema_id_local)
             VALUES (?1, ?2, ?3)",
            params![schema.warehouse_item_id, schema.schema_name, schema.schema_id_local],
        )
        .map_err(|e| {
            log::error!(
                "fabric_upsert_schemas: failed to upsert schema {}.{}: {e}",
                schema.warehouse_item_id,
                schema.schema_name
            );
            CommandError::from(e)
        })?;
    }
    tx.commit().map_err(|e| {
        log::error!("fabric_upsert_schemas: failed to commit: {e}");
        CommandError::from(e)
    })?;
    Ok(())
}

#[tauri::command]
pub fn fabric_upsert_tables(
    items: Vec<WarehouseTable>,
    state: State<DbState>,
) -> Result<(), CommandError> {
    log::info!("fabric_upsert_tables: count={}", items.len());
    let conn = state.0.lock().unwrap();
    let tx = conn.unchecked_transaction().map_err(|e| {
        log::error!("fabric_upsert_tables: failed to begin transaction: {e}");
        CommandError::from(e)
    })?;
    for table in &items {
        log::debug!(
            "fabric_upsert_tables: upserting table {}.{}",
            table.schema_name,
            table.table_name
        );
        tx.execute(
            "INSERT OR REPLACE INTO warehouse_tables(warehouse_item_id, schema_name, table_name, object_id_local)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                table.warehouse_item_id,
                table.schema_name,
                table.table_name,
                table.object_id_local,
            ],
        )
        .map_err(|e| {
            log::error!(
                "fabric_upsert_tables: failed to upsert table {}.{}: {e}",
                table.schema_name,
                table.table_name
            );
            CommandError::from(e)
        })?;
    }
    tx.commit().map_err(|e| {
        log::error!("fabric_upsert_tables: failed to commit: {e}");
        CommandError::from(e)
    })?;
    Ok(())
}

#[tauri::command]
pub fn fabric_upsert_procedures(
    items: Vec<WarehouseProcedure>,
    state: State<DbState>,
) -> Result<(), CommandError> {
    log::info!("fabric_upsert_procedures: count={}", items.len());
    let conn = state.0.lock().unwrap();
    let tx = conn.unchecked_transaction().map_err(|e| {
        log::error!("fabric_upsert_procedures: failed to begin transaction: {e}");
        CommandError::from(e)
    })?;
    for proc in &items {
        log::debug!(
            "fabric_upsert_procedures: upserting procedure {}.{}",
            proc.schema_name,
            proc.procedure_name
        );
        tx.execute(
            "INSERT OR REPLACE INTO warehouse_procedures(warehouse_item_id, schema_name, procedure_name, object_id_local, sql_body)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                proc.warehouse_item_id,
                proc.schema_name,
                proc.procedure_name,
                proc.object_id_local,
                proc.sql_body,
            ],
        )
        .map_err(|e| {
            log::error!(
                "fabric_upsert_procedures: failed to upsert procedure {}.{}: {e}",
                proc.schema_name,
                proc.procedure_name
            );
            CommandError::from(e)
        })?;
    }
    tx.commit().map_err(|e| {
        log::error!("fabric_upsert_procedures: failed to commit: {e}");
        CommandError::from(e)
    })?;
    Ok(())
}

#[tauri::command]
pub fn fabric_upsert_pipeline_activities(
    items: Vec<PipelineActivity>,
    state: State<DbState>,
) -> Result<(), CommandError> {
    log::info!("fabric_upsert_pipeline_activities: count={}", items.len());
    let conn = state.0.lock().unwrap();
    let tx = conn.unchecked_transaction().map_err(|e| {
        log::error!("fabric_upsert_pipeline_activities: failed to begin transaction: {e}");
        CommandError::from(e)
    })?;
    for activity in &items {
        log::debug!(
            "fabric_upsert_pipeline_activities: upserting activity name={}",
            activity.activity_name
        );
        tx.execute(
            "INSERT OR REPLACE INTO pipeline_activities(pipeline_item_id, activity_name, activity_type, target_warehouse_item_id, target_schema_name, target_procedure_name, parameters_json, depends_on_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                activity.pipeline_item_id,
                activity.activity_name,
                activity.activity_type,
                activity.target_warehouse_item_id,
                activity.target_schema_name,
                activity.target_procedure_name,
                activity.parameters_json,
                activity.depends_on_json,
            ],
        )
        .map_err(|e| {
            log::error!(
                "fabric_upsert_pipeline_activities: failed to upsert activity {}: {e}",
                activity.activity_name
            );
            CommandError::from(e)
        })?;
    }
    tx.commit().map_err(|e| {
        log::error!("fabric_upsert_pipeline_activities: failed to commit: {e}");
        CommandError::from(e)
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;

    fn insert_workspace(conn: &rusqlite::Connection, workspace_id: &str) {
        conn.execute(
            "INSERT INTO workspaces(id, display_name, migration_repo_path, created_at) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![workspace_id, "Test Workspace", "/tmp/repo", "2026-01-01T00:00:00Z"],
        )
        .unwrap();
    }

    fn insert_item(
        conn: &rusqlite::Connection,
        item_id: &str,
        workspace_id: &str,
        item_type: &str,
    ) {
        conn.execute(
            "INSERT INTO items(id, workspace_id, display_name, item_type) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![item_id, workspace_id, "Test Item", item_type],
        )
        .unwrap();
    }

    fn insert_schema(conn: &rusqlite::Connection, warehouse_item_id: &str, schema_name: &str) {
        conn.execute(
            "INSERT INTO warehouse_schemas(warehouse_item_id, schema_name) VALUES (?1, ?2)",
            rusqlite::params![warehouse_item_id, schema_name],
        )
        .unwrap();
    }

    #[test]
    fn upsert_items_idempotent() {
        let conn = db::open_in_memory().unwrap();
        let ws_id = "ws-1";
        insert_workspace(&conn, ws_id);

        let item = Item {
            id: "item-1".to_string(),
            workspace_id: ws_id.to_string(),
            display_name: "My Warehouse".to_string(),
            description: None,
            folder_id: None,
            item_type: "Warehouse".to_string(),
            connection_string: None,
            collation_type: None,
        };

        // Insert twice — second call must be a no-op (INSERT OR REPLACE)
        for _ in 0..2 {
            conn.execute(
                "INSERT OR REPLACE INTO items(id, workspace_id, display_name, item_type) VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![item.id, item.workspace_id, item.display_name, item.item_type],
            )
            .unwrap();
        }

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM items WHERE id=?1",
                rusqlite::params!["item-1"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn upsert_schemas_idempotent() {
        let conn = db::open_in_memory().unwrap();
        let ws_id = "ws-2";
        insert_workspace(&conn, ws_id);
        insert_item(&conn, "wh-1", ws_id, "Warehouse");

        let schema = WarehouseSchema {
            warehouse_item_id: "wh-1".to_string(),
            schema_name: "dbo".to_string(),
            schema_id_local: Some(1),
        };

        for _ in 0..2 {
            conn.execute(
                "INSERT OR REPLACE INTO warehouse_schemas(warehouse_item_id, schema_name, schema_id_local) VALUES (?1, ?2, ?3)",
                rusqlite::params![schema.warehouse_item_id, schema.schema_name, schema.schema_id_local],
            )
            .unwrap();
        }

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM warehouse_schemas WHERE warehouse_item_id=?1 AND schema_name=?2",
                rusqlite::params!["wh-1", "dbo"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn upsert_tables_idempotent() {
        let conn = db::open_in_memory().unwrap();
        let ws_id = "ws-3";
        insert_workspace(&conn, ws_id);
        insert_item(&conn, "wh-2", ws_id, "Warehouse");
        insert_schema(&conn, "wh-2", "dbo");

        let table = WarehouseTable {
            warehouse_item_id: "wh-2".to_string(),
            schema_name: "dbo".to_string(),
            table_name: "orders".to_string(),
            object_id_local: Some(42),
            row_count: None,
        };

        for _ in 0..2 {
            conn.execute(
                "INSERT OR REPLACE INTO warehouse_tables(warehouse_item_id, schema_name, table_name, object_id_local) VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![table.warehouse_item_id, table.schema_name, table.table_name, table.object_id_local],
            )
            .unwrap();
        }

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM warehouse_tables WHERE warehouse_item_id=?1 AND schema_name=?2 AND table_name=?3",
                rusqlite::params!["wh-2", "dbo", "orders"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn upsert_procedures_idempotent() {
        let conn = db::open_in_memory().unwrap();
        let ws_id = "ws-4";
        insert_workspace(&conn, ws_id);
        insert_item(&conn, "wh-3", ws_id, "Warehouse");
        insert_schema(&conn, "wh-3", "dbo");

        let proc = WarehouseProcedure {
            warehouse_item_id: "wh-3".to_string(),
            schema_name: "dbo".to_string(),
            procedure_name: "sp_load_orders".to_string(),
            object_id_local: Some(99),
            sql_body: Some("SELECT 1".to_string()),
        };

        for _ in 0..2 {
            conn.execute(
                "INSERT OR REPLACE INTO warehouse_procedures(warehouse_item_id, schema_name, procedure_name, object_id_local, sql_body) VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![proc.warehouse_item_id, proc.schema_name, proc.procedure_name, proc.object_id_local, proc.sql_body],
            )
            .unwrap();
        }

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM warehouse_procedures WHERE warehouse_item_id=?1 AND schema_name=?2 AND procedure_name=?3",
                rusqlite::params!["wh-3", "dbo", "sp_load_orders"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn upsert_pipeline_activities_idempotent() {
        let conn = db::open_in_memory().unwrap();
        let ws_id = "ws-5";
        insert_workspace(&conn, ws_id);
        insert_item(&conn, "pipe-1", ws_id, "DataPipeline");

        let activity = PipelineActivity {
            id: None,
            pipeline_item_id: "pipe-1".to_string(),
            activity_name: "Load Orders".to_string(),
            activity_type: "SqlServerStoredProcedure".to_string(),
            target_warehouse_item_id: None,
            target_schema_name: None,
            target_procedure_name: None,
            parameters_json: None,
            depends_on_json: None,
        };

        for _ in 0..2 {
            conn.execute(
                "INSERT OR REPLACE INTO pipeline_activities(pipeline_item_id, activity_name, activity_type) VALUES (?1, ?2, ?3)",
                rusqlite::params![activity.pipeline_item_id, activity.activity_name, activity.activity_type],
            )
            .unwrap();
        }

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pipeline_activities WHERE pipeline_item_id=?1 AND activity_name=?2",
                rusqlite::params!["pipe-1", "Load Orders"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }
}
