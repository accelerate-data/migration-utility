use crate::types::CommandError;

#[allow(clippy::enum_variant_names)]
#[derive(Clone, Copy, Debug)]
pub enum SourceQuery {
    DiscoverDatabases,
    DiscoverContainerId,
    DiscoverSchemas,
    DiscoverTables,
    DiscoverColumns,
    DiscoverProcedures,
}

impl SourceQuery {
    pub fn name(self) -> &'static str {
        match self {
            SourceQuery::DiscoverDatabases => "discover_databases",
            SourceQuery::DiscoverContainerId => "discover_container_id",
            SourceQuery::DiscoverSchemas => "discover_schemas",
            SourceQuery::DiscoverTables => "discover_tables",
            SourceQuery::DiscoverColumns => "discover_columns",
            SourceQuery::DiscoverProcedures => "discover_procedures",
        }
    }
}

pub fn resolve_source_query(
    source_type: &str,
    query: SourceQuery,
) -> Result<&'static str, CommandError> {
    match (source_type, query) {
        ("sql_server", SourceQuery::DiscoverDatabases) => Ok(include_str!(
            "../sql/source/sql_server/discover_databases.sql"
        )),
        ("fabric_warehouse", SourceQuery::DiscoverDatabases) => Ok(include_str!(
            "../sql/source/fabric_warehouse/discover_databases.sql"
        )),
        ("sql_server", SourceQuery::DiscoverContainerId) => Ok(include_str!(
            "../sql/source/sql_server/discover_container_id.sql"
        )),
        ("sql_server", SourceQuery::DiscoverSchemas) => Ok(include_str!(
            "../sql/source/sql_server/discover_schemas.sql"
        )),
        ("sql_server", SourceQuery::DiscoverTables) => {
            Ok(include_str!("../sql/source/sql_server/discover_tables.sql"))
        }
        ("sql_server", SourceQuery::DiscoverColumns) => {
            Ok(include_str!("../sql/source/sql_server/discover_columns.sql"))
        }
        ("sql_server", SourceQuery::DiscoverProcedures) => Ok(include_str!(
            "../sql/source/sql_server/discover_procedures.sql"
        )),
        _ => Err(CommandError::Io(format!(
            "Unsupported source query lookup: source_type={source_type}, query={}",
            query.name()
        ))),
    }
}

pub fn should_log_source_sql() -> bool {
    std::env::var("MIGRATION_DEBUG_SQL")
        .ok()
        .map(|v| {
            let normalized = v.trim().to_ascii_lowercase();
            normalized == "1" || normalized == "true" || normalized == "yes" || normalized == "on"
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tiberius::{AuthMethod, Client, Config, EncryptionLevel};
    use tokio::net::TcpStream;
    use tokio::runtime::Builder as RuntimeBuilder;
    use tokio_util::compat::TokioAsyncWriteCompatExt;

    #[test]
    fn resolves_discover_db_for_sql_server() {
        let sql = resolve_source_query("sql_server", SourceQuery::DiscoverDatabases).unwrap();
        assert!(sql.contains("sys.databases"));
    }

    #[test]
    fn resolves_discover_db_for_fabric_warehouse() {
        let sql = resolve_source_query("fabric_warehouse", SourceQuery::DiscoverDatabases).unwrap();
        assert!(sql.contains("sys.databases"));
    }

    #[test]
    fn rejects_unsupported_source_type() {
        let err = resolve_source_query("postgres", SourceQuery::DiscoverDatabases).unwrap_err();
        match err {
            CommandError::Io(msg) => assert!(msg.contains("Unsupported source query lookup")),
            _ => panic!("expected io error"),
        }
    }

    #[test]
    fn resolves_sql_server_source_inventory_queries() {
        let container =
            resolve_source_query("sql_server", SourceQuery::DiscoverContainerId).unwrap();
        assert!(container.contains("DB_ID()"));

        let schemas = resolve_source_query("sql_server", SourceQuery::DiscoverSchemas).unwrap();
        assert!(schemas.contains("sys.schemas"));

        let tables = resolve_source_query("sql_server", SourceQuery::DiscoverTables).unwrap();
        assert!(tables.contains("sys.tables"));

        let columns = resolve_source_query("sql_server", SourceQuery::DiscoverColumns).unwrap();
        assert!(columns.contains("sys.columns"));

        let procedures =
            resolve_source_query("sql_server", SourceQuery::DiscoverProcedures).unwrap();
        assert!(procedures.contains("sys.procedures"));
    }

    #[test]
    #[ignore = "requires reachable SQL Server (e.g. Docker)"]
    fn discover_databases_query_executes_against_real_sql_server() {
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

        let sql = resolve_source_query("sql_server", SourceQuery::DiscoverDatabases).unwrap();

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

        rt.block_on(async {
            let tcp = TcpStream::connect(config.get_addr())
                .await
                .expect("failed to connect to SQL Server");
            tcp.set_nodelay(true).expect("failed to set TCP nodelay");

            let mut client = Client::connect(config, tcp.compat_write())
                .await
                .expect("failed to authenticate to SQL Server");

            let rows = client
                .simple_query(sql)
                .await
                .expect("query execution failed")
                .into_first_result()
                .await
                .expect("failed to parse query result");

            let names: Vec<String> = rows
                .into_iter()
                .filter_map(|row| row.get::<&str, _>(0).map(|name| name.to_string()))
                .collect();

            assert!(
                !names.is_empty(),
                "expected at least one accessible database in query result"
            );
            assert!(
                names
                    .iter()
                    .any(|n| n.eq_ignore_ascii_case("WideWorldImportersDW")),
                "expected WideWorldImportersDW to be discoverable; got: {names:?}"
            );
        });
    }

    #[test]
    #[ignore = "requires reachable SQL Server (e.g. Docker)"]
    fn discover_inventory_queries_execute_against_real_sql_server() {
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

        let schemas_sql = resolve_source_query("sql_server", SourceQuery::DiscoverSchemas).unwrap();
        let tables_sql = resolve_source_query("sql_server", SourceQuery::DiscoverTables).unwrap();
        let columns_sql = resolve_source_query("sql_server", SourceQuery::DiscoverColumns).unwrap();
        let procedures_sql =
            resolve_source_query("sql_server", SourceQuery::DiscoverProcedures).unwrap();
        let container_sql =
            resolve_source_query("sql_server", SourceQuery::DiscoverContainerId).unwrap();

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

        rt.block_on(async {
            let tcp = TcpStream::connect(config.get_addr())
                .await
                .expect("failed to connect to SQL Server");
            tcp.set_nodelay(true).expect("failed to set TCP nodelay");

            let mut client = Client::connect(config, tcp.compat_write())
                .await
                .expect("failed to authenticate to SQL Server");

            let schema_rows = client
                .simple_query(schemas_sql)
                .await
                .expect("schema query execution failed")
                .into_first_result()
                .await
                .expect("failed to parse schema query result");
            assert!(
                !schema_rows.is_empty(),
                "expected schema discovery to return at least one row"
            );

            let container_rows = client
                .simple_query(container_sql)
                .await
                .expect("container query execution failed")
                .into_first_result()
                .await
                .expect("failed to parse container query result");
            assert!(
                container_rows
                    .first()
                    .and_then(|row| row.get::<i64, _>(0))
                    .is_some(),
                "expected container discovery to return DB_ID()"
            );

            let table_rows = client
                .simple_query(tables_sql)
                .await
                .expect("table query execution failed")
                .into_first_result()
                .await
                .expect("failed to parse table query result");
            assert!(
                !table_rows.is_empty(),
                "expected table discovery to return at least one row"
            );

            let column_rows = client
                .simple_query(columns_sql)
                .await
                .expect("column query execution failed")
                .into_first_result()
                .await
                .expect("failed to parse column query result");
            assert!(
                !column_rows.is_empty(),
                "expected column discovery to return at least one row"
            );
            // Verify column structure
            let first_col = column_rows.first().expect("expected at least one column");
            assert!(first_col.get::<&str, _>(0).is_some(), "expected schema_name");
            assert!(first_col.get::<&str, _>(1).is_some(), "expected table_name");
            assert!(first_col.get::<i64, _>(2).is_some(), "expected object_id_local");
            assert!(first_col.get::<&str, _>(3).is_some(), "expected column_name");
            assert!(first_col.get::<i32, _>(4).is_some(), "expected column_id");
            assert!(first_col.get::<&str, _>(5).is_some(), "expected data_type");
            assert!(first_col.get::<bool, _>(6).is_some(), "expected is_nullable");

            let procedure_rows = client
                .simple_query(procedures_sql)
                .await
                .expect("procedure query execution failed")
                .into_first_result()
                .await
                .expect("failed to parse procedure query result");
            assert!(
                !procedure_rows.is_empty(),
                "expected procedure discovery to return at least one row"
            );
        });
    }
}
