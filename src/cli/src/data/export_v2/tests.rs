// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::env;
use std::time::Duration;

use clap::Parser;
use common_error::ext::BoxedError;
use serde_json::Value;
use snafu::ResultExt;
use tempfile::tempdir;
use url::Url;

use super::command::{ExportCreateCommand, ExportVerifyCommand};
use crate::common::ObjectStoreConfig;
use crate::data::export_v2::manifest::ChunkStatus;
use crate::data::import_v2::ImportV2Command;
use crate::data::import_v2::coordinator::build_import_tasks;
use crate::data::import_v2::state::{ImportState, ImportTaskStatus, save_import_state};
use crate::data::path::data_dir_for_schema_chunk;
use crate::data::snapshot_storage::{OpenDalStorage, SnapshotStorage};
use crate::data::sql::escape_sql_identifier;
use crate::database::DatabaseClient;
use crate::error::{FileIoSnafu, InvalidArgumentsSnafu, OtherSnafu, Result};

struct TestConnection {
    addr: String,
    catalog: String,
    auth_basic: Option<String>,
}

impl TestConnection {
    fn from_env() -> Self {
        Self {
            addr: env::var("GREPTIME_ADDR").unwrap_or_else(|_| "127.0.0.1:4000".to_string()),
            catalog: env::var("GREPTIME_CATALOG").unwrap_or_else(|_| "greptime".to_string()),
            auth_basic: env::var("GREPTIME_AUTH_BASIC").ok(),
        }
    }

    fn client(&self) -> DatabaseClient {
        DatabaseClient::new(
            self.addr.clone(),
            self.catalog.clone(),
            self.auth_basic.clone(),
            Duration::from_secs(60),
            None,
            false,
        )
    }
}

fn path_to_uri(path: &std::path::Path) -> Result<String> {
    Url::from_directory_path(path)
        .map(|url| url.to_string())
        .map_err(|_| {
            InvalidArgumentsSnafu {
                msg: "invalid temp dir path".to_string(),
            }
            .build()
        })
}

async fn query_count(database_client: &DatabaseClient, schema: &str, table: &str) -> Result<u64> {
    let sql = format!("SELECT COUNT(*) FROM {}", escape_sql_identifier(table));
    let rows = database_client.sql(&sql, schema).await?;
    let first_row = rows.as_ref().and_then(|rows| rows.first()).ok_or_else(|| {
        InvalidArgumentsSnafu {
            msg: format!("empty result for query: {sql}"),
        }
        .build()
    })?;
    let first_value = first_row.first().ok_or_else(|| {
        InvalidArgumentsSnafu {
            msg: format!("no first column for query: {sql}"),
        }
        .build()
    })?;
    match first_value {
        Value::Number(n) => n.as_u64().ok_or_else(|| {
            InvalidArgumentsSnafu {
                msg: format!("count is not u64 for query: {sql}"),
            }
            .build()
        }),
        _ => InvalidArgumentsSnafu {
            msg: format!("unexpected count type for query: {sql}"),
        }
        .fail(),
    }
}

async fn query_hosts(database_client: &DatabaseClient, schema: &str) -> Result<Vec<String>> {
    let rows = database_client
        .sql("SELECT host FROM metrics ORDER BY host", schema)
        .await?
        .unwrap_or_default();
    rows.into_iter()
        .map(|row| match row.first() {
            Some(Value::String(value)) => Ok(value.clone()),
            _ => InvalidArgumentsSnafu {
                msg: "unexpected host value".to_string(),
            }
            .fail(),
        })
        .collect()
}

async fn schema_exists(database_client: &DatabaseClient, schema: &str) -> Result<bool> {
    let rows = database_client
        .sql_in_public("SHOW DATABASES")
        .await?
        .unwrap_or_default();
    Ok(rows
        .iter()
        .any(|row| matches!(row.first(), Some(Value::String(value)) if value == schema)))
}

#[tokio::test]
#[ignore]
async fn export_import_v2_schema_parity_e2e() -> Result<()> {
    let addr = env::var("GREPTIME_ADDR").unwrap_or_else(|_| "127.0.0.1:4000".to_string());
    let catalog = env::var("GREPTIME_CATALOG").unwrap_or_else(|_| "greptime".to_string());
    let auth_basic = env::var("GREPTIME_AUTH_BASIC").ok();
    let schema = "test_db_schema_parity";

    let database_client = DatabaseClient::new(
        addr.clone(),
        catalog.clone(),
        auth_basic.clone(),
        Duration::from_secs(60),
        None,
        false,
    );

    database_client
        .sql_in_public(&format!("DROP DATABASE IF EXISTS {schema}"))
        .await?;
    database_client
        .sql_in_public(&format!("CREATE DATABASE {schema}"))
        .await?;
    database_client
        .sql(
            "CREATE TABLE metrics (\
                ts TIMESTAMP TIME INDEX, \
                host STRING PRIMARY KEY, \
                cpu DOUBLE DEFAULT 0.0, \
                region_name STRING \
            ) ENGINE = mito WITH (ttl='7d', 'compaction.type'='twcs')",
            schema,
        )
        .await?;
    database_client
        .sql(
            "CREATE TABLE logs (\
                ts TIMESTAMP TIME INDEX, \
                app STRING PRIMARY KEY, \
                msg STRING NOT NULL COMMENT 'log message' \
            ) ENGINE = mito",
            schema,
        )
        .await?;
    database_client
        .sql(
            "CREATE TABLE metrics_physical (\
                ts TIMESTAMP TIME INDEX, \
                host STRING, \
                region_name STRING, \
                cpu DOUBLE DEFAULT 0.0, \
                PRIMARY KEY (host, region_name) \
            ) ENGINE = metric WITH (physical_metric_table='true')",
            schema,
        )
        .await?;
    database_client
        .sql(
            "CREATE TABLE metrics_logical (\
                ts TIMESTAMP TIME INDEX, \
                host STRING, \
                region_name STRING, \
                cpu DOUBLE DEFAULT 0.0, \
                PRIMARY KEY (host, region_name) \
            ) ENGINE = metric WITH (on_physical_table='metrics_physical')",
            schema,
        )
        .await?;
    database_client
        .sql(
            "CREATE VIEW metrics_view AS SELECT * FROM metrics WHERE cpu > 0.5",
            schema,
        )
        .await?;

    let src_dir = tempdir().context(FileIoSnafu)?;
    let src_uri = path_to_uri(src_dir.path())?;

    let mut export_args = vec![
        "export-v2-create",
        "--addr",
        &addr,
        "--to",
        &src_uri,
        "--catalog",
        &catalog,
        "--schemas",
        schema,
        "--schema-only",
    ];
    if let Some(auth) = &auth_basic {
        export_args.push("--auth-basic");
        export_args.push(auth);
    }
    let export_cmd = ExportCreateCommand::parse_from(export_args);
    export_cmd
        .build()
        .await
        .context(OtherSnafu)?
        .do_work()
        .await
        .context(OtherSnafu)?;

    database_client
        .sql_in_public(&format!("DROP DATABASE {schema}"))
        .await?;

    let mut import_args = vec![
        "import-v2",
        "--addr",
        &addr,
        "--from",
        &src_uri,
        "--catalog",
        &catalog,
        "--schemas",
        schema,
    ];
    if let Some(auth) = &auth_basic {
        import_args.push("--auth-basic");
        import_args.push(auth);
    }
    let import_cmd = ImportV2Command::parse_from(import_args);
    import_cmd
        .build()
        .await
        .context(OtherSnafu)?
        .do_work()
        .await
        .context(OtherSnafu)?;

    let dst_dir = tempdir().context(FileIoSnafu)?;
    let dst_uri = Url::from_directory_path(dst_dir.path())
        .map_err(|_| {
            InvalidArgumentsSnafu {
                msg: "invalid temp dir path".to_string(),
            }
            .build()
        })?
        .to_string();

    let mut export_args = vec![
        "export-v2-create",
        "--addr",
        &addr,
        "--to",
        &dst_uri,
        "--catalog",
        &catalog,
        "--schemas",
        schema,
        "--schema-only",
    ];
    if let Some(auth) = &auth_basic {
        export_args.push("--auth-basic");
        export_args.push(auth);
    }
    let export_cmd = ExportCreateCommand::parse_from(export_args);
    export_cmd
        .build()
        .await
        .context(OtherSnafu)?
        .do_work()
        .await
        .context(OtherSnafu)?;

    let storage_config = ObjectStoreConfig::default();
    let src_storage = OpenDalStorage::from_uri(&src_uri, &storage_config)
        .map_err(BoxedError::new)
        .context(OtherSnafu)?;
    let dst_storage = OpenDalStorage::from_uri(&dst_uri, &storage_config)
        .map_err(BoxedError::new)
        .context(OtherSnafu)?;

    let src_schema_snapshot = src_storage
        .read_schema()
        .await
        .map_err(BoxedError::new)
        .context(OtherSnafu)?;
    let dst_schema_snapshot = dst_storage
        .read_schema()
        .await
        .map_err(BoxedError::new)
        .context(OtherSnafu)?;
    assert_eq!(src_schema_snapshot, dst_schema_snapshot);

    database_client
        .sql_in_public(&format!("DROP DATABASE IF EXISTS {schema}"))
        .await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn import_v2_ddl_dry_run_e2e() -> Result<()> {
    let addr = env::var("GREPTIME_ADDR").unwrap_or_else(|_| "127.0.0.1:4000".to_string());
    let catalog = env::var("GREPTIME_CATALOG").unwrap_or_else(|_| "greptime".to_string());
    let auth_basic = env::var("GREPTIME_AUTH_BASIC").ok();
    let schema = "test_db_ddl_dry_run";

    let database_client = DatabaseClient::new(
        addr.clone(),
        catalog.clone(),
        auth_basic.clone(),
        Duration::from_secs(60),
        None,
        false,
    );

    database_client
        .sql_in_public(&format!("DROP DATABASE IF EXISTS {schema}"))
        .await?;
    database_client
        .sql_in_public(&format!("CREATE DATABASE {schema}"))
        .await?;
    database_client
        .sql(
            "CREATE TABLE metrics (\
                ts TIMESTAMP TIME INDEX, \
                host STRING PRIMARY KEY, \
                cpu DOUBLE DEFAULT 0.0, \
                region_name STRING \
            ) ENGINE = mito WITH (ttl='7d', 'compaction.type'='twcs')",
            schema,
        )
        .await?;
    database_client
        .sql(
            "CREATE TABLE logs (\
                ts TIMESTAMP TIME INDEX, \
                app STRING PRIMARY KEY, \
                msg STRING NOT NULL COMMENT 'log message' \
            ) ENGINE = mito",
            schema,
        )
        .await?;

    let src_dir = tempdir().context(FileIoSnafu)?;
    let src_uri = path_to_uri(src_dir.path())?;

    let mut export_args = vec![
        "export-v2-create",
        "--addr",
        &addr,
        "--to",
        &src_uri,
        "--catalog",
        &catalog,
        "--schemas",
        schema,
        "--schema-only",
    ];
    if let Some(auth) = &auth_basic {
        export_args.push("--auth-basic");
        export_args.push(auth);
    }
    let export_cmd = ExportCreateCommand::parse_from(export_args);
    export_cmd
        .build()
        .await
        .context(OtherSnafu)?
        .do_work()
        .await
        .context(OtherSnafu)?;

    let mut import_args = vec![
        "import-v2",
        "--addr",
        &addr,
        "--from",
        &src_uri,
        "--catalog",
        &catalog,
        "--schemas",
        schema,
        "--dry-run",
    ];
    if let Some(auth) = &auth_basic {
        import_args.push("--auth-basic");
        import_args.push(auth);
    }
    let import_cmd = ImportV2Command::parse_from(import_args);
    import_cmd
        .build()
        .await
        .context(OtherSnafu)?
        .do_work()
        .await
        .context(OtherSnafu)?;

    database_client
        .sql_in_public(&format!("DROP DATABASE IF EXISTS {schema}"))
        .await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn export_import_v2_data_roundtrip_e2e() -> Result<()> {
    let addr = env::var("GREPTIME_ADDR").unwrap_or_else(|_| "127.0.0.1:4000".to_string());
    let catalog = env::var("GREPTIME_CATALOG").unwrap_or_else(|_| "greptime".to_string());
    let auth_basic = env::var("GREPTIME_AUTH_BASIC").ok();
    let schema = "test_db_data_roundtrip";

    let database_client = DatabaseClient::new(
        addr.clone(),
        catalog.clone(),
        auth_basic.clone(),
        Duration::from_secs(60),
        None,
        false,
    );

    database_client
        .sql_in_public(&format!("DROP DATABASE IF EXISTS {schema}"))
        .await?;
    database_client
        .sql_in_public(&format!("CREATE DATABASE {schema}"))
        .await?;
    database_client
        .sql(
            "CREATE TABLE metrics (\
                ts TIMESTAMP TIME INDEX, \
                host STRING PRIMARY KEY, \
                cpu DOUBLE \
            ) ENGINE=mito",
            schema,
        )
        .await?;
    database_client
        .sql(
            "INSERT INTO metrics (ts, host, cpu) VALUES \
             ('2025-01-01T00:00:00Z', 'h1', 1.0), \
             ('2025-01-01T01:00:00Z', 'h2', 2.0)",
            schema,
        )
        .await?;

    let expected_rows = query_count(&database_client, schema, "metrics").await?;

    let src_dir = tempdir().context(FileIoSnafu)?;
    let src_uri = path_to_uri(src_dir.path())?;

    let mut export_args = vec![
        "export-v2-create",
        "--addr",
        &addr,
        "--to",
        &src_uri,
        "--catalog",
        &catalog,
        "--schemas",
        schema,
        "--start-time",
        "2025-01-01T00:00:00Z",
        "--end-time",
        "2025-01-01T02:00:00Z",
        "--chunk-time-window",
        "1h",
        "--chunk-parallelism",
        "2",
        "--progress",
        "never",
    ];
    if let Some(auth) = &auth_basic {
        export_args.push("--auth-basic");
        export_args.push(auth);
    }
    let export_cmd = ExportCreateCommand::parse_from(export_args);
    export_cmd
        .build()
        .await
        .context(OtherSnafu)?
        .do_work()
        .await
        .context(OtherSnafu)?;

    let verify_cmd = ExportVerifyCommand::parse_from(["export-v2-verify", "--snapshot", &src_uri]);
    verify_cmd
        .build()
        .await
        .context(OtherSnafu)?
        .do_work()
        .await
        .context(OtherSnafu)?;

    database_client
        .sql_in_public(&format!("DROP DATABASE IF EXISTS {schema}"))
        .await?;

    let import_state_dir = tempdir().context(FileIoSnafu)?;
    let import_state_path = import_state_dir.path().join("import-state.json");
    let import_state_path = import_state_path.to_string_lossy().into_owned();
    let mut import_args = vec![
        "import-v2",
        "--addr",
        &addr,
        "--from",
        &src_uri,
        "--catalog",
        &catalog,
        "--schemas",
        schema,
        "--task-parallelism",
        "2",
        "--state-path",
        &import_state_path,
        "--progress",
        "never",
    ];
    if let Some(auth) = &auth_basic {
        import_args.push("--auth-basic");
        import_args.push(auth);
    }
    let import_cmd = ImportV2Command::parse_from(import_args);
    import_cmd
        .build()
        .await
        .context(OtherSnafu)?
        .do_work()
        .await
        .context(OtherSnafu)?;

    let actual_rows = query_count(&database_client, schema, "metrics").await?;
    assert_eq!(actual_rows, expected_rows);

    database_client
        .sql_in_public(&format!("DROP DATABASE IF EXISTS {schema}"))
        .await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn import_v2_resume_from_completed_chunk_e2e() -> Result<()> {
    let conn = TestConnection::from_env();
    let schema = "test_db_import_resume_completed_chunk";

    let database_client = conn.client();

    database_client
        .sql_in_public(&format!("DROP DATABASE IF EXISTS {schema}"))
        .await?;
    database_client
        .sql_in_public(&format!("CREATE DATABASE {schema}"))
        .await?;
    database_client
        .sql(
            "CREATE TABLE metrics (\
                ts TIMESTAMP TIME INDEX, \
                host STRING, \
                cpu DOUBLE \
            ) ENGINE=mito",
            schema,
        )
        .await?;
    database_client
        .sql(
            "INSERT INTO metrics (ts, host, cpu) VALUES \
             ('2025-01-01T00:00:00Z', 'h1', 1.0), \
             ('2025-01-01T01:00:00Z', 'h2', 2.0)",
            schema,
        )
        .await?;

    let src_dir = tempdir().context(FileIoSnafu)?;
    let src_uri = path_to_uri(src_dir.path())?;

    let mut export_args = vec![
        "export-v2-create",
        "--addr",
        &conn.addr,
        "--to",
        &src_uri,
        "--catalog",
        &conn.catalog,
        "--schemas",
        schema,
        "--start-time",
        "2025-01-01T00:00:00Z",
        "--end-time",
        "2025-01-01T02:00:00Z",
        "--chunk-time-window",
        "1h",
        "--chunk-parallelism",
        "1",
        "--progress",
        "never",
    ];
    if let Some(auth) = &conn.auth_basic {
        export_args.push("--auth-basic");
        export_args.push(auth);
    }
    let export_cmd = ExportCreateCommand::parse_from(export_args);
    export_cmd
        .build()
        .await
        .context(OtherSnafu)?
        .do_work()
        .await
        .context(OtherSnafu)?;

    let storage_config = ObjectStoreConfig::default();
    let storage = OpenDalStorage::from_uri(&src_uri, &storage_config)
        .map_err(BoxedError::new)
        .context(OtherSnafu)?;
    let full_manifest = storage
        .read_manifest()
        .await
        .map_err(BoxedError::new)
        .context(OtherSnafu)?;
    assert_eq!(full_manifest.chunks.len(), 2);

    database_client
        .sql_in_public(&format!("DROP DATABASE IF EXISTS {schema}"))
        .await?;

    let mut partial_manifest = full_manifest.clone();
    partial_manifest.chunks.truncate(1);
    storage
        .write_manifest(&partial_manifest)
        .await
        .map_err(BoxedError::new)
        .context(OtherSnafu)?;

    let mut partial_import_args = vec![
        "import-v2",
        "--addr",
        &conn.addr,
        "--from",
        &src_uri,
        "--catalog",
        &conn.catalog,
        "--schemas",
        schema,
        "--task-parallelism",
        "1",
        "--progress",
        "never",
    ];
    if let Some(auth) = &conn.auth_basic {
        partial_import_args.push("--auth-basic");
        partial_import_args.push(auth);
    }
    let partial_import_cmd = ImportV2Command::parse_from(partial_import_args);
    partial_import_cmd
        .build()
        .await
        .context(OtherSnafu)?
        .do_work()
        .await
        .context(OtherSnafu)?;

    let rows_after_partial_import = query_count(&database_client, schema, "metrics").await?;
    assert_eq!(rows_after_partial_import, 1);

    storage
        .write_manifest(&full_manifest)
        .await
        .map_err(BoxedError::new)
        .context(OtherSnafu)?;

    let import_state_dir = tempdir().context(FileIoSnafu)?;
    let import_state_path = import_state_dir.path().join("import-state.json");
    let schemas = vec![schema.to_string()];
    let tasks = build_import_tasks(&full_manifest.chunks, &schemas);
    assert_eq!(tasks.len(), 2);

    let mut import_state = ImportState::new(
        full_manifest.snapshot_id.to_string(),
        conn.addr.clone(),
        conn.catalog.clone(),
        &schemas,
        tasks,
    );
    import_state.mark_ddl_completed();
    import_state
        .set_task_status(
            full_manifest.chunks[0].id,
            schema,
            ImportTaskStatus::Completed,
            None,
        )
        .map_err(BoxedError::new)
        .context(OtherSnafu)?;
    save_import_state(&import_state_path, &import_state)
        .await
        .map_err(BoxedError::new)
        .context(OtherSnafu)?;

    let import_state_path_arg = import_state_path.to_string_lossy().into_owned();
    let mut import_args = vec![
        "import-v2",
        "--addr",
        &conn.addr,
        "--from",
        &src_uri,
        "--catalog",
        &conn.catalog,
        "--schemas",
        schema,
        "--state-path",
        &import_state_path_arg,
        "--task-parallelism",
        "1",
        "--progress",
        "never",
    ];
    if let Some(auth) = &conn.auth_basic {
        import_args.push("--auth-basic");
        import_args.push(auth);
    }
    let import_cmd = ImportV2Command::parse_from(import_args);
    import_cmd
        .build()
        .await
        .context(OtherSnafu)?
        .do_work()
        .await
        .context(OtherSnafu)?;

    let actual_rows = query_count(&database_client, schema, "metrics").await?;
    assert_eq!(actual_rows, 2);
    let hosts = query_hosts(&database_client, schema).await?;
    assert_eq!(hosts, vec!["h1".to_string(), "h2".to_string()]);
    assert!(!import_state_path.exists());

    database_client
        .sql_in_public(&format!("DROP DATABASE IF EXISTS {schema}"))
        .await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn import_v2_fails_on_incomplete_snapshot_e2e() -> Result<()> {
    let addr = env::var("GREPTIME_ADDR").unwrap_or_else(|_| "127.0.0.1:4000".to_string());
    let catalog = env::var("GREPTIME_CATALOG").unwrap_or_else(|_| "greptime".to_string());
    let auth_basic = env::var("GREPTIME_AUTH_BASIC").ok();
    let schema = "test_db_incomplete_snapshot";

    let database_client = DatabaseClient::new(
        addr.clone(),
        catalog.clone(),
        auth_basic.clone(),
        Duration::from_secs(60),
        None,
        false,
    );

    database_client
        .sql_in_public(&format!("DROP DATABASE IF EXISTS {schema}"))
        .await?;
    database_client
        .sql_in_public(&format!("CREATE DATABASE {schema}"))
        .await?;
    database_client
        .sql(
            "CREATE TABLE metrics (\
                ts TIMESTAMP TIME INDEX, \
                host STRING PRIMARY KEY, \
                cpu DOUBLE \
            ) ENGINE=mito",
            schema,
        )
        .await?;
    database_client
        .sql(
            "INSERT INTO metrics (ts, host, cpu) VALUES ('2025-01-01T00:00:00Z', 'h1', 1.0)",
            schema,
        )
        .await?;

    let src_dir = tempdir().context(FileIoSnafu)?;
    let src_uri = path_to_uri(src_dir.path())?;

    let mut export_args = vec![
        "export-v2-create",
        "--addr",
        &addr,
        "--to",
        &src_uri,
        "--catalog",
        &catalog,
        "--schemas",
        schema,
    ];
    if let Some(auth) = &auth_basic {
        export_args.push("--auth-basic");
        export_args.push(auth);
    }
    let export_cmd = ExportCreateCommand::parse_from(export_args);
    export_cmd
        .build()
        .await
        .context(OtherSnafu)?
        .do_work()
        .await
        .context(OtherSnafu)?;

    let storage_config = ObjectStoreConfig::default();
    let storage = OpenDalStorage::from_uri(&src_uri, &storage_config)
        .map_err(BoxedError::new)
        .context(OtherSnafu)?;
    let mut manifest = storage
        .read_manifest()
        .await
        .map_err(BoxedError::new)
        .context(OtherSnafu)?;
    if let Some(first_chunk) = manifest.chunks.first_mut() {
        first_chunk.status = ChunkStatus::Failed;
    }
    storage
        .write_manifest(&manifest)
        .await
        .map_err(BoxedError::new)
        .context(OtherSnafu)?;

    database_client
        .sql_in_public(&format!("DROP DATABASE IF EXISTS {schema}"))
        .await?;

    let mut import_args = vec![
        "import-v2",
        "--addr",
        &addr,
        "--from",
        &src_uri,
        "--catalog",
        &catalog,
        "--schemas",
        schema,
    ];
    if let Some(auth) = &auth_basic {
        import_args.push("--auth-basic");
        import_args.push(auth);
    }
    let import_cmd = ImportV2Command::parse_from(import_args);
    let err = import_cmd
        .build()
        .await
        .context(OtherSnafu)?
        .do_work()
        .await
        .expect_err("import should fail on incomplete snapshot");
    assert!(err.to_string().contains("Incomplete snapshot"));

    Ok(())
}

#[tokio::test]
#[ignore]
async fn import_v2_schema_filter_data_e2e() -> Result<()> {
    let addr = env::var("GREPTIME_ADDR").unwrap_or_else(|_| "127.0.0.1:4000".to_string());
    let catalog = env::var("GREPTIME_CATALOG").unwrap_or_else(|_| "greptime".to_string());
    let auth_basic = env::var("GREPTIME_AUTH_BASIC").ok();
    let schema_a = "test_db_filter_a";
    let schema_b = "test_db_filter_b";

    let database_client = DatabaseClient::new(
        addr.clone(),
        catalog.clone(),
        auth_basic.clone(),
        Duration::from_secs(60),
        None,
        false,
    );

    for schema in [schema_a, schema_b] {
        database_client
            .sql_in_public(&format!("DROP DATABASE IF EXISTS {schema}"))
            .await?;
        database_client
            .sql_in_public(&format!("CREATE DATABASE {schema}"))
            .await?;
        database_client
            .sql(
                "CREATE TABLE metrics (\
                    ts TIMESTAMP TIME INDEX, \
                    host STRING PRIMARY KEY, \
                    cpu DOUBLE \
                ) ENGINE=mito",
                schema,
            )
            .await?;
    }
    database_client
        .sql(
            "INSERT INTO metrics (ts, host, cpu) VALUES \
             ('2025-01-01T00:00:00Z', 'a1', 1.0), \
             ('2025-01-01T01:00:00Z', 'a2', 1.5)",
            schema_a,
        )
        .await?;
    database_client
        .sql(
            "INSERT INTO metrics (ts, host, cpu) VALUES \
             ('2025-01-01T00:00:00Z', 'b1', 2.0), \
             ('2025-01-01T01:00:00Z', 'b2', 2.5)",
            schema_b,
        )
        .await?;

    let expected_rows_a = query_count(&database_client, schema_a, "metrics").await?;
    assert_eq!(expected_rows_a, 2);
    let expected_rows_b = query_count(&database_client, schema_b, "metrics").await?;
    assert_eq!(expected_rows_b, 2);

    let src_dir = tempdir().context(FileIoSnafu)?;
    let src_uri = path_to_uri(src_dir.path())?;

    let mut export_args = vec![
        "export-v2-create",
        "--addr",
        &addr,
        "--to",
        &src_uri,
        "--catalog",
        &catalog,
        "--schemas",
        schema_a,
        "--schemas",
        schema_b,
        "--start-time",
        "2025-01-01T00:00:00Z",
        "--end-time",
        "2025-01-01T02:00:00Z",
        "--chunk-time-window",
        "1h",
        "--chunk-parallelism",
        "2",
        "--progress",
        "never",
    ];
    if let Some(auth) = &auth_basic {
        export_args.push("--auth-basic");
        export_args.push(auth);
    }
    let export_cmd = ExportCreateCommand::parse_from(export_args);
    export_cmd
        .build()
        .await
        .context(OtherSnafu)?
        .do_work()
        .await
        .context(OtherSnafu)?;

    let storage_config = ObjectStoreConfig::default();
    let storage = OpenDalStorage::from_uri(&src_uri, &storage_config)
        .map_err(BoxedError::new)
        .context(OtherSnafu)?;
    let manifest = storage
        .read_manifest()
        .await
        .map_err(BoxedError::new)
        .context(OtherSnafu)?;
    assert_eq!(manifest.chunks.len(), 2);
    for chunk in &manifest.chunks {
        assert_eq!(chunk.status, ChunkStatus::Completed);
        for schema in [schema_a, schema_b] {
            let prefix = data_dir_for_schema_chunk(schema, chunk.id);
            assert!(
                chunk.files.iter().any(|file| file.starts_with(&prefix)),
                "chunk {} should include exported data for {schema}",
                chunk.id
            );
        }
    }

    let verify_cmd = ExportVerifyCommand::parse_from(["export-v2-verify", "--snapshot", &src_uri]);
    verify_cmd
        .build()
        .await
        .context(OtherSnafu)?
        .do_work()
        .await
        .context(OtherSnafu)?;

    for schema in [schema_a, schema_b] {
        database_client
            .sql_in_public(&format!("DROP DATABASE IF EXISTS {schema}"))
            .await?;
    }

    let import_state_dir = tempdir().context(FileIoSnafu)?;
    let import_state_path = import_state_dir.path().join("import-state.json");
    let import_state_path = import_state_path.to_string_lossy().into_owned();
    let mut import_args = vec![
        "import-v2",
        "--addr",
        &addr,
        "--from",
        &src_uri,
        "--catalog",
        &catalog,
        "--schemas",
        schema_a,
        "--task-parallelism",
        "2",
        "--state-path",
        &import_state_path,
        "--progress",
        "never",
    ];
    if let Some(auth) = &auth_basic {
        import_args.push("--auth-basic");
        import_args.push(auth);
    }
    let import_cmd = ImportV2Command::parse_from(import_args);
    import_cmd
        .build()
        .await
        .context(OtherSnafu)?
        .do_work()
        .await
        .context(OtherSnafu)?;

    let actual_rows_a = query_count(&database_client, schema_a, "metrics").await?;
    assert_eq!(actual_rows_a, expected_rows_a);
    let hosts_a = query_hosts(&database_client, schema_a).await?;
    assert_eq!(hosts_a, vec!["a1".to_string(), "a2".to_string()]);

    assert!(
        !schema_exists(&database_client, schema_b).await?,
        "schema_b should not be imported"
    );

    for schema in [schema_a, schema_b] {
        database_client
            .sql_in_public(&format!("DROP DATABASE IF EXISTS {schema}"))
            .await?;
    }

    Ok(())
}

#[tokio::test]
#[ignore]
async fn export_import_v2_skipped_chunk_e2e() -> Result<()> {
    let addr = env::var("GREPTIME_ADDR").unwrap_or_else(|_| "127.0.0.1:4000".to_string());
    let catalog = env::var("GREPTIME_CATALOG").unwrap_or_else(|_| "greptime".to_string());
    let auth_basic = env::var("GREPTIME_AUTH_BASIC").ok();
    let schema = "test_db_skipped_chunk";

    let database_client = DatabaseClient::new(
        addr.clone(),
        catalog.clone(),
        auth_basic.clone(),
        Duration::from_secs(60),
        None,
        false,
    );

    database_client
        .sql_in_public(&format!("DROP DATABASE IF EXISTS {schema}"))
        .await?;
    database_client
        .sql_in_public(&format!("CREATE DATABASE {schema}"))
        .await?;
    database_client
        .sql(
            "CREATE TABLE metrics (\
                ts TIMESTAMP TIME INDEX, \
                host STRING PRIMARY KEY, \
                cpu DOUBLE \
            ) ENGINE=mito",
            schema,
        )
        .await?;
    database_client
        .sql(
            "INSERT INTO metrics (ts, host, cpu) VALUES \
             ('2025-01-01T00:00:00Z', 'h1', 1.0), \
             ('2025-01-01T01:00:00Z', 'h2', 2.0)",
            schema,
        )
        .await?;

    let src_dir = tempdir().context(FileIoSnafu)?;
    let src_uri = path_to_uri(src_dir.path())?;

    let mut export_args = vec![
        "export-v2-create",
        "--addr",
        &addr,
        "--to",
        &src_uri,
        "--catalog",
        &catalog,
        "--schemas",
        schema,
        "--start-time",
        "2025-01-01T00:00:00Z",
        "--end-time",
        "2025-01-01T02:00:00Z",
        "--chunk-time-window",
        "1h",
    ];
    if let Some(auth) = &auth_basic {
        export_args.push("--auth-basic");
        export_args.push(auth);
    }
    let export_cmd = ExportCreateCommand::parse_from(export_args);
    export_cmd
        .build()
        .await
        .context(OtherSnafu)?
        .do_work()
        .await
        .context(OtherSnafu)?;

    let storage_config = ObjectStoreConfig::default();
    let storage = OpenDalStorage::from_uri(&src_uri, &storage_config)
        .map_err(BoxedError::new)
        .context(OtherSnafu)?;
    let mut manifest = storage
        .read_manifest()
        .await
        .map_err(BoxedError::new)
        .context(OtherSnafu)?;
    assert_eq!(manifest.chunks.len(), 2);
    manifest.chunks[0].status = ChunkStatus::Skipped;
    manifest.chunks[0].files.clear();
    storage
        .write_manifest(&manifest)
        .await
        .map_err(BoxedError::new)
        .context(OtherSnafu)?;

    database_client
        .sql_in_public(&format!("DROP DATABASE IF EXISTS {schema}"))
        .await?;

    let mut import_args = vec![
        "import-v2",
        "--addr",
        &addr,
        "--from",
        &src_uri,
        "--catalog",
        &catalog,
        "--schemas",
        schema,
    ];
    if let Some(auth) = &auth_basic {
        import_args.push("--auth-basic");
        import_args.push(auth);
    }
    let import_cmd = ImportV2Command::parse_from(import_args);
    import_cmd
        .build()
        .await
        .context(OtherSnafu)?
        .do_work()
        .await
        .context(OtherSnafu)?;

    let actual_rows = query_count(&database_client, schema, "metrics").await?;
    assert_eq!(actual_rows, 1);

    database_client
        .sql_in_public(&format!("DROP DATABASE IF EXISTS {schema}"))
        .await?;

    Ok(())
}
