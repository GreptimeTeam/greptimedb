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

use super::command::ExportCreateCommand;
use crate::common::ObjectStoreConfig;
use crate::data::export_v2::manifest::ChunkStatus;
use crate::data::import_v2::ImportV2Command;
use crate::data::snapshot_storage::{OpenDalStorage, SnapshotStorage};
use crate::data::sql::escape_sql_identifier;
use crate::database::DatabaseClient;
use crate::error::{FileIoSnafu, InvalidArgumentsSnafu, OtherSnafu, Result};

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
        Value::String(s) => s.parse::<u64>().map_err(|_| {
            InvalidArgumentsSnafu {
                msg: format!("count is not numeric for query: {sql}, value: {s}"),
            }
            .build()
        }),
        _ => InvalidArgumentsSnafu {
            msg: format!("unexpected count type for query: {sql}"),
        }
        .fail(),
    }
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
    let src_uri = Url::from_directory_path(src_dir.path())
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
    let src_uri = Url::from_directory_path(src_dir.path())
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
    let src_uri = Url::from_directory_path(src_dir.path())
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
    assert_eq!(actual_rows, expected_rows);

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
    let src_uri = Url::from_directory_path(src_dir.path())
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
            "INSERT INTO metrics (ts, host, cpu) VALUES ('2025-01-01T00:00:00Z', 'a1', 1.0)",
            schema_a,
        )
        .await?;
    database_client
        .sql(
            "INSERT INTO metrics (ts, host, cpu) VALUES ('2025-01-01T00:00:00Z', 'b1', 2.0)",
            schema_b,
        )
        .await?;

    let expected_rows_a = query_count(&database_client, schema_a, "metrics").await?;

    let src_dir = tempdir().context(FileIoSnafu)?;
    let src_uri = Url::from_directory_path(src_dir.path())
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
        &src_uri,
        "--catalog",
        &catalog,
        "--schemas",
        schema_a,
        "--schemas",
        schema_b,
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

    for schema in [schema_a, schema_b] {
        database_client
            .sql_in_public(&format!("DROP DATABASE IF EXISTS {schema}"))
            .await?;
    }

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

    let schema_b_query = database_client
        .sql("SELECT COUNT(*) FROM metrics", schema_b)
        .await;
    assert!(schema_b_query.is_err(), "schema_b should not be imported");

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
    let src_uri = Url::from_directory_path(src_dir.path())
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
