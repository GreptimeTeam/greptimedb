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

use std::cmp::Ordering;
use std::env;
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use common_error::ext::BoxedError;
use serde_json::{Value, json};
use snafu::ResultExt;
use tempfile::tempdir;
use url::Url;

use crate::common::ObjectStoreConfig;
use crate::data::export_v2::ExportCreateCommand;
use crate::data::export_v2::manifest::ChunkStatus;
use crate::data::import_v2::ImportV2Command;
use crate::data::snapshot_storage::{OpenDalStorage, SnapshotStorage};
use crate::database::DatabaseClient;
use crate::error::{FileIoSnafu, InvalidArgumentsSnafu, OtherSnafu, Result, SerdeJsonSnafu};

fn normalize_schema_json(value: &mut Value) {
    if let Some(schemas) = value.get_mut("schemas").and_then(Value::as_array_mut) {
        sort_array_by_keys(schemas, &["name"]);
    }
    if let Some(tables) = value.get_mut("tables").and_then(Value::as_array_mut) {
        for table in tables.iter_mut() {
            if let Some(obj) = table.as_object_mut() {
                obj.remove("table_id");
            }
        }
        sort_array_by_keys(tables, &["schema", "name"]);
    }
    if let Some(views) = value.get_mut("views").and_then(Value::as_array_mut) {
        for view in views.iter_mut() {
            if let Some(obj) = view.as_object_mut()
                && let Some(Value::String(definition)) = obj.get_mut("definition")
            {
                let normalized = normalize_view_definition(definition);
                *definition = normalized;
            }
        }
        sort_array_by_keys(views, &["schema", "name"]);
    }
}

fn sort_array_by_keys(array: &mut [Value], keys: &[&str]) {
    array.sort_by(|a, b| compare_value_keys(a, b, keys));
}

fn compare_value_keys(a: &Value, b: &Value, keys: &[&str]) -> Ordering {
    for key in keys {
        let a_key = a.get(*key).and_then(Value::as_str).unwrap_or("");
        let b_key = b.get(*key).and_then(Value::as_str).unwrap_or("");
        let ordering = a_key.cmp(b_key);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

fn normalize_view_definition(definition: &str) -> String {
    if let Some(query) = extract_view_query(definition) {
        query.to_string()
    } else {
        definition.trim().to_string()
    }
}

fn extract_view_query(definition: &str) -> Option<&str> {
    let trimmed = definition.trim_start();
    let tokens = tokenize(trimmed);
    if tokens.is_empty() {
        return None;
    }
    if !token_eq(trimmed, tokens[0], "CREATE") {
        return None;
    }

    let mut saw_view = false;
    for (start, end) in tokens.iter().skip(1) {
        if !saw_view {
            if token_eq(trimmed, (*start, *end), "VIEW") {
                saw_view = true;
            }
            continue;
        }

        if token_eq(trimmed, (*start, *end), "AS") {
            let rest = &trimmed[*end..];
            return Some(rest.trim_start());
        }
    }

    None
}

fn tokenize(input: &str) -> Vec<(usize, usize)> {
    let mut tokens = Vec::new();
    let mut start = None;
    for (i, ch) in input.char_indices() {
        if ch.is_whitespace() {
            if let Some(s) = start {
                tokens.push((s, i));
                start = None;
            }
        } else if start.is_none() {
            start = Some(i);
        }
    }
    if let Some(s) = start {
        tokens.push((s, input.len()));
    }
    tokens
}

fn token_eq(input: &str, token: (usize, usize), keyword: &str) -> bool {
    input[token.0..token.1].eq_ignore_ascii_case(keyword)
}

async fn query_count(database_client: &DatabaseClient, schema: &str, table: &str) -> Result<u64> {
    let sql = format!("SELECT COUNT(*) FROM {table}");
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

// Scenario:
// - Export/import schema-only snapshot and compare normalized schema JSON parity.
// Assert:
// - Exported schema snapshot before and after import is identical.
// Run:
// - cargo test -p cli export_import_v2_schema_parity_e2e -- --ignored --nocapture
#[tokio::test]
#[ignore]
async fn export_import_v2_schema_parity_e2e() -> Result<()> {
    let addr = env::var("GREPTIME_ADDR").unwrap_or_else(|_| "127.0.0.1:4000".to_string());
    let catalog = env::var("GREPTIME_CATALOG").unwrap_or_else(|_| "greptime".to_string());
    let auth_basic = env::var("GREPTIME_AUTH_BASIC").ok();

    let database_client = DatabaseClient::new(
        addr.clone(),
        catalog.clone(),
        auth_basic.clone(),
        Duration::from_secs(60),
        None,
        false,
    );
    let schema = "test_db_m1_schema_parity";

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
    let mut src_schema = serde_json::to_value(src_schema_snapshot).context(SerdeJsonSnafu)?;
    let mut dst_schema = serde_json::to_value(dst_schema_snapshot).context(SerdeJsonSnafu)?;

    normalize_schema_json(&mut src_schema);
    normalize_schema_json(&mut dst_schema);

    assert_eq!(src_schema, dst_schema);

    database_client
        .sql_in_public(&format!("DROP DATABASE IF EXISTS {schema}"))
        .await?;

    Ok(())
}

// Scenario:
// - Export schema-only snapshot with DDL files and run import-v2 in dry-run + use-ddl mode.
// Assert:
// - Dry-run path executes successfully with DDL parsing.
// Run:
// - cargo test -p cli import_v2_use_ddl_dry_run_e2e -- --ignored --nocapture
#[tokio::test]
#[ignore]
async fn import_v2_use_ddl_dry_run_e2e() -> Result<()> {
    let addr = env::var("GREPTIME_ADDR").unwrap_or_else(|_| "127.0.0.1:4000".to_string());
    let catalog = env::var("GREPTIME_CATALOG").unwrap_or_else(|_| "greptime".to_string());
    let auth_basic = env::var("GREPTIME_AUTH_BASIC").ok();

    let database_client = DatabaseClient::new(
        addr.clone(),
        catalog.clone(),
        auth_basic.clone(),
        Duration::from_secs(60),
        None,
        false,
    );

    database_client
        .sql_in_public("DROP DATABASE IF EXISTS test_db")
        .await?;
    database_client
        .sql_in_public("CREATE DATABASE test_db")
        .await?;
    database_client
        .sql(
            "CREATE TABLE metrics (\
                ts TIMESTAMP TIME INDEX, \
                host STRING PRIMARY KEY, \
                cpu DOUBLE DEFAULT 0.0, \
                region_name STRING \
            ) ENGINE = mito WITH (ttl='7d', 'compaction.type'='twcs')",
            "test_db",
        )
        .await?;
    database_client
        .sql(
            "CREATE TABLE logs (\
                ts TIMESTAMP TIME INDEX, \
                app STRING PRIMARY KEY, \
                msg STRING NOT NULL COMMENT 'log message' \
            ) ENGINE = mito",
            "test_db",
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
        "test_db",
        "--schema-only",
        "--include-ddl",
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
        "test_db",
        "--use-ddl",
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
        .sql_in_public("DROP DATABASE IF EXISTS test_db")
        .await?;

    Ok(())
}

// Scenario:
// - Export non-schema-only snapshot and import it back.
// Assert:
// - Row count of imported table matches exported source table.
// Run:
// - cargo test -p cli export_import_v2_data_roundtrip_e2e -- --ignored --nocapture
#[tokio::test]
#[ignore]
async fn export_import_v2_data_roundtrip_e2e() -> Result<()> {
    let addr = env::var("GREPTIME_ADDR").unwrap_or_else(|_| "127.0.0.1:4000".to_string());
    let catalog = env::var("GREPTIME_CATALOG").unwrap_or_else(|_| "greptime".to_string());
    let auth_basic = env::var("GREPTIME_AUTH_BASIC").ok();

    let database_client = DatabaseClient::new(
        addr.clone(),
        catalog.clone(),
        auth_basic.clone(),
        Duration::from_secs(60),
        None,
        false,
    );

    let schema = "test_db_m3";
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

// Scenario:
// - Corrupt manifest by setting one chunk status to Failed before import.
// Assert:
// - Import fails with incomplete snapshot error.
// Run:
// - cargo test -p cli import_v2_fails_on_incomplete_snapshot_e2e -- --ignored --nocapture
#[tokio::test]
#[ignore]
async fn import_v2_fails_on_incomplete_snapshot_e2e() -> Result<()> {
    let addr = env::var("GREPTIME_ADDR").unwrap_or_else(|_| "127.0.0.1:4000".to_string());
    let catalog = env::var("GREPTIME_CATALOG").unwrap_or_else(|_| "greptime".to_string());
    let auth_basic = env::var("GREPTIME_AUTH_BASIC").ok();

    let database_client = DatabaseClient::new(
        addr.clone(),
        catalog.clone(),
        auth_basic.clone(),
        Duration::from_secs(60),
        None,
        false,
    );

    let schema = "test_db_m3_fail";
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
    assert!(
        err.to_string().contains("Incomplete snapshot"),
        "unexpected error: {err}"
    );

    Ok(())
}

// Scenario:
// - Export two schemas but import with --schemas filter for only one schema.
// Assert:
// - Selected schema is imported; unselected schema is not imported.
// Run:
// - cargo test -p cli import_v2_schema_filter_data_e2e -- --ignored --nocapture
#[tokio::test]
#[ignore]
async fn import_v2_schema_filter_data_e2e() -> Result<()> {
    let addr = env::var("GREPTIME_ADDR").unwrap_or_else(|_| "127.0.0.1:4000".to_string());
    let catalog = env::var("GREPTIME_CATALOG").unwrap_or_else(|_| "greptime".to_string());
    let auth_basic = env::var("GREPTIME_AUTH_BASIC").ok();

    let database_client = DatabaseClient::new(
        addr.clone(),
        catalog.clone(),
        auth_basic.clone(),
        Duration::from_secs(60),
        None,
        false,
    );

    let schema_a = "test_db_m3_filter_a";
    let schema_b = "test_db_m3_filter_b";
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

// Scenario: Export with chunk parallelism and rerun to verify resume uses existing manifest.
// Assert: Manifest chunks are completed or skipped after resume run.
// Run: cargo test -p cli export_v2_chunk_parallelism_resume_e2e -- --ignored --nocapture
#[tokio::test]
#[ignore]
async fn export_v2_chunk_parallelism_resume_e2e() -> Result<()> {
    let addr = env::var("GREPTIME_ADDR").unwrap_or_else(|_| "127.0.0.1:4000".to_string());
    let catalog = env::var("GREPTIME_CATALOG").unwrap_or_else(|_| "greptime".to_string());
    let auth_basic = env::var("GREPTIME_AUTH_BASIC").ok();

    let database_client = DatabaseClient::new(
        addr.clone(),
        catalog.clone(),
        auth_basic.clone(),
        Duration::from_secs(60),
        None,
        false,
    );

    let schema = "test_db_m4_export_parallel";
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
            ('2025-01-02T00:00:00Z', 'h2', 2.0)",
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
        "2025-01-03T00:00:00Z",
        "--chunk-time-window",
        "1d",
        "--worker-parallelism",
        "2",
        "--chunk-parallelism",
        "2",
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

    let mut resume_args = vec![
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
        "2025-01-03T00:00:00Z",
        "--chunk-time-window",
        "1d",
    ];
    if let Some(auth) = &auth_basic {
        resume_args.push("--auth-basic");
        resume_args.push(auth);
    }
    let export_cmd = ExportCreateCommand::parse_from(resume_args);
    export_cmd
        .build()
        .await
        .context(OtherSnafu)?
        .do_work()
        .await
        .context(OtherSnafu)?;

    let storage = OpenDalStorage::from_uri(&src_uri, &ObjectStoreConfig::default())
        .map_err(BoxedError::new)
        .context(OtherSnafu)?;
    let manifest = storage
        .read_manifest()
        .await
        .map_err(BoxedError::new)
        .context(OtherSnafu)?;
    assert!(
        manifest
            .chunks
            .iter()
            .all(|c| matches!(c.status, ChunkStatus::Completed | ChunkStatus::Skipped)),
        "manifest should be completed after resume"
    );

    database_client
        .sql_in_public(&format!("DROP DATABASE IF EXISTS {schema}"))
        .await?;

    Ok(())
}

// Scenario: Pre-create import state to resume from a partially imported snapshot.
// Assert: Import skips completed chunk and final row count matches full dataset.
// Run: cargo test -p cli import_v2_resume_state_e2e -- --ignored --nocapture
#[tokio::test]
#[ignore]
async fn import_v2_resume_state_e2e() -> Result<()> {
    let addr = env::var("GREPTIME_ADDR").unwrap_or_else(|_| "127.0.0.1:4000".to_string());
    let catalog = env::var("GREPTIME_CATALOG").unwrap_or_else(|_| "greptime".to_string());
    let auth_basic = env::var("GREPTIME_AUTH_BASIC").ok();

    let database_client = DatabaseClient::new(
        addr.clone(),
        catalog.clone(),
        auth_basic.clone(),
        Duration::from_secs(60),
        None,
        false,
    );

    let schema = "test_db_m4_import_resume";
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
            ('2025-01-02T00:00:00Z', 'h2', 2.0)",
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
        "2025-01-03T00:00:00Z",
        "--chunk-time-window",
        "1d",
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

    let storage = OpenDalStorage::from_uri(&src_uri, &ObjectStoreConfig::default())
        .map_err(BoxedError::new)
        .context(OtherSnafu)?;
    let manifest = storage
        .read_manifest()
        .await
        .map_err(BoxedError::new)
        .context(OtherSnafu)?;
    assert!(
        manifest.chunks.len() >= 2,
        "expected at least 2 chunks, got {}",
        manifest.chunks.len()
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
            ('2025-01-01T00:00:00Z', 'h1', 1.0)",
            schema,
        )
        .await?;

    let original_home = env::var("HOME").ok();
    let home_dir = tempdir().context(FileIoSnafu)?;
    unsafe {
        env::set_var("HOME", home_dir.path());
    }

    let state_dir = PathBuf::from(home_dir.path())
        .join(".greptime")
        .join("import_state");
    std::fs::create_dir_all(&state_dir).context(FileIoSnafu)?;
    let state_path = state_dir.join(format!("{}.json", manifest.snapshot_id));

    let state_json = json!({
        "snapshot_id": manifest.snapshot_id.to_string(),
        "target_addr": addr,
        "updated_at": "2026-02-05T00:00:00Z",
        "chunks": [
            { "id": manifest.chunks[0].id, "status": "completed" },
            { "id": manifest.chunks[1].id, "status": "pending" }
        ]
    });
    let state_bytes = serde_json::to_vec_pretty(&state_json).context(SerdeJsonSnafu)?;
    std::fs::write(&state_path, state_bytes).context(FileIoSnafu)?;

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
        "--chunk-parallelism",
        "2",
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

    let total_rows = query_count(&database_client, schema, "metrics").await?;
    assert_eq!(total_rows, 2);

    if let Some(home) = original_home {
        unsafe {
            env::set_var("HOME", home);
        }
    }

    database_client
        .sql_in_public(&format!("DROP DATABASE IF EXISTS {schema}"))
        .await?;

    Ok(())
}

// Scenario: Import with a transient failure injected via env var to trigger retry logging.
// Assert: Import succeeds and injected failure is observed.
// Run: cargo test -p cli import_v2_retry_logging_e2e -- --ignored --nocapture
#[tokio::test]
#[ignore]
async fn import_v2_retry_logging_e2e() -> Result<()> {
    let addr = env::var("GREPTIME_ADDR").unwrap_or_else(|_| "127.0.0.1:4000".to_string());
    let catalog = env::var("GREPTIME_CATALOG").unwrap_or_else(|_| "greptime".to_string());
    let auth_basic = env::var("GREPTIME_AUTH_BASIC").ok();

    let database_client = DatabaseClient::new(
        addr.clone(),
        catalog.clone(),
        auth_basic.clone(),
        Duration::from_secs(60),
        None,
        false,
    );

    let schema = "test_db_m4_retry_log";
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
            ('2025-01-01T00:00:00Z', 'h1', 1.0)",
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
        "2025-01-02T00:00:00Z",
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

    let original_fail_once = env::var("GREPTIME_TEST_IMPORT_FAIL_ONCE").ok();
    let original_fail_triggered = env::var("GREPTIME_TEST_IMPORT_FAIL_TRIGGERED").ok();
    unsafe {
        env::set_var("GREPTIME_TEST_IMPORT_FAIL_ONCE", "1");
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
        schema,
        "--max-retries",
        "2",
        "--retry-backoff",
        "1s",
    ];
    if let Some(auth) = &auth_basic {
        import_args.push("--auth-basic");
        import_args.push(auth);
    }

    let import_cmd = ImportV2Command::parse_from(import_args);
    let import_result = import_cmd
        .build()
        .await
        .context(OtherSnafu)?
        .do_work()
        .await
        .context(OtherSnafu);

    if let Some(value) = original_fail_once {
        unsafe {
            env::set_var("GREPTIME_TEST_IMPORT_FAIL_ONCE", value);
        }
    } else {
        unsafe {
            env::remove_var("GREPTIME_TEST_IMPORT_FAIL_ONCE");
        }
    }
    import_result?;

    let triggered = env::var("GREPTIME_TEST_IMPORT_FAIL_TRIGGERED")
        .ok()
        .unwrap_or_default();
    assert_eq!(triggered, "1", "retry injection did not trigger");
    if let Some(value) = original_fail_triggered {
        unsafe {
            env::set_var("GREPTIME_TEST_IMPORT_FAIL_TRIGGERED", value);
        }
    } else {
        unsafe {
            env::remove_var("GREPTIME_TEST_IMPORT_FAIL_TRIGGERED");
        }
    }

    let _rows = query_count(&database_client, schema, "metrics").await?;

    database_client
        .sql_in_public(&format!("DROP DATABASE IF EXISTS {schema}"))
        .await?;

    Ok(())
}

// Scenario: Resume export with mismatched config should fail.
// Assert: Command returns error when time range differs from existing snapshot.
// Run: cargo test -p cli export_v2_resume_config_mismatch_e2e -- --ignored --nocapture
#[tokio::test]
#[ignore]
async fn export_v2_resume_config_mismatch_e2e() -> Result<()> {
    let addr = env::var("GREPTIME_ADDR").unwrap_or_else(|_| "127.0.0.1:4000".to_string());
    let catalog = env::var("GREPTIME_CATALOG").unwrap_or_else(|_| "greptime".to_string());
    let auth_basic = env::var("GREPTIME_AUTH_BASIC").ok();

    let database_client = DatabaseClient::new(
        addr.clone(),
        catalog.clone(),
        auth_basic.clone(),
        Duration::from_secs(60),
        None,
        false,
    );

    let schema = "test_db_m4_resume_mismatch";
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
            ('2025-01-01T00:00:00Z', 'h1', 1.0)",
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
        "2025-01-02T00:00:00Z",
        "--chunk-time-window",
        "1d",
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

    let mismatch_cmd = ExportCreateCommand::parse_from(vec![
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
        "2025-01-03T00:00:00Z",
        "--chunk-time-window",
        "1d",
    ]);
    let result = mismatch_cmd
        .build()
        .await
        .context(OtherSnafu)?
        .do_work()
        .await;
    assert!(result.is_err(), "resume should fail on config mismatch");

    database_client
        .sql_in_public(&format!("DROP DATABASE IF EXISTS {schema}"))
        .await?;

    Ok(())
}

// Scenario: Resume export with manifest version mismatch should fail.
// Assert: Command returns error when manifest version is bumped.
// Run: cargo test -p cli export_v2_resume_version_mismatch_e2e -- --ignored --nocapture
#[tokio::test]
#[ignore]
async fn export_v2_resume_version_mismatch_e2e() -> Result<()> {
    let addr = env::var("GREPTIME_ADDR").unwrap_or_else(|_| "127.0.0.1:4000".to_string());
    let catalog = env::var("GREPTIME_CATALOG").unwrap_or_else(|_| "greptime".to_string());
    let auth_basic = env::var("GREPTIME_AUTH_BASIC").ok();

    let database_client = DatabaseClient::new(
        addr.clone(),
        catalog.clone(),
        auth_basic.clone(),
        Duration::from_secs(60),
        None,
        false,
    );

    let schema = "test_db_m4_resume_version";
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
            ('2025-01-01T00:00:00Z', 'h1', 1.0)",
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
        "2025-01-02T00:00:00Z",
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

    let storage = OpenDalStorage::from_uri(&src_uri, &ObjectStoreConfig::default())
        .map_err(BoxedError::new)
        .context(OtherSnafu)?;
    let mut manifest = storage
        .read_manifest()
        .await
        .map_err(BoxedError::new)
        .context(OtherSnafu)?;
    manifest.version += 1;
    storage
        .write_manifest(&manifest)
        .await
        .map_err(BoxedError::new)
        .context(OtherSnafu)?;

    let resume_cmd = ExportCreateCommand::parse_from(vec![
        "export-v2-create",
        "--addr",
        &addr,
        "--to",
        &src_uri,
        "--catalog",
        &catalog,
        "--schemas",
        schema,
    ]);
    let result = resume_cmd
        .build()
        .await
        .context(OtherSnafu)?
        .do_work()
        .await;
    assert!(result.is_err(), "resume should fail on version mismatch");

    database_client
        .sql_in_public(&format!("DROP DATABASE IF EXISTS {schema}"))
        .await?;

    Ok(())
}

// Scenario: Import resume should fail when target_addr mismatches state.
// Assert: Command returns error containing State target mismatch.
// Run: cargo test -p cli import_v2_state_target_mismatch_e2e -- --ignored --nocapture
#[tokio::test]
#[ignore]
async fn import_v2_state_target_mismatch_e2e() -> Result<()> {
    let addr = env::var("GREPTIME_ADDR").unwrap_or_else(|_| "127.0.0.1:4000".to_string());
    let catalog = env::var("GREPTIME_CATALOG").unwrap_or_else(|_| "greptime".to_string());
    let auth_basic = env::var("GREPTIME_AUTH_BASIC").ok();

    let database_client = DatabaseClient::new(
        addr.clone(),
        catalog.clone(),
        auth_basic.clone(),
        Duration::from_secs(60),
        None,
        false,
    );

    let schema = "test_db_m4_target_mismatch";
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
            ('2025-01-01T00:00:00Z', 'h1', 1.0)",
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

    database_client
        .sql_in_public(&format!("DROP DATABASE IF EXISTS {schema}"))
        .await?;

    let original_home = env::var("HOME").ok();
    let home_dir = tempdir().context(FileIoSnafu)?;
    unsafe {
        env::set_var("HOME", home_dir.path());
    }

    let storage = OpenDalStorage::from_uri(&src_uri, &ObjectStoreConfig::default())
        .map_err(BoxedError::new)
        .context(OtherSnafu)?;
    let manifest = storage
        .read_manifest()
        .await
        .map_err(BoxedError::new)
        .context(OtherSnafu)?;

    let state_dir = PathBuf::from(home_dir.path())
        .join(".greptime")
        .join("import_state");
    std::fs::create_dir_all(&state_dir).context(FileIoSnafu)?;
    let state_path = state_dir.join(format!("{}.json", manifest.snapshot_id));

    let state_json = json!({
        "snapshot_id": manifest.snapshot_id.to_string(),
        "target_addr": "127.0.0.1:4001",
        "updated_at": "2026-02-05T00:00:00Z",
        "chunks": [
            { "id": manifest.chunks[0].id, "status": "pending" }
        ]
    });
    let state_bytes = serde_json::to_vec_pretty(&state_json).context(SerdeJsonSnafu)?;
    std::fs::write(&state_path, state_bytes).context(FileIoSnafu)?;

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
        .expect_err("import should fail on target mismatch");
    assert!(
        err.to_string().contains("State target mismatch"),
        "unexpected error: {err}"
    );

    if let Some(home) = original_home {
        unsafe {
            env::set_var("HOME", home);
        }
    }

    Ok(())
}

// =============================================================================
// Management commands (list / verify / delete) unit tests
// =============================================================================

use crate::data::export_v2::manifest::{
    ChunkMeta, DataFormat, MANIFEST_VERSION, Manifest, TimeRange,
};
use crate::data::export_v2::schema::{DDL_DIR, SCHEMA_DIR};

/// Helper: create a minimal complete manifest and write it to storage.
async fn write_test_manifest(storage: &dyn SnapshotStorage, manifest: &Manifest) {
    storage.write_manifest(manifest).await.unwrap();
    // Write minimal schema files so verify doesn't warn about missing ones.
    let schema_snapshot = crate::data::export_v2::schema::SchemaSnapshot {
        schemas: manifest
            .schemas
            .iter()
            .map(|s| crate::data::export_v2::schema::SchemaDefinition {
                catalog: manifest.catalog.clone(),
                name: s.clone(),
                options: Default::default(),
            })
            .collect(),
        tables: vec![],
        views: vec![],
    };
    storage.write_schema(&schema_snapshot).await.unwrap();
}

fn make_complete_manifest(schemas: Vec<String>) -> Manifest {
    Manifest {
        version: MANIFEST_VERSION,
        snapshot_id: uuid::Uuid::new_v4(),
        catalog: "greptime".to_string(),
        schemas,
        time_range: TimeRange::unbounded(),
        schema_only: false,
        format: DataFormat::Parquet,
        chunks: vec![
            {
                let mut c = ChunkMeta::new(1, TimeRange::unbounded());
                c.mark_completed(vec!["data/public/1/t1.parquet".to_string()]);
                c
            },
            {
                let mut c = ChunkMeta::new(2, TimeRange::unbounded());
                c.mark_skipped();
                c
            },
        ],
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    }
}

fn make_schema_only_manifest() -> Manifest {
    Manifest {
        version: MANIFEST_VERSION,
        snapshot_id: uuid::Uuid::new_v4(),
        catalog: "greptime".to_string(),
        schemas: vec!["public".to_string()],
        time_range: TimeRange::unbounded(),
        schema_only: true,
        format: DataFormat::Parquet,
        chunks: vec![],
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    }
}

// --- List tests ---

use crate::data::export_v2::list::ExportList;

#[tokio::test]
async fn test_list_finds_snapshots() {
    let parent_dir = tempdir().unwrap();

    // Create two snapshot subdirectories with valid manifests.
    for name in ["snap-a", "snap-b"] {
        let snap_path = parent_dir.path().join(name);
        std::fs::create_dir_all(&snap_path).unwrap();
        let uri = format!("file://{}", snap_path.display());
        let storage = OpenDalStorage::from_uri(&uri, &ObjectStoreConfig::default()).unwrap();
        let manifest = make_complete_manifest(vec!["public".to_string()]);
        write_test_manifest(&storage, &manifest).await;
    }

    // Create a non-snapshot directory (no manifest.json).
    std::fs::create_dir_all(parent_dir.path().join("not-a-snapshot")).unwrap();
    std::fs::write(
        parent_dir.path().join("not-a-snapshot").join("random.txt"),
        "hello",
    )
    .unwrap();

    let parent_uri = format!("file://{}", parent_dir.path().display());
    let list = ExportList::new(parent_uri, ObjectStoreConfig::default());
    let result = list.scan_snapshots().await.unwrap();

    assert_eq!(result.snapshots.len(), 2);
    assert!(result.unreadable.is_empty());
}

#[tokio::test]
async fn test_list_empty_location() {
    let parent_dir = tempdir().unwrap();

    let parent_uri = format!("file://{}", parent_dir.path().display());
    let list = ExportList::new(parent_uri, ObjectStoreConfig::default());
    let result = list.scan_snapshots().await.unwrap();

    assert_eq!(result.snapshots.len(), 0);
    assert!(result.unreadable.is_empty());
}

#[tokio::test]
async fn test_list_skips_corrupt_manifest() {
    let parent_dir = tempdir().unwrap();

    // Create a directory with corrupt manifest.json.
    let corrupt_path = parent_dir.path().join("corrupt-snap");
    std::fs::create_dir_all(&corrupt_path).unwrap();
    std::fs::write(corrupt_path.join("manifest.json"), "not valid json{{{").unwrap();

    // Create a valid snapshot.
    let valid_path = parent_dir.path().join("valid-snap");
    std::fs::create_dir_all(&valid_path).unwrap();
    let uri = format!("file://{}", valid_path.display());
    let storage = OpenDalStorage::from_uri(&uri, &ObjectStoreConfig::default()).unwrap();
    let manifest = make_complete_manifest(vec!["public".to_string()]);
    write_test_manifest(&storage, &manifest).await;

    let parent_uri = format!("file://{}", parent_dir.path().display());
    let list = ExportList::new(parent_uri, ObjectStoreConfig::default());
    let result = list.scan_snapshots().await.unwrap();

    assert_eq!(result.snapshots.len(), 1);
    assert_eq!(result.unreadable.len(), 1);
}

// --- Verify tests ---

#[tokio::test]
async fn test_verify_complete_snapshot() {
    let dir = tempdir().unwrap();
    let snap_path = dir.path().join("snapshots").join("prod");
    std::fs::create_dir_all(&snap_path).unwrap();

    let uri = format!("file://{}", snap_path.display());
    let storage = OpenDalStorage::from_uri(&uri, &ObjectStoreConfig::default()).unwrap();

    let manifest = make_complete_manifest(vec!["public".to_string()]);
    write_test_manifest(&storage, &manifest).await;

    // Write the data file that chunk 1 references.
    storage
        .write_text("data/public/1/t1.parquet", "fake parquet data")
        .await
        .unwrap();

    // Verify should pass (run via Tool trait).
    use crate::Tool;
    let verify_tool = crate::data::export_v2::verify::ExportVerify::new(Box::new(storage));
    let result = verify_tool.do_work().await;
    assert!(result.is_ok(), "Expected valid snapshot, got: {:?}", result);
}

#[tokio::test]
async fn test_verify_missing_data_file_is_error() {
    let dir = tempdir().unwrap();
    let snap_path = dir.path().join("snapshots").join("prod");
    std::fs::create_dir_all(&snap_path).unwrap();

    let uri = format!("file://{}", snap_path.display());
    let storage = OpenDalStorage::from_uri(&uri, &ObjectStoreConfig::default()).unwrap();

    let manifest = make_complete_manifest(vec!["public".to_string()]);
    write_test_manifest(&storage, &manifest).await;

    // Do NOT write the data file — chunk 1 references data/public/1/t1.parquet.

    use crate::Tool;
    let verify_tool = crate::data::export_v2::verify::ExportVerify::new(Box::new(storage));
    let result = verify_tool.do_work().await;
    assert!(
        result.is_err(),
        "Expected verification failure for missing data file"
    );
}

#[tokio::test]
async fn test_verify_failed_chunk_is_error() {
    let dir = tempdir().unwrap();
    let snap_path = dir.path().join("snapshots").join("prod");
    std::fs::create_dir_all(&snap_path).unwrap();

    let uri = format!("file://{}", snap_path.display());
    let storage = OpenDalStorage::from_uri(&uri, &ObjectStoreConfig::default()).unwrap();

    let mut manifest = make_complete_manifest(vec!["public".to_string()]);
    // Mark chunk 1 as failed.
    manifest.chunks[0].mark_failed("Connection timeout".to_string());
    write_test_manifest(&storage, &manifest).await;

    use crate::Tool;
    let verify_tool = crate::data::export_v2::verify::ExportVerify::new(Box::new(storage));
    let result = verify_tool.do_work().await;
    assert!(
        result.is_err(),
        "Expected verification failure for failed chunk"
    );
}

#[tokio::test]
async fn test_verify_completed_chunk_with_empty_files_is_error() {
    let dir = tempdir().unwrap();
    let snap_path = dir.path().join("snapshots").join("prod");
    std::fs::create_dir_all(&snap_path).unwrap();

    let uri = format!("file://{}", snap_path.display());
    let storage = OpenDalStorage::from_uri(&uri, &ObjectStoreConfig::default()).unwrap();

    let mut manifest = make_complete_manifest(vec!["public".to_string()]);
    manifest.chunks[0].mark_completed(vec![]);
    write_test_manifest(&storage, &manifest).await;

    use crate::Tool;
    let verify_tool = crate::data::export_v2::verify::ExportVerify::new(Box::new(storage));
    let result = verify_tool.do_work().await;
    assert!(
        result.is_err(),
        "Expected verification failure for completed chunk with empty files"
    );
}

#[tokio::test]
async fn test_verify_skipped_chunk_with_files_is_error() {
    let dir = tempdir().unwrap();
    let snap_path = dir.path().join("snapshots").join("prod");
    std::fs::create_dir_all(&snap_path).unwrap();

    let uri = format!("file://{}", snap_path.display());
    let storage = OpenDalStorage::from_uri(&uri, &ObjectStoreConfig::default()).unwrap();

    let mut manifest = make_complete_manifest(vec!["public".to_string()]);
    manifest.chunks[1].status = ChunkStatus::Skipped;
    manifest.chunks[1].files = vec!["data/public/2/t2.parquet".to_string()];
    write_test_manifest(&storage, &manifest).await;

    // Keep completed chunk file valid to isolate skipped/files consistency check.
    storage
        .write_text("data/public/1/t1.parquet", "fake parquet data")
        .await
        .unwrap();

    use crate::Tool;
    let verify_tool = crate::data::export_v2::verify::ExportVerify::new(Box::new(storage));
    let result = verify_tool.do_work().await;
    assert!(
        result.is_err(),
        "Expected verification failure for skipped chunk with files"
    );
}

#[tokio::test]
async fn test_verify_schema_only_snapshot() {
    let dir = tempdir().unwrap();
    let snap_path = dir.path().join("snapshots").join("prod");
    std::fs::create_dir_all(&snap_path).unwrap();

    let uri = format!("file://{}", snap_path.display());
    let storage = OpenDalStorage::from_uri(&uri, &ObjectStoreConfig::default()).unwrap();

    let manifest = make_schema_only_manifest();
    write_test_manifest(&storage, &manifest).await;

    use crate::Tool;
    let verify_tool = crate::data::export_v2::verify::ExportVerify::new(Box::new(storage));
    let result = verify_tool.do_work().await;
    assert!(result.is_ok(), "Schema-only snapshot should be valid");
}

#[tokio::test]
async fn test_verify_ddl_file_missing_is_error() {
    let dir = tempdir().unwrap();
    let snap_path = dir.path().join("snapshots").join("prod");
    std::fs::create_dir_all(&snap_path).unwrap();

    let uri = format!("file://{}", snap_path.display());
    let storage = OpenDalStorage::from_uri(&uri, &ObjectStoreConfig::default()).unwrap();

    let manifest = make_complete_manifest(vec!["public".to_string(), "metrics".to_string()]);
    write_test_manifest(&storage, &manifest).await;

    // Write data file for chunk 1.
    storage
        .write_text("data/public/1/t1.parquet", "fake data")
        .await
        .unwrap();

    // Write DDL for "public" but NOT for "metrics" — should trigger ERROR.
    let ddl_path = format!("{}/{}/public.sql", SCHEMA_DIR, DDL_DIR);
    storage
        .write_text(&ddl_path, "CREATE TABLE public.t1 ();")
        .await
        .unwrap();

    use crate::Tool;
    let verify_tool = crate::data::export_v2::verify::ExportVerify::new(Box::new(storage));
    let result = verify_tool.do_work().await;
    assert!(
        result.is_err(),
        "Expected error for missing DDL file for 'metrics'"
    );
}

// --- Delete tests ---

#[tokio::test]
async fn test_delete_with_yes_flag() {
    let dir = tempdir().unwrap();
    let snap_path = dir.path().join("snapshots").join("prod");
    std::fs::create_dir_all(&snap_path).unwrap();

    let uri = format!("file://{}", snap_path.display());
    let storage = OpenDalStorage::from_uri(&uri, &ObjectStoreConfig::default()).unwrap();

    let manifest = make_complete_manifest(vec!["public".to_string()]);
    write_test_manifest(&storage, &manifest).await;
    storage
        .write_text("data/public/1/t1.parquet", "fake data")
        .await
        .unwrap();

    // Confirm snapshot exists.
    assert!(storage.exists().await.unwrap());

    use crate::Tool;
    let delete_tool =
        crate::data::export_v2::delete::ExportDelete::new(uri.clone(), Box::new(storage), true);
    let result = delete_tool.do_work().await;
    assert!(result.is_ok(), "Delete with --yes should succeed");

    // Confirm snapshot is gone.
    let storage2 = OpenDalStorage::from_uri(&uri, &ObjectStoreConfig::default()).unwrap();
    assert!(!storage2.exists().await.unwrap());
}

#[tokio::test]
async fn test_delete_rejects_nonexistent_snapshot() {
    let dir = tempdir().unwrap();
    let snap_path = dir.path().join("snapshots").join("prod");
    std::fs::create_dir_all(&snap_path).unwrap();

    let uri = format!("file://{}", snap_path.display());
    let storage = OpenDalStorage::from_uri(&uri, &ObjectStoreConfig::default()).unwrap();

    // Do NOT write manifest — snapshot doesn't exist.
    use crate::Tool;
    let delete_tool =
        crate::data::export_v2::delete::ExportDelete::new(uri.clone(), Box::new(storage), true);
    let result = delete_tool.do_work().await;
    let error = result
        .expect_err("Delete should fail when no manifest exists")
        .to_string();
    assert!(
        error.contains("Snapshot not found at"),
        "Expected snapshot-not-found error, got: {error}"
    );
    assert!(
        error.contains(&uri),
        "Expected error to contain target URI '{uri}', got: {error}"
    );
}

#[tokio::test]
async fn test_import_build_accepts_shallow_file_uri() {
    let cmd = ImportV2Command::parse_from(vec![
        "import-v2",
        "--addr",
        "127.0.0.1:4000",
        "--from",
        "file:///tmp",
    ]);

    let result = cmd.build().await;
    assert!(
        result.is_ok(),
        "Import build should accept shallow file URI"
    );
}

// Scenario: --clean-state removes existing state and exits without importing.
// Assert: State file is removed.
// Run: cargo test -p cli import_v2_clean_state_e2e -- --ignored --nocapture
#[tokio::test]
#[ignore]
async fn import_v2_clean_state_e2e() -> Result<()> {
    let addr = env::var("GREPTIME_ADDR").unwrap_or_else(|_| "127.0.0.1:4000".to_string());
    let catalog = env::var("GREPTIME_CATALOG").unwrap_or_else(|_| "greptime".to_string());
    let auth_basic = env::var("GREPTIME_AUTH_BASIC").ok();

    let database_client = DatabaseClient::new(
        addr.clone(),
        catalog.clone(),
        auth_basic.clone(),
        Duration::from_secs(60),
        None,
        false,
    );

    let schema = "test_db_m4_clean_state";
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
            ('2025-01-01T00:00:00Z', 'h1', 1.0)",
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

    let original_home = env::var("HOME").ok();
    let home_dir = tempdir().context(FileIoSnafu)?;
    unsafe {
        env::set_var("HOME", home_dir.path());
    }

    let storage = OpenDalStorage::from_uri(&src_uri, &ObjectStoreConfig::default())
        .map_err(BoxedError::new)
        .context(OtherSnafu)?;
    let manifest = storage
        .read_manifest()
        .await
        .map_err(BoxedError::new)
        .context(OtherSnafu)?;
    let state_dir = PathBuf::from(home_dir.path())
        .join(".greptime")
        .join("import_state");
    std::fs::create_dir_all(&state_dir).context(FileIoSnafu)?;
    let state_path = state_dir.join(format!("{}.json", manifest.snapshot_id));
    let state_json = json!({
        "snapshot_id": manifest.snapshot_id.to_string(),
        "target_addr": addr,
        "updated_at": "2026-02-05T00:00:00Z",
        "chunks": [
            { "id": manifest.chunks[0].id, "status": "pending" }
        ]
    });
    let state_bytes = serde_json::to_vec_pretty(&state_json).context(SerdeJsonSnafu)?;
    std::fs::write(&state_path, state_bytes).context(FileIoSnafu)?;

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
        "--clean-state",
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

    assert!(
        !state_path.exists(),
        "state file should be removed by --clean-state"
    );

    if let Some(home) = original_home {
        unsafe {
            env::set_var("HOME", home);
        }
    }

    Ok(())
}

// Scenario: --keep-state retains state after a successful import.
// Assert: State file still exists after import completes.
// Run: cargo test -p cli import_v2_keep_state_e2e -- --ignored --nocapture
#[tokio::test]
#[ignore]
async fn import_v2_keep_state_e2e() -> Result<()> {
    let addr = env::var("GREPTIME_ADDR").unwrap_or_else(|_| "127.0.0.1:4000".to_string());
    let catalog = env::var("GREPTIME_CATALOG").unwrap_or_else(|_| "greptime".to_string());
    let auth_basic = env::var("GREPTIME_AUTH_BASIC").ok();

    let database_client = DatabaseClient::new(
        addr.clone(),
        catalog.clone(),
        auth_basic.clone(),
        Duration::from_secs(60),
        None,
        false,
    );

    let schema = "test_db_m4_keep_state";
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
            ('2025-01-01T00:00:00Z', 'h1', 1.0)",
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

    database_client
        .sql_in_public(&format!("DROP DATABASE IF EXISTS {schema}"))
        .await?;

    let original_home = env::var("HOME").ok();
    let home_dir = tempdir().context(FileIoSnafu)?;
    unsafe {
        env::set_var("HOME", home_dir.path());
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
        schema,
        "--keep-state",
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

    let storage = OpenDalStorage::from_uri(&src_uri, &ObjectStoreConfig::default())
        .map_err(BoxedError::new)
        .context(OtherSnafu)?;
    let manifest = storage
        .read_manifest()
        .await
        .map_err(BoxedError::new)
        .context(OtherSnafu)?;
    let state_path = PathBuf::from(home_dir.path())
        .join(".greptime")
        .join("import_state")
        .join(format!("{}.json", manifest.snapshot_id));
    assert!(state_path.exists(), "state file should be kept");

    if let Some(home) = original_home {
        unsafe {
            env::set_var("HOME", home);
        }
    }

    Ok(())
}
