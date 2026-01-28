use std::cmp::Ordering;
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
    database_client
        .sql(
            "CREATE TABLE metrics_physical (\
                ts TIMESTAMP TIME INDEX, \
                host STRING, \
                region_name STRING, \
                cpu DOUBLE DEFAULT 0.0, \
                PRIMARY KEY (host, region_name) \
            ) ENGINE = metric WITH (physical_metric_table='true')",
            "test_db",
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
            "test_db",
        )
        .await?;
    database_client
        .sql(
            "CREATE VIEW metrics_view AS SELECT * FROM metrics WHERE cpu > 0.5",
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
        .sql_in_public("DROP DATABASE test_db")
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
        "test_db",
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
        "test_db",
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
        .sql_in_public("DROP DATABASE IF EXISTS test_db")
        .await?;

    Ok(())
}

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
