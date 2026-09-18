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

use std::sync::Arc;

use common_query::OutputData;
use common_time::Timestamp;
use common_time::range::TimestampRange;
use frontend::instance::Instance;
use operator::statement::export_logical_tables::{LogicalTableExport, LogicalTableExportLimits};
use session::context::QueryContext;
use tests_integration::cluster::GreptimeDbClusterBuilder;
use tests_integration::standalone::GreptimeDbStandaloneBuilder;
use tests_integration::test_util::execute_sql as sql;
use tokio_util::sync::CancellationToken;

async fn table(instance: &Arc<Instance>, name: &str) -> table::TableRef {
    instance
        .catalog_manager()
        .table("greptime", "public", name, Some(&QueryContext::arc()))
        .await
        .unwrap()
        .unwrap()
}

async fn values(instance: &Arc<Instance>, query: &str) -> Vec<Vec<datatypes::value::Value>> {
    let output = sql(instance, query).await;
    let batches = match output.data {
        OutputData::Stream(stream) => common_recordbatch::util::collect_batches(stream)
            .await
            .unwrap(),
        OutputData::RecordBatches(batches) => batches,
        _ => panic!("expected query rows"),
    };
    batches
        .iter()
        .flat_map(|b| {
            let columns = datatypes::vectors::Helper::try_into_vectors(b.columns()).unwrap();
            (0..b.num_rows())
                .map(|row| columns.iter().map(|col| col.get(row)).collect::<Vec<_>>())
                .collect::<Vec<_>>()
        })
        .collect()
}

async fn create_metric_export_source_tables(
    instance: &Arc<Instance>,
    physical: &str,
    encoding: &str,
) -> ([String; 3], Vec<table::TableRef>, String) {
    sql(instance, &format!("CREATE TABLE {physical} (ts TIMESTAMP TIME INDEX, val DOUBLE, host STRING PRIMARY KEY) PARTITION ON COLUMNS (host) (host < 'm', host >= 'm') ENGINE=metric WITH (physical_metric_table='', primary_key_encoding='{encoding}')")).await;
    for (suffix, extra, key) in [
        ("cpu.v1", "zone_tag STRING,", ", zone_tag"),
        ("requests", "service_tag STRING,", ", service_tag"),
        ("empty", "", ""),
    ] {
        let name = format!("{physical}_{suffix}");
        sql(instance, &format!("CREATE TABLE \"{name}\" (host STRING, {extra} val DOUBLE, ts TIMESTAMP TIME INDEX, PRIMARY KEY(host{key})) ENGINE=metric WITH (on_physical_table='{physical}')")).await;
        if suffix != "empty" {
            sql(instance, &format!("INSERT INTO \"{name}\" (host,val,ts) VALUES ('a',1,1),('a',NULL,2),('z',3,3),('z',4,4)")).await;
        }
    }
    // This live but unselected table models rows outside the routing whitelist.
    sql(instance, &format!("CREATE TABLE {physical}_excluded (host STRING, huge_tag STRING, val DOUBLE, ts TIMESTAMP TIME INDEX, PRIMARY KEY(host, huge_tag)) ENGINE=metric WITH (on_physical_table='{physical}')")).await;
    sql(
        instance,
        &format!(
            "INSERT INTO {physical}_excluded (host, huge_tag, val, ts) VALUES ('z','ignore',9,2)"
        ),
    )
    .await;
    let names = ["cpu.v1", "requests", "empty"].map(|suffix| format!("{physical}_{suffix}"));
    let tables = vec![
        table(instance, &names[0]).await,
        table(instance, &names[1]).await,
        table(instance, &names[2]).await,
    ];
    let renamed = format!("renamed_{physical}");
    sql(
        instance,
        &format!("ALTER TABLE {physical} RENAME {renamed}"),
    )
    .await;
    (names, tables, renamed)
}

async fn roundtrip(instance: &Arc<Instance>) {
    let destination = tempfile::tempdir_in(common_test_util::find_workspace_path(".")).unwrap();
    for (physical, encoding) in [("phy", "dense"), ("other_phy", "sparse")] {
        let (names, tables, renamed) =
            create_metric_export_source_tables(instance, physical, encoding).await;
        let unit = LogicalTableExport::try_new(table(instance, &renamed).await, &tables).unwrap();
        let range =
            TimestampRange::new(Timestamp::new_millisecond(2), Timestamp::new_millisecond(4))
                .unwrap();
        sql(instance, &format!("CREATE TABLE target_{physical} (ts TIMESTAMP TIME INDEX, val DOUBLE, host STRING PRIMARY KEY) ENGINE=metric WITH (physical_metric_table='')")).await;
        let mut limits = LogicalTableExportLimits::default();
        limits.writer.row_group_rows = 1;
        for partitions in [1, 2, 4] {
            let directory = destination.path().join(format!("{physical}_{partitions}"));
            let mut ctx = QueryContext::with("greptime", "public");
            ctx.set_extension(
                query::datafusion::QUERY_PARALLELISM_HINT,
                partitions.to_string(),
            );
            let summary = instance
                .statement_executor()
                .export_logical_tables(
                    &unit,
                    directory.to_str().unwrap(),
                    &Default::default(),
                    Some(&range),
                    limits,
                    &CancellationToken::new(),
                    Arc::new(ctx),
                )
                .await
                .unwrap();
            assert_eq!(summary.rows, 4);
            assert_eq!(summary.files, 3);
            assert_eq!(summary.skipped_rows, 1);
            for (index, name) in names.iter().enumerate() {
                let restored = format!("restore_{physical}_{partitions}_{index}");
                let (extra, key) = [
                    ("zone_tag STRING,", ", zone_tag"),
                    ("service_tag STRING,", ", service_tag"),
                    ("", ""),
                ][index];
                sql(instance, &format!("CREATE TABLE {restored} (host STRING, {extra} val DOUBLE, ts TIMESTAMP TIME INDEX, PRIMARY KEY(host{key})) ENGINE=metric WITH (on_physical_table='target_{physical}')")).await;
                sql(
                    instance,
                    &format!(
                        "COPY {restored} FROM '{}/{}.parquet' WITH (FORMAT='parquet')",
                        directory.display(),
                        name
                    ),
                )
                .await;
                let expected = values(
                    instance,
                    &format!("SELECT * FROM \"{name}\" WHERE ts >= 2 AND ts < 4 ORDER BY host, ts"),
                )
                .await;
                let actual = values(
                    instance,
                    &format!("SELECT * FROM {restored} ORDER BY host, ts"),
                )
                .await;
                assert_eq!(actual, expected);
            }
        }
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let directory = destination.path().join(format!("cancel_{physical}"));
        let result = instance
            .statement_executor()
            .export_logical_tables(
                &unit,
                directory.to_str().unwrap(),
                &Default::default(),
                None,
                LogicalTableExportLimits::default(),
                &cancellation,
                QueryContext::arc(),
            )
            .await;
        assert!(matches!(
            result,
            Err(operator::error::Error::LogicalTableExportCancelled { .. })
        ));
        assert!(!directory.exists());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn physical_export_standalone_roundtrip() {
    common_telemetry::init_default_ut_logging();
    let standalone = GreptimeDbStandaloneBuilder::new("physical_export")
        .build()
        .await;
    roundtrip(standalone.fe_instance()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn physical_export_distributed_roundtrip() {
    common_telemetry::init_default_ut_logging();
    let cluster = GreptimeDbClusterBuilder::new("physical_export")
        .await
        .with_datanodes(2)
        .with_local_file_access(
            common_datasource::object_store::LocalFileAccess::sandboxed(
                common_test_util::find_workspace_path("."),
            )
            .unwrap(),
        )
        .build(false)
        .await;
    roundtrip(cluster.fe_instance()).await;
}

fn database_export_request(directory: &std::path::Path) -> table::requests::CopyDatabaseRequest {
    table::requests::CopyDatabaseRequest {
        catalog_name: "greptime".into(),
        schema_name: "public".into(),
        location: format!("{}/", directory.display()),
        with: [
            ("format".into(), "parquet".into()),
            ("parallelism".into(), "2".into()),
        ]
        .into(),
        connection: Default::default(),
        time_range: Some(
            TimestampRange::new(Timestamp::new_millisecond(2), Timestamp::new_millisecond(4))
                .unwrap(),
        ),
    }
}

async fn database_export_roundtrip(instance: &Arc<Instance>) {
    let destination = tempfile::tempdir_in(common_test_util::find_workspace_path(".")).unwrap();
    let (first_logical_table_names, _, renamed_physical_table) =
        create_metric_export_source_tables(instance, "db_a", "dense").await;
    let (second_logical_table_names, _, _) =
        create_metric_export_source_tables(instance, "db_b", "sparse").await;
    sql(
        instance,
        "CREATE TABLE audit (host STRING, val DOUBLE, ts TIMESTAMP TIME INDEX, PRIMARY KEY(host))",
    )
    .await;
    sql(
        instance,
        "INSERT INTO audit VALUES ('a',1,1),('z',NULL,2),('z',4,3)",
    )
    .await;
    sql(instance, "CREATE VIEW dashboard AS SELECT * FROM audit").await;
    let selected = vec![
        first_logical_table_names[0].clone(),
        first_logical_table_names[2].clone(),
        second_logical_table_names[1].clone(),
        "audit".into(),
    ];
    let mut names = selected.clone();
    names.extend([renamed_physical_table, "dashboard".into()]);
    let req = database_export_request(&destination.path().join("data"));
    let executor = instance.statement_executor();
    let captured = executor
        .capture_database_export_tables(&req, None, &QueryContext::arc())
        .await
        .unwrap();
    assert_eq!(captured.len(), 9);
    let captured = executor
        .capture_database_export_tables(&req, Some(&names), &QueryContext::arc())
        .await
        .unwrap();
    assert_eq!(captured.len(), 4);
    let plan = executor
        .prepare_database_export(req.clone(), captured)
        .await
        .unwrap();
    assert_eq!(plan.job_count_for_test(), 3);
    assert_eq!(
        std::fs::read_dir(destination.path().join("data"))
            .unwrap()
            .count(),
        0
    );
    let summary = instance
        .export_database_for_test(
            req,
            Some(&names),
            &CancellationToken::new(),
            QueryContext::arc(),
        )
        .await
        .unwrap();
    assert_eq!(summary.rows, 6);
    let expected = selected
        .iter()
        .map(|name| {
            destination
                .path()
                .join("data")
                .join(format!("{name}.parquet"))
                .to_str()
                .unwrap()
                .to_string()
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        summary
            .output_files
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>(),
        expected
    );
    assert_eq!(
        std::fs::read_dir(destination.path().join("data"))
            .unwrap()
            .count(),
        4
    );
    sql(instance, "CREATE TABLE restored_phy (ts TIMESTAMP TIME INDEX, val DOUBLE, host STRING PRIMARY KEY) ENGINE=metric WITH (physical_metric_table='')").await;
    for (index, name) in selected.iter().enumerate() {
        let restored = format!("restored_{index}");
        if index < 3 {
            let (extra, key) = [
                ("zone_tag STRING,", ", zone_tag"),
                ("", ""),
                ("service_tag STRING,", ", service_tag"),
            ][index];
            sql(instance, &format!("CREATE TABLE {restored} (host STRING, {extra} val DOUBLE, ts TIMESTAMP TIME INDEX, PRIMARY KEY(host{key})) ENGINE=metric WITH (on_physical_table='restored_phy')")).await;
        } else {
            sql(
                instance,
                &format!("CREATE TABLE {restored} LIKE \"{name}\""),
            )
            .await;
        }
        sql(
            instance,
            &format!(
                "COPY {restored} FROM '{}/data/{name}.parquet' WITH (FORMAT='parquet')",
                destination.path().display()
            ),
        )
        .await;
        assert_eq!(
            table(instance, name).await.schema().column_schemas(),
            table(instance, &restored).await.schema().column_schemas()
        );
        assert_eq!(
            values(
                instance,
                &format!("SELECT * FROM {restored} ORDER BY host, ts")
            )
            .await,
            values(
                instance,
                &format!("SELECT * FROM \"{name}\" WHERE ts >= 2 AND ts < 4 ORDER BY host, ts")
            )
            .await
        );
    }
    let ordinary_req = database_export_request(&destination.path().join("captured"));
    let captured = executor
        .capture_database_export_tables(
            &ordinary_req,
            Some(&["audit".into()]),
            &QueryContext::arc(),
        )
        .await
        .unwrap();
    let plan = executor
        .prepare_database_export(ordinary_req, captured)
        .await
        .unwrap();
    sql(instance, "ALTER TABLE audit RENAME original_audit").await;
    sql(instance, "CREATE TABLE audit LIKE original_audit").await;
    sql(instance, "INSERT INTO audit VALUES ('replacement',99,2)").await;
    let result = executor
        .export_database(plan, &CancellationToken::new(), QueryContext::arc())
        .await
        .unwrap();
    assert_eq!(result.rows, 2);
    sql(
        instance,
        "CREATE TABLE captured_restore LIKE original_audit",
    )
    .await;
    sql(
        instance,
        &format!(
            "COPY captured_restore FROM '{}/captured/audit.parquet' WITH (FORMAT='parquet')",
            destination.path().display()
        ),
    )
    .await;
    assert_eq!(
        values(instance, "SELECT * FROM captured_restore ORDER BY host,ts").await,
        values(
            instance,
            "SELECT * FROM original_audit WHERE ts >= 2 AND ts < 4 ORDER BY host,ts"
        )
        .await
    );
    for suffix in ["?attempt=/", "#attempt/"] {
        let path = destination.path().join("invalid_destination");
        let mut req = database_export_request(&path);
        req.location = format!("{}{suffix}", url::Url::from_file_path(&path).unwrap());
        req.with.insert("parallelism".into(), "1".into());
        let result = instance
            .export_database_for_test(
                req,
                Some(&["audit".into(), "original_audit".into()]),
                &CancellationToken::new(),
                QueryContext::arc(),
            )
            .await;
        assert!(matches!(
            result,
            Err(frontend::error::Error::TableOperation {
                source: operator::error::Error::InvalidCopyDatabasePath { .. },
                ..
            })
        ));
        assert!(!path.exists());
        assert!(
            tests_integration::test_util::try_execute_sql(
                instance,
                &format!(
                    "COPY DATABASE public TO '{}{suffix}' WITH (FORMAT='parquet')",
                    url::Url::from_file_path(&path).unwrap()
                )
            )
            .await
            .is_err()
        );
        assert!(!path.exists());
    }
    let token = CancellationToken::new();
    token.cancel();
    let req = database_export_request(&destination.path().join("cancelled"));
    let result = instance
        .export_database_for_test(req, Some(&names), &token, QueryContext::arc())
        .await;
    assert!(matches!(
        result,
        Err(frontend::error::Error::TableOperation {
            source: operator::error::Error::DatabaseExportCancelled { .. },
            ..
        })
    ));
    assert!(!destination.path().join("cancelled").exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn database_export_standalone_roundtrip() {
    let standalone = GreptimeDbStandaloneBuilder::new("database_export")
        .build()
        .await;
    database_export_roundtrip(standalone.fe_instance()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn database_export_distributed_roundtrip() {
    let cluster = GreptimeDbClusterBuilder::new("database_export")
        .await
        .with_datanodes(2)
        .with_local_file_access(
            common_datasource::object_store::LocalFileAccess::sandboxed(
                common_test_util::find_workspace_path("."),
            )
            .unwrap(),
        )
        .build(false)
        .await;
    database_export_roundtrip(cluster.fe_instance()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn database_export_rejects_invalid_members_before_output() {
    use common_meta::key::table_info::TableInfoKey;
    use common_meta::key::table_route::{TableRouteKey, TableRouteValue};
    use common_meta::key::{MetadataKey, MetadataValue};
    use common_meta::rpc::store::PutRequest;

    let standalone = GreptimeDbStandaloneBuilder::new("invalid_database_export")
        .build()
        .await;
    let instance = standalone.fe_instance();
    let (_, tables, physical) =
        create_metric_export_source_tables(instance, "invalid_phy", "dense").await;
    let executor = instance.statement_executor();
    let destination = tempfile::tempdir_in(common_test_util::find_workspace_path(".")).unwrap();
    for name in ["Foo", "foo"] {
        sql(
            instance,
            &format!("CREATE TABLE \"{name}\" (ts TIMESTAMP TIME INDEX)"),
        )
        .await;
    }
    let case_tables = vec![table(instance, "Foo").await, table(instance, "foo").await];
    assert_ne!(
        case_tables[0].table_info().table_id(),
        case_tables[1].table_info().table_id()
    );
    let case_path = destination.path().join("case_aliases");
    let mut case_req = database_export_request(&case_path);
    case_req.with.insert("parallelism".into(), "1".into());
    let result = instance
        .export_database_for_test(
            case_req.clone(),
            Some(&["Foo".into(), "foo".into()]),
            &CancellationToken::new(),
            QueryContext::arc(),
        )
        .await;
    assert!(matches!(result,
        Err(frontend::error::Error::TableOperation {
            source: operator::error::Error::InvalidDatabaseExport { reason }, ..
        }) if reason == "duplicate output name: foo"));
    assert!(!case_path.exists());
    case_req.location = "s3://export-bucket/data/".into();
    case_req.connection.extend([
        ("region".into(), "us-east-1".into()),
        ("access_key_id".into(), "test-key".into()),
        ("secret_access_key".into(), "test-secret".into()),
    ]);
    let plan = executor
        .prepare_database_export(case_req, case_tables)
        .await
        .unwrap();
    assert_eq!(plan.job_count_for_test(), 2);
    let req = database_export_request(&destination.path().join("data"));
    let key = TableRouteKey::new(tables[2].table_info().table_id()).to_bytes();
    let original = standalone
        .kv_backend
        .get(&key)
        .await
        .unwrap()
        .unwrap()
        .value;
    for route in [
        None,
        Some(TableRouteValue::physical(vec![])),
        Some(TableRouteValue::logical(u32::MAX)),
    ] {
        if let Some(route) = route {
            standalone
                .kv_backend
                .put(
                    PutRequest::new()
                        .with_key(key.clone())
                        .with_value(route.try_as_raw_value().unwrap()),
                )
                .await
                .unwrap();
        } else {
            standalone.kv_backend.delete(&key, false).await.unwrap();
        }
        let result = executor
            .prepare_database_export(req.clone(), tables.clone())
            .await;
        assert!(matches!(
            result,
            Err(operator::error::Error::InvalidDatabaseExport { .. })
        ));
        assert!(!destination.path().join("data").exists());
    }
    standalone
        .kv_backend
        .put(PutRequest::new().with_key(key.clone()).with_value(original))
        .await
        .unwrap();
    let plan = executor
        .prepare_database_export(req.clone(), tables.clone())
        .await
        .unwrap();
    // Preparation does not waive PR3's membership revalidation before a scan.
    standalone.kv_backend.delete(&key, false).await.unwrap();
    let result = executor
        .export_database(plan, &CancellationToken::new(), QueryContext::arc())
        .await;
    assert!(matches!(
        result,
        Err(operator::error::Error::InvalidLogicalTableExport { .. })
    ));
    assert_eq!(
        std::fs::read_dir(destination.path().join("data"))
            .unwrap()
            .count(),
        0
    );
    let physical_id = table(instance, &physical).await.table_info().table_id();
    let route = TableRouteValue::logical(physical_id)
        .try_as_raw_value()
        .unwrap();
    standalone
        .kv_backend
        .put(PutRequest::new().with_key(key).with_value(route))
        .await
        .unwrap();
    standalone
        .kv_backend
        .delete(&TableInfoKey::new(physical_id).to_bytes(), false)
        .await
        .unwrap();
    let result = executor
        .prepare_database_export(req.clone(), tables.clone())
        .await;
    assert!(matches!(
        result,
        Err(operator::error::Error::InvalidDatabaseExport { .. })
    ));
    let duplicate_name = tables[0].table_info().name.clone();
    let result = executor
        .prepare_database_export(req, vec![tables[0].clone(), tables[0].clone()])
        .await;
    assert!(matches!(result,
        Err(operator::error::Error::InvalidDatabaseExport { reason })
            if reason == format!("duplicate output name: {duplicate_name}")));
    assert_eq!(
        std::fs::read_dir(destination.path().join("data"))
            .unwrap()
            .count(),
        0
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn database_export_preserves_valid_table_names() {
    let standalone = GreptimeDbStandaloneBuilder::new("database_export_names")
        .build()
        .await;
    let instance = standalone.fe_instance();
    let destination = tempfile::tempdir_in(common_test_util::find_workspace_path(".")).unwrap();
    sql(instance, "CREATE TABLE names_physical (ts TIMESTAMP TIME INDEX, val DOUBLE, host STRING PRIMARY KEY) ENGINE=metric WITH (physical_metric_table='')").await;
    let mut names = vec!["ordinary#b".to_string(), "metric#b".to_string()];
    if !cfg!(windows) {
        names.extend(["ordinary:b".to_string(), "metric:b".to_string()]);
    }
    for name in &names {
        let engine = if name.starts_with("metric") {
            "ENGINE=metric WITH (on_physical_table='names_physical')"
        } else {
            ""
        };
        sql(instance, &format!("CREATE TABLE \"{name}\" (ts TIMESTAMP TIME INDEX, val DOUBLE, host STRING PRIMARY KEY) {engine}")).await;
        sql(
            instance,
            &format!("INSERT INTO \"{name}\" (ts,val,host) VALUES (1,42,'h')"),
        )
        .await;
    }
    sql(
        instance,
        "CREATE VIEW names_view AS SELECT * FROM \"ordinary#b\"",
    )
    .await;
    sql(instance, "CREATE DATABASE names_restored").await;
    for name in &names {
        sql(instance, &format!("CREATE TABLE names_restored.\"{name}\" (ts TIMESTAMP TIME INDEX, val DOUBLE, host STRING PRIMARY KEY)")).await;
    }
    for (attempt, file_url, legacy) in [
        ("plain", false, false),
        ("url", true, false),
        ("legacy", true, true),
    ] {
        let directory = destination.path().join(attempt);
        let mut req = database_export_request(&directory);
        req.time_range = None;
        if file_url {
            req.location = url::Url::from_directory_path(&directory)
                .unwrap()
                .to_string();
        }
        if legacy {
            let output = sql(
                instance,
                &format!(
                    "COPY DATABASE public TO '{}' WITH (FORMAT='parquet')",
                    req.location
                ),
            )
            .await;
            assert!(matches!(output.data, OutputData::AffectedRows(rows) if rows == names.len()));
        } else {
            let summary = instance
                .export_database_for_test(
                    req.clone(),
                    None,
                    &CancellationToken::new(),
                    QueryContext::arc(),
                )
                .await
                .unwrap();
            assert_eq!(summary.rows, names.len());
            let expected = names
                .iter()
                .map(|name| {
                    let path = directory.join(format!("{name}.parquet"));
                    if file_url {
                        url::Url::from_file_path(path).unwrap().to_string()
                    } else {
                        path.to_str().unwrap().to_string()
                    }
                })
                .collect::<std::collections::BTreeSet<_>>();
            assert_eq!(
                summary
                    .output_files
                    .into_iter()
                    .collect::<std::collections::BTreeSet<_>>(),
                expected
            );
        }
        for name in &names {
            let path = directory.join(format!("{name}.parquet"));
            assert!(path.is_file());
        }
        assert_eq!(std::fs::read_dir(&directory).unwrap().count(), names.len());
        let output = sql(
            instance,
            &format!(
                "COPY DATABASE names_restored FROM '{}' WITH (FORMAT='parquet')",
                req.location
            ),
        )
        .await;
        assert!(matches!(output.data, OutputData::AffectedRows(rows) if rows == names.len()));
        for name in &names {
            assert_eq!(
                values(
                    instance,
                    &format!("SELECT ts, val, host FROM names_restored.\"{name}\"")
                )
                .await,
                values(instance, &format!("SELECT ts, val, host FROM \"{name}\"")).await,
            );
            sql(
                instance,
                &format!("TRUNCATE TABLE names_restored.\"{name}\""),
            )
            .await;
        }
    }
}

#[derive(clap::Parser)]
struct ExportDataCli {
    #[command(subcommand)]
    command: cli::DataCommand,
}

async fn run_data_cli(args: &[&str]) -> Result<(), common_error::ext::BoxedError> {
    use clap::Parser;
    ExportDataCli::try_parse_from(std::iter::once("greptime-data").chain(args.iter().copied()))
        .unwrap()
        .command
        .build()
        .await?
        .do_work()
        .await
}

async fn export_http(instance: Arc<Instance>) -> (String, tokio::task::JoinHandle<()>) {
    let server = servers::http::HttpServerBuilder::new(servers::http::HttpOptions::default())
        .with_sql_handler(instance)
        .build();
    let app = server.build(server.make_app()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (addr, task)
}

struct FailSecondChunk {
    fail: std::sync::atomic::AtomicBool,
    copies: std::sync::atomic::AtomicUsize,
}

impl servers::interceptor::SqlQueryInterceptor for FailSecondChunk {
    type Error = frontend::error::Error;

    fn pre_execute(
        &self,
        statement: Option<&sql::statements::statement::Statement>,
        _plan: Option<&datafusion_expr::LogicalPlan>,
        _ctx: session::context::QueryContextRef,
    ) -> Result<(), Self::Error> {
        use sql::statements::copy::{Copy, CopyDatabase};
        use sql::statements::statement::Statement;
        if let Some(Statement::Copy(Copy::CopyDatabase(CopyDatabase::To(arg)))) = statement {
            self.copies
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if arg.location.ends_with("/z_later/2/")
                && self.fail.swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(operator::error::InvalidDatabaseExportSnafu {
                    reason: "injected terminated COPY failure",
                }
                .build()
                .into());
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn metric_export_v2_cli_resume_roundtrip() {
    metric_export_v2_cli_roundtrip(false).await;
}

#[tokio::test]
#[ignore = "requires GT_S3_* credentials and an S3-compatible test bucket"]
async fn metric_export_v2_cli_s3_resume_roundtrip() {
    metric_export_v2_cli_roundtrip(true).await;
}

async fn metric_export_v2_cli_roundtrip(s3: bool) {
    use servers::interceptor::SqlQueryInterceptorRef;
    let plugins = common_base::Plugins::new();
    let faults = Arc::new(FailSecondChunk {
        fail: std::sync::atomic::AtomicBool::new(true),
        copies: std::sync::atomic::AtomicUsize::new(0),
    });
    plugins.insert::<SqlQueryInterceptorRef<frontend::error::Error>>(faults.clone());
    let source = GreptimeDbStandaloneBuilder::new("metric_v2_source")
        .with_experimental_metric_export()
        .with_plugin(plugins)
        .build()
        .await;
    let instance = source.fe_instance();
    let mut names = Vec::new();
    for physical in ["v2_a", "v2_b"] {
        let (logical, _, renamed) =
            create_metric_export_source_tables(instance, physical, "dense").await;
        sql(
            instance,
            &format!("ALTER TABLE {renamed} RENAME {physical}"),
        )
        .await;
        names.extend(logical);
        names.push(format!("{physical}_excluded"));
    }
    sql(
        instance,
        "CREATE TABLE audit (host STRING PRIMARY KEY, val DOUBLE, ts TIMESTAMP TIME INDEX)",
    )
    .await;
    sql(instance, "INSERT INTO audit VALUES ('a',1,1),('z',NULL,3)").await;
    sql(instance, "CREATE VIEW dashboard AS SELECT * FROM audit").await;
    sql(instance, "CREATE DATABASE z_later").await;
    sql(
        instance,
        "CREATE TABLE z_later.events (ts TIMESTAMP TIME INDEX, val BIGINT)",
    )
    .await;
    sql(instance, "INSERT INTO z_later.events VALUES (1,10),(3,20)").await;
    names.push("audit".into());
    if s3 {
        sql(
            instance,
            "CREATE TABLE bulk (host STRING PRIMARY KEY, payload STRING, ts TIMESTAMP TIME INDEX)",
        )
        .await;
        for batch in 0..64 {
            let rows = (0..64)
                .map(|row| {
                    let payload = (0..128)
                        .map(|_| uuid::Uuid::new_v4().simple().to_string())
                        .collect::<String>();
                    format!("('{}','{payload}',1)", batch * 64 + row)
                })
                .collect::<Vec<_>>()
                .join(",");
            sql(instance, &format!("INSERT INTO bulk VALUES {rows}")).await;
        }
        names.push("bulk".into());
    }
    let (addr, server) = export_http(instance.clone()).await;
    let destination = tempfile::tempdir_in(common_test_util::find_workspace_path(".")).unwrap();
    let (uri, store, storage_args) = if s3 {
        let endpoint = std::env::var("GT_S3_ENDPOINT_URL").unwrap();
        let bucket = std::env::var("GT_S3_BUCKET").unwrap();
        let region = std::env::var("GT_S3_REGION").unwrap();
        let key = std::env::var("GT_S3_ACCESS_KEY_ID").unwrap();
        let secret = std::env::var("GT_S3_ACCESS_KEY").unwrap();
        let root = format!("pr04b-{}", uuid::Uuid::new_v4());
        let store = object_store::ObjectStore::new(
            object_store::services::S3::default()
                .endpoint(&endpoint)
                .bucket(&bucket)
                .root(&root)
                .region(&region)
                .access_key_id(&key)
                .secret_access_key(&secret),
        )
        .unwrap();
        (
            format!("s3://{bucket}/{root}"),
            store,
            vec![
                "--s3".into(),
                "--s3-endpoint".into(),
                endpoint,
                "--s3-region".into(),
                region,
                "--s3-access-key-id".into(),
                key,
                "--s3-secret-access-key".into(),
                secret,
            ],
        )
    } else {
        (
            format!("file://{}", destination.path().display()),
            object_store::ObjectStore::new(
                object_store::services::Fs::default().root(destination.path().to_str().unwrap()),
            )
            .unwrap(),
            Vec::<String>::new(),
        )
    };
    let mut args = vec![
        "export-v2",
        "create",
        "--addr",
        &addr,
        "--to",
        &uri,
        "--schemas",
        "public,z_later",
        "--experimental-metric-export",
        "--no-proxy",
        "--start-time",
        "1970-01-01T00:00:00Z",
        "--end-time",
        "1970-01-01T00:00:00.006Z",
        "--chunk-time-window",
        "3ms",
        "--progress",
        "never",
    ];
    args.extend(storage_args.iter().map(String::as_str));
    assert!(run_data_cli(&args).await.is_err());
    let before: cli::export_v2::manifest::Manifest =
        serde_json::from_slice(&store.read("manifest.json").await.unwrap().to_vec()).unwrap();
    assert_eq!(
        before.chunks[0].status,
        cli::export_v2::manifest::ChunkStatus::Completed
    );
    assert_eq!(
        before.chunks[1].status,
        cli::export_v2::manifest::ChunkStatus::Failed
    );
    if s3 {
        assert!(
            store
                .stat("data/public/1/bulk.parquet")
                .await
                .unwrap()
                .content_length()
                > 5 * 1024 * 1024
        );
    }
    let mut preserved = Vec::new();
    for path in &before.chunks[0].files {
        preserved.push((path.clone(), store.read(path).await.unwrap().to_vec()));
    }
    assert!(store.exists("data/public/2/audit.parquet").await.unwrap());
    store
        .write("data/public/2/unknown.txt", "keep")
        .await
        .unwrap();
    let copies = faults.copies.load(std::sync::atomic::Ordering::SeqCst);
    assert!(run_data_cli(&args).await.is_err());
    assert_eq!(
        faults.copies.load(std::sync::atomic::Ordering::SeqCst),
        copies
    );
    assert_eq!(
        store
            .read("data/public/2/unknown.txt")
            .await
            .unwrap()
            .to_vec(),
        b"keep"
    );
    store.delete("data/public/2/unknown.txt").await.unwrap();
    run_data_cli(&args).await.unwrap();
    let after: cli::export_v2::manifest::Manifest =
        serde_json::from_slice(&store.read("manifest.json").await.unwrap().to_vec()).unwrap();
    assert!(after.is_complete());
    assert_eq!(
        serde_json::to_value(&before.chunks[0]).unwrap(),
        serde_json::to_value(&after.chunks[0]).unwrap()
    );
    for (path, bytes) in preserved {
        assert_eq!(bytes, store.read(&path).await.unwrap().to_vec());
    }
    let mut verify = vec!["export-v2", "verify", "--snapshot", &uri];
    verify.extend(storage_args.iter().map(String::as_str));
    run_data_cli(&verify).await.unwrap();
    let target = GreptimeDbStandaloneBuilder::new("metric_v2_target")
        .build()
        .await;
    for id in 0..24 {
        sql(
            target.fe_instance(),
            &format!("CREATE TABLE occupied_{id} (ts TIMESTAMP TIME INDEX)"),
        )
        .await;
    }
    let (target_addr, target_server) = export_http(target.fe_instance().clone()).await;
    let state = destination.path().join("restore-state.json");
    let mut import = vec![
        "import-v2",
        "--addr",
        &target_addr,
        "--from",
        &uri,
        "--no-proxy",
        "--state-path",
        state.to_str().unwrap(),
        "--progress",
        "never",
    ];
    import.extend(storage_args.iter().map(String::as_str));
    run_data_cli(&import).await.unwrap();
    for name in names {
        let source_table = table(instance, &name).await;
        let target_table = table(target.fe_instance(), &name).await;
        assert_ne!(
            source_table.table_info().table_id(),
            target_table.table_info().table_id()
        );
        assert_eq!(
            source_table.schema().column_schemas(),
            target_table.schema().column_schemas()
        );
        let query = format!("SELECT * FROM \"{name}\" ORDER BY ts,host");
        assert_eq!(
            values(instance, &query).await,
            values(target.fe_instance(), &query).await
        );
    }
    assert_eq!(
        values(instance, "SELECT * FROM z_later.events ORDER BY ts").await,
        values(
            target.fe_instance(),
            "SELECT * FROM z_later.events ORDER BY ts"
        )
        .await
    );
    server.abort();
    target_server.abort();
    if s3 {
        store.delete_with("/").recursive(true).await.unwrap();
    }
}

#[tokio::test]
async fn metric_export_v2_disabled_and_legacy_cli() {
    let source = GreptimeDbStandaloneBuilder::new("metric_v2_disabled")
        .build()
        .await;
    let (addr, server) = export_http(source.fe_instance().clone()).await;
    let destination = tempfile::tempdir_in(common_test_util::find_workspace_path(".")).unwrap();
    let uri = format!("file://{}", destination.path().display());
    let manifest = destination.path().join("manifest.json");
    std::fs::write(&manifest, b"preserve-before-capability-check").unwrap();
    let args = [
        "export-v2",
        "create",
        "--addr",
        &addr,
        "--to",
        &uri,
        "--force",
        "--experimental-metric-export",
        "--no-proxy",
        "--progress",
        "never",
    ];
    assert!(run_data_cli(&args).await.is_err());
    assert_eq!(
        std::fs::read(&manifest).unwrap(),
        b"preserve-before-capability-check"
    );
    std::fs::remove_file(&manifest).unwrap();
    sql(
        source.fe_instance(),
        "CREATE TABLE ordinary (ts TIMESTAMP TIME INDEX, val BIGINT)",
    )
    .await;
    sql(source.fe_instance(), "INSERT INTO ordinary VALUES (1,2)").await;
    run_data_cli(&[
        "export-v2",
        "create",
        "--addr",
        &addr,
        "--to",
        &uri,
        "--schemas",
        "public",
        "--no-proxy",
        "--progress",
        "never",
    ])
    .await
    .unwrap();
    run_data_cli(&["export-v2", "verify", "--snapshot", &uri])
        .await
        .unwrap();
    server.abort();
}

#[tokio::test]
async fn metric_export_v2_refuses_missing_or_malformed_capability_before_force() {
    use axum::http::StatusCode;
    use serde_json::json;
    for (status, rows) in [
        (StatusCode::BAD_REQUEST, json!([])),
        (StatusCode::OK, json!([])),
        (StatusCode::OK, json!([[true]])),
        (StatusCode::OK, json!([["true", "extra"]])),
        (StatusCode::OK, json!([["true"], ["true"]])),
    ] {
        let app = axum::Router::new().route(
            "/v1/sql",
            axum::routing::post(move |axum::Form(form): axum::Form<std::collections::HashMap<String, String>>| async move {
                assert_eq!(form["sql"], "SHOW VARIABLES experimental_metric_export");
                (status, axum::Json(json!({
                    "execution_time_ms": 0,
                    "output": [{"records": {
                        "schema": {"column_schemas": [{"name": "EXPERIMENTAL_METRIC_EXPORT", "data_type": "String"}]},
                        "rows": rows
                    }}]
                })))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let destination = tempfile::tempdir().unwrap();
        let snapshot = destination.path().join("snapshot");
        let uri = format!("file://{}", snapshot.display());
        let args = [
            "export-v2",
            "create",
            "--addr",
            &addr,
            "--to",
            &uri,
            "--experimental-metric-export",
            "--force",
            "--no-proxy",
            "--progress",
            "never",
        ];
        let error = run_data_cli(&args).await.unwrap_err();
        if status == StatusCode::OK {
            assert!(
                error.to_string().contains("Metric export requires"),
                "{error}"
            );
        }
        assert!(!snapshot.exists());
        std::fs::create_dir(&snapshot).unwrap();
        let manifest = snapshot.join("manifest.json");
        std::fs::write(&manifest, b"preserve").unwrap();
        assert!(run_data_cli(&args).await.is_err());
        assert_eq!(std::fs::read(manifest).unwrap(), b"preserve");
        server.abort();
    }
}
