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

async fn source_tables(
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
        let (names, tables, renamed) = source_tables(instance, physical, encoding).await;
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

fn database_request(directory: &std::path::Path) -> table::requests::CopyDatabaseRequest {
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

async fn database_roundtrip(instance: &Arc<Instance>) {
    let destination = tempfile::tempdir_in(common_test_util::find_workspace_path(".")).unwrap();
    let (a, _, physical) = source_tables(instance, "db_a", "dense").await;
    let (b, _, _) = source_tables(instance, "db_b", "sparse").await;
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
    let selected = vec![a[0].clone(), a[2].clone(), b[1].clone(), "audit".into()];
    let mut names = selected.clone();
    names.extend([physical, "dashboard".into()]);
    let req = database_request(&destination.path().join("data"));
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
    let ordinary_req = database_request(&destination.path().join("captured"));
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
        let mut req = database_request(&path);
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
    }
    let token = CancellationToken::new();
    token.cancel();
    let req = database_request(&destination.path().join("cancelled"));
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
    database_roundtrip(standalone.fe_instance()).await;
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
    database_roundtrip(cluster.fe_instance()).await;
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
    let (_, tables, physical) = source_tables(instance, "invalid_phy", "dense").await;
    let executor = instance.statement_executor();
    let destination = tempfile::tempdir_in(common_test_util::find_workspace_path(".")).unwrap();
    let req = database_request(&destination.path().join("data"));
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
            if reason == format!("unsafe or duplicate output name: {duplicate_name}")));
    assert_eq!(
        std::fs::read_dir(destination.path().join("data"))
            .unwrap()
            .count(),
        0
    );
}
