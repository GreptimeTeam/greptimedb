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

async fn roundtrip(instance: &Arc<Instance>) {
    let destination = tempfile::tempdir_in(common_test_util::find_workspace_path(".")).unwrap();
    for (physical, encoding) in [("phy", "dense"), ("other_phy", "sparse")] {
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
            &format!("INSERT INTO {physical}_excluded (host, huge_tag, val, ts) VALUES ('z','ignore',9,2)"),
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
