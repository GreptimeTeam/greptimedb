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

use common_base::readable_size::ReadableSize;
use common_query::{Output, OutputData};
use common_recordbatch::RecordBatches;
use datatypes::arrow::array::Int64Array;
use frontend::instance::Instance;
use mito2::config::MitoConfig;
use mito2::sst::file::RegionFileId;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::QueryContext;
use store_api::metric_engine_consts::METRIC_DATA_REGION_GROUP;
use tests_integration::cluster::GreptimeDbClusterBuilder;
use tests_integration::standalone::GreptimeDbStandaloneBuilder;

const COUNT_SQL: &str = "SELECT count(*) FROM metadata_count";

#[rstest::rstest]
#[case::enabled(true)]
#[case::disabled(false)]
#[tokio::test(flavor = "multi_thread")]
async fn test_metadata_count_non_append(#[case] enabled: bool) {
    common_telemetry::init_default_ut_logging();
    let standalone = GreptimeDbStandaloneBuilder::new("metadata_count_non_append")
        .with_mito_config(MitoConfig {
            experimental_metadata_aggr: enabled,
            schedule_compaction_after_edit: false,
            ..Default::default()
        })
        .build()
        .await;
    let frontend = standalone.fe_instance();

    execute_sql(
        frontend,
        "CREATE TABLE metadata_count (
            host STRING PRIMARY KEY, ts TIMESTAMP TIME INDEX, v BIGINT
        ) ENGINE=mito WITH (append_mode = 'false')",
    )
    .await;
    execute_sql(
        frontend,
        "INSERT INTO metadata_count VALUES ('a', 1000, 1), ('a', 2000, 2), ('b', 1000, 3)",
    )
    .await;
    execute_sql(frontend, "ADMIN FLUSH_TABLE('metadata_count')").await;

    // The region has one SST and no mutable rows. Every row can be counted
    // from metadata, but only when the flag is enabled.
    assert_count_and_scan(frontend, 3, !enabled).await;

    // A tombstone overlaps one of the settled rows. COUNT must fall back to
    // the ordinary scan (with either setting) to reconcile the deletion.
    execute_sql(
        frontend,
        "DELETE FROM metadata_count WHERE host = 'a' AND ts = 1000",
    )
    .await;
    assert_count_and_scan(frontend, 2, true).await;
}

#[rstest::rstest]
#[case::enabled(true)]
#[case::disabled(false)]
#[tokio::test(flavor = "multi_thread")]
async fn test_metadata_count_distributed(#[case] enabled: bool) {
    let cluster = GreptimeDbClusterBuilder::new("metadata_count_distributed")
        .await
        .with_datanodes(2)
        .with_mito_config(MitoConfig {
            experimental_metadata_aggr: enabled,
            ..Default::default()
        })
        .build(false)
        .await;
    let frontend = cluster.fe_instance();
    execute_sql(
        frontend,
        "CREATE TABLE metadata_count (
            host STRING PRIMARY KEY, ts TIMESTAMP TIME INDEX, v BIGINT
        ) PARTITION ON COLUMNS (host) (host < 'm', host >= 'm')
        ENGINE=mito WITH (append_mode = 'false')",
    )
    .await;
    execute_sql(
        frontend,
        "INSERT INTO metadata_count VALUES
            ('a', 1000, 1), ('a', 2000, 2), ('z', 1000, 3), ('z', 2000, 4)",
    )
    .await;
    execute_sql(frontend, "ADMIN FLUSH_TABLE('metadata_count')").await;
    assert_count_and_scan(frontend, 4, !enabled).await;
}

#[rstest::rstest]
#[case::enabled(true)]
#[case::disabled(false)]
#[tokio::test(flavor = "multi_thread")]
async fn test_metadata_count_distributed_with_filter_and_memtables(#[case] enabled: bool) {
    let cluster = GreptimeDbClusterBuilder::new("metadata_count_distributed_split")
        .await
        .with_datanodes(2)
        .with_mito_config(MitoConfig {
            experimental_metadata_aggr: enabled,
            schedule_compaction_after_edit: false,
            ..Default::default()
        })
        .build(false)
        .await;
    let frontend = cluster.fe_instance();
    execute_sql(
        frontend,
        "CREATE TABLE split_count (
            host STRING PRIMARY KEY, ts TIMESTAMP TIME INDEX, v BIGINT
        ) PARTITION ON COLUMNS (host) (host < 'm', host >= 'm')
        ENGINE=mito WITH (append_mode = 'false', 'memtable.type' = 'bulk')",
    )
    .await;
    execute_sql(frontend, "INSERT INTO split_count VALUES ('a', 500, 0), ('z', 500, 0)").await;
    execute_sql(frontend, "ADMIN FLUSH_TABLE('split_count')").await;
    execute_sql(
        frontend,
        "INSERT INTO split_count VALUES
            ('a', 1000, 1), ('a', 2000, 2), ('z', 1000, 3), ('z', 2000, 4)",
    )
    .await;
    execute_sql(frontend, "ADMIN FLUSH_TABLE('split_count')").await;
    execute_sql(frontend, "INSERT INTO split_count VALUES ('a', 3000, 5), ('z', 3000, 6)").await;

    let sql = "SELECT count(*) FROM split_count WHERE ts >= TIMESTAMP '1970-01-01 00:00:01'";
    assert_count(frontend, sql, 6).await;
    let plan = explain_count(frontend, sql).await;
    assert!(plan.contains("__count_state"), "{plan}");
    assert!(plan.contains("SeqScan:"), "{plan}");
    assert_eq!(
        plan.contains("MetadataAggrExec: exact_rows=2"),
        enabled,
        "{plan}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_metadata_count_logical_metric_skips_other_table_footers() {
    let standalone = GreptimeDbStandaloneBuilder::new("metadata_count_logical_metric")
        .with_mito_config(MitoConfig {
            experimental_metadata_aggr: true,
            sst_meta_cache_size: ReadableSize::mb(64),
            enable_write_cache: false,
            preload_index_cache: false,
            schedule_compaction_after_edit: false,
            ..Default::default()
        })
        .build()
        .await;
    let frontend = standalone.fe_instance();
    execute_sql(
        frontend,
        "CREATE TABLE physical_count (ts TIMESTAMP TIME INDEX, v DOUBLE, host STRING PRIMARY KEY)
         ENGINE=metric WITH (physical_metric_table = 'true', primary_key_encoding = 'sparse',
                             append_mode = 'false', 'memtable.type' = 'bulk')",
    )
    .await;
    for name in ["logical_a", "logical_b"] {
        execute_sql(
            frontend,
            &format!(
                "CREATE TABLE {name} (ts TIMESTAMP TIME INDEX, v DOUBLE, host STRING PRIMARY KEY)
                 ENGINE=metric WITH (on_physical_table = 'physical_count')"
            ),
        )
        .await;
    }

    // Flush each logical table separately. The two files overlap in time in
    // the same physical region, but their leading table IDs are disjoint.
    execute_sql(frontend, "INSERT INTO logical_a VALUES (1000, 1, 'a'), (2000, 2, 'a')").await;
    execute_sql(frontend, "ADMIN FLUSH_TABLE('physical_count')").await;
    execute_sql(
        frontend,
        "INSERT INTO logical_b VALUES (1000, 1, 'b'), (1500, 2, 'b'), (2000, 3, 'b')",
    )
    .await;
    execute_sql(frontend, "ADMIN FLUSH_TABLE('physical_count')").await;
    execute_sql(frontend, "INSERT INTO logical_a VALUES (3000, 3, 'a')").await;

    let mut files = standalone
        .mito_engine
        .all_ssts_from_manifest()
        .await
        .into_iter()
        .filter(|file| file.region_group == METRIC_DATA_REGION_GROUP && file.visible)
        .collect::<Vec<_>>();
    files.sort_unstable_by_key(|file| file.num_rows);
    let [a, b] = files.as_slice() else {
        panic!("expected two data SSTs: {files:?}");
    };
    assert_eq!(a.region_id, b.region_id);
    assert_eq!((a.min_ts, a.max_ts), (b.min_ts, b.max_ts));
    assert_eq!((a.num_rows, b.num_rows), (2, 3));
    let target = RegionFileId::new(a.origin_region_id, a.file_id.parse().unwrap());
    let unrelated = RegionFileId::new(b.origin_region_id, b.file_id.parse().unwrap());
    let cache = standalone.mito_engine.cache_manager();
    for file in [target, unrelated] {
        cache.remove_parquet_meta_data(file).await;
        assert!(cache.get_parquet_meta_data_from_mem_cache(file).await.is_none());
    }

    let sql = "SELECT count(*) FROM logical_a";
    assert_count(frontend, sql, 3).await;
    let plan = explain_count(frontend, sql).await;
    assert!(plan.contains("MetadataAggrExec: exact_rows=2"), "{plan}");
    assert!(plan.contains("SeqScan:"), "{plan}");

    // Loading a footer through this read path populates the enabled cache.
    // Only the file whose rows contributed to the metadata count was loaded.
    assert!(cache.get_parquet_meta_data_from_mem_cache(target).await.is_some());
    assert!(cache.get_parquet_meta_data_from_mem_cache(unrelated).await.is_none());
    assert_count(frontend, "SELECT count(*) FROM logical_b", 3).await;
    assert!(cache.get_parquet_meta_data_from_mem_cache(unrelated).await.is_some());
}

async fn execute_sql(frontend: &Instance, sql: &str) -> Output {
    SqlQueryHandler::do_query(frontend, sql, QueryContext::arc())
        .await
        .remove(0)
        .unwrap_or_else(|err| panic!("{sql}: {err}"))
}

async fn assert_count_and_scan(frontend: &Instance, expected: i64, expect_scan: bool) {
    assert_count(frontend, COUNT_SQL, expected).await;

    // ANALYZE includes the actual region plan below the frontend MergeScan.
    let plan = explain_count(frontend, COUNT_SQL).await;
    assert_eq!(
        plan.contains("SeqScan:"),
        expect_scan,
        "unexpected region plan:\n{plan}"
    );
    if !expect_scan {
        assert!(
            plan.contains("PlaceholderRowExec"),
            "expected the region scan to be replaced by a literal projection:\n{plan}"
        );
    }
}

async fn assert_count(frontend: &Instance, sql: &str, expected: i64) {
    let output = execute_sql(frontend, sql).await;
    let batches = match output.data {
        OutputData::RecordBatches(batches) => batches,
        OutputData::Stream(stream) => RecordBatches::try_collect(stream).await.unwrap(),
        OutputData::AffectedRows(rows) => {
            panic!("expected a COUNT result, got {rows} affected rows")
        }
    };
    let counts = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
        })
        .collect::<Vec<_>>();
    assert_eq!(counts, vec![Some(expected)], "{sql}");
}

async fn explain_count(frontend: &Instance, sql: &str) -> String {
    execute_sql(frontend, &format!("EXPLAIN ANALYZE {sql}"))
        .await
        .data
        .pretty_print()
        .await
}
