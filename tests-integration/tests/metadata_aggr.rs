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

use common_query::{Output, OutputData};
use common_recordbatch::RecordBatches;
use datatypes::arrow::array::Int64Array;
use frontend::instance::Instance;
use mito2::config::MitoConfig;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::QueryContext;
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

async fn execute_sql(frontend: &Instance, sql: &str) -> Output {
    SqlQueryHandler::do_query(frontend, sql, QueryContext::arc())
        .await
        .remove(0)
        .unwrap_or_else(|err| panic!("{sql}: {err}"))
}

async fn assert_count_and_scan(frontend: &Instance, expected: i64, expect_scan: bool) {
    let output = execute_sql(frontend, COUNT_SQL).await;
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
    assert_eq!(counts, vec![Some(expected)]);

    // ANALYZE includes the actual region plan below the frontend MergeScan.
    let plan = execute_sql(frontend, &format!("EXPLAIN ANALYZE {COUNT_SQL}"))
        .await
        .data
        .pretty_print()
        .await;
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
