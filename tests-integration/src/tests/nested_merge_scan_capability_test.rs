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

//! Exercises hand-built nested `MergeScan` plans at datanode execution, not optimizer behavior.
//! The outer `handle_remote_read` request is in-process; inner region reads use real TCP Flight.
//! The probe scan resolves against the request's region catalog, while the nested payload resolves
//! against the engine catalog; distinct column names ensure these scopes cannot be confused.
//! Success compares the complete duplicate-sensitive result multiset with frontend SQL and known
//! rows. Missing inner tables and closed remote inner regions must each fail the whole query.
//!
//! [`test_nested_broadcast_join_rewrite_executes_on_probe_regions`] also exercises the SQL/planner
//! capability, including the BIG local probe, SMALL remote build, complete output schema, and
//! BIG-region pruning.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use api::v1::region::{QueryRequest, RegionRequestHeader};
use common_query::Output;
use common_recordbatch::{RecordBatch, RecordBatches, SendableRecordBatchStream};
use common_telemetry::info;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_expr::{JoinType, LogicalPlan, LogicalPlanBuilder};
use datanode::region_server::RegionServer;
use frontend::instance::Instance;
use query::datafusion::QUERY_PARALLELISM_HINT;
use query::dist_plan::{MergeScanExec, MergeScanLogicalPlan};
use query::parser::QueryLanguageParser;
use query::query_engine::DefaultSerializer;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::{QueryContext, QueryContextRef};
use store_api::region_request::{RegionCloseRequest, RegionRequest};
use store_api::storage::RegionId;
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};

use crate::cluster::{GreptimeDbCluster, GreptimeDbClusterBuilder};
use crate::test_util::execute_sql;

/// Partitioned probe table.
const PROBE_TABLE: &str = "nested_cap_probe";
/// Partitioned build table with duplicate join keys.
const BUILD_TABLE: &str = "nested_cap_build";
/// Missing inner table used to verify plan-resolution failure.
const MISSING_INNER_TABLE: &str = "nested_cap_missing_build";

/// `(a_id, probe_key, probe_v)` fixture rows; `a_id` is the partition key.
const PROBE_ROWS: &[(i32, i32, i32)] = &[
    (1, 10, 100),
    (2, 20, 200),
    // The second partition of the probe table.
    (120, 30, 300),
    (121, 10, 400),
];

/// `(b_id, build_key, build_v)` fixture rows; key `10` duplicates and `40` is unmatched.
const BUILD_ROWS: &[(i32, i32, i32)] = &[
    (1, 10, 1),
    (2, 10, 2),
    (3, 20, 3),
    // The second partition of the build table.
    (120, 30, 4),
    (121, 40, 5),
];

#[tokio::test(flavor = "multi_thread")]
async fn test_nested_merge_scan_capability_join_across_datanodes() {
    common_telemetry::init_default_ut_logging();

    let cluster = build_cluster("test_nested_merge_scan_capability_join_across_datanodes").await;
    let frontend = cluster.fe_instance().clone();

    prepare_tables(&frontend).await;

    let probe_leaders = region_leaders(&frontend, PROBE_TABLE).await;
    let build_leaders = region_leaders(&frontend, BUILD_TABLE).await;
    assert_eq!(
        2,
        datanodes(&probe_leaders).len(),
        "expected the regions of {PROBE_TABLE} to be spread over both datanodes, actual region \
         leaders: {probe_leaders:?}"
    );
    assert_eq!(
        2,
        datanodes(&build_leaders).len(),
        "expected the regions of {BUILD_TABLE} to be spread over both datanodes, actual region \
         leaders: {build_leaders:?}"
    );

    let reference = query_pretty(&frontend, &join_sql(), query_ctx()).await;
    assert_eq!(
        expected_join_rows(),
        multiset(table_cells(&reference)),
        "unexpected result of the join on the frontend:\n{reference}"
    );

    let query_ctx = query_ctx();
    let nested_plan = nested_plan(&frontend, &query_ctx).await;
    let plan = encode(&nested_plan);

    let remote_datanodes = datanodes(&build_leaders);
    let baseline_requests = remote_datanodes
        .iter()
        .map(|datanode| (*datanode, rpc_requests(&cluster, *datanode)))
        .collect::<BTreeMap<_, _>>();

    let rows = run_nested_queries(&cluster, &probe_leaders, &plan, &query_ctx).await;

    let actual_rows = multiset(rows);
    assert_eq!(
        expected_join_rows(),
        actual_rows,
        "the nested plans of the datanodes returned an unexpected multiset of rows"
    );
    assert_eq!(
        multiset(table_cells(&reference)),
        actual_rows,
        "frontend result differs:\n{reference}"
    );
    assert_cross_datanode_region_query(&cluster, &remote_datanodes, &baseline_requests);
}

/// The SQL setting and region statistics select the nested plan through the production frontend
/// path. An unfiltered BIG-table case proves the local probe and remote build roles, complete
/// output, and schema; a separate filtered query verifies BIG-region pruning. Both exercise the
/// executed SQL result with duplicate keys, NULL keys, and a residual join filter.
#[tokio::test(flavor = "multi_thread")]
async fn test_nested_broadcast_join_rewrite_executes_on_probe_regions() {
    common_telemetry::init_default_ut_logging();

    let cluster =
        build_cluster("test_nested_broadcast_join_rewrite_executes_on_probe_regions").await;
    let frontend = cluster.fe_instance().clone();

    prepare_tables(&frontend).await;
    // A NULL join key on both sides and a second probe row of the duplicated join key `10`. They
    // are inserted here, so the shared fixture of the capability tests keeps its rows.
    execute_sql(
        &frontend,
        &format!("INSERT INTO {PROBE_TABLE}(a_id, probe_key, probe_v, a_ts) VALUES (3, NULL, 500, 3000), (4, 10, 600, 4000)"),
    )
    .await;
    execute_sql(
        &frontend,
        &format!(
            "INSERT INTO {BUILD_TABLE}(b_id, build_key, build_v, b_ts) VALUES (4, NULL, 6, 5000)"
        ),
    )
    .await;
    insert_extra_probe_rows(&frontend).await;

    // Leave the BIG probe table unfiltered so all padding rows reach the local probe input. The
    // projection order, duplicate keys, NULLs, and residual condition remain part of the check.
    let sql = format!(
        "SELECT p.probe_v, p.a_id, b.build_v
         FROM {PROBE_TABLE} p JOIN {BUILD_TABLE} b
         ON p.probe_key = b.build_key AND p.probe_v + b.build_v > 105"
    );
    let expected = expected_full_broadcast_rows();
    let query_ctx = query_ctx();

    // The reference: the frontend executes the same SQL with its default distributed plan, i.e.
    // both sides of the join are read with a `MergeScan` and the join runs on the frontend.
    let reference = output_batches(run_sql(&frontend, &sql, query_ctx.clone()).await).await;
    assert_eq!(
        vec!["probe_v", "a_id", "build_v"],
        reference
            .schema()
            .column_schemas()
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        "the unfiltered query must preserve its explicit projection order"
    );
    let reference_pretty = reference.pretty_print().unwrap();
    info!("reference result of the frontend:\n{reference_pretty}");
    let star_sql = format!(
        "SELECT *
         FROM {PROBE_TABLE} p JOIN {BUILD_TABLE} b
         ON p.probe_key = b.build_key AND p.probe_v + b.build_v > 105
         WHERE p.a_id < 100 AND p.a_ts < 10000::TIMESTAMP"
    );
    let star_reference =
        output_batches(run_sql(&frontend, &star_sql, query_ctx.clone()).await).await;
    let expected_star_columns = vec![
        "a_id",
        "probe_key",
        "probe_v",
        "a_ts",
        "b_id",
        "build_key",
        "build_v",
        "b_ts",
    ];
    assert_eq!(
        expected_star_columns,
        star_reference
            .schema()
            .column_schemas()
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        "SELECT * must retain BIG columns followed by SMALL columns"
    );
    assert_eq!(
        3,
        table_cells(&star_reference.pretty_print().unwrap()).len(),
        "the filtered SELECT * baseline must contain all three rows"
    );
    assert_eq!(
        expected,
        multiset(table_cells(&reference_pretty)),
        "unexpected result of the join on the frontend:\n{reference_pretty}"
    );

    // Use the actual session setting and SQL entrypoint. The region statistics above drive the
    // production selector; no analyzer options or plans are injected by this test.
    let (probe_bytes, build_bytes) =
        wait_for_region_statistics(&frontend, BUILD_ROWS.len() as u64 + 1).await;
    assert!(
        2 * build_bytes < probe_bytes,
        "the fixture must make the heuristic favor the build side, actual region statistics: \
         probe {probe_bytes} bytes in 2 regions, build {build_bytes} bytes in 2 regions"
    );
    run_sql(
        &frontend,
        "SET experimental_dist_join = true",
        query_ctx.clone(),
    )
    .await;

    let actual = run_sql(&frontend, &sql, query_ctx.clone()).await;
    let physical = actual
        .meta
        .plan
        .clone()
        .expect("the output of the query must carry its physical plan");
    info!("physical plan of the SQL-selected nested join:\\n{physical:?}");

    // The unfiltered BIG probe scan dispatches both of its regions.
    let probe_leaders = region_leaders(&frontend, PROBE_TABLE).await;
    assert!(
        probe_leaders.len() > 1,
        "expected {PROBE_TABLE} to have several regions, actual region leaders: {probe_leaders:?}"
    );
    let merge_scan = find_merge_scan_exec(&physical)
        .unwrap_or_else(|| panic!("expected the outer MergeScan to be planned, got: {physical:?}"));
    let regions = merge_scan.regions().to_vec();
    info!("the outer MergeScan is planned for the regions {regions:?}");
    assert_eq!(
        probe_leaders.len(),
        regions.len(),
        "the unfiltered BIG table scan must dispatch all {} regions, actual selection: {regions:?}",
        probe_leaders.len()
    );
    assert!(
        regions
            .iter()
            .all(|region| probe_leaders.contains_key(&region.as_u64())),
        "the outer MergeScan must select regions of {PROBE_TABLE}, actual selection: {regions:?}"
    );

    let actual = output_batches(actual).await;
    assert_eq!(
        reference.schema(),
        actual.schema(),
        "broadcast execution must preserve full schema and types"
    );
    let actual_pretty = actual.pretty_print().unwrap();
    info!("result of the SQL-selected nested plan:\n{actual_pretty}");
    let stages = merge_scan.sub_stage_metrics();
    assert_eq!(
        regions.len(),
        stages.len(),
        "each dispatched outer region must report its stage metrics"
    );
    let hash_joins = stages
        .iter()
        .flat_map(|stage| &stage.plan_metrics)
        .filter(|metric| metric.plan_name == "HashJoinExec")
        .collect::<Vec<_>>();
    assert_eq!(
        regions.len(),
        hash_joins.len(),
        "every dispatched outer region must report its actual DN HashJoinExec: {stages:?}"
    );
    // The ordinary equijoin pre-scan optimizer filters NULL join keys before the DN HashJoin.
    // The six-row SMALL table (including its NULL-key row) is still covered by result/schema
    // checks and heartbeat readiness; the actual hash build therefore contains BUILD_ROWS only.
    let expected_build_rows = BUILD_ROWS.len();
    assert!(
        hash_joins.iter().all(|metric| {
            metric
                .metrics
                .iter()
                .any(|(name, value)| *name == "build_input_rows" && *value == expected_build_rows)
                && metric
                    .metrics
                    .iter()
                    .any(|(name, value)| *name == "input_rows" && *value > expected_build_rows)
        }),
        "each DN HashJoinExec must build {expected_build_rows} non-NULL remote SMALL rows and consume more BIG probe rows: {hash_joins:?}"
    );

    assert_eq!(
        expected,
        multiset(table_cells(&actual_pretty)),
        "the nested plan returned an unexpected multiset of rows:\n{actual_pretty}"
    );
    assert_eq!(
        multiset(table_cells(&reference_pretty)),
        multiset(table_cells(&actual_pretty)),
        "the nested plan returned a different multiset of rows than the frontend:\n{actual_pretty}"
    );

    // Keep the original selective query as a separate routing regression. Its filtered local
    // side can be smaller than the remote table, so this asserts pruning/results only, not build
    // orientation.
    let pruned_sql = format!(
        "SELECT p.probe_v, p.a_id, b.build_v
         FROM {PROBE_TABLE} p JOIN {BUILD_TABLE} b
         ON p.probe_key = b.build_key AND p.probe_v + b.build_v > 105
         WHERE p.a_id < 100 AND p.a_ts < 10000::TIMESTAMP"
    );
    let pruned_expected = multiset(vec![
        vec!["200".to_string(), "2".to_string(), "3".to_string()],
        vec!["600".to_string(), "4".to_string(), "1".to_string()],
        vec!["600".to_string(), "4".to_string(), "2".to_string()],
    ]);
    let pruned = run_sql(&frontend, &pruned_sql, query_ctx.clone()).await;
    let pruned_plan = pruned
        .meta
        .plan
        .clone()
        .expect("the pruned query output must carry its physical plan");
    let pruned_merge_scan = find_merge_scan_exec(&pruned_plan)
        .unwrap_or_else(|| panic!("expected pruned query outer MergeScan: {pruned_plan:?}"));
    let pruned_regions = pruned_merge_scan.regions();
    assert_eq!(
        1,
        pruned_regions.len(),
        "the predicate must prune to one BIG region"
    );
    assert!(
        pruned_regions
            .iter()
            .all(|region| probe_leaders.contains_key(&region.as_u64())),
        "the pruned region must belong to BIG table {PROBE_TABLE}: {pruned_regions:?}"
    );
    let pruned_batches = output_batches(pruned).await;
    assert_eq!(
        pruned_expected,
        multiset(table_cells(&pruned_batches.pretty_print().unwrap())),
        "pruned query must preserve the expected complete result"
    );

    // SELECT * must also restore the original BIG-then-SMALL schema after rewriting.
    let star_actual = output_batches(run_sql(&frontend, &star_sql, query_ctx.clone()).await).await;
    assert_eq!(
        star_reference.schema(),
        star_actual.schema(),
        "rewritten SELECT * must preserve all column types and order"
    );
    assert_eq!(
        expected_star_columns,
        star_actual
            .schema()
            .column_schemas()
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        "rewritten SELECT * must retain BIG columns followed by SMALL columns"
    );
    assert_eq!(
        multiset(table_cells(&star_reference.pretty_print().unwrap())),
        multiset(table_cells(&star_actual.pretty_print().unwrap())),
        "rewritten SELECT * must preserve all three baseline rows"
    );
}

/// Complete expected output for the unfiltered broadcast SQL, in its projected column order.
fn expected_full_broadcast_rows() -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    let mut probes = PROBE_ROWS.to_vec();
    // SQL equality never matches the explicit NULL-key probe row.
    probes.extend(
        extra_probe_rows()
            .into_iter()
            .map(|(a_id, key, value, _)| (a_id, key, value)),
    );
    probes.push((4, 10, 600));
    for (a_id, probe_key, probe_v) in probes {
        for (_, build_key, build_v) in BUILD_ROWS.iter().copied() {
            if probe_key == build_key && probe_v + build_v > 105 {
                rows.push(vec![
                    probe_v.to_string(),
                    a_id.to_string(),
                    build_v.to_string(),
                ]);
            }
        }
    }
    multiset(rows)
}

/// Returns the `MergeScanExec` of `plan`, descending through the physical plan tree.
fn find_merge_scan_exec(plan: &Arc<dyn ExecutionPlan>) -> Option<&MergeScanExec> {
    if let Some(merge_scan) = plan.downcast_ref::<MergeScanExec>() {
        return Some(merge_scan);
    }

    plan.children().into_iter().find_map(find_merge_scan_exec)
}

/// The join keys of [`BUILD_ROWS`], so the extra probe rows of
/// [`test_experimental_dist_join_setting_activates_rewrite`] match every build key.
const BUILD_JOIN_KEYS: &[i32] = &[10, 20, 30, 40];

/// The number of extra rows of [`extra_probe_rows`]: enough for the cost heuristic of the rewrite
/// to favor the build side of the join by a wide margin, see
/// [`test_experimental_dist_join_setting_activates_rewrite`].
const EXTRA_PROBE_ROWS: i32 = 4_000;

/// `(a_id, probe_key, probe_v, a_ts)` of the extra probe rows: `a_id` alternates between the two
/// partitions of the probe table and `probe_key` cycles the build keys, so every build key is
/// matched.
fn extra_probe_rows() -> Vec<(i32, i32, i32, i64)> {
    (0..EXTRA_PROBE_ROWS)
        .map(|i| {
            (
                if i % 2 == 0 { i } else { 1_000 + i },
                BUILD_JOIN_KEYS[i as usize % BUILD_JOIN_KEYS.len()],
                i,
                i as i64 + 10_000,
            )
        })
        .collect()
}

/// Inserts the extra probe rows of [`extra_probe_rows`] in batches.
async fn insert_extra_probe_rows(frontend: &Arc<Instance>) {
    for chunk in extra_probe_rows().chunks(500) {
        let values = chunk
            .iter()
            .map(|(a_id, probe_key, probe_v, a_ts)| {
                format!("({a_id}, {probe_key}, {probe_v}, {a_ts})")
            })
            .collect::<Vec<_>>()
            .join(",");
        execute_sql(
            frontend,
            &format!("INSERT INTO {PROBE_TABLE}(a_id, probe_key, probe_v, a_ts) VALUES {values}"),
        )
        .await;
    }
}

/// Whether `plan` contains a hash join, the physical node of the join of the rewrite.
fn contains_hash_join(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.name().contains("HashJoin") || plan.children().into_iter().any(contains_hash_join)
}

/// The region statistics of `table`: `(leader reports, summed disk bytes, minimum disk bytes, summed rows)`.
///
/// A region that has not reported yet, reports zero bytes, or is not served by a leader makes the
/// cost heuristic of the rewrite skip the table, so a test that expects the rewrite has to wait
/// until every region of every table has an ordinary leader report. A missing or NULL aggregate
/// counts as `0`, i.e. as "not reported yet".
async fn region_statistics(frontend: &Arc<Instance>, table: &str) -> (u64, u64, u64, u64) {
    let sql = format!(
        "SELECT count(*) AS regions, sum(disk_size) AS bytes, min(disk_size) AS min_bytes,
                sum(region_rows) AS rows
         FROM information_schema.region_statistics
         WHERE table_id IN (SELECT table_id FROM information_schema.tables
                            WHERE table_schema = 'public' AND table_name = '{table}')
           AND region_role = 'Leader'"
    );
    let pretty = query_pretty(frontend, &sql, query_ctx()).await;
    let cells = table_cells(&pretty);
    let row = cells.first().cloned().unwrap_or_default();
    let parse = |index: usize| {
        row.get(index)
            .and_then(|cell| cell.parse::<u64>().ok())
            .unwrap_or(0)
    };

    (parse(0), parse(1), parse(2), parse(3))
}

/// Waits until both tables of the join have one ordinary leader report per region with a non-zero
/// size, and returns the summed disk bytes of the probe and of the build table.
///
/// The frontend reads these statistics from the region statistics that the datanodes report to
/// metasrv, so they appear a few heartbeats after the tables are created.
async fn wait_for_region_statistics(
    frontend: &Arc<Instance>,
    expected_build_rows: u64,
) -> (u64, u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    loop {
        let mut stats = Vec::new();
        for table in [PROBE_TABLE, BUILD_TABLE] {
            stats.push(region_statistics(frontend, table).await);
        }
        let probe = stats[0];
        let build = stats[1];
        info!("region statistics of {PROBE_TABLE}: {probe:?}, of {BUILD_TABLE}: {build:?}");
        if probe.0 == 2
            && probe.2 > 0
            && probe.3 > 0
            && build.0 == 2
            && build.2 > 0
            && build.3 == expected_build_rows
        {
            return (probe.1, build.1);
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the region statistics of {PROBE_TABLE} {probe:?} and {BUILD_TABLE} {build:?} must be \
             reported"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Whether the logical plan text of an EXPLAIN of the join shows the nested rewrite: the outer
/// `MergeScan` of the rewritten plan wraps the join, so the boundary appears before the join,
/// while the plan without the rewrite starts at the join of the two boundaries.
fn nests_join_in_merge_scan(explained: &str) -> bool {
    match (explained.find("Join:"), explained.find("MergeScan")) {
        (Some(join), Some(merge_scan)) => merge_scan < join,
        _ => false,
    }
}

/// `SET experimental_dist_join` is the session opt-in of the cost heuristic: with the setting, the
/// frontend nests the build side of a join into the probe side's `MergeScan` when the region
/// statistics favor it, and executes that plan; without it, the join of the two boundaries stays.
///
/// The fixture makes the heuristic favor the build side by a wide margin: the probe table gets
/// [`EXTRA_PROBE_ROWS`] extra rows while the build table keeps the rows of [`BUILD_ROWS`], and the
/// test asserts the region statistics it reads really satisfy `regions * build_bytes <
/// probe_bytes`. The query of the test selects the rows of the fixture it can predict by hand, so
/// the parity of the execution with and without the opt-in is checked on the full multiset of the
/// projected rows, not on a row count: a rewritten plan that loses, duplicates or mixes up values
/// fails the comparison.
#[tokio::test(flavor = "multi_thread")]
async fn test_experimental_dist_join_setting_activates_rewrite() {
    common_telemetry::init_default_ut_logging();

    let cluster = build_cluster("test_experimental_dist_join_setting_activates_rewrite").await;
    let frontend = cluster.fe_instance().clone();

    prepare_tables(&frontend).await;
    insert_extra_probe_rows(&frontend).await;

    // The join of the test: the projection reorders the columns, and the equality and the residual
    // filter of the `ON` clause stay in the join. The predicate on the time index of the probe
    // table is what keeps the result readable by hand: `10000::TIMESTAMP` is the raw `10000` of
    // the (millisecond) time index, so it keeps the rows that [`prepare_tables`] inserted (raw
    // timestamps `1000` to `4000`) and drops every row of [`insert_extra_probe_rows`] (raw
    // timestamps `10000` and above). The fixture is not changed, and the heuristic reads the
    // region statistics of the whole tables anyway.
    let sql = format!(
        "SELECT p.a_id, p.probe_v, b.b_id, b.build_v
         FROM {PROBE_TABLE} p JOIN {BUILD_TABLE} b
         ON p.probe_key = b.build_key AND p.probe_v + b.build_v > 105
         WHERE p.a_ts < 10000::TIMESTAMP"
    );
    // The rows of the join as `(a_id, probe_v, b_id, build_v)`: the probe row `(2, 20, 200)` joins
    // `(3, 20, 3)`, the probe row `(120, 30, 300)` joins `(120, 30, 4)`, the probe row
    // `(121, 10, 400)` joins the two build rows of the duplicated key `10` (`400 + 1` and
    // `400 + 2`), and the residual filter rejects the probe row `(1, 10, 100)` (`100 + 1` and
    // `100 + 2` are not greater than `105`).
    let expected = multiset(vec![
        vec![
            "2".to_string(),
            "200".to_string(),
            "3".to_string(),
            "3".to_string(),
        ],
        vec![
            "120".to_string(),
            "300".to_string(),
            "120".to_string(),
            "4".to_string(),
        ],
        vec![
            "121".to_string(),
            "400".to_string(),
            "1".to_string(),
            "1".to_string(),
        ],
        vec![
            "121".to_string(),
            "400".to_string(),
            "2".to_string(),
            "2".to_string(),
        ],
    ]);
    let query_ctx = query_ctx();

    let reference = run_sql(&frontend, &sql, query_ctx.clone()).await;
    let plan = reference
        .meta
        .plan
        .clone()
        .expect("the output of the query must carry its physical plan");
    info!("the physical plan of the join without the opt-in:\n{plan:?}");
    assert!(
        contains_hash_join(&plan),
        "without the opt-in the join must run on the frontend, got:\n{plan:?}"
    );
    let reference = reference.data.pretty_print().await;
    info!("the join without the opt-in returns:\n{reference}");
    assert_eq!(
        expected,
        multiset(table_cells(&reference)),
        "unexpected result of the join without the opt-in:\n{reference}"
    );
    // The heuristic is off by default: the EXPLAIN of the same join keeps the join of the two
    // boundaries.
    let explained = query_pretty(&frontend, &format!("EXPLAIN {sql}"), query_ctx.clone()).await;
    info!("EXPLAIN without the opt-in:\n{explained}");
    assert!(
        !nests_join_in_merge_scan(&explained),
        "without `SET experimental_dist_join = true` the join must keep its two MergeScan \
         boundaries:\n{explained}"
    );

    // The statistics the heuristic reads: every region of both tables has a leader report with a
    // non-zero size, and broadcasting the build table to the probe regions is much cheaper than
    // the probe table itself.
    let (probe_bytes, build_bytes) =
        wait_for_region_statistics(&frontend, BUILD_ROWS.len() as u64).await;
    assert!(
        2 * build_bytes < probe_bytes,
        "the fixture must make the heuristic favor the build side, actual region statistics: \
         probe {probe_bytes} bytes in 2 regions, build {build_bytes} bytes in 2 regions"
    );

    // The setting is only an opt-in: with the small build table on the left and the padded probe
    // table on the right, it must leave the unfavorable join placement on the frontend.
    run_sql(
        &frontend,
        "SET experimental_dist_join = true",
        query_ctx.clone(),
    )
    .await;
    let reversed_sql = format!(
        "SELECT p.a_id, p.probe_v, b.b_id, b.build_v
         FROM {BUILD_TABLE} b JOIN {PROBE_TABLE} p
         ON p.probe_key = b.build_key AND p.probe_v + b.build_v > 105
         WHERE p.a_ts < 10000::TIMESTAMP"
    );
    let reversed_explained = query_pretty(
        &frontend,
        &format!("EXPLAIN {reversed_sql}"),
        query_ctx.clone(),
    )
    .await;
    assert!(
        !nests_join_in_merge_scan(&reversed_explained),
        "the opt-in must leave the unfavorable small-left/big-right placement on the frontend:\n\
         {reversed_explained}"
    );
    let reversed = run_sql(&frontend, &reversed_sql, query_ctx.clone()).await;
    let reversed_plan = reversed
        .meta
        .plan
        .clone()
        .expect("the output of the reversed query must carry its physical plan");
    assert!(
        contains_hash_join(&reversed_plan),
        "the unfavorable small-left/big-right placement must keep its frontend hash join, got:\n\
         {reversed_plan:?}"
    );
    assert_eq!(
        expected,
        multiset(table_cells(&reversed.data.pretty_print().await)),
        "the reversed join must return the same expected rows"
    );

    let explained = query_pretty(&frontend, &format!("EXPLAIN {sql}"), query_ctx.clone()).await;
    info!("EXPLAIN with the opt-in:\n{explained}");
    assert!(
        nests_join_in_merge_scan(&explained),
        "with `SET experimental_dist_join = true` the EXPLAIN must show the nested rewrite:\n\
         {explained}"
    );

    let rewritten = run_sql(&frontend, &sql, query_ctx.clone()).await;
    let plan = rewritten
        .meta
        .plan
        .clone()
        .expect("the output of the query must carry its physical plan");
    info!("the physical plan of the join with the opt-in:\n{plan:?}");
    assert!(
        !contains_hash_join(&plan) && find_merge_scan_exec(&plan).is_some(),
        "the executed plan of the opted-in join must read the join through a MergeScan instead of \
         joining on the frontend, got:\n{plan:?}"
    );
    let rewritten = rewritten.data.pretty_print().await;
    info!("the join with the opt-in returns:\n{rewritten}");
    assert_eq!(
        multiset(table_cells(&reference)),
        multiset(table_cells(&rewritten)),
        "the executed plan of the opted-in join must return the same multiset of rows as the \
         frontend without the opt-in:\nfrontend:\n{reference}\nrewritten:\n{rewritten}"
    );

    // Turning the setting off restores the existing plan for the same session.
    run_sql(
        &frontend,
        "SET experimental_dist_join = false",
        query_ctx.clone(),
    )
    .await;

    let explained = query_pretty(&frontend, &format!("EXPLAIN {sql}"), query_ctx).await;
    info!("EXPLAIN after turning the opt-in off:\n{explained}");
    assert!(
        !nests_join_in_merge_scan(&explained),
        "`SET experimental_dist_join = false` must restore the join of the two boundaries:\n\
         {explained}"
    );
}

/// A failing inner region makes the whole query fail: the datanode must not return the rows of the
/// inner regions that succeed.
#[tokio::test(flavor = "multi_thread")]
async fn test_nested_merge_scan_capability_inner_failure_fails_query() {
    common_telemetry::init_default_ut_logging();

    let cluster =
        build_cluster("test_nested_merge_scan_capability_inner_failure_fails_query").await;
    let frontend = cluster.fe_instance().clone();

    prepare_tables(&frontend).await;

    let probe_leaders = region_leaders(&frontend, PROBE_TABLE).await;
    let build_leaders = region_leaders(&frontend, BUILD_TABLE).await;

    let (probe_region, (probe_datanode, _)) = probe_leaders
        .iter()
        .find(|(_, (datanode, _))| *datanode == 1)
        .unwrap_or_else(|| {
            panic!(
                "expected a region of {PROBE_TABLE} on the datanode 1, actual region leaders: \
                 {probe_leaders:?}"
            )
        });
    let (build_region, (build_datanode, _)) = build_leaders
        .iter()
        .find(|(_, (datanode, _))| *datanode != *probe_datanode)
        .unwrap_or_else(|| {
            panic!(
                "expected the regions of {BUILD_TABLE} to be spread over both datanodes, actual \
                 region leaders: {build_leaders:?}"
            )
        });

    // The nested payload resolves against the engine catalog, not the request's region catalog.
    let query_ctx = query_ctx();
    let missing_inner_plan =
        nested_plan_with_inner_table(&frontend, &query_ctx, MISSING_INNER_TABLE).await;
    let plan = encode(&missing_inner_plan);
    let error = region_server(&cluster, *probe_datanode)
        .handle_remote_read(
            QueryRequest {
                header: Some(region_request_header(&query_ctx)),
                region_id: *probe_region,
                plan,
            },
            query_ctx.clone(),
        )
        .await
        .err()
        .expect("a nested plan whose inner relation does not exist must not be executed");
    // The top level error of the datanode only names the failing step (`Failed to decode logical
    // plan`): the reason is the source of it, which names the table that could not be resolved.
    let chain = error_chain(&error);
    assert!(
        chain.contains(MISSING_INNER_TABLE),
        "expected the datanode to fail while resolving the inner relation {MISSING_INNER_TABLE}, \
         actual error: {chain}"
    );

    // A closed remote inner region must fail the whole query, not return a partial result.
    region_server(&cluster, *build_datanode)
        .handle_request(
            RegionId::from_u64(*build_region),
            RegionRequest::Close(RegionCloseRequest {
                flush_on_close: false,
            }),
        )
        .await
        .expect("the datanode must close the region of the build table");

    let nested_plan = nested_plan(&frontend, &query_ctx).await;
    let plan = encode(&nested_plan);
    let baseline = rpc_requests(&cluster, *build_datanode);
    let stream = region_server(&cluster, *probe_datanode)
        .handle_remote_read(
            QueryRequest {
                header: Some(region_request_header(&query_ctx)),
                region_id: *probe_region,
                plan,
            },
            query_ctx.clone(),
        )
        .await
        .expect("the request itself must be accepted by the datanode");
    let error = collect_error(stream)
        .await
        .expect("the query must fail when one of its inner regions fails");
    info!("the nested plan failed as a whole: {error}");

    // The increased Flight DoGet count proves execution reached the remote region owner.
    let actual = rpc_requests(&cluster, *build_datanode);
    assert!(
        actual > baseline,
        "expected the failed nested plan to reach datanode {build_datanode} for region {build_region} over Flight DoGet (count {actual}, baseline {baseline})"
    );
}

async fn build_cluster(test_name: &str) -> GreptimeDbCluster {
    GreptimeDbClusterBuilder::new(test_name)
        .await
        // A datanode serves the inner `MergeScan` of a nested plan by querying the datanodes that
        // own the regions of the inner table, so every datanode must be reachable at the address it
        // registers in metasrv, i.e. the address of the table routes. The cluster then also counts
        // the requests of the remote nodes, see [`assert_cross_datanode_region_query`].
        .with_real_datanode_grpc_addr(true)
        .with_datanodes(2)
        .build(false)
        .await
}

fn assert_cross_datanode_region_query(
    cluster: &GreptimeDbCluster,
    remote_datanodes: &BTreeSet<u64>,
    baseline_requests: &BTreeMap<u64, usize>,
) {
    assert_eq!(
        2,
        remote_datanodes.len(),
        "build table must span both datanodes"
    );
    for datanode in remote_datanodes {
        let actual = rpc_requests(cluster, *datanode);
        let baseline = baseline_requests[datanode];
        assert!(
            actual > baseline,
            "expected a nested Flight DoGet at datanode {datanode}, DoGet count {actual}, baseline {baseline}"
        );
    }
}

fn rpc_requests(cluster: &GreptimeDbCluster, datanode_id: u64) -> usize {
    cluster
        .datanode_rpc_stats(datanode_id)
        .unwrap_or_else(|| panic!("expected the rpc stats of the datanode {datanode_id}"))
        .requests()
}

fn region_server(cluster: &GreptimeDbCluster, datanode_id: u64) -> RegionServer {
    cluster
        .datanode_instances
        .get(&datanode_id)
        .unwrap_or_else(|| panic!("expected a datanode {datanode_id}"))
        .region_server()
}

async fn run_nested_queries(
    cluster: &GreptimeDbCluster,
    probe_leaders: &BTreeMap<u64, (u64, String)>,
    plan: &[u8],
    query_ctx: &QueryContextRef,
) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    for (probe_region, (probe_datanode, _)) in probe_leaders {
        let stream = region_server(cluster, *probe_datanode)
            .handle_remote_read(
                QueryRequest {
                    header: Some(region_request_header(query_ctx)),
                    region_id: *probe_region,
                    plan: plan.to_vec(),
                },
                query_ctx.clone(),
            )
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "the datanode {probe_datanode} must execute the nested plan of the region \
                     {probe_region}: {e}"
                )
            });
        let batches = RecordBatches::try_collect(stream)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "the datanode {probe_datanode} must execute the nested plan of the region \
                     {probe_region} completely: {e}"
                )
            });
        let actual = batches
            .pretty_print()
            .expect("the result of the datanode must be printable");
        rows.extend(table_cells(&actual));
    }

    rows
}

/// Creates the probe and build tables of the tests and inserts the rows.
async fn prepare_tables(frontend: &Arc<Instance>) {
    execute_sql(
        frontend,
        &format!(
            "CREATE TABLE {PROBE_TABLE} (
                a_id INT,
                probe_key INT,
                probe_v INT,
                a_ts TIMESTAMP,
                TIME INDEX (a_ts),
                PRIMARY KEY(a_id)
            )
            PARTITION ON COLUMNS (a_id) (
                a_id < 100,
                a_id >= 100
            )
            engine=mito"
        ),
    )
    .await;
    execute_sql(
        frontend,
        &format!(
            "CREATE TABLE {BUILD_TABLE} (
                b_id INT,
                build_key INT,
                build_v INT,
                b_ts TIMESTAMP,
                TIME INDEX (b_ts),
                PRIMARY KEY(b_id)
            )
            PARTITION ON COLUMNS (b_id) (
                b_id < 100,
                b_id >= 100
            )
            engine=mito"
        ),
    )
    .await;

    execute_sql(
        frontend,
        &format!(
            "INSERT INTO {PROBE_TABLE}(a_id, probe_key, probe_v, a_ts) VALUES {}",
            PROBE_ROWS
                .iter()
                .enumerate()
                .map(|(i, (a_id, probe_key, probe_v))| {
                    format!("({a_id}, {probe_key}, {probe_v}, {}000)", i + 1)
                })
                .collect::<Vec<_>>()
                .join(",")
        ),
    )
    .await;
    execute_sql(
        frontend,
        &format!(
            "INSERT INTO {BUILD_TABLE}(b_id, build_key, build_v, b_ts) VALUES {}",
            BUILD_ROWS
                .iter()
                .enumerate()
                .map(|(i, (b_id, build_key, build_v))| {
                    format!("({b_id}, {build_key}, {build_v}, {}000)", i + 1)
                })
                .collect::<Vec<_>>()
                .join(",")
        ),
    )
    .await;
}

fn encode(plan: &LogicalPlan) -> Vec<u8> {
    DFLogicalSubstraitConvertor
        .encode(plan, DefaultSerializer)
        .expect("the plan must be encodable")
        .to_vec()
}

/// Plans the hand-built nested join sent in the region request.
async fn nested_plan(frontend: &Arc<Instance>, query_ctx: &QueryContextRef) -> LogicalPlan {
    nested_plan_with_inner_table(frontend, query_ctx, BUILD_TABLE).await
}

async fn nested_plan_with_inner_table(
    frontend: &Arc<Instance>,
    query_ctx: &QueryContextRef,
    inner_table: &str,
) -> LogicalPlan {
    let probe = plan_sql(
        frontend,
        &format!("SELECT a_id, probe_key, probe_v FROM {PROBE_TABLE}"),
        query_ctx,
    )
    .await;
    let build = plan_sql(
        frontend,
        &format!("SELECT build_key, build_v FROM {BUILD_TABLE}"),
        query_ctx,
    )
    .await;
    let build = rename_scans(build, inner_table);

    nested_plan_of(&probe, build)
}

fn nested_plan_of(probe: &LogicalPlan, build: LogicalPlan) -> LogicalPlan {
    info!("probe side of the nested plan:\n{probe}");
    info!("build side of the nested plan:\n{build}");

    let inner = MergeScanLogicalPlan::new(build, false, Default::default()).into_logical_plan();
    let plan = LogicalPlanBuilder::from(probe.clone())
        .join(
            inner,
            JoinType::Inner,
            (vec!["probe_key"], vec!["build_key"]),
            None,
        )
        .expect("the join keys must be resolvable in the sides of the join")
        .build()
        .expect("the nested plan must be a valid logical plan");
    info!("nested plan sent to the datanode:\n{plan}");

    plan
}

fn rename_scans(plan: LogicalPlan, table_name: &str) -> LogicalPlan {
    use datafusion::common::TableReference;
    use datafusion::common::tree_node::{Transformed, TreeNode};

    plan.transform_up(|plan| match plan {
        LogicalPlan::TableScan(mut scan) => {
            info!(
                "renaming the scan of the nested plan from {} to {table_name}",
                scan.table_name
            );
            scan.table_name = TableReference::bare(table_name);
            Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
        }
        other => Ok(Transformed::no(other)),
    })
    .expect("the plan must be rewritable")
    .data
}

async fn plan_sql(frontend: &Arc<Instance>, sql: &str, query_ctx: &QueryContextRef) -> LogicalPlan {
    let stmt = QueryLanguageParser::parse_sql(sql, query_ctx)
        .unwrap_or_else(|e| panic!("failed to parse `{sql}`: {e}"));
    frontend
        .statement_executor()
        .plan(&stmt, query_ctx.clone())
        .await
        .unwrap_or_else(|e| panic!("failed to plan `{sql}`: {e}"))
}

/// Frontend reference join over the nested plan's output columns.
fn join_sql() -> String {
    format!(
        "SELECT p.a_id, p.probe_key, p.probe_v, b.build_key, b.build_v
         FROM {PROBE_TABLE} p JOIN {BUILD_TABLE} b ON p.probe_key = b.build_key"
    )
}

/// Expected complete join result, retaining duplicate rows.
fn expected_join_rows() -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    for (a_id, probe_key, probe_v) in PROBE_ROWS {
        for (_, build_key, build_v) in BUILD_ROWS {
            if probe_key == build_key {
                rows.push(vec![
                    a_id.to_string(),
                    probe_key.to_string(),
                    probe_v.to_string(),
                    build_key.to_string(),
                    build_v.to_string(),
                ]);
            }
        }
    }
    multiset(rows)
}

async fn region_leaders(frontend: &Arc<Instance>, table: &str) -> BTreeMap<u64, (u64, String)> {
    use datatypes::arrow::array::AsArray;
    use datatypes::arrow::datatypes::UInt64Type;

    let sql = format!(
        "SELECT region_id, peer_id, peer_addr FROM information_schema.region_peers
         WHERE table_schema = 'public' AND table_name = '{table}' AND is_leader = 'Yes'
         ORDER BY region_id"
    );
    let batches = output_batches(run_sql(frontend, &sql, query_ctx()).await).await;

    let mut leaders = BTreeMap::new();
    for batch in batches.iter() {
        let batch = batch.df_record_batch();
        let region_ids = batch
            .column_by_name("region_id")
            .expect("region_id column")
            .as_primitive::<UInt64Type>();
        let peer_ids = batch
            .column_by_name("peer_id")
            .expect("peer_id column")
            .as_primitive::<UInt64Type>();
        let peer_addrs = batch
            .column_by_name("peer_addr")
            .expect("peer_addr column")
            .as_string::<i32>();
        for row in 0..batch.num_rows() {
            leaders.insert(
                region_ids.value(row),
                (peer_ids.value(row), peer_addrs.value(row).to_string()),
            );
        }
    }

    leaders
}

fn datanodes(leaders: &BTreeMap<u64, (u64, String)>) -> BTreeSet<u64> {
    leaders.values().map(|(id, _)| *id).collect()
}

async fn output_batches(output: Output) -> RecordBatches {
    match output.data {
        common_query::OutputData::Stream(stream) => RecordBatches::try_collect(stream)
            .await
            .expect("the output stream must be collectable"),
        common_query::OutputData::RecordBatches(batches) => batches,
        common_query::OutputData::AffectedRows(rows) => {
            panic!("expected a query output, got {rows} affected rows")
        }
    }
}

/// Consumes the full stream to detect errors after any partial rows.
async fn collect_error(stream: SendableRecordBatchStream) -> Option<String> {
    use futures::TryStreamExt;

    let result = stream.try_collect::<Vec<RecordBatch>>().await;
    match result {
        Ok(rows) => {
            info!(
                "the query returned {} batches instead of failing",
                rows.len()
            );
            None
        }
        Err(error) => Some(error_chain(&error)),
    }
}

fn error_chain(error: &(dyn std::error::Error + 'static)) -> String {
    let mut messages = vec![error.to_string()];
    let mut source = error.source();
    while let Some(inner) = source {
        messages.push(inner.to_string());
        source = inner.source();
    }
    messages.join(": ")
}

async fn query_pretty(frontend: &Arc<Instance>, sql: &str, query_ctx: QueryContextRef) -> String {
    run_sql(frontend, sql, query_ctx)
        .await
        .data
        .pretty_print()
        .await
}

async fn run_sql(frontend: &Arc<Instance>, sql: &str, query_ctx: QueryContextRef) -> Output {
    SqlQueryHandler::do_query(frontend.as_ref(), sql, query_ctx)
        .await
        .remove(0)
        .unwrap()
}

/// Uses one target partition to keep the plans stable.
fn query_ctx() -> QueryContextRef {
    let mut query_ctx = QueryContext::with_db_name(None);
    query_ctx.set_extension(QUERY_PARALLELISM_HINT, "1");
    Arc::new(query_ctx)
}

fn region_request_header(query_ctx: &QueryContextRef) -> RegionRequestHeader {
    RegionRequestHeader {
        query_context: Some(query_ctx.as_ref().into()),
        ..Default::default()
    }
}

fn table_cells(pretty: &str) -> Vec<Vec<String>> {
    let mut lines = pretty.lines().filter(|line| line.starts_with('|'));
    // The first `|` line is the header of the table.
    lines.next();

    lines
        .map(|line| {
            line.trim_matches('|')
                .split('|')
                .map(|cell| cell.trim().to_string())
                .collect()
        })
        .collect()
}

fn multiset(mut rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    rows.sort();
    rows
}
