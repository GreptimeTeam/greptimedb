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

//! End-to-end tests of the PoC nested broadcast join rewrite
//! (`dist_planner.nested_broadcast_join_build_table`) on a cluster with two datanodes.
//!
//! With the switch enabled the frontend rewrites
//! `Join(<proj> MergeScan(a), <proj> MergeScan(b))` into a nested plan
//! `MergeScan(a regions) { Join(<local a region>, MergeScan(all b regions)) }`: the outer
//! `MergeScan` is routed by the regions of the probe table `a`, so the join runs on the datanode
//! that owns a region of `a`, and the inner `MergeScan` makes that datanode query the datanodes
//! that own the regions of the build table `b`.
//!
//! The tests check
//!
//! - that the frontend rewrites the plan (`outer MergeScan { Join(probe region, inner MergeScan) }`)
//!   and routes the outer `MergeScan` by the regions of the probe table only,
//! - that the nested plan returns the same multiset of rows as the default distributed plan
//!   (including the boundary cases: duplicated build keys, NULL join keys, an empty build side and
//!   a build side split over several regions),
//! - that the join is executed by a datanode and not silently by the frontend: the join appears in
//!   the region level (stage >= 1) plans of `EXPLAIN ANALYZE VERBOSE` and not in the plan of the
//!   frontend (stage 0),
//! - that a datanode issues a real region query to the *other* datanode for the regions of the
//!   build table (see [`assert_cross_datanode_region_query`]).
//!
//! [`test_nested_broadcast_join_distinct_build_columns`] covers the build table whose column names
//! differ from the probe table, which is not covered by the other tests: they share the column
//! names of the probe table.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use client::OutputData;
use common_query::Output;
use common_recordbatch::RecordBatches;
use common_telemetry::info;
use datatypes::arrow::array::{Array, AsArray};
use datatypes::arrow::datatypes::{UInt32Type, UInt64Type};
use frontend::instance::Instance;
use query::datafusion::{NESTED_BROADCAST_JOIN_BUILD_TABLE_HINT, QUERY_PARALLELISM_HINT};
use servers::query_handler::sql::SqlQueryHandler;
use session::context::{QueryContext, QueryContextRef};

use crate::cluster::{GreptimeDbCluster, GreptimeDbClusterBuilder};
use crate::test_util::execute_sql;

/// Probe table of the nested broadcast join: partitioned, so that its regions are spread over
/// both datanodes.
const PROBE_TABLE: &str = "nbj_probe";
/// Build table of the nested broadcast join: small, a single region, with duplicate keys.
const BUILD_TABLE: &str = "nbj_build";
/// Build table without any matching row in the probe table.
const NO_MATCH_BUILD_TABLE: &str = "nbj_no_match_build";
/// Probe and build tables whose join key may be NULL.
const NULL_PROBE_TABLE: &str = "nbj_null_probe";
const NULL_BUILD_TABLE: &str = "nbj_null_build";
/// Build table split over two regions.
const MULTI_BUILD_TABLE: &str = "nbj_multi_build";

/// `(k, kk, v)` of the probe tables: `k` is the primary key (and the partition column of the
/// partitioned tables), `kk` is the join key and `v` is the value that the join filters.
///
/// The nested join of the build side is decoded with the schema of the build table, so the probe
/// and build tables of the tests share the same column names and types.
const PROBE_ROWS: &[(i32, Option<i32>, i32)] = &[
    (1, Some(1), 10),      // 0: can match the duplicated build key `1`
    (2, Some(50), 20),     // 1: no row in the build table
    (120, Some(120), 110), // 2: matches the build key `120`
    (150, None, 90),       // 3: NULL join key
    (210, None, 300),      // 4: NULL join key
    (320, Some(320), 5),   // 5: matches the build key `320`
    (520, Some(600), 900), // 6: matches the build key `600` of `nbj_multi_build`
    (480, Some(480), 30),  // 7: the build row `(480, 480, 0)` is filtered by `v > v`
];

/// `(k, kk, v)` of the build tables: `k` is the primary key, `kk` is the join key, `v` is the
/// threshold of the join filter.
///
/// The join keys `1` (twice) and `480` are duplicated and no row has the join key of the probe
/// key `2`: the tests cover both the multiplication of the rows and a build row that the join
/// filter drops.
const BUILD_ROWS: &[(i32, Option<i32>, i32)] = &[
    (1, Some(1), 0),
    (2, Some(1), 5),
    (3, Some(120), 100),
    (4, Some(320), 0),
    (5, Some(480), 30),
    (6, Some(999), 0),
];

/// `(k, kk, v)` of `nbj_multi_build`, the build table split over two regions.
const MULTI_BUILD_ROWS: &[(i32, Option<i32>, i32)] =
    &[(1, Some(1), 0), (2, Some(120), 100), (600, Some(600), 500)];

/// `(k, kk, v)` of [`NULL_PROBE_TABLE`]: the join keys `kk` are NULL for the primary keys `1` and
/// `10`.
const NULL_PROBE_ROWS: &[(i32, Option<i32>, i32)] = &[
    (1, None, 10),
    (2, Some(1), 20),
    (5, Some(2), 30),
    (10, None, 40),
    (11, Some(3), 50),
];

/// `(k, kk, v)` of [`NULL_BUILD_TABLE`].
const NULL_BUILD_ROWS: &[(i32, Option<i32>, i32)] = &[
    (1, Some(1), 0),
    (2, None, 0),
    (3, Some(3), 45),
    (4, Some(4), 0),
];

/// The frontend rewrite and the result of the default distributed plan.
#[tokio::test(flavor = "multi_thread")]
async fn test_nested_broadcast_join_fe_plan_shape() {
    common_telemetry::init_default_ut_logging();

    let cluster = build_two_datanode_cluster("test_nested_broadcast_join_fe_plan_shape").await;
    let frontend = cluster.fe_instance().clone();

    prepare_tables(&frontend, true).await;

    // The outer `MergeScan` of the nested plan is routed by the regions of the probe table, so the
    // probe table must have a region on each datanode: that is what makes the join of every
    // partition read the build table from another datanode as well.
    let probe_leaders = region_leaders(&frontend, PROBE_TABLE).await;
    let build_leaders = region_leaders(&frontend, BUILD_TABLE).await;
    info!("region leaders of {PROBE_TABLE}: {probe_leaders:?}");
    info!("region leaders of {BUILD_TABLE}: {build_leaders:?}");
    assert!(
        datanodes(&probe_leaders).len() >= 2,
        "expected the regions of {PROBE_TABLE} to be spread over both datanodes, actual region \
         leaders: {probe_leaders:?}"
    );
    assert!(
        !build_leaders.is_empty(),
        "expected {BUILD_TABLE} to have a region, actual region leaders: {build_leaders:?}"
    );

    let join_sql = join_sql(PROBE_TABLE, BUILD_TABLE);
    let baseline_ctx = query_ctx();
    let nested_ctx = query_ctx();
    // The `SET` statement is one of the injection paths of the PoC switch, see
    // `set_nested_broadcast_join_build_table`. It configures the session of the query context.
    enable_nested_broadcast_join_by_set(&frontend, &nested_ctx, BUILD_TABLE).await;

    // The default distributed plan is unchanged and returns the expected rows.
    let baseline = query_pretty(&frontend, &join_sql, baseline_ctx.clone()).await;
    info!("join result of the default distributed plan:\n{baseline}");
    assert_eq!(
        expected_join_rows(PROBE_ROWS, BUILD_ROWS),
        multiset(table_cells(&baseline)),
        "unexpected result of the default distributed plan:\n{baseline}"
    );

    // The switch rewrites the plan of the frontend.
    let explain_verbose = format!("EXPLAIN VERBOSE {join_sql}");
    let (baseline_logical, baseline_physical) =
        explain_verbose_plans(&frontend, &explain_verbose, baseline_ctx).await;
    let (nested_logical, nested_physical) =
        explain_verbose_plans(&frontend, &explain_verbose, nested_ctx).await;
    info!("logical plan of the default distributed plan:\n{baseline_logical}");
    info!("logical plan of the nested plan:\n{nested_logical}");
    info!("physical plan of the default distributed plan:\n{baseline_physical}");
    info!("physical plan of the nested plan:\n{nested_physical}");

    // The default plan keeps the join above the two `MergeScan`s of the frontend plan, and the
    // frontend executes the join.
    let baseline_shape = squeeze_whitespace(&baseline_logical);
    assert!(
        !baseline_shape.contains("remote_input=[ Inner Join"),
        "expected the default distributed plan to keep the join above both MergeScans, actual \
         logical plan:\n{baseline_logical}"
    );
    assert!(
        baseline_physical.contains("HashJoinExec"),
        "expected the default distributed plan to run the join on the frontend:\n{baseline_physical}"
    );

    // The nested plan is `MergeScan(Join(<local probe region>, <build side as is>))`, so the outer
    // `MergeScan` wraps the join, which stays above the inner `MergeScan` of the build side.
    let nested_shape = squeeze_whitespace(&nested_logical);
    assert!(
        nested_shape.contains("remote_input=[ Inner Join"),
        "expected the outer MergeScan of the nested plan to wrap the join, actual logical \
         plan:\n{nested_logical}"
    );
    assert!(
        nested_shape
            .matches("MergeScan [is_placeholder=false")
            .count()
            >= 2,
        "expected the nested plan to keep the inner MergeScan of the build side, actual logical \
         plan:\n{nested_logical}"
    );
    // The join is pushed down into the outer `MergeScan`: the frontend only merges the results of
    // the region level joins, it does not execute the join itself.
    assert!(
        nested_physical.contains("MergeScanExec"),
        "expected the nested plan to read the probe table regions with a MergeScan:\n{nested_physical}"
    );
    assert!(
        !nested_physical.contains("HashJoinExec"),
        "expected the join of the nested plan to be pushed down instead of being executed by the \
         frontend:\n{nested_physical}"
    );

    // The outer `MergeScan` is routed by the regions of the probe table, not by the regions of the
    // build table.
    let nested_regions = merge_scan_region_ids(&nested_physical);
    let probe_regions = probe_leaders.keys().copied().collect::<BTreeSet<_>>();
    assert_eq!(
        probe_regions,
        nested_regions.iter().copied().collect::<BTreeSet<_>>(),
        "expected the outer MergeScanExec of the nested plan to scan exactly the regions of \
         {PROBE_TABLE} {probe_regions:?}, actual physical plan:\n{nested_physical}"
    );
    assert!(
        nested_regions
            .iter()
            .all(|region| !build_leaders.contains_key(region)),
        "expected the outer MergeScanExec of the nested plan not to scan a region of \
         {BUILD_TABLE}, actual physical plan:\n{nested_physical}"
    );
}

/// The end-to-end check of the nested plan: the same result as the default plan, the join executed
/// by a datanode and a real region query from that datanode to the other datanode.
#[tokio::test(flavor = "multi_thread")]
async fn test_nested_broadcast_join_merge_scan_e2e() {
    common_telemetry::init_default_ut_logging();

    let cluster = build_two_datanode_cluster("test_nested_broadcast_join_merge_scan_e2e").await;
    let frontend = cluster.fe_instance().clone();

    prepare_tables(&frontend, true).await;

    let probe_leaders = region_leaders(&frontend, PROBE_TABLE).await;
    let build_leaders = region_leaders(&frontend, BUILD_TABLE).await;
    info!("region leaders of {PROBE_TABLE}: {probe_leaders:?}");
    info!("region leaders of {BUILD_TABLE}: {build_leaders:?}");
    assert!(
        datanodes(&probe_leaders).len() >= 2,
        "expected the regions of {PROBE_TABLE} to be spread over both datanodes, actual region \
         leaders: {probe_leaders:?}"
    );

    let join_sql = join_sql(PROBE_TABLE, BUILD_TABLE);
    let baseline_ctx = query_ctx();
    let nested_ctx = query_ctx();
    enable_nested_broadcast_join_by_set(&frontend, &nested_ctx, BUILD_TABLE).await;

    // 1. A/B: the nested plan returns the same rows as the default distributed plan.
    let baseline = query_pretty(&frontend, &join_sql, baseline_ctx.clone()).await;
    let nested = query_pretty(&frontend, &join_sql, nested_ctx.clone()).await;
    info!("join result of the default distributed plan:\n{baseline}");
    info!("join result of the nested plan:\n{nested}");
    assert_eq!(
        expected_join_rows(PROBE_ROWS, BUILD_ROWS),
        multiset(table_cells(&baseline)),
        "unexpected result of the default distributed plan:\n{baseline}"
    );
    assert_eq!(
        multiset(table_cells(&baseline)),
        multiset(table_cells(&nested)),
        "the nested plan returned a different multiset of rows than the default plan:\n\
         default plan:\n{baseline}\nnested plan:\n{nested}"
    );

    // 2. The join runs on a datanode. `EXPLAIN ANALYZE VERBOSE` reports the metrics of every stage:
    // stage 0 is the frontend plan, stage 1 and above are the region level plans that the datanodes
    // executed (see `DistAnalyzeExec` and `MergeScanExec::sub_stage_metrics`). A silent fallback to
    // the default distributed plan would run the join on the frontend, i.e. in stage 0.
    let datanode_join_plans = assert_join_runs_on_datanode(&frontend, &join_sql, nested_ctx).await;

    // 3. The nested `MergeScan` of those datanode plans reads the regions of the build table from
    // the datanodes that own them: for the regions of the probe table that are owned by the other
    // datanode, the datanode issues a region query over the network.
    assert_cross_datanode_region_query(
        &cluster,
        &datanode_join_plans,
        &probe_leaders,
        &build_leaders,
        PROBE_TABLE,
        BUILD_TABLE,
    );
}

/// The boundary cases of the nested plan: duplicated build keys, the build rows that the join
/// filter drops, NULL join keys, an empty build side and a build side split over several regions.
#[tokio::test(flavor = "multi_thread")]
async fn test_nested_broadcast_join_merge_scan_boundary_cases() {
    common_telemetry::init_default_ut_logging();

    let cluster =
        build_two_datanode_cluster("test_nested_broadcast_join_merge_scan_boundary_cases").await;
    let frontend = cluster.fe_instance().clone();

    prepare_tables(&frontend, true).await;

    // Boundary case 1: the duplicated build keys `1` multiply the probe row, and the build row of
    // the join key `480` is dropped by the join filter.
    let baseline = query_pretty(&frontend, &join_sql(PROBE_TABLE, BUILD_TABLE), query_ctx()).await;
    info!("join of the build table with duplicated keys:\n{baseline}");
    assert_eq!(
        expected_join_rows(PROBE_ROWS, BUILD_ROWS),
        multiset(table_cells(&baseline)),
        "unexpected result of the default distributed plan:\n{baseline}"
    );
    let build_key_1_rows = table_cells(&baseline)
        .iter()
        .filter(|row| row.first().is_some_and(|k| k.as_str() == "1"))
        .count();
    assert_eq!(
        2, build_key_1_rows,
        "expected the probe row of the duplicated build key `1` to appear twice:\n{baseline}"
    );
    let nested = assert_nested_join_matches_baseline(
        &frontend,
        PROBE_TABLE,
        BUILD_TABLE,
        &join_sql(PROBE_TABLE, BUILD_TABLE),
        &expected_join_rows(PROBE_ROWS, BUILD_ROWS),
    )
    .await;
    let nested_cells = table_cells(&nested);
    assert_eq!(
        2,
        nested_cells
            .iter()
            .filter(|row| row.first().is_some_and(|k| k.as_str() == "1"))
            .count(),
        "expected the probe row of the duplicated build key `1` to appear twice in the result of \
         the nested plan:\n{nested}"
    );
    assert!(
        !nested_cells.iter().any(|row| row[1] == "480"),
        "expected the build row of the join key `480` to be dropped by the join filter (`480 > 480` \
         is false), actual result:\n{nested}"
    );

    // Boundary case 2: the build table without any matching row in the probe table returns no row.
    assert_nested_join_matches_baseline(
        &frontend,
        PROBE_TABLE,
        NO_MATCH_BUILD_TABLE,
        &join_sql(PROBE_TABLE, NO_MATCH_BUILD_TABLE),
        &[],
    )
    .await;

    // Boundary case 3: NULL join keys never match.
    prepare_null_tables(&frontend).await;
    assert_nested_join_matches_baseline(
        &frontend,
        NULL_PROBE_TABLE,
        NULL_BUILD_TABLE,
        &join_sql(NULL_PROBE_TABLE, NULL_BUILD_TABLE),
        &expected_join_rows(NULL_PROBE_ROWS, NULL_BUILD_ROWS),
    )
    .await;

    // Boundary case 4: the build table has two regions, every region of the probe table reads both
    // of them. The rewrite is enabled with the query context hint here, i.e. the injection path of
    // the `x-greptime-hint` header.
    let multi_build_leaders = region_leaders(&frontend, MULTI_BUILD_TABLE).await;
    info!("region leaders of {MULTI_BUILD_TABLE}: {multi_build_leaders:?}");
    assert!(
        multi_build_leaders.len() >= 2,
        "expected {MULTI_BUILD_TABLE} to have at least two regions, actual region leaders: \
         {multi_build_leaders:?}"
    );
    let multi_build_join_sql = join_sql(PROBE_TABLE, MULTI_BUILD_TABLE);
    let nested = assert_nested_join_matches_baseline_with_hint(
        &frontend,
        MULTI_BUILD_TABLE,
        &multi_build_join_sql,
        &expected_join_rows(PROBE_ROWS, MULTI_BUILD_ROWS),
    )
    .await;
    // `(1, 1, 10, 0)` comes from the region `k < 300` of the build table and `(520, 600, 900, 500)`
    // from the region `k >= 300`: the join of a probe region reads the complete build table.
    let nested_cells = table_cells(&nested);
    for expected_row in [
        row(&["1", "1", "10", "0"]),
        row(&["520", "600", "900", "500"]),
    ] {
        assert!(
            nested_cells.contains(&expected_row),
            "expected the row {expected_row:?} in the result of the nested plan, actual result:\n{nested}"
        );
    }
}

/// The build table whose column names differ from the probe table.
///
/// The build side of the nested plan is decoded by the datanode that executes the join, with the
/// schema of the build table. It used to be decoded with the schema of the region that datanode
/// reads: the tables of a `MergeScan` payload were resolved with the catalog list of the request,
/// which on a datanode binds every table name to the region of the request (`NameAwareCatalogList`),
/// so the build table was decoded with the columns of the probe table (`k`, `v`, `ts` instead of
/// `k`, `threshold`, `ts`), the join condition could not be bound and the region query of the outer
/// `MergeScan` failed:
///
/// ```text
/// ERROR ... datanode::error: Failed to handle request err=0: Failed to decode logical plan
/// 1: Failed to decode logical plan: Failed to decode DataFusion plan
/// 2: Failed to decode DataFusion plan
/// 3: External(SchemaError(FieldNotFound { field: Column { relation: None, name: "threshold" },
///      valid_fields: [Column { relation: Some(Bare { table: "nbj_distinct_build" }), name: "k" },
///      ... name: "v" }, ... name: "ts" }] }))
/// ```
///
/// The payload of a `MergeScan` is now decoded with the catalog of the query engine, which knows
/// every table of the cluster, see `query::query_engine::default_serializer`.
#[tokio::test(flavor = "multi_thread")]
async fn test_nested_broadcast_join_distinct_build_columns() {
    common_telemetry::init_default_ut_logging();

    let cluster =
        build_two_datanode_cluster("test_nested_broadcast_join_distinct_build_columns").await;
    let frontend = cluster.fe_instance().clone();

    prepare_tables(&frontend, false).await;

    // The build table of this test has a `threshold` column instead of `v`.
    let join_sql = "SELECT p.k AS k, p.kk AS kk, p.v AS v, b.threshold AS bv
         FROM nbj_probe p JOIN nbj_build b ON p.kk = b.kk
         WHERE p.v > b.threshold
         ORDER BY p.k, b.threshold"
        .to_string();
    let baseline = query_pretty(&frontend, &join_sql, query_ctx()).await;
    // The last column of the result is the `threshold` of the build table, whose column name
    // differs from the probe table: the first three columns are the ones of the probe table.
    let expected = expected_join_rows(PROBE_ROWS, BUILD_ROWS)
        .into_iter()
        .map(|row| row[..3].to_vec())
        .collect::<Vec<_>>();
    assert_eq!(
        multiset(expected),
        multiset(baseline_cells(&baseline)),
        "unexpected result of the default distributed plan:\n{baseline}"
    );

    let nested_ctx = query_ctx();
    enable_nested_broadcast_join_by_set(&frontend, &nested_ctx, BUILD_TABLE).await;
    let nested = query_pretty(&frontend, &join_sql, nested_ctx).await;
    assert_eq!(
        multiset(baseline_cells(&baseline)),
        multiset(baseline_cells(&nested)),
        "the nested plan returned a different multiset of rows than the default plan:\n\
         default plan:\n{baseline}\nnested plan:\n{nested}"
    );
}

/// Joins `probe` and `build`, runs the query with and without the nested broadcast join switch and
/// asserts that both plans return the same multiset of rows, equal to `expected`.
///
/// Returns the pretty printed result of the nested plan.
async fn assert_nested_join_matches_baseline(
    frontend: &Arc<Instance>,
    probe: &str,
    build: &str,
    join_sql: &str,
    expected: &[Vec<String>],
) -> String {
    let baseline_ctx = query_ctx();
    let nested_ctx = query_ctx();
    enable_nested_broadcast_join_by_set(frontend, &nested_ctx, build).await;

    let baseline = query_pretty(frontend, join_sql, baseline_ctx).await;
    let nested = query_pretty(frontend, join_sql, nested_ctx.clone()).await;
    info!("join of {probe} and {build} with the default distributed plan:\n{baseline}");
    info!("join of {probe} and {build} with the nested plan:\n{nested}");

    assert_eq!(
        multiset(expected.to_vec()),
        multiset(table_cells(&baseline)),
        "unexpected result of the default distributed plan:\n{baseline}"
    );
    assert_eq!(
        multiset(table_cells(&baseline)),
        multiset(table_cells(&nested)),
        "the nested plan returned a different multiset of rows than the default plan:\n\
         default plan:\n{baseline}\nnested plan:\n{nested}"
    );
    // The boundary cases must be covered by the nested plan, not by a fallback.
    assert_join_runs_on_datanode(frontend, join_sql, nested_ctx).await;

    nested
}

/// Same as [`assert_nested_join_matches_baseline`], but enables the rewrite with the query context
/// hint (the path of the `x-greptime-hint` header) instead of the `SET` statement.
async fn assert_nested_join_matches_baseline_with_hint(
    frontend: &Arc<Instance>,
    build: &str,
    join_sql: &str,
    expected: &[Vec<String>],
) -> String {
    let baseline = query_pretty(frontend, join_sql, query_ctx()).await;

    let mut nested_ctx = QueryContext::with_db_name(None);
    nested_ctx.set_extension(QUERY_PARALLELISM_HINT, "1");
    nested_ctx.set_extension(NESTED_BROADCAST_JOIN_BUILD_TABLE_HINT, build);
    let nested_ctx = Arc::new(nested_ctx);
    let nested = query_pretty(frontend, join_sql, nested_ctx.clone()).await;
    info!("join with the build table {build} and the default distributed plan:\n{baseline}");
    info!("join with the build table {build} and the nested plan:\n{nested}");
    assert_eq!(
        multiset(expected.to_vec()),
        multiset(table_cells(&baseline)),
        "unexpected result of the default distributed plan:\n{baseline}"
    );
    assert_eq!(
        multiset(table_cells(&baseline)),
        multiset(table_cells(&nested)),
        "the nested plan returned a different multiset of rows than the default plan:\n\
         default plan:\n{baseline}\nnested plan:\n{nested}"
    );
    // The boundary case must be covered by the nested plan, not by a fallback.
    assert_join_runs_on_datanode(frontend, join_sql, nested_ctx).await;

    nested
}

async fn build_two_datanode_cluster(test_name: &str) -> GreptimeDbCluster {
    GreptimeDbClusterBuilder::new(test_name)
        .await
        // A datanode serves the region query of the nested plan's build side by querying the
        // datanodes that own its regions, so it must be reachable at the address it registers in
        // metasrv, i.e. the address the table routes point to. The cluster then also counts the
        // requests of the remote nodes (see `assert_cross_datanode_region_query`).
        .with_real_datanode_grpc_addr(true)
        .with_datanodes(2)
        .build(false)
        .await
}

/// Creates the probe and build tables of the tests (see `PROBE_ROWS` and `BUILD_ROWS`) and inserts
/// the rows.
///
/// With `same_build_columns`, the build table shares the column names of the probe table. The
/// nested join is decoded with the schema of the build table, see
/// [`test_nested_broadcast_join_distinct_build_columns`] for the case where it doesn't.
async fn prepare_tables(frontend: &Arc<Instance>, same_build_columns: bool) {
    execute_sql(
        frontend,
        &format!(
            "CREATE TABLE {PROBE_TABLE} (
                k INT,
                kk INT,
                v INT,
                ts TIMESTAMP,
                TIME INDEX (ts),
                PRIMARY KEY(k)
            )
            PARTITION ON COLUMNS (k) (
                k < 150,
                k >= 150 AND k < 300,
                k >= 300 AND k < 500,
                k >= 500
            )
            engine=mito"
        ),
    )
    .await;

    let build_columns = if same_build_columns {
        "k INT, kk INT, v INT"
    } else {
        "k INT, kk INT, threshold INT"
    };
    for table in [BUILD_TABLE, NO_MATCH_BUILD_TABLE] {
        execute_sql(
            frontend,
            &format!(
                "CREATE TABLE {table} (
                    {build_columns},
                    ts TIMESTAMP,
                    TIME INDEX (ts),
                    PRIMARY KEY(k)
                )
                engine=mito"
            ),
        )
        .await;
    }
    execute_sql(
        frontend,
        &format!(
            "CREATE TABLE {MULTI_BUILD_TABLE} (
                {build_columns},
                ts TIMESTAMP,
                TIME INDEX (ts),
                PRIMARY KEY(k)
            )
            PARTITION ON COLUMNS (k) (
                k < 300,
                k >= 300
            )
            engine=mito"
        ),
    )
    .await;

    let value_column = if same_build_columns { "v" } else { "threshold" };
    insert_rows(frontend, PROBE_TABLE, "kk, v", PROBE_ROWS).await;
    insert_rows(
        frontend,
        BUILD_TABLE,
        &format!("kk, {value_column}"),
        BUILD_ROWS,
    )
    .await;
    insert_rows(
        frontend,
        MULTI_BUILD_TABLE,
        &format!("kk, {value_column}"),
        MULTI_BUILD_ROWS,
    )
    .await;
    // The build table without any matching row in the probe table.
    execute_sql(
        frontend,
        &format!(
            "INSERT INTO {NO_MATCH_BUILD_TABLE}(k, kk, {value_column}, ts) VALUES (1, 1000, 0, 1000)"
        ),
    )
    .await;
}

/// Creates the probe and build tables whose join key may be NULL and inserts the rows.
async fn prepare_null_tables(frontend: &Arc<Instance>) {
    for (table, partition) in [
        (
            NULL_PROBE_TABLE,
            "PARTITION ON COLUMNS (k) (
                k < 10,
                k >= 10
            )
            engine=mito",
        ),
        (NULL_BUILD_TABLE, "engine=mito"),
    ] {
        execute_sql(
            frontend,
            &format!(
                "CREATE TABLE {table} (
                    k INT,
                    kk INT,
                    v INT,
                    ts TIMESTAMP,
                    TIME INDEX (ts),
                    PRIMARY KEY(k)
                )
                {partition}"
            ),
        )
        .await;
    }

    insert_rows(frontend, NULL_PROBE_TABLE, "kk, v", NULL_PROBE_ROWS).await;
    insert_rows(frontend, NULL_BUILD_TABLE, "kk, v", NULL_BUILD_ROWS).await;
}

/// Inserts the rows of a table, whose `(k, kk, v)` values are `rows`.
async fn insert_rows(
    frontend: &Arc<Instance>,
    table: &str,
    values_columns: &str,
    rows: &[(i32, Option<i32>, i32)],
) {
    let values = rows
        .iter()
        .enumerate()
        .map(|(i, (k, kk, v))| {
            let kk = kk
                .map(|kk| kk.to_string())
                .unwrap_or_else(|| "NULL".to_string());
            format!("({k}, {kk}, {v}, {}000)", i + 1)
        })
        .collect::<Vec<_>>()
        .join(",");
    execute_sql(
        frontend,
        &format!("INSERT INTO {table}(k, {values_columns}, ts) VALUES {values}"),
    )
    .await;
}

/// The join of the tests: an equi-join on `kk` plus a cross table residual predicate, which must
/// stay in the join of both plans.
fn join_sql(probe: &str, build: &str) -> String {
    format!(
        "SELECT p.k AS k, p.kk AS kk, p.v AS v, b.v AS bv
         FROM {probe} p JOIN {build} b ON p.kk = b.kk
         WHERE p.v > b.v
         ORDER BY p.k, b.v"
    )
}

/// The rows of [`join_sql`], which joins the probe rows `probe` and the build rows `build`:
/// `(probe k, probe kk, probe v, build v)`.
fn expected_join_rows(
    probe: &[(i32, Option<i32>, i32)],
    build: &[(i32, Option<i32>, i32)],
) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    for (k, kk, v) in probe {
        for (_, build_kk, build_v) in build {
            // A NULL join key never matches, and a NULL/unknown comparison is filtered out.
            if kk.is_some() && kk == build_kk && *v > *build_v {
                rows.push(vec![
                    k.to_string(),
                    kk.unwrap().to_string(),
                    v.to_string(),
                    build_v.to_string(),
                ]);
            }
        }
    }
    multiset(rows)
}

/// Returns the region leaders of `table`: region id -> (datanode id, datanode address).
async fn region_leaders(frontend: &Arc<Instance>, table: &str) -> BTreeMap<u64, (u64, String)> {
    let sql = format!(
        "SELECT region_id, peer_id, peer_addr FROM information_schema.region_peers
         WHERE table_schema = 'public' AND table_name = '{table}' AND is_leader = 'Yes'
         ORDER BY region_id"
    );
    let batches = output_batches(run_sql(frontend, &sql, query_ctx()).await).await;

    let mut leaders = BTreeMap::new();
    for batch in batches.take() {
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

/// The datanode ids of the region leaders.
fn datanodes(leaders: &BTreeMap<u64, (u64, String)>) -> BTreeSet<u64> {
    leaders.values().map(|(id, _)| *id).collect()
}

/// Asserts that the nested plan executes the join on a datanode and returns the datanode plans that
/// contain the join.
///
/// `EXPLAIN ANALYZE VERBOSE` reports the metrics of every stage: stage 0 is the frontend plan,
/// stage 1 and above are the region level plans the datanodes executed (see `DistAnalyzeExec` and
/// `MergeScanExec::sub_stage_metrics`). A silent fallback to the default distributed plan would run
/// the join on the frontend, i.e. in stage 0.
async fn assert_join_runs_on_datanode(
    frontend: &Arc<Instance>,
    join_sql: &str,
    query_ctx: QueryContextRef,
) -> Vec<(Option<u32>, String)> {
    let explain_analyze = format!("EXPLAIN ANALYZE VERBOSE {join_sql}");
    let stages = explain_analyze_stages(run_sql(frontend, &explain_analyze, query_ctx).await).await;
    for (stage, plan) in &stages {
        info!("stage {stage:?} plan:\n{plan}");
    }

    let datanode_stages = stages
        .iter()
        .filter(|(stage, _)| stage.is_some_and(|stage| stage >= 1))
        .collect::<Vec<_>>();
    assert!(
        !datanode_stages.is_empty(),
        "expected the region level (stage >= 1) plans in the analyze output of `{join_sql}`, \
         actual stages: {stages:?}"
    );

    let datanode_join_plans = datanode_stages
        .iter()
        .filter(|(_, plan)| plan.contains("HashJoinExec"))
        .map(|plan| (*plan).clone())
        .collect::<Vec<_>>();
    assert!(
        !datanode_join_plans.is_empty(),
        "expected the join (HashJoinExec) to be executed by a datanode (stage >= 1) for \
         `{join_sql}`, the join of the nested plan must not be executed by the frontend, actual \
         datanode plans: {datanode_stages:?}"
    );

    let frontend_plans = stages
        .iter()
        .filter(|(stage, _)| *stage == Some(0))
        .collect::<Vec<_>>();
    assert!(
        !frontend_plans
            .iter()
            .any(|(_, plan)| plan.contains("HashJoinExec")),
        "expected the frontend (stage 0) not to execute the join of the nested plan, actual \
         frontend plan: {frontend_plans:?}"
    );

    for (stage, plan) in &datanode_join_plans {
        let probe_regions = seq_scan_region_ids(plan);
        let build_regions = merge_scan_region_ids(plan);
        assert!(
            !probe_regions.is_empty(),
            "expected the plan of stage {stage:?} to scan the region of the probe table that it \
             owns, actual plan:\n{plan}"
        );
        assert!(
            !build_regions.is_empty(),
            "expected the plan of stage {stage:?} to read the regions of the build table with an \
             inner MergeScan, actual plan:\n{plan}"
        );
    }

    datanode_join_plans
}

/// Asserts that a datanode issues a real region query to the other datanode for the regions of the
/// build table.
///
/// A datanode plan that contains the join is the plan of one region of the probe table that a
/// datanode owns, and its inner `MergeScanExec` reads the regions of the build table. When the
/// datanode that owns that probe region is not the datanode that owns a build region, the inner
/// `MergeScanExec` has to query the other datanode.
///
/// The cluster serves every datanode at its own address, while the frontend and the local
/// bookkeeping of the test process reach a datanode through an in-process client, so a network
/// request recorded at the address of a datanode comes from a remote node: a request of the
/// `DoGet` (a region query) or of the region service proves the datanode to datanode region query.
fn assert_cross_datanode_region_query(
    cluster: &GreptimeDbCluster,
    datanode_join_plans: &[(Option<u32>, String)],
    probe_leaders: &BTreeMap<u64, (u64, String)>,
    build_leaders: &BTreeMap<u64, (u64, String)>,
    probe_table: &str,
    build_table: &str,
) {
    let mut expected_remote_datanodes = BTreeSet::new();
    for (stage, plan) in datanode_join_plans {
        let probe_regions = seq_scan_region_ids(plan);
        let build_regions = merge_scan_region_ids(plan);
        for probe_region in &probe_regions {
            let (probe_datanode, probe_addr) =
                probe_leaders.get(probe_region).unwrap_or_else(|| {
                    panic!("expected {probe_region} to be a region of {probe_table}")
                });
            for build_region in &build_regions {
                let (build_datanode, build_addr) =
                    build_leaders.get(build_region).unwrap_or_else(|| {
                        panic!("expected {build_region} to be a region of {build_table}")
                    });
                if probe_datanode != build_datanode {
                    info!(
                        "the datanode {probe_datanode} ({probe_addr}) executing the region \
                         {probe_region} of {probe_table} queries the region {build_region} of \
                         {build_table} from the datanode {build_datanode} ({build_addr}), stage \
                         {stage:?}"
                    );
                    expected_remote_datanodes.insert(*build_datanode);
                }
            }
        }
    }
    assert!(
        !expected_remote_datanodes.is_empty(),
        "expected at least one datanode to read a region of {build_table} that is owned by the \
         other datanode, probe region leaders: {probe_leaders:?}, build region leaders: \
         {build_leaders:?}, datanode plans: {datanode_join_plans:?}"
    );

    for datanode in expected_remote_datanodes {
        let stats = cluster
            .datanode_rpc_stats(datanode)
            .unwrap_or_else(|| panic!("expected the rpc stats of the datanode {datanode}"));
        let paths = stats.paths();
        info!(
            "the datanode {datanode} served {} remote gRPC requests, paths: {paths:?}",
            stats.requests()
        );
        assert!(
            paths
                .iter()
                .any(|path| path.contains("DoGet") || path.contains("Region")),
            "expected the datanode {datanode} to serve the region query of the other datanode, \
             actual remote gRPC paths: {paths:?}"
        );
    }
}

/// Parses the region ids of the `SeqScan` nodes of a plan text, e.g.
/// `SeqScan: region=4411000000(123, 1)`.
fn seq_scan_region_ids(plan: &str) -> Vec<u64> {
    plan.match_indices("SeqScan: region=")
        .filter_map(|(idx, matched)| leading_u64(&plan[idx + matched.len()..]))
        .collect()
}

/// Parses the region ids of the `MergeScanExec` nodes of a plan text, e.g.
/// `MergeScanExec: peers=[4411000000(123, 1), 4411000001(123, 2), ]`.
fn merge_scan_region_ids(plan: &str) -> Vec<u64> {
    plan.match_indices("MergeScanExec: peers=[")
        .flat_map(|(idx, matched)| {
            let regions = &plan[idx + matched.len()..];
            let Some(end) = regions.find(']') else {
                return Vec::new();
            };
            region_ids_in(&regions[..end])
        })
        .collect()
}

/// Parses the leading `u64` of `text`, i.e. the region id of the `SeqScan: region=<region id>`
/// display.
fn leading_u64(text: &str) -> Option<u64> {
    text.chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>()
        .parse()
        .ok()
}

/// Parses every region id of `text`.
///
/// A region id is printed as `<id>(<table id>, <region number>)` (see the `Debug` impl of
/// `RegionId`), so the region ids are the numbers followed by an opening parenthesis.
fn region_ids_in(text: &str) -> Vec<u64> {
    let mut ids = Vec::new();
    let mut digits = String::new();
    for ch in text.chars() {
        if ch.is_ascii_digit() {
            digits.push(ch);
            continue;
        }
        if ch == '(' {
            ids.extend(digits.parse::<u64>().ok());
        }
        digits.clear();
    }
    ids
}

/// Runs `sql` on the frontend and returns the pretty printed result.
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

/// The query context of a new session, with a single target partition so that the plans of the
/// tests are stable.
fn query_ctx() -> QueryContextRef {
    let mut query_ctx = QueryContext::with_db_name(None);
    query_ctx.set_extension(QUERY_PARALLELISM_HINT, "1");
    Arc::new(query_ctx)
}

/// Enables the nested broadcast join rewrite for `build_table` in the session of `query_ctx`.
async fn enable_nested_broadcast_join_by_set(
    frontend: &Arc<Instance>,
    query_ctx: &QueryContextRef,
    build_table: &str,
) {
    let sql = format!("SET dist_planner.nested_broadcast_join_build_table = '{build_table}'");
    let output = run_sql(frontend, &sql, query_ctx.clone()).await;
    info!("{sql}: {}", output.data.pretty_print().await);
}

/// Returns the `(logical, physical)` plans of an `EXPLAIN VERBOSE` output.
async fn explain_verbose_plans(
    frontend: &Arc<Instance>,
    sql: &str,
    query_ctx: QueryContextRef,
) -> (String, String) {
    let batches = output_batches(run_sql(frontend, sql, query_ctx).await).await;

    let mut logical_plans = Vec::new();
    let mut physical_plans = Vec::new();
    for batch in batches.take() {
        let plan_types = batch
            .column_by_name("plan_type")
            .expect("plan_type column")
            .as_string::<i32>();
        let plans_column = batch
            .column_by_name("plan")
            .expect("plan column")
            .as_string::<i32>();
        for row in 0..batch.num_rows() {
            let plan_type = plan_types.value(row);
            if plan_type.contains("logical") {
                logical_plans.push(plans_column.value(row).to_string());
            } else if plan_type.contains("physical") {
                physical_plans.push(plans_column.value(row).to_string());
            }
        }
    }

    assert!(
        !logical_plans.is_empty() && !physical_plans.is_empty(),
        "expected the logical and physical plans in the output of `{sql}`"
    );
    (logical_plans.join("\n"), physical_plans.join("\n"))
}

/// Replaces every whitespace sequence of `text` with a single space, so that a plan text can be
/// matched independent of indentation and line wrapping.
fn squeeze_whitespace(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Returns the `(stage, plan)` rows of an `EXPLAIN ANALYZE VERBOSE` output. The stage of the last
/// row, the total number of rows, is `None`.
async fn explain_analyze_stages(output: Output) -> Vec<(Option<u32>, String)> {
    let batches = output_batches(output).await;

    let mut stages = Vec::new();
    for batch in batches.take() {
        let stages_column = batch
            .column_by_name("stage")
            .expect("stage column")
            .as_primitive::<UInt32Type>();
        let plans_column = batch
            .column_by_name("plan")
            .expect("plan column")
            .as_string::<i32>();
        for row in 0..batch.num_rows() {
            let stage = stages_column
                .is_valid(row)
                .then(|| stages_column.value(row));
            stages.push((stage, plans_column.value(row).to_string()));
        }
    }

    stages
}

/// Collects the record batches of a query output.
async fn output_batches(output: Output) -> RecordBatches {
    match output.data {
        OutputData::Stream(stream) => RecordBatches::try_collect(stream).await.unwrap(),
        OutputData::RecordBatches(batches) => batches,
        OutputData::AffectedRows(rows) => {
            panic!("expected a query output, got {rows} affected rows")
        }
    }
}

/// Parses the cells of the data rows of a pretty printed table.
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

/// The first three columns of the rows of a pretty printed join result (the build value, the last
/// column, may have a different column name, so it is compared separately).
fn baseline_cells(pretty: &str) -> Vec<Vec<String>> {
    table_cells(pretty)
        .into_iter()
        .map(|row| row[..3].to_vec())
        .collect()
}

/// The cells of one expected row.
fn row(cells: &[&str]) -> Vec<String> {
    cells.iter().map(|cell| cell.to_string()).collect()
}

/// Sorts the rows of a result, so that two results can be compared as multisets.
fn multiset(mut rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    rows.sort();
    rows
}
