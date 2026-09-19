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

//! Capability test of the nested `MergeScan` execution of a datanode.
//!
//! A datanode executes the plans it receives. Such a plan may carry a nested plan in a `MergeScan`
//! node, i.e. a `MergeScan` that is not a placeholder, and the datanode then plans that node into
//! a `MergeScanExec` which queries the regions of the *inner* table from the datanodes that own
//! them (see `DatanodeRegionQueryHandler`). That is what lets a distributed sub-plan of a join run
//! on the datanode that owns a region of the probe table while the build side of the join is read
//! from the datanodes that own the regions of the build table.
//!
//! The plans below are built **by hand** and handed to a datanode by calling
//! `RegionServer::handle_remote_read` in process, so these tests exercise the execution capability
//! of the datanode only: no rewriter, hint or switch of this repository takes part in producing the
//! plan, and the outer request itself never goes over the wire. The region queries that the
//! datanode then issues for the regions of the inner table do: the tests build the cluster with
//! `GreptimeDbClusterBuilder::with_real_datanode_grpc_addr`, so every datanode is reachable at the
//! address it registered and the inner `MergeScan` reaches the datanodes that own the regions of
//! the inner table over a real Flight connection.
//!
//! The plan of one probe region is `Join(<probe table scan>, MergeScan(<build table scan>))`:
//!
//! * the probe side is decoded with the region aware catalog list of the request
//!   (`NameAwareCatalogList`), so it reads the region of the probe table that the request names,
//! * the `MergeScan` payload is decoded with the catalog of the query engine (see
//!   `query::query_engine::default_serializer`), so its table scan refers to the *build table* of
//!   the cluster and not to the region of the request. The two tables of the test share no column
//!   name besides their join key on purpose: a payload that was bound to the region of the request
//!   would decode with a schema that does not even match the payload.
//!
//! The checks are:
//!
//! * every region of the probe table is asked to execute the nested plan and the union of the
//!   results is the same multiset of rows as the result of the equivalent join SQL on the frontend
//!   (the build table has duplicate join keys, so a probe row joins several build rows: a lost row
//!   and a row that was counted twice both fail the comparison),
//! * the datanode really queried the *other* datanode for the regions of the build table that it
//!   owns (see [`assert_cross_datanode_region_query`]),
//! * a nested plan whose inner relation does not exist, and a nested plan with an inner region that
//!   is not served any more, both make the whole query fail: the datanode never returns the rows of
//!   the inner regions that happen to succeed,
//! * with the concurrency limit of every datanode set to a single query, every probe region still
//!   executes: the inner region queries of the nested plans are admitted as execution stages of the
//!   outer queries, so they don't wait for the permit that their own query holds (see
//!   [`test_nested_merge_scan_capability_internal_stage_under_concurrency_limit`]),
//! * the inner `MergeScan` resolves the leader of the regions of the build table through the table
//!   route cache of the datanode, whose version counter is shared by every table of the container:
//!   a cold load of the route of the build table still returns the rows of the join while an
//!   unrelated table id is invalidated in a loop (see
//!   [`test_nested_merge_scan_capability_cold_load_with_unrelated_invalidation`]).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use api::v1::region::{QueryRequest, RegionRequestHeader};
use common_meta::cache::{LayeredCacheRegistry, TableRouteCacheRef};
use common_meta::cache_invalidator::{CacheInvalidator, Context};
use common_meta::instruction::CacheIdent;
use common_query::Output;
use common_recordbatch::{RecordBatch, RecordBatches, SendableRecordBatchStream};
use common_telemetry::info;
use datafusion_expr::{JoinType, LogicalPlan, LogicalPlanBuilder};
use datanode::region_server::RegionServer;
use frontend::instance::Instance;
use query::datafusion::QUERY_PARALLELISM_HINT;
use query::dist_plan::MergeScanLogicalPlan;
use query::parser::QueryLanguageParser;
use query::query_engine::DefaultSerializer;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::{QueryContext, QueryContextRef};
use store_api::region_request::{RegionCloseRequest, RegionRequest};
use store_api::storage::{RegionId, TableId};
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};

use crate::cluster::{GreptimeDbCluster, GreptimeDbClusterBuilder};
use crate::test_util::execute_sql;

/// The probe table of the nested join: partitioned, so that its regions are spread over both
/// datanodes.
const PROBE_TABLE: &str = "nested_cap_probe";
/// The build table of the nested join: partitioned as well, and its join keys are duplicated.
const BUILD_TABLE: &str = "nested_cap_build";
/// The build table of the error path: no such table exists, so the inner relation of the nested
/// plan cannot be resolved.
const MISSING_INNER_TABLE: &str = "nested_cap_missing_build";

/// The table id that no table of the cluster uses. The test of the cold route load invalidates this
/// table id while it cold-loads the route of [`BUILD_TABLE`]: the version counter of the version
/// checked table route cache belongs to the cache container and not to a single table, so this
/// unrelated invalidation bumps the version that the cold load compares.
const UNRELATED_TABLE_ID: TableId = 999_999;

/// The interval of the unrelated invalidation loop of
/// [`test_nested_merge_scan_capability_cold_load_with_unrelated_invalidation`].
///
/// Short enough to hit an in-flight load of the table route cache, long enough to leave the retry
/// loop of the version checked load room to finish (a retry sleeps for at least `10ms`): the test
/// must fail because a route was stale or belonged to another table, not because the retry loop of
/// the cache gave up.
const UNRELATED_INVALIDATION_INTERVAL: Duration = Duration::from_millis(20);

/// `(a_id, probe_key, probe_v)` of the probe table: `a_id` is the primary key and the partition
/// column, `probe_key` is the join key and `probe_v` is the value of the row.
///
/// The probe table and the build table share no column name besides their join key on purpose, see
/// the module documentation.
const PROBE_ROWS: &[(i32, i32, i32)] = &[
    (1, 10, 100),
    (2, 20, 200),
    // The second partition of the probe table.
    (120, 30, 300),
    (121, 10, 400),
];

/// `(b_id, build_key, build_v)` of the build table: the join key `10` appears twice and `40`
/// matches no row of the probe table.
const BUILD_ROWS: &[(i32, i32, i32)] = &[
    (1, 10, 1),
    (2, 10, 2),
    (3, 20, 3),
    // The second partition of the build table.
    (120, 30, 4),
    (121, 40, 5),
];

/// The datanodes that have to serve a region query of the nested plan of a probe region.
#[derive(Debug, Default)]
struct CrossDatanodeExpectation {
    /// Datanode id -> the number of (probe region, build region) pairs that the nested plans have
    /// to read from that datanode over the network.
    remote_datanodes: BTreeMap<u64, usize>,
}

/// The capability of a datanode to execute a hand-built nested plan: every region of the probe
/// table is executed with
/// `Join(<its own region of the probe table>, MergeScan(<all regions of the build table>))`, and
/// the datanodes fetch the regions of the build table from the datanode that owns them.
#[tokio::test(flavor = "multi_thread")]
async fn test_nested_merge_scan_capability_join_across_datanodes() {
    common_telemetry::init_default_ut_logging();

    let cluster = build_cluster("test_nested_merge_scan_capability_join_across_datanodes").await;
    let frontend = cluster.fe_instance().clone();

    prepare_tables(&frontend).await;

    let probe_leaders = region_leaders(&frontend, PROBE_TABLE).await;
    let build_leaders = region_leaders(&frontend, BUILD_TABLE).await;
    info!("region leaders of {PROBE_TABLE}: {probe_leaders:?}");
    info!("region leaders of {BUILD_TABLE}: {build_leaders:?}");
    // Every table is partitioned into two regions. Both tables span both datanodes, so a nested
    // plan of a probe region always has to read a region of the build table from the other
    // datanode over the network.
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

    // The reference: the frontend executes the same join with its default distributed plan, i.e.
    // both sides of the join are read with a `MergeScan` and the join runs on the frontend.
    let reference = query_pretty(&frontend, &join_sql(), query_ctx()).await;
    info!("join result of the frontend:\n{reference}");
    assert_eq!(
        expected_join_rows(),
        multiset(table_cells(&reference)),
        "unexpected result of the join on the frontend:\n{reference}"
    );

    // The nested plan is the same for every probe region: the region of the request decides which
    // rows of the probe table the plan reads, the inner `MergeScan` reads all regions of the build
    // table.
    let query_ctx = query_ctx();
    let nested_plan = nested_plan(&frontend, &query_ctx).await;
    let plan = encode(&nested_plan);

    // The requests of the remote nodes are counted per datanode: a nested plan of a probe region
    // that a datanode `d` owns has to query every datanode that owns a region of the build table
    // *other* than `d`.
    let expectation = cross_datanode_expectation(&probe_leaders, &build_leaders);
    let baseline_requests = expectation
        .remote_datanodes
        .keys()
        .map(|datanode| (*datanode, rpc_requests(&cluster, *datanode)))
        .collect::<BTreeMap<_, _>>();

    let mut rows = Vec::new();
    for (probe_region, (probe_datanode, _)) in &probe_leaders {
        let stream = region_server(&cluster, *probe_datanode)
            .handle_remote_read(
                QueryRequest {
                    header: Some(region_request_header(&query_ctx)),
                    region_id: *probe_region,
                    plan: plan.clone(),
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
        info!("join result of the nested plan of the region {probe_region}:\n{actual}");
        rows.extend(table_cells(&actual));
    }

    // The nested plan of a probe region reads that region of the probe table, so the union of the
    // results of all probe regions is the result of the whole join. The join key `10` appears twice
    // in the build table, so the probe rows of that key appear twice as well: comparing the rows as
    // a multiset catches both a lost row and a row that was counted twice.
    assert_eq!(
        expected_join_rows(),
        multiset(rows.clone()),
        "the nested plans of the datanodes returned an unexpected multiset of rows: {rows:?}"
    );
    assert_eq!(
        multiset(table_cells(&reference)),
        multiset(rows),
        "the nested plans of the datanodes returned a different multiset of rows than the \
         frontend:\nfrontend:\n{reference}"
    );

    assert_cross_datanode_region_query(&cluster, &expectation, &baseline_requests);
}

/// The route of the build table is loaded cold while an *unrelated* table id is invalidated: the
/// version counter of the version checked table route cache belongs to the cache container and not
/// to a single table, so an invalidation of any `TableId` bumps the version that a cold load
/// compares, and a load that runs while an unrelated table is invalidated retries (retrying is what
/// keeps the loaded route correct; see the known limitation of `DatanodeRegionQueryHandler`).
///
/// The invalidation of the unrelated table id is injected into the layered cache registry of every
/// datanode, which is exactly the local handling path of the invalidation instruction that a
/// datanode receives from metasrv; the broadcast to the datanode is the only part not exercised
/// here.
///
/// The test pins the correctness of the retry path. The route of the build table is dropped on
/// every datanode (so the nested plans really load it from metasrv, the assertion below checks the
/// cache is cold), then every probe region executes the nested plan while a background task
/// invalidates the unrelated table id in a loop. The rows must be the rows of the join in every
/// round: a retried load must neither return the route of another table nor a stale route.
///
/// The test also reports how often the table route caches loaded a route with and without the
/// unrelated invalidation. A version checked load counts every attempt, so the amplification of the
/// unrelated invalidation is visible in the counter; the test logs the numbers instead of asserting
/// them, because the unrelated invalidation only hits an in-flight load with some probability.
#[tokio::test(flavor = "multi_thread")]
async fn test_nested_merge_scan_capability_cold_load_with_unrelated_invalidation() {
    common_telemetry::init_default_ut_logging();

    let cluster =
        build_cluster("test_nested_merge_scan_capability_cold_load_with_unrelated_invalidation")
            .await;
    let frontend = cluster.fe_instance().clone();

    prepare_tables(&frontend).await;

    let probe_leaders = region_leaders(&frontend, PROBE_TABLE).await;
    let build_leaders = region_leaders(&frontend, BUILD_TABLE).await;
    info!("region leaders of {PROBE_TABLE}: {probe_leaders:?}");
    info!("region leaders of {BUILD_TABLE}: {build_leaders:?}");
    // Both datanodes own a probe region, so both of them resolve the leader of the regions of the
    // build table when they execute the nested plan of their probe region.
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

    let build_table_id = table_id(&build_leaders);
    info!(
        "the regions of {BUILD_TABLE} belong to the table id {build_table_id}; the test \
         invalidates the unrelated table id {UNRELATED_TABLE_ID} while the datanodes load the \
         route of {BUILD_TABLE}"
    );

    // The reference join of the frontend and the nested plan, as in the other capability tests.
    let reference = query_pretty(&frontend, &join_sql(), query_ctx()).await;
    info!("join result of the frontend:\n{reference}");
    assert_eq!(
        expected_join_rows(),
        multiset(table_cells(&reference)),
        "unexpected result of the join on the frontend:\n{reference}"
    );

    let query_ctx = query_ctx();
    let plan = encode(&nested_plan(&frontend, &query_ctx).await);

    // 1. The baseline: the route of the build table is dropped on every datanode and nothing
    // invalidates a table while the nested plans run, so both datanodes load the route of the build
    // table from metasrv exactly once.
    invalidate_table_ids(&cluster, &[build_table_id]).await;
    assert_route_cold(&cluster, build_table_id).await;

    let loads_before = table_route_cache_loads();
    let rows_cold = run_nested_queries(&cluster, &probe_leaders, &plan, &query_ctx).await;
    let cold_loads = table_route_cache_loads() - loads_before;
    assert_eq!(
        expected_join_rows(),
        multiset(rows_cold.clone()),
        "the nested plans returned an unexpected multiset of rows after the route of {BUILD_TABLE} \
         was dropped: {rows_cold:?}"
    );
    // Every datanode owns a probe region, so every datanode resolved the leader of the regions of
    // the build table with a cold route cache: the baseline really started without the route.
    assert!(
        cold_loads >= 2,
        "expected the datanodes to load the dropped route of {BUILD_TABLE} from metasrv, but the \
         table route cache only loaded a route {cold_loads} time(s)"
    );

    // 2. The unrelated invalidation: a background task invalidates the table id of a table that the
    // cluster does not have, the way the datanodes handle the invalidation instruction of a DDL
    // that touched some other table. Every round drops the route of the build table again, so the
    // nested plans load a cold route while the unrelated invalidation runs.
    let stop = Arc::new(AtomicBool::new(false));
    let unrelated_invalidations = Arc::new(AtomicUsize::new(0));
    let invalidation_task = tokio::spawn(invalidate_unrelated_table_id(
        cluster
            .datanode_cache_registries
            .values()
            .cloned()
            .collect::<Vec<Arc<LayeredCacheRegistry>>>(),
        stop.clone(),
        unrelated_invalidations.clone(),
    ));

    let mut loads_with_invalidation = 0;
    for round in 0..3 {
        invalidate_table_ids(&cluster, &[build_table_id]).await;
        assert_route_cold(&cluster, build_table_id).await;

        let loads_before = table_route_cache_loads();
        let rows = run_nested_queries(&cluster, &probe_leaders, &plan, &query_ctx).await;
        loads_with_invalidation += table_route_cache_loads() - loads_before;

        assert_eq!(
            expected_join_rows(),
            multiset(rows.clone()),
            "round {round}: the nested plans returned an unexpected multiset of rows while the \
             unrelated table id {UNRELATED_TABLE_ID} was invalidated: {rows:?}"
        );
        assert_eq!(
            multiset(rows_cold.clone()),
            multiset(rows.clone()),
            "round {round}: the nested plans returned different rows while the unrelated table id \
             {UNRELATED_TABLE_ID} was invalidated, i.e. a retried route load returned another \
             route than the cold load"
        );

        // The retry loop of a route load that the unrelated invalidation interrupted must have
        // loaded the route of the build table: the retry must not leave the cache without the
        // route of the table that the nested plan asked for.
        for datanode in datanodes(&probe_leaders) {
            assert!(
                table_route_cache(&cluster, datanode).contains_key(&build_table_id),
                "round {round}: the datanode {datanode} must hold the route of {BUILD_TABLE} after \
                 it executed the nested plan of its probe region"
            );
        }
    }

    stop.store(true, Ordering::Relaxed);
    invalidation_task
        .await
        .expect("the task that invalidates the unrelated table id must stop");

    // Best effort measurement, not an assertion: the unrelated invalidation retries a cold load
    // only if it lands in the window between the version the load read and the version it compares
    // afterwards, so the counts vary with the timing of the machine. The counter is the
    // `CACHE_CONTAINER_CACHE_MISS` of the cache container, which every attempt of a version checked
    // load increments; the cache name is shared by the datanodes and the frontend of the test
    // process, but the frontend is idle while the nested plans of a round run.
    let unrelated_invalidations = unrelated_invalidations.load(Ordering::Relaxed);
    info!(
        "the table route caches loaded a cold route of {BUILD_TABLE} {cold_loads} time(s) without \
         an unrelated invalidation and {loads_with_invalidation} time(s) while the unrelated table \
         id {UNRELATED_TABLE_ID} was invalidated {unrelated_invalidations} time(s); a count above 2 \
         per round means the shared version counter of the cache container made the cold loads \
         retry"
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
    info!("region leaders of {PROBE_TABLE}: {probe_leaders:?}");
    info!("region leaders of {BUILD_TABLE}: {build_leaders:?}");

    // The request goes to a region of the probe table that the datanode `1` owns, and the nested
    // plan reads the regions of the build table, so one of them comes from the other datanode.
    let (probe_region, (probe_datanode, _)) = probe_leaders
        .iter()
        .find(|(_, (datanode, _))| *datanode == 1)
        .unwrap_or_else(|| {
            panic!(
                "expected a region of {PROBE_TABLE} on the datanode 1, actual region leaders: \
                 {probe_leaders:?}"
            )
        });
    let (build_region, (build_datanode, build_addr)) = build_leaders
        .iter()
        .find(|(_, (datanode, _))| *datanode != *probe_datanode)
        .unwrap_or_else(|| {
            panic!(
                "expected the regions of {BUILD_TABLE} to be spread over both datanodes, actual \
                 region leaders: {build_leaders:?}"
            )
        });

    // 1. The inner relation of the nested plan does not exist. The payload of the `MergeScan` is
    // resolved with the catalog of the datanode, so the datanode fails to decode the plan instead
    // of binding the inner table scan to the region of the request.
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
    info!("the datanode rejected the nested plan with the missing inner relation: {chain}");
    assert!(
        chain.contains(MISSING_INNER_TABLE),
        "expected the datanode to fail while resolving the inner relation {MISSING_INNER_TABLE}, \
         actual error: {chain}"
    );

    // 2. An inner region of the nested plan fails: close the region of the build table that the
    // other datanode owns. The nested plan of the probe region reads every region of the build
    // table, so it has to query that datanode, which then cannot serve the region any more. The
    // query of the probe region must fail as a whole: a partial result would hide the failure of
    // the inner region, because the region of the build table that the datanode owns keeps serving
    // its rows.
    let baseline = rpc_requests(&cluster, *build_datanode);
    let closed = region_server(&cluster, *build_datanode)
        .handle_request(
            RegionId::from_u64(*build_region),
            RegionRequest::Close(RegionCloseRequest {
                flush_on_close: false,
            }),
        )
        .await
        .expect("the datanode must close the region of the build table");
    info!(
        "closed the region {build_region} of {BUILD_TABLE} on the datanode {build_datanode} \
         ({closed:?}), which is reachable at {build_addr}"
    );

    let nested_plan = nested_plan(&frontend, &query_ctx).await;
    let plan = encode(&nested_plan);
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

    // The failure comes from the region query of the *other* datanode: it reached that datanode
    // over the network, i.e. the query failed while reading a region of the inner table, not while
    // planning the local datanode.
    let stats = cluster
        .datanode_rpc_stats(*build_datanode)
        .unwrap_or_else(|| panic!("expected the rpc stats of the datanode {build_datanode}"));
    info!(
        "the datanode {build_datanode} served {} remote gRPC requests, paths: {:?}",
        stats.requests(),
        stats.paths()
    );
    assert!(
        stats.requests() > baseline,
        "expected the failed nested plan to query the datanode {build_datanode} for the region \
         {build_region} it owns, but the requests of the datanode did not grow"
    );
    assert!(
        stats.paths().iter().any(|path| path.contains("DoGet")),
        "expected the datanode {build_datanode} to serve the region query of the nested plan, \
         actual remote gRPC paths: {:?}",
        stats.paths()
    );
}

/// The nested plans execute under the concurrency limit of their datanodes: a datanode admits a
/// single query at a time, the outer query of a probe region holds the only permit of its datanode
/// until its stream is fully consumed, and the nested plan then queries the regions of the build
/// table from the datanode itself and from the other datanode.
///
/// Every inner region query has to be admitted as an execution stage of the outer query, without a
/// permit of its own. The marker of the stage travels from the region query handler of the datanode
/// (`request.internal = true`), through the client that encodes it into the header of the request,
/// to the region server of the peer that reads it back and skips its limiter. If any one of them is
/// missing, the inner region query of the *same* datanode waits for the permit held by the outer
/// query and fails after `concurrent_query_limiter_timeout`, and the inner region query of the
/// *other* datanode waits for the outer query of that datanode, which waits for its own nested
/// plan: the consumption of the outer queries below is bounded by a timeout, so both cases fail the
/// test instead of hanging it.
#[tokio::test(flavor = "multi_thread")]
async fn test_nested_merge_scan_capability_internal_stage_under_concurrency_limit() {
    common_telemetry::init_default_ut_logging();

    let cluster = build_cluster_with_query_limit(
        "test_nested_merge_scan_capability_internal_stage_under_concurrency_limit",
    )
    .await;
    let frontend = cluster.fe_instance().clone();

    prepare_tables(&frontend).await;

    let probe_leaders = region_leaders(&frontend, PROBE_TABLE).await;
    let build_leaders = region_leaders(&frontend, BUILD_TABLE).await;
    info!("region leaders of {PROBE_TABLE}: {probe_leaders:?}");
    info!("region leaders of {BUILD_TABLE}: {build_leaders:?}");
    // Every table is partitioned into two regions. Both tables span both datanodes, so a nested
    // plan of a probe region always has to read a region of the build table from the other
    // datanode, and the two probe regions sit on different datanodes: asking both datanodes for
    // their probe region exhausts the single permit of every datanode of the cluster.
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

    // The reference: the frontend executes the same join with its default distributed plan.
    let reference = query_pretty(&frontend, &join_sql(), query_ctx()).await;
    info!("join result of the frontend:\n{reference}");
    assert_eq!(
        expected_join_rows(),
        multiset(table_cells(&reference)),
        "unexpected result of the join on the frontend:\n{reference}"
    );

    let query_ctx = query_ctx();
    let nested_plan = nested_plan(&frontend, &query_ctx).await;
    let plan = encode(&nested_plan);

    let expectation = cross_datanode_expectation(&probe_leaders, &build_leaders);
    let baseline_requests = expectation
        .remote_datanodes
        .keys()
        .map(|datanode| (*datanode, rpc_requests(&cluster, *datanode)))
        .collect::<BTreeMap<_, _>>();

    // Ask every datanode for its probe region before consuming anything: the region server takes
    // the permit of the datanode when it admits the request and holds it until the stream of the
    // request is consumed, so no datanode has a free permit left while the nested plans run.
    let mut outer_queries = Vec::with_capacity(probe_leaders.len());
    for (probe_region, (probe_datanode, _)) in &probe_leaders {
        let stream = region_server(&cluster, *probe_datanode)
            .handle_remote_read(
                QueryRequest {
                    header: Some(region_request_header(&query_ctx)),
                    region_id: *probe_region,
                    plan: plan.clone(),
                },
                query_ctx.clone(),
            )
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "the datanode {probe_datanode} must admit the nested plan of the region \
                     {probe_region} while it serves a single query at a time: {e}"
                )
            });
        outer_queries.push((*probe_region, *probe_datanode, stream));
    }

    // Consume the outer queries of both datanodes at the same time, so that every nested plan
    // queries the regions of the build table while the only permit of every datanode is held by the
    // outer query of that datanode. An inner region query that misses the stage marker waits for
    // that permit up to `concurrent_query_limiter_timeout` and fails, or the two outer queries wait
    // for each other: the timeout turns the second case into a failure instead of a hang.
    let collected = tokio::time::timeout(
        Duration::from_secs(30),
        futures::future::join_all(outer_queries.into_iter().map(
            |(probe_region, probe_datanode, stream)| async move {
                (
                    probe_region,
                    probe_datanode,
                    RecordBatches::try_collect(stream).await,
                )
            },
        )),
    )
    .await
    .unwrap_or_else(|_| {
        panic!(
            "the nested plans of the probe regions did not return within the timeout while every \
             datanode served a single query at a time: an inner region query is waiting for the \
             concurrency permit held by the outer query that dispatched it, i.e. the stage marker \
             of the inner region query was lost"
        )
    });

    let mut rows = Vec::new();
    for (probe_region, probe_datanode, result) in collected {
        let batches = result.unwrap_or_else(|e| {
            panic!(
                "the datanode {probe_datanode} must execute the nested plan of the region \
                 {probe_region} completely while it serves a single query at a time: {e}"
            )
        });
        let actual = batches
            .pretty_print()
            .expect("the result of the datanode must be printable");
        info!("join result of the nested plan of the region {probe_region}:\n{actual}");
        rows.extend(table_cells(&actual));
    }

    // The nested plan of a probe region reads that region of the probe table, so the union of the
    // results of all probe regions is the result of the whole join. Comparing the rows as a
    // multiset catches both a lost row and a row that was counted twice.
    assert_eq!(
        expected_join_rows(),
        multiset(rows.clone()),
        "the nested plans of the datanodes returned an unexpected multiset of rows under the \
         concurrency limit: {rows:?}"
    );
    assert_eq!(
        multiset(table_cells(&reference)),
        multiset(rows),
        "the nested plans of the datanodes returned a different multiset of rows than the \
         frontend:\nfrontend:\n{reference}"
    );

    assert_cross_datanode_region_query(&cluster, &expectation, &baseline_requests);

    // The nested plan really reached the other datanode over the network: the region query of a
    // `MergeScan` is a Flight `DoGet` (see `RegionRequester::handle_query`), and the remote
    // requests of a datanode are counted at the address it registered, so a `DoGet` recorded there
    // was sent by another datanode of the cluster.
    for datanode in expectation.remote_datanodes.keys() {
        let stats = cluster
            .datanode_rpc_stats(*datanode)
            .unwrap_or_else(|| panic!("expected the rpc stats of the datanode {datanode}"));
        info!(
            "the datanode {datanode} served {} remote gRPC requests, paths: {:?}",
            stats.requests(),
            stats.paths()
        );
        assert!(
            stats.paths().iter().any(|path| path.contains("DoGet")),
            "expected the nested plans of {PROBE_TABLE} to query the datanode {datanode} for the \
             regions of {BUILD_TABLE} it owns with a Flight `DoGet`, actual remote gRPC paths: \
             {:?}",
            stats.paths()
        );
    }
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

/// Same as [`build_cluster`], but every datanode admits a single query at a time.
///
/// The outer region query of a probe region holds the only permit of its datanode until its stream
/// is fully consumed, so a region query that the nested plan of that query issues to the same
/// datanode can only succeed if it is admitted as an execution stage of the query instead of
/// taking a permit of its own (see
/// [`test_nested_merge_scan_capability_internal_stage_under_concurrency_limit`]).
async fn build_cluster_with_query_limit(test_name: &str) -> GreptimeDbCluster {
    GreptimeDbClusterBuilder::new(test_name)
        .await
        .with_real_datanode_grpc_addr(true)
        .with_datanodes(2)
        .with_datanode_options_override(|_, opts| {
            opts.max_concurrent_queries = 1;
            // Wider than the default: the test must fail because the stage marker of an inner
            // region query is lost, not because a slow machine made an admitted query wait for a
            // permit for too long. The timeout around the consumption of the outer queries is much
            // larger than this.
            opts.concurrent_query_limiter_timeout = Duration::from_secs(1);
        })
        .build(false)
        .await
}

/// The `DoGet` requests that the nested plans of the probe regions have to send to the datanodes
/// that own the regions of the build table.
fn cross_datanode_expectation(
    probe_leaders: &BTreeMap<u64, (u64, String)>,
    build_leaders: &BTreeMap<u64, (u64, String)>,
) -> CrossDatanodeExpectation {
    let mut expectation = CrossDatanodeExpectation::default();
    for (probe_region, (probe_datanode, probe_addr)) in probe_leaders {
        for (build_region, (build_datanode, build_addr)) in build_leaders {
            if probe_datanode == build_datanode {
                continue;
            }
            *expectation
                .remote_datanodes
                .entry(*build_datanode)
                .or_default() += 1;
            info!(
                "the datanode {probe_datanode} ({probe_addr}) executing the region {probe_region} \
                 of {PROBE_TABLE} queries the region {build_region} of {BUILD_TABLE} from the \
                 datanode {build_datanode} ({build_addr})"
            );
        }
    }
    expectation
}

/// Asserts that the datanodes issued a real region query to every datanode that owns a region of
/// the build table.
///
/// The cluster serves every datanode at its own address, while the frontend and the local
/// bookkeeping of the test process reach a datanode through an in-process client, so a request
/// recorded at the address of a datanode comes from a remote node: a request of the `DoGet` (a
/// region query) or of the region service proves the datanode-to-datanode region query.
fn assert_cross_datanode_region_query(
    cluster: &GreptimeDbCluster,
    expectation: &CrossDatanodeExpectation,
    baseline_requests: &BTreeMap<u64, usize>,
) {
    assert!(
        !expectation.remote_datanodes.is_empty(),
        "expected the nested plans of the probe regions to read a region of the build table from \
         another datanode, but the regions of {BUILD_TABLE} are not spread over both datanodes"
    );

    for (datanode, probe_regions) in &expectation.remote_datanodes {
        let stats = cluster
            .datanode_rpc_stats(*datanode)
            .unwrap_or_else(|| panic!("expected the rpc stats of the datanode {datanode}"));
        let baseline = baseline_requests.get(datanode).copied().unwrap_or_default();
        info!(
            "the datanode {datanode} served {} remote gRPC requests (baseline {baseline}), \
             paths: {:?}",
            stats.requests(),
            stats.paths()
        );
        assert!(
            stats.requests() > baseline,
            "expected the nested plans of {PROBE_TABLE} to query the datanode {datanode} \
             {probe_regions} times for the regions of {BUILD_TABLE} it owns, but the datanode \
             served no additional remote request ({} requests, baseline {baseline})",
            stats.requests()
        );
        assert!(
            stats
                .paths()
                .iter()
                .any(|path| path.contains("DoGet") || path.contains("Region")),
            "expected the datanode {datanode} to serve the region query of the nested plan, actual \
             remote gRPC paths: {:?}",
            stats.paths()
        );
    }
}

/// The number of gRPC requests that the remote nodes sent to the datanode so far.
fn rpc_requests(cluster: &GreptimeDbCluster, datanode_id: u64) -> usize {
    cluster
        .datanode_rpc_stats(datanode_id)
        .unwrap_or_else(|| panic!("expected the rpc stats of the datanode {datanode_id}"))
        .requests()
}

/// Returns the region server of the datanode `datanode_id`.
fn region_server(cluster: &GreptimeDbCluster, datanode_id: u64) -> RegionServer {
    cluster
        .datanode_instances
        .get(&datanode_id)
        .unwrap_or_else(|| panic!("expected a datanode {datanode_id}"))
        .region_server()
}

/// Executes the nested plan of every probe region on the datanode that owns the region, consumes
/// every stream completely and returns the union of the rows of the results.
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
        info!("join result of the nested plan of the region {probe_region}:\n{actual}");
        rows.extend(table_cells(&actual));
    }

    rows
}

/// The id of the single table that the regions of `leaders` belong to.
fn table_id(leaders: &BTreeMap<u64, (u64, String)>) -> TableId {
    let table_ids = leaders
        .keys()
        .map(|region_id| RegionId::from_u64(*region_id).table_id())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        1,
        table_ids.len(),
        "expected the regions to belong to a single table, actual region leaders: {leaders:?}"
    );

    *table_ids.first().expect("there is at least one region")
}

/// Returns the table route cache of the datanode `datanode_id`: the cache that
/// `DatanodeRegionQueryHandler::select_target` reads to resolve the leader of a region.
fn table_route_cache(cluster: &GreptimeDbCluster, datanode_id: u64) -> TableRouteCacheRef {
    cluster
        .datanode_cache_registries
        .get(&datanode_id)
        .unwrap_or_else(|| panic!("expected the cache registry of the datanode {datanode_id}"))
        .get::<TableRouteCacheRef>()
        .unwrap_or_else(|| panic!("expected the table route cache of the datanode {datanode_id}"))
}

/// Invalidates `table_ids` on every datanode, the way a datanode handles the invalidation
/// instruction of metasrv (see [`GreptimeDbCluster::datanode_cache_registries`]).
async fn invalidate_table_ids(cluster: &GreptimeDbCluster, table_ids: &[TableId]) {
    let idents = table_ids
        .iter()
        .copied()
        .map(CacheIdent::TableId)
        .collect::<Vec<_>>();
    for (datanode_id, registry) in &cluster.datanode_cache_registries {
        registry
            .invalidate(&Context::default(), &idents)
            .await
            .unwrap_or_else(|e| {
                panic!("the datanode {datanode_id} must invalidate {idents:?}: {e}")
            });
    }
}

/// Invalidates [`UNRELATED_TABLE_ID`] on `registries` until `stop` is set, and counts the rounds.
async fn invalidate_unrelated_table_id(
    registries: Vec<Arc<LayeredCacheRegistry>>,
    stop: Arc<AtomicBool>,
    invalidations: Arc<AtomicUsize>,
) {
    while !stop.load(Ordering::Relaxed) {
        for registry in &registries {
            registry
                .invalidate(
                    &Context::default(),
                    &[CacheIdent::TableId(UNRELATED_TABLE_ID)],
                )
                .await
                .expect("the datanode must invalidate the unrelated table id");
        }
        invalidations.fetch_add(1, Ordering::Relaxed);
        tokio::time::sleep(UNRELATED_INVALIDATION_INTERVAL).await;
    }
}

/// Waits until no datanode holds the route of `table_id` any more, i.e. until the route is cold.
///
/// The invalidation is awaited before this call, so the wait is a safety net for the internals of
/// the cache (e.g. its pending tasks) rather than a required step.
async fn assert_route_cold(cluster: &GreptimeDbCluster, table_id: TableId) {
    for attempt in 0..100 {
        let mut cached = Vec::new();
        for datanode_id in cluster.datanode_cache_registries.keys() {
            if table_route_cache(cluster, *datanode_id).contains_key(&table_id) {
                cached.push(*datanode_id);
            }
        }
        if cached.is_empty() {
            return;
        }
        assert!(
            attempt < 99,
            "the datanodes {cached:?} must drop the route of the table {table_id} after the \
             invalidation"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// The number of attempts that the table route caches of the process made to load a route: every
/// attempt of a version checked load (including its retries) increments this counter.
///
/// The counter is labeled with the cache name only, so it adds up the table route caches of the
/// datanodes and of the frontend of the test process.
fn table_route_cache_loads() -> u64 {
    common_meta::metrics::CACHE_CONTAINER_CACHE_MISS
        .with_label_values(&[cache::TABLE_ROUTE_CACHE_NAME])
        .get()
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

/// Encodes a plan the way a frontend sends it to a datanode.
fn encode(plan: &LogicalPlan) -> Vec<u8> {
    DFLogicalSubstraitConvertor
        .encode(plan, DefaultSerializer)
        .expect("the plan must be encodable")
        .to_vec()
}

/// Builds the nested plan of the tests by hand:
/// `Join(<probe table scan>, MergeScan(<build table scan>))`.
///
/// The plan is the remote input of a `MergeScan` that a frontend sends to the datanode that owns a
/// region of the probe table: the probe side is decoded with the region aware catalog list of the
/// request, so it reads the region of the request, and the `MergeScan` is not a placeholder, so the
/// datanode plans it into a `MergeScanExec` over the regions of the build table.
///
/// The table scans are planned by the frontend (a `SELECT` of the columns of the join), which gives
/// them the schema of the tables of the cluster. The join and the nested `MergeScan` around the
/// build side are assembled here: no rewrite of the frontend is involved.
async fn nested_plan(frontend: &Arc<Instance>, query_ctx: &QueryContextRef) -> LogicalPlan {
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

    nested_plan_of(&probe, build)
}

/// Same as [`nested_plan`], but the inner `MergeScan` scans `inner_table` instead of
/// [`BUILD_TABLE`].
///
/// The build side of the nested plan is the build table with the name of its scan replaced: the
/// scan keeps the schema and the source of the build table, so the payload of the `MergeScan` looks
/// exactly like the payload of [`nested_plan`] to the encoder, while the table it names does not
/// exist.
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

/// Assembles `Join(probe, MergeScan(build))` and logs both sides.
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

/// Returns `plan` with every table scan renamed to `table_name`, keeping the schema and the source
/// of the scan.
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

/// Plans `sql` on the frontend and returns the logical plan.
async fn plan_sql(frontend: &Arc<Instance>, sql: &str, query_ctx: &QueryContextRef) -> LogicalPlan {
    let stmt = QueryLanguageParser::parse_sql(sql, query_ctx)
        .unwrap_or_else(|e| panic!("failed to parse `{sql}`: {e}"));
    frontend
        .statement_executor()
        .plan(&stmt, query_ctx.clone())
        .await
        .unwrap_or_else(|e| panic!("failed to plan `{sql}`: {e}"))
}

/// The join of the tests, over the columns of the nested plan of [`nested_plan`] in order:
/// `(a_id, probe_key, probe_v, build_key, build_v)`.
fn join_sql() -> String {
    format!(
        "SELECT p.a_id, p.probe_key, p.probe_v, b.build_key, b.build_v
         FROM {PROBE_TABLE} p JOIN {BUILD_TABLE} b ON p.probe_key = b.build_key"
    )
}

/// The rows of [`join_sql`] as a multiset: every probe row joins every build row of its join key.
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

/// Returns the region leaders of `table`: region id -> (datanode id, datanode address).
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

/// The datanode ids of the region leaders.
fn datanodes(leaders: &BTreeMap<u64, (u64, String)>) -> BTreeSet<u64> {
    leaders.values().map(|(id, _)| *id).collect()
}

/// Collects the record batches of a query output.
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

/// Consumes `stream` and returns the error it failed with, or `None` if it returned completely.
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

/// Concatenates the message of every error of the source chain of `error`, so that the failure of
/// an outer step (e.g. decoding a plan) does not hide its reason (e.g. an unknown table).
fn error_chain(error: &(dyn std::error::Error + 'static)) -> String {
    let mut messages = vec![error.to_string()];
    let mut source = error.source();
    while let Some(inner) = source {
        messages.push(inner.to_string());
        source = inner.source();
    }
    messages.join(": ")
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
/// tests are stable. The datanode builds the query context of its own execution from the header of
/// the request, so the request of the tests carries the same query context.
fn query_ctx() -> QueryContextRef {
    let mut query_ctx = QueryContext::with_db_name(None);
    query_ctx.set_extension(QUERY_PARALLELISM_HINT, "1");
    Arc::new(query_ctx)
}

/// The header of the region query of the tests, carrying the query context of the session of the
/// plan.
fn region_request_header(query_ctx: &QueryContextRef) -> RegionRequestHeader {
    RegionRequestHeader {
        query_context: Some(query_ctx.as_ref().into()),
        ..Default::default()
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

/// Sorts the rows of a result, so that two results can be compared as multisets.
fn multiset(mut rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    rows.sort();
    rows
}
