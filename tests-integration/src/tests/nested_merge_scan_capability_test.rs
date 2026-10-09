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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use api::v1::region::{QueryRequest, RegionRequestHeader};
use client::Client;
use client::region::RegionRequester;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_grpc::channel_manager::ChannelManager;
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

/// Internal read-preference wire metadata is rejected before nested execution on both inbound paths.
#[tokio::test(flavor = "multi_thread")]
async fn test_nested_merge_scan_capability_rejects_internal_read_preference() {
    use common_meta::node_manager::Datanode;
    use common_query::request::QueryRequest as LogicalQueryRequest;
    use futures::TryStreamExt;
    use session::hints::READ_PREFERENCE_EXTENSION_KEY;

    common_telemetry::init_default_ut_logging();

    let cluster = build_cluster("test_nested_merge_scan_internal_read_preference").await;
    let frontend = cluster.fe_instance().clone();
    prepare_tables(&frontend).await;

    let probe_leaders = region_leaders(&frontend, PROBE_TABLE).await;
    let query_ctx = query_ctx();
    let plan = nested_plan(&frontend, &query_ctx).await;
    let (region_id, (datanode_id, address)) = probe_leaders.iter().next().unwrap();
    let target = region_server(&cluster, *datanode_id);
    // Confirm the same plan completes on the valid default-Leader path before asserting rejection.
    let valid_rows = run_nested_queries(&cluster, &probe_leaders, &encode(&plan), &query_ctx).await;
    assert_eq!(expected_join_rows(), multiset(valid_rows));
    let mut baseline = cluster
        .datanode_instances
        .keys()
        .map(|id| (*id, rpc_requests(&cluster, *id)))
        .collect::<BTreeMap<_, _>>();

    for value in ["", "invalid", "follower"] {
        let mut header = region_request_header(&query_ctx);
        header
            .query_context
            .as_mut()
            .unwrap()
            .extensions
            .insert(READ_PREFERENCE_EXTENSION_KEY.to_string(), value.to_string());
        let header = Some(header);
        let error = target
            .handle_read(LogicalQueryRequest {
                header: header.clone(),
                region_id: RegionId::from_u64(*region_id),
                plan: plan.clone(),
            })
            .await
            .err()
            .expect("internal read preference must be rejected at logical ingress");
        assert_eq!(
            StatusCode::InvalidArguments,
            error.status_code(),
            "value={value:?}"
        );
        let logical_message = error.output_msg().to_ascii_lowercase();
        assert!(
            logical_message.contains("read_preference"),
            "expected an invalid query-context read-preference error, got: {logical_message}"
        );
        if !value.is_empty() {
            assert!(
                logical_message.contains(value),
                "unexpected logical error: {logical_message}"
            );
        }
        for id in cluster.datanode_instances.keys() {
            assert_eq!(
                baseline[id],
                rpc_requests(&cluster, *id),
                "logical invalid context caused DoGet at datanode {id}"
            );
        }

        let client = Client::with_query_and_control_managers(
            ChannelManager::new(),
            ChannelManager::new(),
            [address.as_str()],
        );
        let requester = RegionRequester::new(client, false, false);
        let request = LogicalQueryRequest {
            header,
            region_id: RegionId::from_u64(*region_id),
            plan: plan.clone(),
        };
        let flight_message = match Datanode::handle_query(&requester, request).await {
            Err(error) => {
                assert_eq!(StatusCode::InvalidArguments, error.status_code());
                error.output_msg().to_ascii_lowercase()
            }
            Ok(stream) => {
                let error = stream
                    .try_collect::<Vec<RecordBatch>>()
                    .await
                    .expect_err("Flight must reject invalid internal read preference");
                assert_eq!(StatusCode::InvalidArguments, error.status_code());
                error.output_msg().to_ascii_lowercase()
            }
        };
        assert!(
            flight_message.contains("read_preference"),
            "expected an invalid-parameter read-preference error, got: {flight_message}"
        );
        if !value.is_empty() {
            assert!(
                flight_message.contains(value),
                "unexpected Flight error: {flight_message}"
            );
        }
        for id in cluster.datanode_instances.keys() {
            let expected = baseline[id] + u8::from(*id == *datanode_id) as usize;
            assert_eq!(
                expected,
                rpc_requests(&cluster, *id),
                "invalid Flight context caused nested DoGet at datanode {id}"
            );
        }
        for id in cluster.datanode_instances.keys() {
            baseline.insert(*id, rpc_requests(&cluster, *id));
        }
    }
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

    // The increased DoGet count proves execution reached the remote region owner.
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
            "expected a nested Flight DoGet at datanode {datanode}, count {actual}, baseline {baseline}"
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
