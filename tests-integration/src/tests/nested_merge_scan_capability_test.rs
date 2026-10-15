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
//! Existing nested-plan tests use in-process outer `handle_remote_read` requests; inner region
//! reads use real TCP Flight. The standalone requester baseline below calls an actual datanode's
//! `handle_query` over TCP Flight. The probe scan resolves against the request's region catalog,
//! while the nested payload resolves against the engine catalog; distinct column names ensure
//! these scopes cannot be confused. Success compares complete duplicate-sensitive results with
//! frontend SQL and known rows. Missing inner tables and closed remote inner regions must each fail.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use api::v1::region::{QueryRequest, RegionRequestHeader};
use client::Client;
use client::region::RegionRequester;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_grpc::channel_manager::ChannelManager;
use common_meta::error::Result as MetaResult;
use common_query::Output;
use common_query::request::RawSeriesRequest;
use common_recordbatch::{RecordBatch, RecordBatches, SendableRecordBatchStream};
use common_telemetry::info;
use datafusion::logical_expr::TableSource;
use datafusion_expr::{JoinType, LogicalPlan, LogicalPlanBuilder};
use datanode::region_server::RegionServer;
use frontend::instance::Instance;
use query::datafusion::QUERY_PARALLELISM_HINT;
use query::dist_plan::MergeScanLogicalPlan;
use query::parser::QueryLanguageParser;
use query::query_engine::DefaultSerializer;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::{QueryContext, QueryContextRef};
use store_api::region_engine::SendableRawSeriesStream;
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
async fn test_region_requester_scan_over_tcp_flight() {
    use common_meta::node_manager::Datanode;
    use common_query::request::QueryRequest as LogicalQueryRequest;

    common_telemetry::init_default_ut_logging();

    let cluster = build_cluster("test_region_requester_scan_over_tcp_flight").await;
    let frontend = cluster.fe_instance().clone();
    prepare_tables(&frontend).await;

    let leaders = region_leaders(&frontend, PROBE_TABLE).await;
    assert_eq!(
        2,
        leaders.len(),
        "expected every probe region to have a leader"
    );

    let sql = format!("SELECT a_id, probe_key, probe_v FROM {PROBE_TABLE} WHERE probe_key >= 20");
    let reference = query_pretty(&frontend, &sql, query_ctx()).await;
    let expected = multiset(table_cells(&reference));
    assert_eq!(
        vec![vec!["120", "30", "300"], vec!["2", "20", "200"]],
        expected,
        "unexpected filtered frontend SQL result:\n{reference}"
    );

    let query_ctx = query_ctx();
    let plan = plan_sql(&frontend, &sql, &query_ctx).await;
    let mut rows = Vec::new();
    for (region_id, (datanode_id, address)) in &leaders {
        let baseline = rpc_requests(&cluster, *datanode_id);
        let client = Client::with_query_and_control_managers(
            ChannelManager::new(),
            ChannelManager::new(),
            [address.as_str()],
        );
        let requester = RegionRequester::new(client, false, false);
        let stream = Datanode::handle_query(
            &requester,
            LogicalQueryRequest {
                header: Some(region_request_header(&query_ctx)),
                region_id: RegionId::from_u64(*region_id),
                plan: plan.clone(),
            },
        )
        .await
        .unwrap_or_else(|e| panic!("Flight query for region {region_id} failed: {e}"));
        let batches = RecordBatches::try_collect(stream)
            .await
            .unwrap_or_else(|e| panic!("Flight stream for region {region_id} failed: {e}"));
        for batch in batches.iter() {
            let names = batch
                .df_record_batch()
                .schema_ref()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>();
            assert_eq!(vec!["a_id", "probe_key", "probe_v"], names);
        }
        rows.extend(table_cells(
            &batches
                .pretty_print()
                .expect("record batches are printable"),
        ));
        assert_eq!(
            baseline + 1,
            rpc_requests(&cluster, *datanode_id),
            "expected exactly one DoGet for region {region_id} at datanode {datanode_id}"
        );
    }
    assert_eq!(
        expected,
        multiset(rows),
        "Flight results differ from SQL oracle:\n{reference}"
    );
}

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

    for value in ["", "invalid"] {
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

    let inner = MergeScanLogicalPlan::new(build, false).into_logical_plan();
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

fn table_source(plan: &LogicalPlan) -> Option<Arc<dyn TableSource>> {
    match plan {
        LogicalPlan::TableScan(scan) => Some(scan.source.clone()),
        _ => plan.inputs().into_iter().find_map(table_source),
    }
}

async fn expect_raw_unsupported(result: MetaResult<SendableRawSeriesStream>) -> String {
    use futures_util::TryStreamExt;

    match result {
        Err(error) => error_chain(&error),
        Ok(stream) => match stream.try_collect::<Vec<_>>().await {
            Err(error) => error_chain(&error),
            Ok(chunks) => panic!(
                "unsupported raw query unexpectedly returned {} chunks",
                chunks.len()
            ),
        },
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_native_single_region_raw_flight_preserves_typed_filter_and_snapshot() {
    use common_meta::node_manager::Datanode;
    use common_query::request::{QueryRequest as LogicalQueryRequest, RawSeriesTag};
    use datafusion::arrow::array::{Array, Float64Array};
    use datafusion::common::TableReference;
    use datafusion_expr::{col, lit};
    use datatypes::series::raw::RawSeriesOrdering;
    use datatypes::series::samples::SampleRef;
    use futures_util::TryStreamExt;
    use mito2::test_util::sst_util::new_primary_key;
    use store_api::codec::PrimaryKeyEncoding;
    use store_api::region_engine::RawLabelSource;

    const TABLE: &str = "native_raw_flight_single_region";
    common_telemetry::init_default_ut_logging();
    let cluster = build_cluster("test_native_single_region_raw_flight").await;
    let frontend = cluster.fe_instance().clone();
    execute_sql(&frontend, &format!(
        "CREATE TABLE {TABLE} (host STRING, left_value DOUBLE, right_value DOUBLE, predicate_value BIGINT, ts TIMESTAMP, TIME INDEX (ts), PRIMARY KEY(host)) ENGINE=mito"
    )).await;
    execute_sql(&frontend, &format!(
        "INSERT INTO {TABLE} VALUES ('alpha', 1.25, 11.0, 9007199254740992, 1000), ('alpha', NULL, -0.0, 9007199254740993, 2000), ('alpha', 3.75, 13.0, 9007199254740994, 3000)"
    )).await;

    let frontend_sql = format!(
        "SELECT ts, left_value, right_value FROM {TABLE} WHERE predicate_value > 9007199254740992"
    );
    let normal_before = query_pretty(&frontend, &frontend_sql, query_ctx()).await;
    assert_eq!(2, table_cells(&normal_before).len());

    let leaders = region_leaders(&frontend, TABLE).await;
    assert_eq!(1, leaders.len(), "fixture is required to have one region");
    let (datanode, address) = leaders.values().next().unwrap();
    let region_id = *leaders.keys().next().unwrap();
    let engine = region_server(&cluster, *datanode).mito_engine().unwrap();
    let region = engine.find_region(RegionId::from_u64(region_id)).unwrap();
    let metadata = region.metadata();
    let field_id = |name: &str| {
        metadata
            .column_metadatas
            .iter()
            .find(|column| column.column_schema.name == name)
            .unwrap()
            .column_id
    };
    let left_id = field_id("left_value");
    let right_id = field_id("right_value");
    let host_id = field_id("host");
    let projection = ["right_value", "left_value"]
        .iter()
        .map(|name| {
            metadata
                .column_metadatas
                .iter()
                .position(|column| column.column_schema.name == *name)
                .unwrap()
        })
        .collect::<Vec<_>>();
    let snapshot = region.find_committed_sequence();

    let ctx = query_ctx();
    ctx.set_snapshot(region_id, snapshot);
    let base = plan_sql(&frontend, &format!("SELECT * FROM {TABLE}"), &ctx).await;
    // Take only the provider source from the ordinary query plan; build a new root scan with
    // the intended static predicate rather than submitting its SQL projection/filter parents.
    let source = table_source(&base).expect("ordinary plan must contain its table source");
    let filter = col("predicate_value").gt(lit(9_007_199_254_740_992_i64));
    let plan = LogicalPlanBuilder::scan_with_filters(
        TableReference::bare(TABLE),
        source.clone(),
        Some(projection.clone()),
        vec![filter],
    )
    .unwrap()
    .build()
    .unwrap();
    let binding = RawSeriesRequest {
        version: 1,
        physical_metadata: metadata.as_ref().clone(),
        metric_name: TABLE.to_string(),
        logical_tags: vec![RawSeriesTag {
            column_id: host_id,
            name: "host".into(),
        }],
        // Deliberately swapped field order; the predicate column is not projected.
        field_ids: vec![right_id, left_id],
    };
    let baseline = rpc_requests(&cluster, *datanode);
    let client = Client::with_query_and_control_managers(
        ChannelManager::new(),
        ChannelManager::new(),
        [address.as_str()],
    );
    let requester = RegionRequester::new(client, false, false);
    let raw_request = LogicalQueryRequest {
        header: Some(region_request_header(&ctx)),
        region_id: RegionId::from_u64(region_id),
        plan: plan.clone(),
    };
    let raw = requester
        .handle_raw_series_query(raw_request, binding.clone())
        .await
        .unwrap();
    assert_eq!(baseline + 1, rpc_requests(&cluster, *datanode));
    assert_eq!(
        vec!["right_value", "left_value"],
        raw.schema()
            .fields
            .iter()
            .map(|f| f.name.as_ref())
            .collect::<Vec<_>>()
    );
    match raw.label_source() {
        RawLabelSource::PrimaryKey {
            metric_name,
            physical_metadata,
            logical_tags,
        } => {
            assert_eq!(TABLE, metric_name.as_ref());
            assert_eq!(metadata.region_id, physical_metadata.region_id);
            assert_eq!(metadata.primary_key, physical_metadata.primary_key);
            assert_eq!(1, logical_tags.len());
            assert_eq!(host_id, logical_tags[0].column_id);
            assert_eq!("host", logical_tags[0].name.as_ref());
        }
        RawLabelSource::Explicit => panic!("expected key-backed source"),
    }
    assert_eq!(RawSeriesOrdering::ReadKeyThenTime, raw.ordering());
    assert_eq!(PrimaryKeyEncoding::Dense, metadata.primary_key_encoding);
    assert_eq!(&[host_id], metadata.primary_key.as_slice());
    let expected_key = new_primary_key(&["alpha"]);
    let chunks = raw.try_collect::<Vec<_>>().await.unwrap();
    assert!(!chunks.is_empty());
    let mut timestamps = Vec::new();
    for chunk in &chunks {
        assert!(chunk.metric.is_none());
        assert_eq!(
            expected_key,
            chunk.read_key.as_slice(),
            "every chunk must retain the complete encoded primary key"
        );
        timestamps.extend_from_slice(chunk.time.as_explicit().unwrap());
        assert_eq!(2, chunk.fields.len());
        assert_eq!(chunk.time.len(), chunk.fields.get(0).unwrap().len());
        assert_eq!(chunk.time.len(), chunk.fields.get(1).unwrap().len());
        for index in 0..chunk.time.len() {
            let right = chunk.fields.get(0).unwrap().sample(index);
            let left = chunk.fields.get(1).unwrap().sample(index);
            match chunk.time.timestamp(index).unwrap() {
                2000 => {
                    assert!(
                        matches!(right, SampleRef::Float(value) if value == 0.0 && value.is_sign_negative())
                    );
                    assert!(matches!(left, SampleRef::Missing));
                }
                3000 => {
                    assert!(matches!(right, SampleRef::Float(13.0)));
                    assert!(matches!(left, SampleRef::Float(3.75)));
                }
                timestamp => panic!("unexpected returned timestamp {timestamp}"),
            }
        }
    }
    assert_eq!(vec![2000, 3000], timestamps);
    assert!(
        chunks
            .iter()
            .take(chunks.len() - 1)
            .all(|chunk| !chunk.is_last)
    );
    assert!(chunks.last().unwrap().is_last);
    let normal_after_raw = query_pretty(&frontend, &frontend_sql, query_ctx()).await;
    assert_eq!(normal_before, normal_after_raw);

    execute_sql(
        &frontend,
        &format!("INSERT INTO {TABLE} VALUES ('alpha', 4.75, 14.0, 9007199254740995, 4000)"),
    )
    .await;
    // Re-run the identical plan against the OLD snapshot after the write.
    let old_baseline = rpc_requests(&cluster, *datanode);
    let old_again = requester
        .handle_raw_series_query(
            LogicalQueryRequest {
                header: Some(region_request_header(&ctx)),
                region_id: RegionId::from_u64(region_id),
                plan: plan.clone(),
            },
            binding.clone(),
        )
        .await
        .unwrap();
    let old_again_chunks = old_again.try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(old_baseline + 1, rpc_requests(&cluster, *datanode));
    let old_again_times = old_again_chunks
        .iter()
        .flat_map(|chunk| chunk.time.as_explicit().unwrap().iter().copied())
        .collect::<Vec<_>>();
    assert_eq!(vec![2000, 3000], old_again_times);

    let mut wrong_binding = binding.clone();
    wrong_binding.field_ids.swap(0, 1);
    let wrong_order = requester
        .handle_raw_series_query(
            LogicalQueryRequest {
                header: Some(region_request_header(&ctx)),
                region_id: RegionId::from_u64(region_id),
                plan: plan.clone(),
            },
            wrong_binding,
        )
        .await;
    let wrong_order_error = expect_raw_unsupported(wrong_order).await;
    assert!(
        wrong_order_error.contains("unsupported native raw-series plan"),
        "expected unsupported native raw-series plan error, got: {wrong_order_error}"
    );
    assert_eq!(old_baseline + 2, rpc_requests(&cluster, *datanode));

    let parent_filter = LogicalPlanBuilder::from(plan.clone())
        .filter(col("right_value").gt(lit(0.0_f64)))
        .unwrap()
        .build()
        .unwrap();
    let unsupported = requester
        .handle_raw_series_query(
            LogicalQueryRequest {
                header: Some(region_request_header(&ctx)),
                region_id: RegionId::from_u64(region_id),
                plan: parent_filter,
            },
            binding.clone(),
        )
        .await;
    let unsupported_error = expect_raw_unsupported(unsupported).await;
    assert!(
        unsupported_error.contains("unsupported native raw-series plan"),
        "expected unsupported native raw-series plan error, got: {unsupported_error}"
    );
    assert_eq!(old_baseline + 3, rpc_requests(&cluster, *datanode));

    let fresh_ctx = query_ctx();
    fresh_ctx.set_snapshot(region_id, region.find_committed_sequence());
    let fresh_baseline = rpc_requests(&cluster, *datanode);
    let fresh = requester
        .handle_raw_series_query(
            LogicalQueryRequest {
                header: Some(region_request_header(&fresh_ctx)),
                region_id: RegionId::from_u64(region_id),
                plan: plan.clone(),
            },
            binding.clone(),
        )
        .await
        .unwrap();
    let fresh_chunks = fresh.try_collect::<Vec<_>>().await.unwrap();
    let fresh_times = fresh_chunks
        .iter()
        .flat_map(|chunk| chunk.time.as_explicit().unwrap().iter().copied())
        .collect::<Vec<_>>();
    assert_eq!(fresh_baseline + 1, rpc_requests(&cluster, *datanode));
    assert_eq!(vec![2000, 3000, 4000], fresh_times);

    // A raw marker must not contaminate an ordinary request using the same requester and context.
    let ordinary_baseline = rpc_requests(&cluster, *datanode);
    // The scan's pushed-down predicate is inexact for ordinary execution, so keep an explicit
    // residual Filter above an unfiltered full-schema scan before projecting the output fields.
    let ordinary_plan = LogicalPlanBuilder::scan_with_filters(
        TableReference::bare(TABLE),
        source.clone(),
        None,
        vec![],
    )
    .unwrap()
    .filter(col("predicate_value").gt(lit(9_007_199_254_740_992_i64)))
    .unwrap()
    .project(vec![col("right_value"), col("left_value")])
    .unwrap()
    .build()
    .unwrap();
    let ordinary_stream = Datanode::handle_query(
        &requester,
        LogicalQueryRequest {
            header: Some(region_request_header(&ctx)),
            region_id: RegionId::from_u64(region_id),
            plan: ordinary_plan,
        },
    )
    .await
    .unwrap();
    let ordinary_batches = RecordBatches::try_collect(ordinary_stream).await.unwrap();
    assert_eq!(ordinary_baseline + 1, rpc_requests(&cluster, *datanode));
    let mut ordinary_rows = Vec::new();
    for batch in ordinary_batches.iter() {
        let batch = batch.df_record_batch();
        assert_eq!(
            vec!["right_value", "left_value"],
            batch
                .schema_ref()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>()
        );
        let right = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("right_value column");
        let left = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("left_value column");
        for index in 0..batch.num_rows() {
            ordinary_rows.push((right.value(index), left.is_null(index), left.value(index)));
        }
    }
    assert_eq!(2, ordinary_rows.len());
    assert!(ordinary_rows[0].0.is_sign_negative() && ordinary_rows[0].0 == 0.0);
    assert!(ordinary_rows[0].1);
    assert_eq!(13.0, ordinary_rows[1].0);
    assert!(!ordinary_rows[1].1);
    assert_eq!(3.75, ordinary_rows[1].2);

    let ordinary_after = query_pretty(&frontend, &frontend_sql, query_ctx()).await;
    let ordinary_rows = table_cells(&ordinary_after);
    assert_eq!(3, ordinary_rows.len());
    assert!(
        ordinary_rows.contains(&vec![
            "1970-01-01T00:00:04".to_string(),
            "4.75".to_string(),
            "14.0".to_string(),
        ]),
        "unexpected fresh ordinary result:\n{ordinary_after}"
    );

    assert_raw_entrypoint_wrapper_contract(
        &cluster, *datanode, region_id, TABLE, &ctx, &plan, binding,
    )
    .await;
}

async fn assert_raw_entrypoint_wrapper_contract(
    cluster: &GreptimeDbCluster,
    datanode_id: u64,
    region_id: u64,
    table: &str,
    query_ctx: &QueryContextRef,
    plan: &LogicalPlan,
    binding: RawSeriesRequest,
) {
    use common_base::Plugins;
    use common_query::OutputData;
    use common_query::series_frame::raw_series_frame_ordering;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::common::TableReference;
    use datafusion::datasource::DefaultTableSource;
    use datafusion::logical_expr::TableSource;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::coop::CooperativeExec;
    use datafusion::physical_plan::execution_plan::SchedulingType;
    use datafusion_expr::{col, lit};
    use datatypes::series::raw::RawSeriesOrdering;
    use query::dummy_catalog::DummyTableProviderFactory;
    use query::options::QueryOptions;
    use query::physical_wrapper::{PhysicalPlanWrapper, PhysicalPlanWrapperRef};
    use query::query_engine::QueryEngineFactory;
    use std::any::Any;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use table::table::scan::SeriesScanExec;

    struct TestWrapper {
        identity: bool,
        calls: Arc<AtomicUsize>,
    }

    impl PhysicalPlanWrapper for TestWrapper {
        fn wrap(
            &self,
            origin: Arc<dyn ExecutionPlan>,
            _ctx: QueryContextRef,
        ) -> Arc<dyn ExecutionPlan> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.identity {
                origin
            } else {
                Arc::new(CoalescePartitionsExec::new(origin))
            }
        }
    }

    let region_id = RegionId::from_u64(region_id);
    let region_server = region_server(cluster, datanode_id);
    let region_engine = region_server
        .find_engine(region_id)
        .unwrap()
        .expect("table region engine should be registered");
    let provider = DummyTableProviderFactory
        .create_table_provider(region_id, region_engine, Some(query_ctx.clone()))
        .await
        .unwrap();
    let provider_source: Arc<dyn TableSource> =
        Arc::new(DefaultTableSource::new(Arc::new(provider)));
    let LogicalPlan::TableScan(scan) = plan else {
        panic!("fixture plan must be a root table scan");
    };
    let scan = LogicalPlanBuilder::scan_with_filters(
        TableReference::bare(table),
        provider_source.clone(),
        scan.projection.clone(),
        scan.filters.clone(),
    )
    .unwrap()
    .build()
    .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let plugins_for = |identity| {
        let plugins = Plugins::new();
        plugins.insert(Arc::new(TestWrapper {
            identity,
            calls: calls.clone(),
        }) as PhysicalPlanWrapperRef);
        plugins
    };
    let make_engine = |identity| {
        QueryEngineFactory::new_with_plugins(
            catalog::memory::new_memory_catalog_manager().unwrap(),
            None,
            None,
            None,
            None,
            None,
            false,
            plugins_for(identity),
            QueryOptions::default(),
        )
        .query_engine()
    };

    let identity_engine = make_engine(true);
    let identity_output = identity_engine
        .execute_raw_series(scan.clone(), query_ctx.clone(), binding.clone())
        .await
        .unwrap();
    assert_eq!(1, calls.load(Ordering::SeqCst));
    let identity_plan = identity_output
        .meta
        .plan
        .as_ref()
        .expect("raw-series output should retain its physical plan");
    let native_root_schema = identity_plan.schema();
    {
        let root = (identity_plan.as_ref() as &dyn Any)
            .downcast_ref::<CooperativeExec>()
            .expect("native raw-series root should be CooperativeExec");
        let child = (root.input().as_ref() as &dyn Any)
            .downcast_ref::<SeriesScanExec>()
            .expect("native CooperativeExec input should be SeriesScanExec");
        assert!(
            child.children().is_empty(),
            "SeriesScanExec should be a leaf"
        );
        let schema = child.schema();
        assert_eq!(schema.as_ref(), identity_plan.schema().as_ref());
        assert_eq!(
            schema.as_ref(),
            root.properties().equivalence_properties().schema().as_ref()
        );
    }
    assert_eq!(
        1,
        identity_plan
            .properties()
            .output_partitioning()
            .partition_count()
    );
    assert_eq!(
        SchedulingType::Cooperative,
        identity_plan.properties().scheduling_type
    );
    let OutputData::Stream(identity_stream) = identity_output.data else {
        panic!("native raw-series entrypoint should return a stream");
    };
    let native_batches = RecordBatches::try_collect(identity_stream).await.unwrap();
    assert!(native_batches.iter().next().is_some());
    let native_schema = native_batches.schema();
    let native_schema = native_schema.arrow_schema();
    assert_eq!(native_root_schema.as_ref(), native_schema.as_ref());
    assert_eq!(
        RawSeriesOrdering::ReadKeyThenTime,
        raw_series_frame_ordering(native_schema).unwrap()
    );
    assert_eq!(
        vec!["read_key", "is_last", "time", "fields"],
        native_schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>()
    );
    let DataType::Struct(fields) = native_schema.field_with_name("fields").unwrap().data_type()
    else {
        panic!("native raw-series fields column must be a struct");
    };
    assert_eq!(
        vec!["right_value", "left_value"],
        fields
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>()
    );

    calls.store(0, Ordering::SeqCst);
    let wrapped_engine = make_engine(false);
    let raw_error = match wrapped_engine
        .execute_raw_series(scan.clone(), query_ctx.clone(), binding)
        .await
    {
        Err(error) => error,
        Ok(_) => panic!("same-schema one-partition nonidentity wrapper must be rejected"),
    };
    assert_eq!(1, calls.load(Ordering::SeqCst));
    assert!(
        raw_error
            .to_string()
            .contains("unsupported native raw-series plan")
    );

    let ordinary_plan = LogicalPlanBuilder::scan_with_filters(
        TableReference::bare(table),
        provider_source,
        None,
        vec![],
    )
    .unwrap()
    .filter(col("predicate_value").gt(lit(9_007_199_254_740_992_i64)))
    .unwrap()
    .project(vec![col("right_value"), col("left_value")])
    .unwrap()
    .build()
    .unwrap();
    let ordinary = wrapped_engine
        .execute(ordinary_plan, query_ctx.clone())
        .await
        .unwrap();
    let OutputData::Stream(ordinary_stream) = ordinary.data else {
        panic!("ordinary scan should return a stream");
    };
    let ordinary_batches = RecordBatches::try_collect(ordinary_stream).await.unwrap();
    assert_eq!(
        2,
        table_cells(&ordinary_batches.pretty_print().unwrap()).len()
    );
    assert_eq!(2, calls.load(Ordering::SeqCst));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_native_histogram_raw_flight_preserves_payload_across_flush() {
    use api::greptime_proto::io::prometheus::write::v2::histogram::{Count, ZeroCount};
    use api::greptime_proto::io::prometheus::write::v2::{BucketSpan, Histogram};
    use api::v1::helper::{field_column_schema, tag_column_schema, time_index_column_schema};
    use api::v1::value::ValueData;
    use api::v1::{ColumnDataType, Row, RowInsertRequest, RowInsertRequests, Rows, SemanticType};
    use common_query::OutputData;
    use common_query::native_histogram::{
        CounterResetHint, encode_native_histogram, native_histogram_column_schema,
        native_histogram_value_type,
    };
    use common_query::request::{QueryRequest as LogicalQueryRequest, RawSeriesTag};
    use datafusion::common::TableReference;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::series::histogram::{BucketSideView, HistogramView};
    use datatypes::series::raw::RawSeriesOrdering;
    use datatypes::series::samples::SampleRef;
    use futures_util::TryStreamExt;
    use mito2::test_util::sst_util::new_primary_key;
    use store_api::codec::PrimaryKeyEncoding;
    use store_api::region_engine::RawLabelSource;

    const TABLE: &str = "native_histogram_raw_flight_flush";
    common_telemetry::init_default_ut_logging();
    let cluster =
        build_cluster("test_native_histogram_raw_flight_preserves_payload_across_flush").await;
    let frontend = cluster.fe_instance().clone();
    execute_sql(
        &frontend,
        &format!(
            "CREATE TABLE {TABLE} (host STRING, ts TIMESTAMP TIME INDEX, float_value DOUBLE, PRIMARY KEY(host)) ENGINE=mito"
        ),
    )
    .await;

    let histogram_column = native_histogram_column_schema().unwrap();
    let histograms = [
        Histogram {
            count: Some(Count::CountInt(8)),
            sum: 10.0,
            schema: 1,
            zero_threshold: 0.001,
            zero_count: Some(ZeroCount::ZeroCountInt(1)),
            negative_spans: vec![BucketSpan {
                offset: -2,
                length: 1,
            }],
            negative_deltas: vec![1],
            positive_spans: vec![BucketSpan {
                offset: 0,
                length: 3,
            }],
            positive_deltas: vec![1, 2, -1],
            reset_hint: 2,
            timestamp: 1_000,
            start_timestamp: 500,
            ..Default::default()
        },
        Histogram {
            count: Some(Count::CountFloat(6.5)),
            sum: 20.0,
            schema: 2,
            zero_threshold: 0.002,
            zero_count: Some(ZeroCount::ZeroCountFloat(0.5)),
            positive_spans: vec![BucketSpan {
                offset: 3,
                length: 2,
            }],
            positive_counts: vec![2.0, 3.5],
            negative_spans: vec![BucketSpan {
                offset: -1,
                length: 1,
            }],
            negative_counts: vec![0.5],
            reset_hint: 3,
            timestamp: 2_000,
            start_timestamp: 750,
            ..Default::default()
        },
        Histogram {
            count: Some(Count::CountFloat(3.5)),
            sum: -0.0,
            schema: -53,
            zero_count: Some(ZeroCount::ZeroCountFloat(0.0)),
            custom_values: vec![1.5, 2.5],
            positive_spans: vec![BucketSpan {
                offset: 0,
                length: 3,
            }],
            positive_counts: vec![0.5, 1.25, 1.75],
            reset_hint: 77,
            timestamp: 3_000,
            ..Default::default()
        },
    ];
    let histograms = histograms
        .iter()
        .map(|histogram| encode_native_histogram(histogram).unwrap().into())
        .collect::<Vec<_>>();
    let mut schema = vec![
        tag_column_schema("host", ColumnDataType::String),
        time_index_column_schema("ts", ColumnDataType::TimestampMillisecond),
        field_column_schema("float_value", ColumnDataType::Float64),
    ];
    schema.push(histogram_column);
    let rows = [1_000, 2_000, 3_000, 4_000]
        .into_iter()
        .zip(histograms.into_iter().map(Some).chain([None]))
        .zip([None, Some(-0.0), Some(4.75), Some(7.25)])
        .map(|((timestamp, histogram), value)| Row {
            values: vec![
                ValueData::StringValue("alpha".to_string()).into(),
                ValueData::TimestampMillisecondValue(timestamp).into(),
                value
                    .map(ValueData::F64Value)
                    .map(Into::into)
                    .unwrap_or_default(),
                histogram.unwrap_or_default(),
            ],
        })
        .collect();
    let output = frontend
        .handle_row_inserts(
            RowInsertRequests {
                inserts: vec![RowInsertRequest {
                    table_name: TABLE.to_string(),
                    rows: Some(Rows { schema, rows }),
                }],
            },
            QueryContext::arc(),
            false,
            false,
        )
        .await
        .unwrap();
    assert!(matches!(output.data, OutputData::AffectedRows(4)));

    let leaders = region_leaders(&frontend, TABLE).await;
    assert_eq!(1, leaders.len(), "fixture is required to have one region");
    let (datanode, address) = leaders.values().next().unwrap();
    let region_id = *leaders.keys().next().unwrap();
    let region = region_server(&cluster, *datanode)
        .mito_engine()
        .unwrap()
        .find_region(RegionId::from_u64(region_id))
        .unwrap();
    let metadata = region.metadata();
    let histogram_column = metadata
        .column_by_name("greptime_native_histogram")
        .unwrap();
    let float_column = metadata.column_by_name("float_value").unwrap();
    let host_column = metadata.column_by_name("host").unwrap();
    assert_eq!(SemanticType::Field, histogram_column.semantic_type);
    assert_eq!(
        native_histogram_value_type(),
        &histogram_column.column_schema.data_type
    );
    assert_eq!(PrimaryKeyEncoding::Dense, metadata.primary_key_encoding);
    assert_eq!(
        vec!["host"],
        metadata
            .primary_key_columns()
            .map(|column| column.column_schema.name.as_str())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        &ConcreteDataType::float64_datatype(),
        &float_column.column_schema.data_type
    );
    let histogram_index = metadata
        .column_index_by_name("greptime_native_histogram")
        .unwrap();
    let float_index = metadata.column_index_by_name("float_value").unwrap();
    let projection = vec![histogram_index, float_index];
    let binding = RawSeriesRequest {
        version: 1,
        physical_metadata: metadata.as_ref().clone(),
        metric_name: TABLE.to_string(),
        logical_tags: vec![RawSeriesTag {
            column_id: host_column.column_id,
            name: "host".to_string(),
        }],
        field_ids: vec![histogram_column.column_id, float_column.column_id],
    };
    let committed = region.find_committed_sequence();
    assert!(committed > 0);
    assert!(region.manifest_sst_entries().await.is_empty());

    let ctx = query_ctx();
    ctx.set_snapshot(region_id, committed);
    let base = plan_sql(&frontend, &format!("SELECT * FROM {TABLE}"), &ctx).await;
    let source = table_source(&base).expect("ordinary plan must contain its table source");
    let plan = LogicalPlanBuilder::scan_with_filters(
        TableReference::bare(TABLE),
        source,
        Some(projection),
        vec![],
    )
    .unwrap()
    .build()
    .unwrap();
    let expected = [
        HistogramView {
            schema: 1,
            zero_threshold: 0.001,
            sum: 10.0,
            count: 8.0,
            zero_count: 1.0,
            reset_hint: CounterResetHint::NotCounterReset,
            start_timestamp: Some(500),
            custom_values: &[],
            positive: BucketSideView {
                span_offsets: &[0],
                span_lengths: &[3],
                buckets: &[1.0, 3.0, 2.0],
            },
            negative: BucketSideView {
                span_offsets: &[-2],
                span_lengths: &[1],
                buckets: &[1.0],
            },
        },
        HistogramView {
            schema: 2,
            zero_threshold: 0.002,
            sum: 20.0,
            count: 6.5,
            zero_count: 0.5,
            reset_hint: CounterResetHint::Gauge,
            start_timestamp: Some(750),
            custom_values: &[],
            positive: BucketSideView {
                span_offsets: &[3],
                span_lengths: &[2],
                buckets: &[2.0, 3.5],
            },
            negative: BucketSideView {
                span_offsets: &[-1],
                span_lengths: &[1],
                buckets: &[0.5],
            },
        },
        HistogramView {
            schema: -53,
            zero_threshold: 0.0,
            sum: -0.0,
            count: 3.5,
            zero_count: 0.0,
            reset_hint: CounterResetHint::Unrecognized(77),
            start_timestamp: None,
            custom_values: &[1.5, 2.5],
            positive: BucketSideView {
                span_offsets: &[0],
                span_lengths: &[3],
                buckets: &[0.5, 1.25, 1.75],
            },
            negative: BucketSideView {
                span_offsets: &[],
                span_lengths: &[],
                buckets: &[],
            },
        },
    ];
    let expected_key = new_primary_key(&["alpha"]);
    let client = Client::with_query_and_control_managers(
        ChannelManager::new(),
        ChannelManager::new(),
        [address.as_str()],
    );
    let requester = RegionRequester::new(client, false, false);
    for after_flush in [false, true] {
        if after_flush {
            execute_sql(&frontend, &format!("ADMIN FLUSH_TABLE('{TABLE}')")).await;
            assert!(!region.manifest_sst_entries().await.is_empty());
            assert!(region.flushed_sequence() >= committed);
        }
        let baseline = rpc_requests(&cluster, *datanode);
        let stream = requester
            .handle_raw_series_query(
                LogicalQueryRequest {
                    header: Some(region_request_header(&ctx)),
                    region_id: RegionId::from_u64(region_id),
                    plan: plan.clone(),
                },
                binding.clone(),
            )
            .await
            .unwrap();
        assert_eq!(
            vec!["greptime_native_histogram", "float_value"],
            stream
                .schema()
                .fields
                .iter()
                .map(|field| field.name.as_ref())
                .collect::<Vec<_>>()
        );
        assert!(stream.schema().eval_grid.is_none());
        match stream.label_source() {
            RawLabelSource::PrimaryKey {
                metric_name,
                physical_metadata,
                logical_tags,
            } => {
                assert_eq!(TABLE, metric_name.as_ref());
                assert_eq!(metadata.as_ref(), physical_metadata.as_ref());
                assert_eq!(1, logical_tags.len());
                assert_eq!(host_column.column_id, logical_tags[0].column_id);
                assert_eq!("host", logical_tags[0].name.as_ref());
            }
            RawLabelSource::Explicit => panic!("expected key-backed source"),
        }
        assert_eq!(RawSeriesOrdering::ReadKeyThenTime, stream.ordering());
        let chunks = stream.try_collect::<Vec<_>>().await.unwrap();
        let mut timestamps = Vec::new();
        for (chunk_index, chunk) in chunks.iter().enumerate() {
            assert!(chunk.metric.is_none());
            assert_eq!(expected_key, chunk.read_key.as_slice());
            assert_eq!(2, chunk.fields.len());
            assert!(!chunk.time.is_empty());
            assert_eq!(chunk.time.len(), chunk.fields.get(0).unwrap().len());
            assert_eq!(chunk.time.len(), chunk.fields.get(1).unwrap().len());
            assert_eq!(chunk_index + 1 == chunks.len(), chunk.is_last);
            for index in 0..chunk.time.len() {
                let timestamp = chunk.time.timestamp(index).unwrap();
                timestamps.push(timestamp);
                let histogram = chunk.fields.get(0).unwrap().sample(index);
                let float_value = chunk.fields.get(1).unwrap().sample(index);
                match timestamp {
                    1_000 => {
                        let SampleRef::Histogram(actual) = histogram else {
                            panic!("expected histogram at {timestamp}")
                        };
                        assert_histogram_view(actual, expected[0]);
                        assert!(matches!(float_value, SampleRef::Missing));
                    }
                    2_000 => {
                        let SampleRef::Histogram(actual) = histogram else {
                            panic!("expected histogram at {timestamp}")
                        };
                        assert_histogram_view(actual, expected[1]);
                        assert!(
                            matches!(float_value, SampleRef::Float(value) if value.to_bits() == (-0.0_f64).to_bits())
                        );
                    }
                    3_000 => {
                        let SampleRef::Histogram(actual) = histogram else {
                            panic!("expected histogram at {timestamp}")
                        };
                        assert_histogram_view(actual, expected[2]);
                        assert!(
                            matches!(float_value, SampleRef::Float(value) if value.to_bits() == 4.75_f64.to_bits())
                        );
                    }
                    4_000 => {
                        assert!(matches!(histogram, SampleRef::Missing));
                        assert!(
                            matches!(float_value, SampleRef::Float(value) if value.to_bits() == 7.25_f64.to_bits())
                        );
                    }
                    timestamp => panic!("unexpected returned timestamp {timestamp}"),
                }
            }
        }
        assert_eq!(vec![1_000, 2_000, 3_000, 4_000], timestamps);
        assert_eq!(baseline + 1, rpc_requests(&cluster, *datanode));
    }

    fn assert_histogram_view(actual: HistogramView<'_>, expected: HistogramView<'_>) {
        assert_eq!(expected.schema, actual.schema);
        assert_eq!(expected.reset_hint, actual.reset_hint);
        assert_eq!(expected.start_timestamp, actual.start_timestamp);
        assert_eq!(
            expected.zero_threshold.to_bits(),
            actual.zero_threshold.to_bits()
        );
        assert_eq!(expected.sum.to_bits(), actual.sum.to_bits());
        assert_eq!(expected.count.to_bits(), actual.count.to_bits());
        assert_eq!(expected.zero_count.to_bits(), actual.zero_count.to_bits());
        let bits = |values: &[f64]| {
            values
                .iter()
                .map(|value| value.to_bits())
                .collect::<Vec<_>>()
        };
        assert_eq!(bits(expected.custom_values), bits(actual.custom_values));
        for (expected, actual) in [
            (expected.positive, actual.positive),
            (expected.negative, actual.negative),
        ] {
            assert_eq!(expected.span_offsets, actual.span_offsets);
            assert_eq!(expected.span_lengths, actual.span_lengths);
            assert_eq!(bits(expected.buckets), bits(actual.buckets));
        }
    }
}
