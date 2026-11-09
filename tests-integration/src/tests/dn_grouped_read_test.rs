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

//! Grouped datanode reads: one Flight request per datanode instead of one per region.
//!
//! The frontend groups the regions of one ordinary single-table scan by the datanode their leader
//! lives on and sends one GroupRead root around a combine tree per group. The datanode replaces
//! every RegionPlan leaf with that region's bound logical subtree and executes the resulting
//! ordinary multi-input plan, so the rows are the rows of the ordinary one-request-per-region path.
//!
//! The request counts below are the datanodes' own Flight `DoGet` counters, reached over real TCP
//! (the fixture turns the in-process duplex lane off), not the planner's group bookkeeping.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use api::v1::region::{
    QueryRequest as RegionQueryRequest, RegionRequestHeader, RemoteDynFilterRequest,
    RemoteDynFilterUpdate, region_request, remote_dyn_filter_request,
};
use common_query::OutputData;
use common_query::request::{
    DynFilterPayload, INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY, InitialDynFilterReg,
    InitialDynFilterRegs, REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES,
};
use common_recordbatch::adapter::region_scan_output_bytes;
use datafusion::common::TableReference;
use datafusion::datasource::DefaultTableSource;
use datafusion::functions_aggregate::sum::sum;
use datafusion::logical_expr::{LogicalPlanBuilder, col};
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::expressions::lit as physical_lit;
use datanode::error::{Error as DatanodeError, Result as DatanodeResult};
use datatypes::arrow::array::AsArray;
use datatypes::arrow::datatypes::UInt64Type;
use futures::StreamExt;
use meta_srv::metasrv::SelectTarget;
use meta_srv::selector::round_robin::RoundRobinSelector;
use query::dist_plan::{
    MAX_GROUP_READ_PAYLOAD_BYTES, MergeScanLogicalPlan, build_group_read_plan,
    build_group_read_plan_with_combine, peek_group_read_envelope,
};
use query::query_engine::DefaultSerializer;
use servers::grpc::region_server::RegionServerHandler;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::QueryContext;
use store_api::storage::RegionId;
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
use table::table::adapter::DfTableProviderAdapter;
use table::table::scan::REGION_SCAN_EXEC_NAME;

use crate::cluster::{GreptimeDbCluster, GreptimeDbClusterBuilder};

const TABLE: &str = "dn_grouped_read";
const BASE_MILLIS: i64 = 1_700_000_000_000;

/// `(ts offset seconds, host, k, v)`; the `>= 'd'` range of the table stays empty, and two rows
/// carry a null tag and a null value.
const ROWS: [(i64, &str, Option<&str>, Option<f64>); 6] = [
    (1, "a0", Some("k0"), Some(1.0)),
    (2, "a1", None, None),
    (3, "b0", Some("k1"), Some(3.0)),
    (4, "c0", Some("k2"), Some(4.0)),
    (5, "c1", None, None),
    (6, "c2", Some("k3"), Some(5.0)),
];

async fn build_cluster(name: &str) -> GreptimeDbCluster {
    GreptimeDbClusterBuilder::new(name)
        .await
        .with_real_datanode_grpc_addr(true)
        .with_frontend_tcp_datanode_clients(true)
        .with_datanodes(2)
        .with_meta_selector(Arc::new(RoundRobinSelector::new(SelectTarget::Datanode)))
        .build(false)
        .await
}

async fn prepare_fixture(fe: &Arc<frontend::instance::Instance>) {
    execute_sql(
        fe,
        &format!(
            "CREATE TABLE {TABLE} (
                ts TIMESTAMP(3) TIME INDEX,
                host STRING,
                k STRING,
                v DOUBLE,
                PRIMARY KEY (host, k)
            ) PARTITION ON COLUMNS (host) (
                host < 'b',
                host >= 'b' AND host < 'c',
                host >= 'c' AND host < 'd',
                host >= 'd'
            )"
        ),
    )
    .await;
    let values = ROWS
        .iter()
        .map(|(offset, host, k, v)| {
            let ts = BASE_MILLIS + offset * 1000;
            let k = k
                .map(|k| format!("'{k}'"))
                .unwrap_or_else(|| "NULL".to_string());
            let v = v
                .map(|v| format!("{v}"))
                .unwrap_or_else(|| "NULL".to_string());
            format!("({ts}, '{host}', {k}, {v})")
        })
        .collect::<Vec<_>>()
        .join(", ");
    execute_sql(fe, &format!("INSERT INTO {TABLE} VALUES {values}")).await;
}

/// One row of the results below, canonicalized as a string.
type Row = Vec<String>;

fn do_gets(cluster: &GreptimeDbCluster) -> BTreeMap<u64, usize> {
    cluster
        .datanode_instances
        .keys()
        .map(|id| {
            (
                *id,
                cluster
                    .datanode_rpc_stats(*id)
                    .unwrap_or_else(|| panic!("expected rpc stats for datanode {id}"))
                    .requests(),
            )
        })
        .collect()
}

fn delta(before: &BTreeMap<u64, usize>, after: &BTreeMap<u64, usize>) -> BTreeMap<u64, usize> {
    after
        .iter()
        .map(|(id, count)| (*id, count - before[id]))
        .collect()
}

fn total(delta: &BTreeMap<u64, usize>) -> usize {
    delta.values().sum()
}

async fn run_rows(fe: &Arc<frontend::instance::Instance>, sql: &str) -> Vec<Row> {
    let mut rows = run_rows_in_order(fe, sql).await;
    rows.sort();
    rows
}

/// Runs one statement and keeps the engine's row order, for statements that promise an order.
async fn run_rows_in_order(fe: &Arc<frontend::instance::Instance>, sql: &str) -> Vec<Row> {
    run_rows_with_context(fe, sql, QueryContext::arc()).await
}

async fn run_rows_with_context(
    fe: &Arc<frontend::instance::Instance>,
    sql: &str,
    query_ctx: session::context::QueryContextRef,
) -> Vec<Row> {
    let output = SqlQueryHandler::do_query(fe.as_ref(), sql, query_ctx)
        .await
        .remove(0)
        .unwrap_or_else(|error| panic!("`{sql}` failed: {error}"));
    let batches = match output.data {
        OutputData::Stream(stream) => common_recordbatch::util::collect_batches(stream)
            .await
            .unwrap_or_else(|error| panic!("`{sql}` produced no batches: {error}")),
        OutputData::RecordBatches(batches) => batches,
        other => panic!("`{sql}` produced no batches: {other:?}"),
    };
    let pretty = batches
        .pretty_print()
        .unwrap_or_else(|error| panic!("`{sql}` produced no printable batches: {error}"));
    let rows = pretty
        .lines()
        .filter(|line| line.starts_with('|'))
        .skip(1)
        .map(|line| {
            line.trim_matches('|')
                .split('|')
                .map(|cell| cell.trim().to_string())
                .collect::<Row>()
        })
        .collect::<Vec<_>>();
    rows
}

async fn execute_sql(fe: &Arc<frontend::instance::Instance>, sql: &str) {
    for result in fe.do_query(sql, QueryContext::arc()).await {
        result.unwrap_or_else(|error| panic!("`{sql}` failed: {error}"));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn grouped_read_batches_one_request_per_datanode_and_keeps_rows() {
    common_telemetry::init_default_ut_logging();
    let cluster = build_cluster("dn_grouped_read_scan").await;
    let fe = cluster.fe_instance().clone();
    prepare_fixture(&fe).await;

    // The grouped shape: one ordinary scan with a projection and a filter, so the frontend groups
    // the four regions by their leaders and sends one envelope per datanode.
    let grouped_sql = format!("SELECT ts, host, k, v FROM {TABLE} WHERE v IS NOT NULL");
    let before = do_gets(&cluster);
    let grouped = run_rows(&fe, &grouped_sql).await;
    let grouped_requests = delta(&before, &do_gets(&cluster));
    assert_eq!(
        2,
        total(&grouped_requests),
        "two datanodes must receive one grouped request each, got {grouped_requests:?}"
    );
    assert!(
        grouped_requests.values().all(|count| *count == 1),
        "each datanode must receive exactly one grouped request, got {grouped_requests:?}"
    );

    // A low query parallelism must not disable batching or change the rows.
    let mut low_parallelism = QueryContext::with("greptime", "public");
    low_parallelism.set_extension("query.parallelism", "1");
    let before = do_gets(&cluster);
    let mut low_rows = run_rows_with_context(&fe, &grouped_sql, Arc::new(low_parallelism)).await;
    low_rows.sort();
    assert_eq!(grouped, low_rows);
    let low_requests = delta(&before, &do_gets(&cluster));
    assert_eq!(
        2,
        total(&low_requests),
        "query.parallelism=1 must still batch to one request per datanode: {low_requests:?}"
    );

    // Both Flow source and sink contexts must keep one request per selected region.
    let table = fe
        .catalog_manager()
        .table("greptime", "public", TABLE, None)
        .await
        .unwrap()
        .unwrap();
    for (key, value) in [
        ("flow.return_region_seq", "true".to_string()),
        (
            "flow.sink_table_id",
            table.table_info().table_id().to_string(),
        ),
    ] {
        let mut ctx = QueryContext::with("greptime", "public");
        ctx.set_extension(key, value);
        let before = do_gets(&cluster);
        let mut flow_rows = run_rows_with_context(&fe, &grouped_sql, Arc::new(ctx)).await;
        flow_rows.sort();
        assert_eq!(grouped, flow_rows, "Flow context {key}");
        let requests = delta(&before, &do_gets(&cluster));
        assert_eq!(
            4,
            total(&requests),
            "Flow context {key} must send one ordinary request per region: {requests:?}"
        );
    }

    // An ordered statement must stay globally ordered whether the scan below it is batched or not:
    // the frontend sort does not rely on any region order below it, and the grouped Append path
    // is not chosen for a scan that itself promises an order.
    let ordered_sql = format!("SELECT ts, host, k, v FROM {TABLE} WHERE v IS NOT NULL ORDER BY ts");
    let before = do_gets(&cluster);
    let ordered = run_rows_in_order(&fe, &ordered_sql).await;
    let ordered_requests = delta(&before, &do_gets(&cluster));
    assert!(
        (2..=4).contains(&total(&ordered_requests)),
        "an ordered statement may batch the scan, but never issues more requests than regions: got \
         {ordered_requests:?}"
    );
    let mut ordered_multiset = ordered.clone();
    ordered_multiset.sort();
    assert_eq!(
        grouped, ordered_multiset,
        "the grouped rows must be the rows of the ordinary per-region path"
    );
    assert!(
        ordered.windows(2).all(|pair| pair[0][0] <= pair[1][0]),
        "the ordered statement must stay globally ordered under grouping: {ordered:#?}"
    );

    // Independent expectation: the fixture's three non-null rows, in the rows' `host` column.
    let hosts = grouped.iter().map(|row| row[1].clone()).collect::<Vec<_>>();
    assert_eq!(
        vec![
            "a0".to_string(),
            "b0".to_string(),
            "c0".to_string(),
            "c2".to_string(),
        ],
        hosts,
        "unexpected rows:\n{grouped:#?}"
    );

    // A pruned subset: `host >= 'b'` selects the second, third and fourth ranges. The third range
    // shares the second and fourth range's datanode, so one group is a multi-region group and the
    // other a singleton, and the request count is two instead of three.
    let pruned_sql = format!("SELECT ts, host, v FROM {TABLE} WHERE host >= 'b' AND v IS NOT NULL");
    let before = do_gets(&cluster);
    let pruned = run_rows(&fe, &pruned_sql).await;
    let pruned_requests = delta(&before, &do_gets(&cluster));
    assert_eq!(
        2,
        total(&pruned_requests),
        "the pruned selection must be exactly two grouped requests, got {pruned_requests:?}"
    );
    assert!(
        pruned_requests.values().all(|count| *count == 1),
        "the multi-region group and the singleton group must be one request each, got \
         {pruned_requests:?}"
    );
    assert_eq!(3, pruned.len(), "unexpected pruned rows: {pruned:#?}");

    // A limit must keep its semantics as well: at most two rows, in order.
    let limited_sql =
        format!("SELECT ts, host FROM {TABLE} WHERE v IS NOT NULL ORDER BY ts LIMIT 2");
    let before = do_gets(&cluster);
    let limited = run_rows_in_order(&fe, &limited_sql).await;
    let limited_requests = delta(&before, &do_gets(&cluster));
    assert!(
        (2..=4).contains(&total(&limited_requests)),
        "a limited statement may batch the scan, but never issues more requests than regions: got \
         {limited_requests:?}"
    );
    assert_eq!(2, limited.len(), "unexpected limited rows: {limited:#?}");
    assert!(
        limited.windows(2).all(|pair| pair[0][0] <= pair[1][0]),
        "a limited statement must stay globally ordered under grouping: {limited:#?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn grouped_read_rdf_and_rejections() {
    common_telemetry::init_default_ut_logging();
    let cluster = build_cluster("dn_grouped_read_rdf").await;
    let fe = cluster.fe_instance().clone();
    prepare_fixture(&fe).await;

    // A scalar subquery produces a two-phase captured dynamic filter on the scan; the remote plan
    // stays a single-table scan, so the grouped path is used and must answer the same rows.
    let rdf_sql = format!(
        "SELECT ts, host, v FROM {TABLE} WHERE v > (SELECT max(v) FROM {TABLE} WHERE host = 'a0')"
    );
    let before = do_gets(&cluster);
    let rdf_rows = run_rows(&fe, &rdf_sql).await;
    let rdf_requests = delta(&before, &do_gets(&cluster));
    // The scalar subquery is a second scan of the table: its own predicate selects one region, so
    // it adds one request, and the grouped outer scan adds one per datanode.
    assert_eq!(
        3,
        total(&rdf_requests),
        "the dynamic-filtered outer scan must batch to one request per datanode on top of the \
         subquery's single-region read, got {rdf_requests:?}"
    );
    assert!(
        rdf_requests.values().all(|count| *count >= 1),
        "both datanodes must serve the grouped outer scan, got {rdf_requests:?}"
    );
    let rdf_hosts = rdf_rows
        .iter()
        .map(|row| row[1].clone())
        .collect::<Vec<_>>();
    assert_eq!(
        vec!["b0".to_string(), "c0".to_string(), "c2".to_string()],
        rdf_hosts,
        "unexpected rows under a remotely pushed dynamic filter: {rdf_rows:#?}"
    );
    let mut rdf_disabled_ctx = QueryContext::with("greptime", "public");
    rdf_disabled_ctx.set_extension(
        query::options::QUERY_ENABLE_REMOTE_DYNAMIC_FILTER_PUSHDOWN,
        "false",
    );
    let mut rdf_disabled_rows =
        run_rows_with_context(&fe, &rdf_sql, Arc::new(rdf_disabled_ctx)).await;
    rdf_disabled_rows.sort();
    assert_eq!(
        rdf_rows, rdf_disabled_rows,
        "turning off remote filter pushdown must not change the grouped result"
    );
    // The dynamic filter's answer must be the answer of the same predicate written as a literal:
    // the outer scan may be batched and filtered remotely, but it may not change the rows.
    let literal_rows = run_rows(&fe, &format!("SELECT ts, host, v FROM {TABLE} WHERE v > 1")).await;
    assert_eq!(
        literal_rows, rdf_rows,
        "a remotely pushed dynamic filter must not change the rows of the equivalent literal \
         predicate"
    );

    let table = fe
        .catalog_manager()
        .table("greptime", "public", TABLE, None)
        .await
        .unwrap()
        .unwrap();
    let info = table.table_info();
    let nested = LogicalPlanBuilder::scan(
        TableReference::bare(TABLE),
        Arc::new(DefaultTableSource::new(Arc::new(
            DfTableProviderAdapter::new(table.clone()),
        ))),
        None,
    )
    .unwrap()
    .build()
    .unwrap();
    let all_regions = region_ids(&fe, TABLE).await;
    assert_eq!(4, all_regions.len(), "the fixture must have four regions");

    // A well-formed envelope sent straight to a datanode executes every region it names. One
    // envelope per datanode, naming that datanode's own regions, must partition the table's rows:
    // together they are every row and no row is returned twice.
    let mut rows_by_datanode = BTreeMap::new();
    let mut base = None;
    for datanode_id in cluster.datanode_instances.keys().copied() {
        let mut own_regions = region_ids_of_datanode(&fe, TABLE, datanode_id).await;
        own_regions.sort();
        let server = cluster
            .datanode_instances
            .get(&datanode_id)
            .unwrap()
            .region_server();
        // Keep only regions with rows so the heartbeat assertions cover non-empty owners. A
        // single-region GroupRead is the smallest way to establish the baseline.
        let mut non_empty = Vec::new();
        for region in &own_regions {
            let probe = build_group_read_plan(&[*region], info.table_id(), &nested).unwrap();
            let ctx = QueryContext::arc();
            let mut probe = server
                .handle_remote_read(
                    RegionQueryRequest {
                        header: Some(RegionRequestHeader {
                            query_context: Some(ctx.as_ref().into()),
                            ..Default::default()
                        }),
                        region_id: region.as_u64(),
                        plan: encode(&probe),
                    },
                    ctx,
                )
                .await
                .unwrap();
            let mut rows = 0;
            while let Some(batch) = probe.next().await {
                rows += batch.unwrap().num_rows();
            }
            let expected_bytes = probe
                .metrics()
                .as_ref()
                .map(region_scan_output_bytes)
                .unwrap_or(0);
            drop(probe);
            if rows > 0 {
                non_empty.push((*region, expected_bytes));
            }
        }
        assert!(
            !non_empty.is_empty(),
            "datanode {datanode_id} must own a non-empty region"
        );
        assert!(
            non_empty.iter().map(|(_, bytes)| bytes).sum::<usize>() > 0,
            "the non-empty probe must report scan bytes"
        );
        let (own_regions, expected_bytes): (Vec<_>, Vec<_>) = non_empty.into_iter().unzip();
        let envelope = build_group_read_plan(&own_regions, info.table_id(), &nested).unwrap();
        let bytes = encode(&envelope);
        let payload = peek_group_read_envelope(&bytes).unwrap().unwrap();
        assert_eq!(
            own_regions.iter().map(RegionId::as_u64).collect::<Vec<_>>(),
            payload.region_ids().to_vec(),
            "the envelope must name exactly the regions of datanode {datanode_id}"
        );
        if expected_bytes.len() >= 2 {
            assert_ne!(
                expected_bytes[0], expected_bytes[1],
                "the two non-empty regions must have different scan byte sizes"
            );
        }
        let stats_before = own_regions
            .iter()
            .map(|region| server.region_statistic(*region).unwrap())
            .collect::<Vec<_>>();
        let mut stream = server
            .handle_remote_read(
                RegionQueryRequest {
                    header: Some(RegionRequestHeader {
                        query_context: Some(QueryContext::arc().as_ref().into()),
                        ..Default::default()
                    }),
                    region_id: own_regions[0].as_u64(),
                    plan: bytes.clone(),
                },
                QueryContext::arc(),
            )
            .await
            .unwrap_or_else(|error| {
                panic!("the grouped envelope must execute on datanode {datanode_id}: {error}")
            });
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch.unwrap_or_else(|error| {
                panic!("the grouped stream on datanode {datanode_id} failed: {error}")
            }));
        }
        let metrics = stream
            .metrics()
            .expect("a finished grouped stream must report cumulative member metrics");
        let region_scans = metrics
            .plan_metrics
            .iter()
            .filter(|plan| plan.plan_name == REGION_SCAN_EXEC_NAME)
            .collect::<Vec<_>>();
        assert!(
            region_scans.len() >= own_regions.len(),
            "each member region must contribute its own RegionScanExec metrics, expected at least              {} entries: {metrics:?}",
            own_regions.len()
        );
        let scanned_bytes = region_scans
            .iter()
            .flat_map(|plan| &plan.metrics)
            .filter(|(name, _)| name == "output_bytes")
            .map(|(_, value)| *value)
            .sum::<usize>();
        assert!(
            scanned_bytes > 0,
            "the cumulative metrics must include region scan bytes: {metrics:?}"
        );
        assert!(
            metrics.region_watermarks.is_empty(),
            "grouped reads reject Flow contexts, so they carry no Flow watermarks: {metrics:?}"
        );
        assert_eq!(
            None, metrics.query_load_region_id,
            "a grouped request cannot attribute query load to one region"
        );
        drop(stream);
        for ((region, expected_bytes), before) in
            own_regions.iter().zip(expected_bytes).zip(stats_before)
        {
            let after = server.region_statistic(*region).unwrap();
            assert_eq!(
                expected_bytes,
                (after.query_scanned_bytes - before.query_scanned_bytes) as usize,
                "region {region} heartbeat scanned bytes must match its own scan"
            );
            assert!(
                after.query_cpu_time > before.query_cpu_time,
                "region {region} heartbeat CPU time must increase: {} -> {}",
                before.query_cpu_time,
                after.query_cpu_time
            );
        }
        let mut rows = Vec::new();
        for batch in batches.iter() {
            let batch = batch.df_record_batch();
            // The region emits dictionary-encoded tags, so normalize before reading strings.
            let host = datatypes::arrow::compute::cast(
                batch.column_by_name("host").unwrap(),
                &datatypes::arrow::datatypes::DataType::Utf8,
            )
            .unwrap();
            let host = host.as_string::<i32>();
            for row in 0..batch.num_rows() {
                rows.push(host.value(row).to_string());
            }
        }
        assert!(
            !rows.is_empty(),
            "datanode {datanode_id} owns regions, so its grouped read must return rows"
        );
        if own_regions.len() >= 2 {
            base.get_or_insert((server, own_regions, bytes));
        }
        rows_by_datanode.insert(datanode_id, rows);
    }
    let (server, own_regions, bytes) = base.expect("one datanode must own two non-empty regions");
    assert!(own_regions.len() >= 2);
    let returned = rows_by_datanode
        .values()
        .flatten()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let all_rows = ["a0", "a1", "b0", "c0", "c1", "c2"]
        .map(str::to_string)
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        all_rows, returned,
        "the grouped read of each datanode must return exactly its own regions: \
         {rows_by_datanode:?}"
    );
    assert_eq!(
        6,
        rows_by_datanode.values().map(Vec::len).sum::<usize>(),
        "no row may be returned by both datanodes: {rows_by_datanode:?}"
    );

    // Rejections: the request anchor must be named, regions and table must agree, and the nested
    // plan must be the eligible single-table shape.
    let request_for = |region: RegionId, plan: Vec<u8>| RegionQueryRequest {
        header: Some(RegionRequestHeader {
            query_context: Some(QueryContext::arc().as_ref().into()),
            ..Default::default()
        }),
        region_id: region.as_u64(),
        plan,
    };
    let assert_rejected = |what: &'static str, result: DatanodeResult<_>| -> DatanodeError {
        let error = result
            .err()
            .unwrap_or_else(|| panic!("{what} must be rejected"));
        assert!(
            !error.to_string().is_empty(),
            "{what}: the rejection must explain itself"
        );
        error
    };

    // The whole request is bounded before the request-scoped decoder runs. Padding uses an
    // unknown protobuf field so the root still peeks as a valid GroupRead.
    let mut oversized = bytes.clone();
    oversized.push(0x7a); // field 15, wire type 2
    let padding = MAX_GROUP_READ_PAYLOAD_BYTES + 1024 - oversized.len() - 4;
    push_varint(padding, &mut oversized);
    oversized.resize(MAX_GROUP_READ_PAYLOAD_BYTES + 1024, 0);
    let error = assert_rejected(
        "a request above the total size bound",
        server
            .handle_remote_read(request_for(own_regions[0], oversized), QueryContext::arc())
            .await,
    );
    assert!(
        error.to_string().contains("above the") && error.to_string().contains("byte bound"),
        "{error}"
    );

    let anchor_missing = mutate_payload(&bytes, |payload| {
        payload["region_ids"] = serde_json::json!([own_regions[0].as_u64()]);
    });
    assert_rejected(
        "an anchor outside the named regions",
        server
            .handle_remote_read(
                request_for(own_regions[1], anchor_missing),
                QueryContext::arc(),
            )
            .await,
    );
    let duplicate = mutate_payload(&bytes, |payload| {
        payload["region_ids"] =
            serde_json::json!([own_regions[0].as_u64(), own_regions[0].as_u64()]);
    });
    assert_rejected(
        "a duplicated region",
        server
            .handle_remote_read(request_for(own_regions[0], duplicate), QueryContext::arc())
            .await,
    );
    let wrong_table = mutate_payload(&bytes, |payload| {
        payload["table_id"] = serde_json::json!(info.table_id() + 1);
    });
    assert_rejected(
        "a table id that does not match the regions",
        server
            .handle_remote_read(
                request_for(own_regions[0], wrong_table),
                QueryContext::arc(),
            )
            .await,
    );
    let misnamed = mutate_payload(&bytes, |payload| {
        payload["table_name"] = serde_json::json!("greptime.public.another_table");
    });
    assert_rejected(
        "a table name that does not match the nested plan",
        server
            .handle_remote_read(request_for(own_regions[0], misnamed), QueryContext::arc())
            .await,
    );
    let unsupported_version = mutate_payload(&bytes, |payload| {
        payload["version"] = serde_json::json!(999);
    });
    assert_rejected(
        "an unsupported envelope version",
        server
            .handle_remote_read(
                request_for(own_regions[0], unsupported_version),
                QueryContext::arc(),
            )
            .await,
    );
    // An unrelated extension is owned by the ordinary decoder, not the grouped-read peek.
    let unknown_root = encode(&MergeScanLogicalPlan::new(nested.clone(), true).into_logical_plan());
    assert!(peek_group_read_envelope(&unknown_root).unwrap().is_none());

    // An ineligible nested shape (a join) is rejected by the datanode even though the envelope
    // itself is well formed.
    let joined = LogicalPlanBuilder::scan(
        TableReference::bare(TABLE),
        Arc::new(DefaultTableSource::new(Arc::new(
            DfTableProviderAdapter::new(table.clone()),
        ))),
        None,
    )
    .unwrap()
    .join(
        LogicalPlanBuilder::scan(
            TableReference::bare(TABLE),
            Arc::new(DefaultTableSource::new(Arc::new(
                DfTableProviderAdapter::new(table.clone()),
            ))),
            None,
        )
        .unwrap()
        .alias("other")
        .unwrap()
        .build()
        .unwrap(),
        datafusion_expr::JoinType::Inner,
        (
            vec![format!("{TABLE}.host")],
            vec!["other.host".to_string()],
        ),
        None,
    )
    .unwrap()
    .build()
    .unwrap();
    // The frontend refuses to build an envelope around a join, so the request is assembled from a
    // valid envelope with its nested plan replaced: the datanode has to reject the shape itself.
    let joined_envelope = mutate_region_plans(&bytes, &encode(&joined));
    assert_rejected(
        "a join nested plan",
        server
            .handle_remote_read(
                request_for(own_regions[0], joined_envelope),
                QueryContext::arc(),
            )
            .await,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn dn_level_poc_group_read_sum_over_union_all_returns_partial_rows() {
    // DN-level POC only: the test drives GroupRead directly and sums the per-datanode partial
    // rows itself. FE SQL/Flight aggregate pushdown is not part of this case.
    common_telemetry::init_default_ut_logging();
    let cluster = build_cluster("dn_grouped_read_sum").await;
    let fe = cluster.fe_instance().clone();
    prepare_fixture(&fe).await;

    let table = fe
        .catalog_manager()
        .table("greptime", "public", TABLE, None)
        .await
        .unwrap()
        .unwrap();
    let info = table.table_info();
    let nested = LogicalPlanBuilder::scan(
        TableReference::bare(TABLE),
        Arc::new(DefaultTableSource::new(Arc::new(
            DfTableProviderAdapter::new(table.clone()),
        ))),
        None,
    )
    .unwrap()
    .build()
    .unwrap();

    let mut total = 0.0f64;
    let mut datanodes = 0;
    for datanode_id in cluster.datanode_instances.keys().copied() {
        let mut own_regions = region_ids_of_datanode(&fe, TABLE, datanode_id).await;
        own_regions.sort();
        let envelope =
            build_group_read_plan_with_combine(&own_regions, info.table_id(), &nested, |union| {
                LogicalPlanBuilder::from(union)
                    .aggregate(Vec::<datafusion_expr::Expr>::new(), vec![sum(col("v"))])?
                    .build()
            })
            .unwrap();
        let stream = cluster
            .datanode_instances
            .get(&datanode_id)
            .unwrap()
            .region_server()
            .handle_remote_read(
                RegionQueryRequest {
                    header: Some(RegionRequestHeader {
                        query_context: Some(QueryContext::arc().as_ref().into()),
                        ..Default::default()
                    }),
                    region_id: own_regions[0].as_u64(),
                    plan: encode(&envelope),
                },
                QueryContext::arc(),
            )
            .await
            .unwrap_or_else(|error| {
                panic!("SUM GroupRead failed on datanode {datanode_id}: {error}")
            });
        let batches = common_recordbatch::util::collect_batches(stream)
            .await
            .unwrap_or_else(|error| panic!("SUM GroupRead produced no batches: {error}"));
        let rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
        assert_eq!(
            1, rows,
            "Aggregate over UnionAll(RegionPlan...) must emit one partial row per datanode"
        );
        let batch = batches
            .iter()
            .next()
            .expect("one partial SUM row")
            .df_record_batch();
        let values = batch
            .column(0)
            .as_primitive::<datatypes::arrow::datatypes::Float64Type>();
        total += values.value(0);
        datanodes += 1;
    }
    assert_eq!(2, datanodes);
    assert_eq!(
        13.0, total,
        "the frontend sums the per-datanode partial SUM rows to the global SUM"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn grouped_read_active_remote_filter_update_and_cancel_releases_permit() {
    use datanode::metrics::REMOTE_DYN_FILTER_UPDATE_OUTCOME_TOTAL;

    common_telemetry::init_default_ut_logging();
    let cluster = GreptimeDbClusterBuilder::new("dn_grouped_read_active_rdf")
        .await
        .with_real_datanode_grpc_addr(true)
        .with_frontend_tcp_datanode_clients(true)
        .with_datanodes(2)
        .with_datanode_max_concurrent_queries(1)
        .with_meta_selector(Arc::new(RoundRobinSelector::new(SelectTarget::Datanode)))
        .build(false)
        .await;
    let fe = cluster.fe_instance().clone();
    prepare_fixture(&fe).await;
    let table = fe
        .catalog_manager()
        .table("greptime", "public", TABLE, None)
        .await
        .unwrap()
        .unwrap();
    let nested = LogicalPlanBuilder::scan(
        TableReference::bare(TABLE),
        Arc::new(DefaultTableSource::new(Arc::new(
            DfTableProviderAdapter::new(table.clone()),
        ))),
        None,
    )
    .unwrap()
    .build()
    .unwrap();

    // Choose the datanode whose two regions each contain rows, then make its region order explicit.
    let mut selected = None;
    for id in cluster.datanode_instances.keys().copied() {
        let mut regions = region_ids_of_datanode(&fe, TABLE, id).await;
        regions.sort();
        assert_eq!(2, regions.len());
        let server = cluster.datanode_instances.get(&id).unwrap().region_server();
        let mut counts = Vec::new();
        for region in &regions {
            let envelope =
                build_group_read_plan(&[*region], table.table_info().table_id(), &nested).unwrap();
            let ctx = QueryContext::arc();
            let stream = server
                .handle_remote_read(
                    RegionQueryRequest {
                        header: Some(RegionRequestHeader {
                            query_context: Some(ctx.as_ref().into()),
                            ..Default::default()
                        }),
                        region_id: region.as_u64(),
                        plan: encode(&envelope),
                    },
                    ctx,
                )
                .await
                .unwrap();
            let batches = common_recordbatch::util::collect_batches(stream)
                .await
                .unwrap();
            counts.push(batches.iter().map(|batch| batch.num_rows()).sum::<usize>());
        }
        if counts.iter().all(|count| *count > 0) {
            selected = Some((server, regions, counts));
            break;
        }
    }
    let (server, regions, counts) =
        selected.expect("one datanode owns both nonempty first and third partitions");
    let bytes =
        encode(&build_group_read_plan(&regions, table.table_info().table_id(), &nested).unwrap());
    let filter_id = "active-grouped-filter";
    let regs = InitialDynFilterRegs::new(vec![InitialDynFilterReg::new(filter_id, vec![])]);
    let mut ctx = QueryContext::with("greptime", "public");
    ctx.set_extension(
        INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY,
        regs.to_extension_value().unwrap(),
    );
    let ctx = Arc::new(ctx);
    let query_id = ctx.remote_query_id().unwrap().to_string();
    let request = || RegionQueryRequest {
        header: Some(RegionRequestHeader {
            query_context: Some(ctx.as_ref().into()),
            ..Default::default()
        }),
        region_id: regions[0].as_u64(),
        plan: bytes.clone(),
    };
    let payload_expr: Arc<dyn PhysicalExpr> = physical_lit(false);
    let payload = match DynFilterPayload::from_datafusion_expr(
        &payload_expr,
        REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES,
    )
    .unwrap()
    {
        DynFilterPayload::Datafusion(bytes) => bytes,
        _ => unreachable!(),
    };
    let update = || {
        region_request::Body::RemoteDynFilter(RemoteDynFilterRequest {
            query_id: query_id.clone(),
            action: Some(remote_dyn_filter_request::Action::Update(
                RemoteDynFilterUpdate {
                    filter_id: filter_id.to_string(),
                    payload: payload.clone(),
                    generation: 1,
                    is_complete: true,
                },
            )),
        })
    };
    let counter = |label: &str| {
        REMOTE_DYN_FILTER_UPDATE_OUTCOME_TOTAL
            .with_label_values(&[label])
            .get()
    };

    let stream = server
        .handle_remote_read(request(), ctx.clone())
        .await
        .unwrap();
    // A live grouped stream holds the only permit, not one per region. A second read times out.
    let blocked = server.handle_remote_read(request(), ctx.clone()).await;
    assert!(
        matches!(
            blocked,
            Err(DatanodeError::ConcurrentQueryLimiterTimeout { .. })
        ),
        "a live grouped stream must retain the single permit"
    );
    // The stream has completed preflight and has not polled a region yet. The filters are
    // registered without a scan runtime, so the update is either buffered or applied; when the
    // first poll starts the query engine, both region scans must consume the false filter.
    let applied = counter("applied");
    let buffered = counter("buffered");
    server.handle(update()).await.unwrap();
    assert!(
        counter("applied") > applied || counter("buffered") > buffered,
        "the real remote update must reach the registered grouped filter"
    );
    let batches = common_recordbatch::util::collect_batches(stream)
        .await
        .unwrap();
    assert_eq!(
        0,
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        "the false update must filter both regions, whose baseline row counts are {counts:?}"
    );
    let missing = counter("missing_registration");
    server.handle(update()).await.unwrap();
    assert!(
        counter("missing_registration") > missing,
        "EOF must remove every member registration"
    );

    // A new stream proves EOF released the permit; cancel it after the first batch and verify both
    // the registrations and permit are released, then scan both regions again with the same id.
    let mut cancelled = server
        .handle_remote_read(request(), ctx.clone())
        .await
        .unwrap();
    assert!(cancelled.next().await.unwrap().unwrap().num_rows() > 0);
    drop(cancelled);
    let missing = counter("missing_registration");
    server.handle(update()).await.unwrap();
    assert!(
        counter("missing_registration") > missing,
        "cancellation must remove all grouped registrations"
    );
    let replacement = server
        .handle_remote_read(request(), ctx.clone())
        .await
        .unwrap();
    let batches = common_recordbatch::util::collect_batches(replacement)
        .await
        .unwrap();
    assert_eq!(
        counts.iter().sum::<usize>(),
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        "after cancellation the same query id starts with a fresh filter and can scan both regions"
    );

    // Closing the second member after preflight causes a real mid-stream failure. The read must
    // release the grouped permit and both filter registrations without waiting for stream drop.
    use store_api::region_request::{RegionCloseRequest, RegionRequest};
    let mut failed = server
        .handle_remote_read(request(), ctx.clone())
        .await
        .unwrap();
    // The lazy stream has not started the query engine yet. Closing one member before the first
    // poll makes the scan-time failure deterministic.
    server
        .handle_request(
            regions[1],
            RegionRequest::Close(RegionCloseRequest {
                flush_on_close: false,
            }),
        )
        .await
        .unwrap();
    let error = tokio::time::timeout(Duration::from_secs(5), failed.next())
        .await
        .unwrap();
    assert!(
        matches!(error, Some(Err(_))),
        "the failing member must fail the whole grouped stream: {error:?}"
    );
    let missing = counter("missing_registration");
    server.handle(update()).await.unwrap();
    assert!(
        counter("missing_registration") > missing,
        "failure must clean up every member"
    );
    let ordinary_ctx = QueryContext::arc();
    let ordinary = server
        .handle_remote_read(
            RegionQueryRequest {
                header: Some(RegionRequestHeader {
                    query_context: Some(ordinary_ctx.as_ref().into()),
                    ..Default::default()
                }),
                region_id: regions[0].as_u64(),
                plan: encode(&nested),
            },
            ordinary_ctx,
        )
        .await
        .unwrap();
    let ordinary_batches = common_recordbatch::util::collect_batches(ordinary)
        .await
        .unwrap();
    assert_eq!(
        counts[0],
        ordinary_batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>()
    );
    drop(failed);

    // A grouped request is rejected for Flow source and sink contexts, including malformed keys.
    for (key, value) in [
        ("flow.return_region_seq", "true".to_string()),
        (
            "flow.sink_table_id",
            table.table_info().table_id().to_string(),
        ),
        ("flow.incremental_mode", "malformed".to_string()),
    ] {
        let mut flow_ctx = QueryContext::with("greptime", "public");
        flow_ctx.set_extension(key, value);
        let flow_ctx = Arc::new(flow_ctx);
        let result = server
            .handle_remote_read(
                RegionQueryRequest {
                    header: Some(RegionRequestHeader {
                        query_context: Some(flow_ctx.as_ref().into()),
                        ..Default::default()
                    }),
                    region_id: regions[0].as_u64(),
                    plan: bytes.clone(),
                },
                flow_ctx,
            )
            .await;
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("Flow query contexts"),
            "{key}"
        );
    }
}

fn push_varint(mut value: usize, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn encode(plan: &datafusion_expr::LogicalPlan) -> Vec<u8> {
    DFLogicalSubstraitConvertor
        .encode(plan, DefaultSerializer)
        .unwrap()
        .to_vec()
}

/// Rebuilds a valid envelope with mutated GroupRead metadata, so a datanode's rejection can be
/// observed without touching the combine tree.
fn mutate_payload(bytes: &[u8], mutate: impl FnOnce(&mut serde_json::Value)) -> Vec<u8> {
    use prost::Message;
    use substrait::substrait_proto_df::proto::{Plan, plan_rel, rel};

    let payload = peek_group_read_envelope(bytes).unwrap().unwrap();
    let mut value: serde_json::Value = serde_json::from_slice(&payload.encode().unwrap()).unwrap();
    mutate(&mut value);
    let value = serde_json::to_vec(&value).unwrap();

    let mut plan = Plan::decode(bytes).unwrap();
    let relation = plan.relations.first_mut().unwrap();
    let root = match relation.rel_type.as_mut().unwrap() {
        plan_rel::RelType::Rel(root) => root,
        plan_rel::RelType::Root(root) => root.input.as_mut().unwrap(),
    };
    let rel::RelType::ExtensionSingle(single) = root.rel_type.as_mut().unwrap() else {
        panic!("the fixture must have a GroupRead root");
    };
    single.detail.as_mut().unwrap().value = value.into();
    plan.encode_to_vec()
}

/// Rewrites every RegionPlan leaf with another regional plan, keeping the outer tree shape.
fn mutate_region_plans(bytes: &[u8], regional_plan: &[u8]) -> Vec<u8> {
    use prost::Message;
    use substrait::substrait_proto_df::proto::{Plan, plan_rel, rel};

    fn walk(rel: &mut substrait::substrait_proto_df::proto::Rel, regional_plan: &[u8]) {
        match rel.rel_type.as_mut().unwrap() {
            rel::RelType::ExtensionLeaf(leaf) => {
                let detail = leaf.detail.as_mut().unwrap();
                if detail.type_url == query::dist_plan::REGION_PLAN_NAME {
                    let payload =
                        query::dist_plan::RegionPlanPayload::decode(&detail.value).unwrap();
                    let payload = query::dist_plan::RegionPlanPayload::try_new(
                        payload.region_id(),
                        regional_plan.to_vec(),
                    )
                    .unwrap();
                    detail.value = payload.encode().unwrap().into();
                }
            }
            rel::RelType::ExtensionSingle(single) => {
                if let Some(input) = single.input.as_mut() {
                    walk(input, regional_plan);
                }
            }
            rel::RelType::Set(set) => {
                for input in &mut set.inputs {
                    walk(input, regional_plan);
                }
            }
            _ => {}
        }
    }

    let mut plan = Plan::decode(bytes).unwrap();
    let relation = plan.relations.first_mut().unwrap();
    let root = match relation.rel_type.as_mut().unwrap() {
        plan_rel::RelType::Rel(root) => root,
        plan_rel::RelType::Root(root) => root.input.as_mut().unwrap(),
    };
    walk(root, regional_plan);
    plan.encode_to_vec()
}

async fn region_ids(fe: &Arc<frontend::instance::Instance>, table: &str) -> Vec<RegionId> {
    let sql = format!(
        "SELECT region_id FROM information_schema.region_peers WHERE table_schema = 'public' AND \
         table_name = '{table}' AND is_leader = 'Yes' ORDER BY region_id"
    );
    let output = SqlQueryHandler::do_query(fe.as_ref(), &sql, QueryContext::arc())
        .await
        .remove(0)
        .unwrap();
    let batches = match output.data {
        OutputData::Stream(stream) => common_recordbatch::util::collect_batches(stream)
            .await
            .unwrap(),
        OutputData::RecordBatches(batches) => batches,
        other => panic!("unexpected output {other:?}"),
    };
    let mut ids = Vec::new();
    for batch in batches.iter() {
        let batch = batch.df_record_batch();
        let ids_column = batch
            .column_by_name("region_id")
            .unwrap()
            .as_primitive::<UInt64Type>();
        for row in 0..batch.num_rows() {
            ids.push(RegionId::from_u64(ids_column.value(row)));
        }
    }
    ids
}

async fn region_ids_of_datanode(
    fe: &Arc<frontend::instance::Instance>,
    table: &str,
    datanode_id: u64,
) -> Vec<RegionId> {
    let sql = format!(
        "SELECT region_id FROM information_schema.region_peers WHERE table_schema = 'public' AND \
         table_name = '{table}' AND is_leader = 'Yes' AND peer_id = {datanode_id} ORDER BY \
         region_id"
    );
    let output = SqlQueryHandler::do_query(fe.as_ref(), &sql, QueryContext::arc())
        .await
        .remove(0)
        .unwrap();
    let batches = match output.data {
        OutputData::Stream(stream) => common_recordbatch::util::collect_batches(stream)
            .await
            .unwrap(),
        OutputData::RecordBatches(batches) => batches,
        other => panic!("unexpected output {other:?}"),
    };
    let mut ids = Vec::new();
    for batch in batches.iter() {
        let batch = batch.df_record_batch();
        let ids_column = batch
            .column_by_name("region_id")
            .unwrap()
            .as_primitive::<UInt64Type>();
        for row in 0..batch.num_rows() {
            ids.push(RegionId::from_u64(ids_column.value(row)));
        }
    }
    ids
}
