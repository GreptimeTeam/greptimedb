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

//! Real-TCP acceptance of the cross-region native PromQL path on a distributed frontend.
//!
//! Two datanodes serve one time-partitioned metric table: one region per partition and one region
//! leader per datanode, so the single series of the table spans both datanodes. Only the frontend
//! is configured with `experimental_native_promql`, exactly like a deployment where the planning
//! node carries the flag. The frontend reaches the datanodes over their real Flight listeners
//! ([`GreptimeDbClusterBuilder::with_real_frontend_clients`]), so the per-datanode RPC counters
//! prove that the frontend itself performed the named raw-series preflight (Flight `DoAction`) and
//! the raw-series read (Flight `DoGet`) against both region leaders before the rows are consumed.
//! The disabled frontend proves the flag gates the whole path, and a filter the native planner
//! cannot push down proves unsupported shapes are rejected before any preflight RPC leaves the
//! frontend.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use common_query::{Output, OutputData};
use common_recordbatch::RecordBatches;
use datafusion::arrow::array::{Array, Float64Array, StringArray, TimestampMillisecondArray};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use frontend::instance::Instance;
use query::options::QueryOptions;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::QueryContext;

use crate::cluster::{DatanodeRpcStats, GreptimeDbCluster, GreptimeDbClusterBuilder};
use crate::test_util::execute_sql;

/// One series in two time partitions, so the two regions of the table each hold one sample.
const TABLE: &str = "native_cluster_parts";
const CREATE_TABLE: &str = "CREATE TABLE native_cluster_parts (ts TIMESTAMP(3), host STRING, val DOUBLE, TIME INDEX (ts), PRIMARY KEY (host)) PARTITION ON COLUMNS (ts) (ts < '1970-01-01 00:00:02', ts >= '1970-01-01 00:00:02') ENGINE=mito";
const INSERT_ROWS: &str =
    "INSERT INTO native_cluster_parts VALUES (1000, 'alpha', 1), (3000, 'alpha', 3)";
const ANALYZE: &str = "TQL ANALYZE VERBOSE (1, 3, '1s') native_cluster_parts";
const EVAL: &str = "TQL EVAL (1, 3, '1s') native_cluster_parts";
/// A filter the native planner cannot push down, so the shape is rejected before any RPC.
const FILTERED_ANALYZE: &str = "TQL ANALYZE VERBOSE (1, 3, '1s') native_cluster_parts > 6";
const FILTERED_EVAL: &str = "TQL EVAL (1, 3, '1s') native_cluster_parts > 6";

#[tokio::test(flavor = "multi_thread")]
async fn test_native_promql_cluster_cross_region_parallelism_1() {
    run_cross_region_case("native_promql_cluster_p1", 1).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_native_promql_cluster_cross_region_parallelism_4() {
    run_cross_region_case("native_promql_cluster_p4", 4).await;
}

/// Builds one two-datanode cluster whose frontend owns the query options and the real clients.
async fn build_cluster(
    name: &str,
    parallelism: usize,
    experimental_native_promql: bool,
) -> GreptimeDbCluster {
    GreptimeDbClusterBuilder::new(name)
        .await
        .with_datanodes(2)
        // The datanodes must be reachable at the addresses of the table routes, and the frontend
        // must really dial them, or neither its `DoAction` preflight nor its `DoGet` reads are
        // counted.
        .with_real_datanode_grpc_addr(true)
        .with_real_frontend_clients(true)
        .with_frontend_query_options(QueryOptions {
            parallelism,
            experimental_native_promql,
            ..Default::default()
        })
        .build(false)
        .await
}

async fn run_cross_region_case(name: &str, parallelism: usize) {
    common_telemetry::init_default_ut_logging();

    let enabled = build_cluster(&format!("{name}_enabled"), parallelism, true).await;
    let disabled = build_cluster(&format!("{name}_disabled"), parallelism, false).await;
    let enabled_frontend = enabled.fe_instance().clone();
    let disabled_frontend = disabled.fe_instance().clone();

    for frontend in [&enabled_frontend, &disabled_frontend] {
        execute_sql(frontend, CREATE_TABLE).await;
        execute_sql(frontend, INSERT_ROWS).await;
    }

    // One region per time partition, and the partitions must be served by different datanodes:
    // only then does the single series really span both datanodes.
    let leaders = region_leaders(&enabled_frontend, TABLE).await;
    assert_eq!(
        2,
        leaders.len(),
        "expected one region per time partition of {TABLE}, actual region leaders: {leaders:?}"
    );
    assert_eq!(
        BTreeSet::from([1_u64, 2_u64]),
        leaders.values().copied().collect::<BTreeSet<u64>>(),
        "expected the time partitions of {TABLE} to be served by both datanodes, actual region \
         leaders: {leaders:?}"
    );

    // The ordinary read proves the seeded rows are complete before TQL runs.
    let stored = query_batches(
        &enabled_frontend,
        &format!("SELECT ts, host, val FROM {TABLE} ORDER BY ts"),
    )
    .await;
    assert_eq!(expected_columns(), columns(&stored));
    assert_eq!(
        expected_stored_rows(),
        rows(&stored),
        "unexpected stored rows of {TABLE}"
    );

    // The enabled frontend must reach the native plan that consumes the raw series of both
    // regions; the disabled frontend must keep the ordinary plan.
    let enabled_plan = query_pretty(&enabled_frontend, ANALYZE).await;
    assert!(
        enabled_plan.contains("NativeInstantManipulateExec"),
        "the enabled frontend did not use the native instant manipulation:\n{enabled_plan}"
    );
    assert!(
        enabled_plan.contains("NativeRawMergeExec"),
        "the enabled frontend did not merge the native raw series of the regions:\n{enabled_plan}"
    );
    let disabled_plan = query_pretty(&disabled_frontend, ANALYZE).await;
    assert!(
        !disabled_plan.contains("NativeInstantManipulateExec"),
        "the disabled frontend unexpectedly used the native instant manipulation:\n{disabled_plan}"
    );
    assert!(
        !disabled_plan.contains("NativeRawMergeExec"),
        "the disabled frontend unexpectedly merged native raw series:\n{disabled_plan}"
    );

    // Seeding and planning are done: from here on only the TQL EVAL below may move the counters.
    let baseline = leaders
        .values()
        .map(|datanode| {
            (
                *datanode,
                (
                    rpc_requests(&enabled, *datanode),
                    prepare_requests(&enabled, *datanode),
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();

    let native_batches = query_batches(&enabled_frontend, EVAL).await;

    // Both protocols of both datanodes ran before the rows above were consumed: the named
    // raw-series preflight of the planning phase and the raw-series read of the execution phase.
    for (datanode, (do_get, prepare)) in &baseline {
        assert_eq!(
            do_get + 1,
            rpc_requests(&enabled, *datanode),
            "expected the TQL EVAL to read the region of the datanode {datanode} exactly once over \
             Flight DoGet (baseline {do_get})"
        );
        assert_eq!(
            prepare + 1,
            prepare_requests(&enabled, *datanode),
            "expected the TQL EVAL to preflight the named raw series at the datanode {datanode} \
             exactly once over Flight DoAction (baseline {prepare})"
        );
    }

    assert_eq!(expected_columns(), columns(&native_batches));
    assert_eq!(expected_eval_rows(), rows(&native_batches));

    let ordinary_batches = query_batches(&disabled_frontend, EVAL).await;
    assert_eq!(columns(&native_batches), columns(&ordinary_batches));
    // The column comparison above only covers names and types, so compare the complete public
    // schemas as well: the GreptimeDB schema and its Arrow schema, including nullability and
    // field and schema metadata. The frontend has already expanded scan-time dictionaries when the
    // rows left it, so a dictionary-shaped native schema must fail here instead of being
    // normalized away.
    assert_eq!(native_batches.schema(), ordinary_batches.schema());
    assert_eq!(
        native_batches.schema().arrow_schema(),
        ordinary_batches.schema().arrow_schema()
    );
    assert_eq!(rows(&native_batches), rows(&ordinary_batches));
    assert_eq!(expected_eval_rows(), rows(&ordinary_batches));

    // An unsupported filtered shape is rejected locally, before any preflight RPC leaves the
    // frontend, and the ordinary path still returns the complete known result.
    let prepare_baseline = baseline
        .keys()
        .map(|datanode| (*datanode, prepare_requests(&enabled, *datanode)))
        .collect::<BTreeMap<_, _>>();

    let filtered_plan = query_pretty(&enabled_frontend, FILTERED_ANALYZE).await;
    assert!(
        !filtered_plan.contains("NativeInstantManipulateExec"),
        "the unsupported filtered plan unexpectedly used the native instant manipulation:\n\
         {filtered_plan}"
    );
    assert!(
        !filtered_plan.contains("NativeRawMergeExec"),
        "the unsupported filtered plan unexpectedly merged native raw series:\n{filtered_plan}"
    );

    let filtered_batches = query_batches(&enabled_frontend, FILTERED_EVAL).await;
    let filtered_ordinary = query_batches(&disabled_frontend, FILTERED_EVAL).await;
    assert_eq!(expected_columns(), columns(&filtered_batches));
    assert_eq!(columns(&filtered_batches), columns(&filtered_ordinary));
    // The unsupported filtered shape runs the ordinary path on both frontends; the same complete
    // public schema comparison applies to it.
    assert_eq!(filtered_batches.schema(), filtered_ordinary.schema());
    assert_eq!(
        filtered_batches.schema().arrow_schema(),
        filtered_ordinary.schema().arrow_schema()
    );
    // No stored sample of this fixture passes `> 6`, so the complete filtered result is empty.
    let expected_filtered: Vec<(i64, Option<String>, Option<f64>)> = Vec::new();
    assert_eq!(
        expected_filtered,
        rows(&filtered_batches),
        "unexpected filtered result"
    );
    assert_eq!(rows(&filtered_batches), rows(&filtered_ordinary));

    for (datanode, prepare) in &prepare_baseline {
        assert_eq!(
            *prepare,
            prepare_requests(&enabled, *datanode),
            "the unsupported filtered shape must be rejected before any prepare RPC at the \
             datanode {datanode}"
        );
    }

    // The disabled frontend never preflights, whatever the query shape.
    for datanode in leaders.values() {
        assert_eq!(
            0,
            prepare_requests(&disabled, *datanode),
            "the disabled frontend must not preflight named raw series at the datanode {datanode}"
        );
    }
}

async fn run_sql(frontend: &Arc<Instance>, sql: &str) -> Output {
    SqlQueryHandler::do_query(frontend.as_ref(), sql, QueryContext::arc())
        .await
        .remove(0)
        .unwrap_or_else(|error| panic!("`{sql}` failed: {error}"))
}

async fn query_batches(frontend: &Arc<Instance>, sql: &str) -> RecordBatches {
    output_batches(run_sql(frontend, sql).await).await
}

async fn query_pretty(frontend: &Arc<Instance>, sql: &str) -> String {
    run_sql(frontend, sql).await.data.pretty_print().await
}

async fn output_batches(output: Output) -> RecordBatches {
    match output.data {
        OutputData::RecordBatches(batches) => batches,
        OutputData::Stream(stream) => RecordBatches::try_collect(stream)
            .await
            .expect("the output stream must be collectable"),
        OutputData::AffectedRows(rows) => {
            panic!("expected a query output, got {rows} affected rows")
        }
    }
}

/// The complete schema of the TQL result the ordinary and the native path must share.
fn expected_columns() -> Vec<(String, DataType)> {
    vec![
        (
            "ts".to_string(),
            DataType::Timestamp(TimeUnit::Millisecond, None),
        ),
        ("host".to_string(), DataType::Utf8),
        ("val".to_string(), DataType::Float64),
    ]
}

fn columns(batches: &RecordBatches) -> Vec<(String, DataType)> {
    batches
        .schema()
        .arrow_schema()
        .fields()
        .iter()
        .map(|field| (field.name().clone(), field.data_type().clone()))
        .collect()
}

/// The complete stored rows of the fixture: one series with one sample per time partition.
fn expected_stored_rows() -> Vec<(i64, Option<String>, Option<f64>)> {
    vec![
        (1_000, Some("alpha".to_string()), Some(1.0)),
        (3_000, Some("alpha".to_string()), Some(3.0)),
    ]
}

/// The complete TQL result: every evaluation time carries the series, and 2000 is filled by the
/// lookback window from the sample of 1000.
fn expected_eval_rows() -> Vec<(i64, Option<String>, Option<f64>)> {
    vec![
        (1_000, Some("alpha".to_string()), Some(1.0)),
        (2_000, Some("alpha".to_string()), Some(1.0)),
        (3_000, Some("alpha".to_string()), Some(3.0)),
    ]
}

/// The complete rows of a three-column TQL result, sorted into the canonical order.
fn rows(batches: &RecordBatches) -> Vec<(i64, Option<String>, Option<f64>)> {
    let mut rows = Vec::new();
    for batch in batches.iter() {
        let batch = batch.df_record_batch();
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        let host = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let val = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for index in 0..batch.num_rows() {
            rows.push((
                ts.value(index),
                (!host.is_null(index)).then(|| host.value(index).to_string()),
                (!val.is_null(index)).then(|| val.value(index)),
            ));
        }
    }
    rows.sort_by(|left, right| {
        left.0.cmp(&right.0).then_with(|| {
            left.1
                .is_none()
                .cmp(&right.1.is_none())
                .then_with(|| left.1.cmp(&right.1))
        })
    });
    rows
}

/// Maps every region of `table` to the id of its leader datanode.
async fn region_leaders(frontend: &Arc<Instance>, table: &str) -> BTreeMap<u64, u64> {
    use datatypes::arrow::array::AsArray;
    use datatypes::arrow::datatypes::UInt64Type;

    let sql = format!(
        "SELECT region_id, peer_id FROM information_schema.region_peers
         WHERE table_schema = 'public' AND table_name = '{table}' AND is_leader = 'Yes'
         ORDER BY region_id"
    );
    let batches = query_batches(frontend, &sql).await;

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
        for row in 0..batch.num_rows() {
            leaders.insert(region_ids.value(row), peer_ids.value(row));
        }
    }
    leaders
}

fn rpc_requests(cluster: &GreptimeDbCluster, datanode_id: u64) -> usize {
    rpc_stats(cluster, datanode_id).requests()
}

fn prepare_requests(cluster: &GreptimeDbCluster, datanode_id: u64) -> usize {
    rpc_stats(cluster, datanode_id).prepare_requests()
}

fn rpc_stats(cluster: &GreptimeDbCluster, datanode_id: u64) -> &Arc<DatanodeRpcStats> {
    cluster
        .datanode_rpc_stats(datanode_id)
        .unwrap_or_else(|| panic!("expected the rpc stats of the datanode {datanode_id}"))
}

/// Bounded acceptance tests of the frontend-visible failures of the cross-region native PromQL
/// path.
///
/// Every case builds its own two-datanode cluster whose datanodes serve the real TCP Flight
/// listener through [`FaultFlight`], while the in-process duplex listener keeps serving the bare
/// region server; the frontend, its real clients, the table route and the fixture rows are the
/// ones the positive cases above already use. A case seeds its table and reads its rows before it
/// arms its fault, so the window of the faulted query starts empty, and a fault only ever replaces
/// the named raw-series preflight or the raw-series read of the armed regions. No case arms a
/// fault while `TQL ANALYZE` runs: its one-shot answers would be consumed by the analyze.
///
/// A wrapper only ever applies its fault after it forwarded the real payload prefix of its actual
/// response, never before: the failed read of a case is released only once its sibling read is
/// held with its actual response still open, and a held read is only released by the client that
/// cancels the query. Every cancellation of a case is therefore driven by the failure of that
/// case, and every query phase is bounded by [`QUERY_TIMEOUT`] or [`CANCEL_ACK_TIMEOUT`].
mod fault_acceptance {
    use std::collections::{BTreeMap, BTreeSet};
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use std::time::Duration;

    use api::v1::region::QueryRequest as RegionQueryRequest;
    use arrow_flight::{Action, FlightData, Ticket};
    use common_error::ext::ErrorExt;
    use common_error::status_code::StatusCode;
    use common_grpc::flight::{FlightDecoder, FlightMessage};
    use common_meta::DatanodeId;
    use common_query::request::{
        PREPARE_RAW_SERIES_ACTION, RAW_SERIES_REQUEST_EXTENSION_KEY,
        encode_prepare_raw_series_response,
    };
    use common_recordbatch::SendableRecordBatchStream;
    use datanode::region_server::RegionServer as DatanodeRegionServer;
    use futures::{Stream, StreamExt, ready};
    use prost::Message;
    use servers::grpc::flight::{FlightCraft, FlightCraftRef, TonicStream};
    use tokio::sync::{mpsc, oneshot};
    use tonic::{Request, Response, Status};

    use crate::cluster::{GreptimeDbCluster, GreptimeDbClusterBuilder};
    use crate::test_util::{execute_sql, try_execute_sql};

    use super::*;

    /// The unique marker of the failing preflight of the second case.
    const PREFLIGHT_ERROR_MARKER: &str = "fault acceptance: preflight internal error";
    /// The unique marker of the late raw-series body error of the third case.
    const LATE_BODY_ERROR_MARKER: &str = "fault acceptance: late raw-series body error";
    /// The bound of one cancellation acknowledgement.
    const CANCEL_ACK_TIMEOUT: Duration = Duration::from_secs(5);
    /// The bound of one query phase of a case: the faulted `TQL EVAL` with its planning call and
    /// the readiness phase that drives it, and the follow-up queries of a disarmed case.
    const QUERY_TIMEOUT: Duration = Duration::from_secs(30);

    /// One observed Flight request of a datanode.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Event {
        /// A named raw-series preflight (`Flight` `DoAction`).
        Prepare(DatanodeId),
        /// A raw-series read (`Flight` `DoGet` carrying the raw-series extension).
        RawGet(DatanodeId),
        /// An ordinary region read (`Flight` `DoGet` without the raw-series extension).
        OrdinaryGet(DatanodeId),
    }

    /// The observed Flight traffic of one datanode.
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    struct Traffic {
        /// Named raw-series preflights.
        prepare: usize,
        /// Raw-series reads.
        raw_get: usize,
        /// Ordinary region reads.
        ordinary_get: usize,
    }

    impl Traffic {
        /// Counts the events of `node` in the log window `window`.
        fn of(window: &[Event], node: DatanodeId) -> Self {
            let mut traffic = Self::default();
            for event in window {
                match event {
                    Event::Prepare(observed) if *observed == node => traffic.prepare += 1,
                    Event::RawGet(observed) if *observed == node => traffic.raw_get += 1,
                    Event::OrdinaryGet(observed) if *observed == node => traffic.ordinary_get += 1,
                    _ => {}
                }
            }
            traffic
        }
    }

    /// What a wrapper stream does once it forwarded the real payload prefix of its actual response.
    enum AfterPrefix {
        /// Holds this raw read pending, with its actual response open and unfinished, until the
        /// client cancels the query; the first park reports itself as the witness of the case.
        Park,
        /// Fails this raw read with `status`, and only once the sibling raw read of the case is
        /// held, so a released error never hides a sibling response that was not held open.
        Fail(Status),
    }

    /// The fault a case arms after its fixture is ready.
    #[derive(Debug)]
    enum ArmedFault {
        /// Answers the preflight of `region` with the frozen unsupported body and a clean end of
        /// stream.
        UnsupportedPreflight { region: u64 },
        /// Fails the preflight of `region` with the internal peer status of `marker`.
        FailingPreflight { region: u64, marker: &'static str },
        /// Holds the raw read of `sibling` with its real payload prefix already forwarded, and
        /// fails the raw read of `selected` late, once `sibling` is held.
        LateRawError {
            selected: u64,
            sibling: u64,
            marker: &'static str,
        },
        /// Holds the raw read of every region of `regions`, each with its real payload prefix
        /// already forwarded, until the client cancels the query.
        ParkedRawStreams { regions: BTreeSet<u64> },
    }

    /// The real payload prefix one wrapper stream forwarded from the actual region server.
    ///
    /// The witness only exists once a record batch frame of the actual response was forwarded, so
    /// a schema-only prefix never produces one.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct PayloadPrefix {
        /// The datanode whose region server produced the frames.
        node: DatanodeId,
        /// The actual response frames forwarded until the record batch, schema frame included.
        frames: usize,
    }

    /// The shared state of one case: the ordered event log and its one-shot witnesses.
    ///
    /// Both datanodes share one state, so the log is a single ordered vector. Every lock is a
    /// short critical section that appends or takes one value; no lock is held across an await.
    struct FaultState {
        events: Mutex<Vec<Event>>,
        armed: Mutex<Option<ArmedFault>>,
        prefixes: Mutex<Vec<PayloadPrefix>>,
        /// Reports that a raw read of the case is held: its real payload prefix was forwarded and
        /// its actual response is still open behind the pending stream.
        sibling_hold: Mutex<Option<oneshot::Sender<()>>>,
        /// Waits for that report before the late error of the selected raw read is released.
        sibling_hold_waiter: Mutex<Option<oneshot::Receiver<()>>>,
        parked: mpsc::UnboundedSender<DatanodeId>,
        dropped: mpsc::UnboundedSender<DatanodeId>,
    }

    impl FaultState {
        /// Creates the shared state of one case; the witnesses report to the test's channels.
        fn new(
            parked: mpsc::UnboundedSender<DatanodeId>,
            dropped: mpsc::UnboundedSender<DatanodeId>,
        ) -> Self {
            Self {
                events: Mutex::new(Vec::new()),
                armed: Mutex::new(None),
                prefixes: Mutex::new(Vec::new()),
                sibling_hold: Mutex::new(None),
                sibling_hold_waiter: Mutex::new(None),
                parked,
                dropped,
            }
        }

        /// Arms `fault` and prepares the hold witness of the case.
        ///
        /// The witness is taken by the first held raw read of the case, so a case that holds more
        /// than one read still reports the hold exactly once.
        fn arm(&self, fault: ArmedFault) {
            *self.armed.lock().unwrap() = Some(fault);
            let (hold, hold_waiter) = oneshot::channel();
            *self.sibling_hold.lock().unwrap() = Some(hold);
            *self.sibling_hold_waiter.lock().unwrap() = Some(hold_waiter);
        }

        /// Disarms the case, so every later raw read is forwarded unwrapped.
        ///
        /// A held raw read is only released by the client that cancels its query, so this is
        /// called after every cancellation of the case was acknowledged.
        fn disarm(&self) {
            *self.armed.lock().unwrap() = None;
        }

        /// The observed event log of the case.
        fn events(&self) -> Vec<Event> {
            self.events.lock().unwrap().clone()
        }

        /// Appends one observation.
        fn push(&self, event: Event) {
            self.events.lock().unwrap().push(event);
        }

        /// The fault waiting for one raw read of `region`, `None` when the case does not touch it.
        fn raw_read_plan(&self, region: u64) -> Option<AfterPrefix> {
            let fault = match self.armed.lock().unwrap().as_ref()? {
                ArmedFault::LateRawError {
                    selected, marker, ..
                } if region == *selected => {
                    AfterPrefix::Fail(peer_status(StatusCode::Internal, marker))
                }
                ArmedFault::LateRawError { sibling, .. } if region == *sibling => AfterPrefix::Park,
                ArmedFault::ParkedRawStreams { regions } if regions.contains(&region) => {
                    AfterPrefix::Park
                }
                _ => return None,
            };
            Some(fault)
        }
    }

    /// Serves the real region server of one datanode, records the fixture traffic of its real TCP
    /// listener, and injects the armed fault of the case.
    struct FaultFlight {
        /// The datanode that owns the real region server behind this handler.
        node: DatanodeId,
        /// The real handler of the datanode; every request that is not a fault delegates to it.
        inner: FlightCraftRef,
        /// The case state shared by both datanodes of the cluster.
        state: Arc<FaultState>,
    }

    #[async_trait::async_trait]
    impl FlightCraft for FaultFlight {
        async fn do_get(
            &self,
            request: Request<Ticket>,
        ) -> std::result::Result<Response<TonicStream<FlightData>>, Status> {
            let Some(region) = raw_series_region(&request.get_ref().ticket) else {
                self.state.push(Event::OrdinaryGet(self.node));
                return self.inner.do_get(request).await;
            };
            self.state.push(Event::RawGet(self.node));
            // The actual response is opened, and its region-server stream is retained by the
            // wrapper, before the case decides how the body of this read continues.
            let response = self.inner.do_get(request).await?;
            let Some(fault) = self.state.raw_read_plan(region) else {
                return Ok(response);
            };
            // Only a read that is armed to fail waits for the hold of the case; a read that is
            // armed to be held is self-contained.
            let sibling = match fault {
                AfterPrefix::Fail(_) => self.state.sibling_hold_waiter.lock().unwrap().take(),
                AfterPrefix::Park => None,
            };
            Ok(Response::new(Box::pin(WrappedRawStream {
                node: self.node,
                inner: Some(response.into_inner()),
                decoder: FlightDecoder::default(),
                frames: 0,
                prefix: None,
                after_prefix: Some(fault),
                sibling,
                parked: false,
                done: false,
                state: Arc::clone(&self.state),
            })))
        }

        async fn do_put(
            &self,
            request: Request<tonic::Streaming<FlightData>>,
        ) -> std::result::Result<Response<TonicStream<arrow_flight::PutResult>>, Status> {
            self.inner.do_put(request).await
        }

        async fn do_action(
            &self,
            request: Request<Action>,
        ) -> std::result::Result<Response<TonicStream<arrow_flight::Result>>, Status> {
            let Some(region) = prepare_region(request.get_ref()) else {
                return self.inner.do_action(request).await;
            };
            self.state.push(Event::Prepare(self.node));
            match self.state.armed.lock().unwrap().as_ref() {
                Some(ArmedFault::UnsupportedPreflight { region: armed }) if *armed == region => {
                    // The frozen unsupported shape is one explicit result followed by a clean end
                    // of stream, never a bare error of the action call.
                    let body = encode_prepare_raw_series_response(None)
                        .expect("the frozen unsupported body must encode");
                    let answer: [std::result::Result<arrow_flight::Result, Status>; 1] =
                        [Ok(arrow_flight::Result { body: body.into() })];
                    return Ok(Response::new(Box::pin(futures_util::stream::iter(answer))));
                }
                Some(ArmedFault::FailingPreflight {
                    region: armed,
                    marker,
                }) if *armed == region => {
                    return Err(peer_status(StatusCode::Internal, marker));
                }
                _ => {}
            }
            self.inner.do_action(request).await
        }
    }

    /// The body of one raw read of the armed fixture.
    ///
    /// It owns the actual stream of the real region server, forwards its real payload prefix,
    /// applies the fault of the case, and drops the actual stream before it acknowledges the
    /// cancellation of its client.
    struct WrappedRawStream {
        /// The datanode whose region server produced the stream.
        node: DatanodeId,
        /// The actual stream of the real region server; [`Drop`] takes and drops it first.
        inner: Option<TonicStream<FlightData>>,
        /// Decodes the forwarded frames, which witnesses the first real record batch.
        decoder: FlightDecoder,
        /// The actual response frames forwarded so far, schema frame included.
        frames: usize,
        /// The real payload prefix witness, set once a record batch was forwarded.
        prefix: Option<PayloadPrefix>,
        /// The fault waiting for this read, applied once the prefix above was forwarded.
        after_prefix: Option<AfterPrefix>,
        /// Waits for the hold of the sibling raw read of the third case before the late error of
        /// this read is released; `None` for a read that does not fail.
        sibling: Option<oneshot::Receiver<()>>,
        /// Whether the stream already reported its held state.
        parked: bool,
        /// Whether the stream already released its terminal item.
        done: bool,
        /// The case state of the cluster.
        state: Arc<FaultState>,
    }

    impl Stream for WrappedRawStream {
        type Item = std::result::Result<FlightData, Status>;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let this = self.get_mut();
            if this.done {
                return Poll::Ready(None);
            }
            if this.prefix.is_none() {
                return this.poll_actual(cx);
            }
            match this.after_prefix.take() {
                Some(AfterPrefix::Fail(status)) => {
                    if let Some(mut sibling) = this.sibling.take()
                        && Pin::new(&mut sibling).poll(cx).is_pending()
                    {
                        this.sibling = Some(sibling);
                        this.after_prefix = Some(AfterPrefix::Fail(status));
                        return Poll::Pending;
                    }
                    this.done = true;
                    Poll::Ready(Some(Err(status)))
                }
                Some(AfterPrefix::Park) => {
                    this.after_prefix = Some(AfterPrefix::Park);
                    if !this.parked {
                        this.parked = true;
                        let _ = this.state.parked.send(this.node);
                        // The actual response of this read stays open behind the pending stream,
                        // and the failing sibling of the case is only released after this report.
                        if let Some(hold) = this.state.sibling_hold.lock().unwrap().take() {
                            let _ = hold.send(());
                        }
                    }
                    Poll::Pending
                }
                // A wrapper only exists for a read with a fault, and both arms above either
                // complete the stream or restore the fault before they return.
                None => unreachable!("a wrapped raw read applies its fault exactly once"),
            }
        }
    }

    impl WrappedRawStream {
        /// Forwards the actual stream and witnesses the first real record batch of the prefix.
        fn poll_actual(&mut self, cx: &mut Context<'_>) -> Poll<Option<<Self as Stream>::Item>> {
            let Some(inner) = self.inner.as_mut() else {
                return Poll::Ready(None);
            };
            match ready!(inner.as_mut().poll_next(cx)) {
                Some(Ok(flight_data)) => {
                    self.frames += 1;
                    let decoded = self.decoder.try_decode(&flight_data);
                    if self.prefix.is_none()
                        && matches!(decoded, Ok(Some(FlightMessage::RecordBatch(_))))
                    {
                        let prefix = PayloadPrefix {
                            node: self.node,
                            frames: self.frames,
                        };
                        self.prefix = Some(prefix);
                        self.state.prefixes.lock().unwrap().push(prefix);
                    }
                    Poll::Ready(Some(Ok(flight_data)))
                }
                Some(Err(status)) => {
                    self.done = true;
                    Poll::Ready(Some(Err(status)))
                }
                None => {
                    self.done = true;
                    Poll::Ready(None)
                }
            }
        }
    }

    impl Drop for WrappedRawStream {
        fn drop(&mut self) {
            // The actual region-server stream is dropped first, so the acknowledgement below
            // reports a cancellation the real response has already observed.
            drop(self.inner.take());
            let _ = self.state.dropped.send(self.node);
        }
    }

    /// The region of a Flight `DoGet` when its ticket carries the raw-series extension.
    ///
    /// The ticket of a region read is the encoded `api::v1::region::QueryRequest` of the caller,
    /// so the extension key of its query context classifies the read without executing it.
    fn raw_series_region(ticket: &[u8]) -> Option<u64> {
        let request = RegionQueryRequest::decode(ticket).ok()?;
        let extensions = request.header?.query_context?.extensions;
        extensions
            .contains_key(RAW_SERIES_REQUEST_EXTENSION_KEY)
            .then_some(request.region_id)
    }

    /// The region of a named raw-series preflight action.
    fn prepare_region(action: &Action) -> Option<u64> {
        if action.r#type != PREPARE_RAW_SERIES_ACTION {
            return None;
        }
        RegionQueryRequest::decode(action.body.as_ref())
            .ok()
            .map(|request| request.region_id)
    }

    /// The peer status of a failed preflight or a failed raw read of a case.
    ///
    /// It carries the greptime error code as metadata, so the client preserves the code and can
    /// never report the status as a bare `Unimplemented` decline of an absent action.
    fn peer_status(code: StatusCode, marker: &str) -> Status {
        let headers = common_error::from_err_code_msg_to_header(code as u32, marker);
        Status::with_metadata(
            tonic::Code::Internal,
            marker,
            tonic::metadata::MetadataMap::from_headers(headers),
        )
    }

    /// The hook a case installs on the real TCP Flight listener of both datanodes.
    type FaultFlightHook = dyn Fn(DatanodeId, DatanodeRegionServer) -> FlightCraftRef + Send + Sync;

    /// Builds one two-datanode cluster whose frontend owns the query options and the real clients,
    /// with the case hook on the real TCP Flight listener of every datanode.
    async fn build_hooked_cluster(name: &str, state: Arc<FaultState>) -> GreptimeDbCluster {
        let hook: Arc<FaultFlightHook> =
            Arc::new(move |node: DatanodeId, server: DatanodeRegionServer| {
                let handler: FlightCraftRef = Arc::new(FaultFlight {
                    node,
                    inner: Arc::new(server),
                    state: Arc::clone(&state),
                });
                handler
            });
        GreptimeDbClusterBuilder::new(name)
            .await
            .with_datanodes(2)
            .with_real_datanode_grpc_addr(true)
            .with_real_frontend_clients(true)
            .with_frontend_query_options(QueryOptions {
                parallelism: 1,
                experimental_native_promql: true,
                ..Default::default()
            })
            .with_datanode_flight_hook(hook)
            .build(false)
            .await
    }

    /// Seeds the two-datanode fixture and returns the leader of every time partition.
    async fn seed_fixture(frontend: &Arc<Instance>) -> BTreeMap<u64, u64> {
        execute_sql(frontend, CREATE_TABLE).await;
        execute_sql(frontend, INSERT_ROWS).await;
        let leaders = region_leaders(frontend, TABLE).await;
        assert_eq!(
            2,
            leaders.len(),
            "expected one region per time partition of {TABLE}, actual region leaders: {leaders:?}"
        );
        assert_eq!(
            BTreeSet::from([1_u64, 2_u64]),
            leaders.values().copied().collect::<BTreeSet<u64>>(),
            "expected the time partitions of {TABLE} to be served by both datanodes, actual region \
             leaders: {leaders:?}"
        );
        leaders
    }

    /// Reads the stored fixture of `frontend` and proves it is complete.
    ///
    /// This runs for the seed check of a case and for the recovery check of a disarmed case.
    async fn assert_stored_rows(frontend: &Arc<Instance>) {
        let stored = query_batches_within(
            frontend,
            &format!("SELECT ts, host, val FROM {TABLE} ORDER BY ts"),
            QUERY_TIMEOUT,
        )
        .await;
        assert_eq!(expected_columns(), columns(&stored));
        assert_eq!(
            expected_stored_rows(),
            rows(&stored),
            "unexpected stored rows of {TABLE}"
        );
    }

    /// The datanodes of the fixture, which its two regions must span.
    fn nodes(leaders: &BTreeMap<u64, u64>) -> BTreeSet<DatanodeId> {
        leaders.values().copied().collect()
    }

    /// The sibling datanode of `node` in the two-datanode fixture.
    fn sibling_node(leaders: &BTreeMap<u64, u64>, node: DatanodeId) -> DatanodeId {
        *leaders
            .values()
            .find(|leader| **leader != node)
            .expect("the fixture must span two datanodes")
    }

    /// Proves the expected traffic of every datanode of the fixture in one query window.
    fn assert_traffic(
        window: &[Event],
        leaders: &BTreeMap<u64, u64>,
        expected: impl Fn(DatanodeId) -> Traffic,
    ) {
        for node in leaders.values() {
            assert_eq!(
                expected(*node),
                Traffic::of(window, *node),
                "unexpected traffic of the datanode {node}: {window:?}"
            );
        }
    }

    /// Proves that every preflight of `window` preceded its first read.
    fn assert_prepares_first(window: &[Event]) {
        let last_prepare = window
            .iter()
            .rposition(|event| matches!(event, Event::Prepare(_)));
        let first_read = window
            .iter()
            .position(|event| !matches!(event, Event::Prepare(_)));
        match (last_prepare, first_read) {
            (Some(prepare), Some(read)) => assert!(
                prepare < read,
                "every preflight must precede the first read: {window:?}"
            ),
            _ => panic!("the window must contain a preflight and a read: {window:?}"),
        }
    }

    /// Proves that every node of `nodes` forwarded a real payload prefix of its actual response.
    ///
    /// A wrapper stream only reports a prefix once a record batch of the actual response was
    /// forwarded, so a witness proves that the read really carried the payload of the region
    /// server and not just the schema of an empty answer.
    fn assert_payload_prefixes(state: &FaultState, nodes: impl IntoIterator<Item = DatanodeId>) {
        let prefixes = state.prefixes.lock().unwrap().clone();
        for node in nodes {
            let prefix = prefixes
                .iter()
                .find(|prefix| prefix.node == node)
                .unwrap_or_else(|| {
                    panic!("the read of the datanode {node} must forward a real payload prefix")
                });
            assert!(
                prefix.frames >= 2,
                "the prefix of the datanode {node} must include the schema frame and a record \
                 batch: {prefix:?}"
            );
        }
    }

    /// Runs the native `TQL EVAL` of the preflight-failure case and returns the debug chain of the
    /// failure it must surface.
    ///
    /// A failure of the planning phase surfaces as the error of the call, and a failure of the
    /// body surfaces as the terminal error of the returned stream; both are valid observations of
    /// a preflight that fails before execution, so the case accepts either. Both branches assert
    /// the peer status code of the failure before the diagnostic string is built, so a case that
    /// loses the code cannot pass on a message that merely looks right.
    async fn eval_failure(frontend: &Arc<Instance>) -> String {
        tokio::time::timeout(QUERY_TIMEOUT, async {
            match try_execute_sql(frontend, EVAL).await {
                Ok(output) => {
                    let OutputData::Stream(stream) = output.data else {
                        panic!("expected the native TQL EVAL to fail");
                    };
                    let error = collect_body_error(stream).await;
                    assert_eq!(
                        StatusCode::Internal,
                        error.status_code(),
                        "the peer status code must survive the body error, actual: {}",
                        error_chain(&error)
                    );
                    error_chain(&error)
                }
                Err(error) => {
                    assert_eq!(
                        StatusCode::Internal,
                        error.status_code(),
                        "the peer status code must survive the planning error, actual: {}",
                        error_chain(&error)
                    );
                    error_chain(&error)
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the faulted TQL EVAL must fail within {QUERY_TIMEOUT:?}"))
    }

    /// Consumes a result stream and returns its terminal error.
    ///
    /// The consumed stream is the only owner of the query, so dropping it here is what cancels the
    /// frontend side of the raw reads.
    async fn collect_body_error(
        mut stream: SendableRecordBatchStream,
    ) -> common_recordbatch::error::Error {
        loop {
            match stream.next().await {
                Some(Ok(_)) => continue,
                Some(Err(error)) => return error,
                None => panic!("the faulted body ended without the observed error"),
            }
        }
    }

    /// Runs a query that must complete within `bound`.
    async fn query_batches_within(
        frontend: &Arc<Instance>,
        sql: &str,
        bound: Duration,
    ) -> RecordBatches {
        tokio::time::timeout(bound, query_batches(frontend, sql))
            .await
            .unwrap_or_else(|_| panic!("`{sql}` must finish within {bound:?}"))
    }

    /// Awaits one cancellation acknowledgement per datanode, bounded by [`CANCEL_ACK_TIMEOUT`].
    async fn await_drop_acks(
        acknowledgements: &mut mpsc::UnboundedReceiver<DatanodeId>,
        leaders: &BTreeMap<u64, u64>,
    ) -> BTreeSet<DatanodeId> {
        let mut acknowledged = BTreeSet::new();
        while acknowledged.len() < leaders.len() {
            let acknowledgement = tokio::time::timeout(CANCEL_ACK_TIMEOUT, acknowledgements.recv())
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "every cancelled raw read must drop its actual region-server stream within \
                     {CANCEL_ACK_TIMEOUT:?}"
                    )
                });
            let Some(node) = acknowledgement else {
                panic!("the drop acknowledgement channel must stay open");
            };
            acknowledged.insert(node);
        }
        acknowledged
    }

    /// The debug view of an error and every source behind it.
    ///
    /// A peer status travels through several wrappers before it reaches a test, and the wrappers
    /// of different layers print different parts of it, so the whole chain is searched instead of
    /// the outermost display.
    fn error_chain<E: std::error::Error + ?Sized>(error: &E) -> String {
        let mut text = format!("{error:?}");
        let mut source = std::error::Error::source(error);
        while let Some(cause) = source {
            text.push_str(&format!("\n{cause:?}\n{cause}"));
            source = std::error::Error::source(cause);
        }
        text
    }

    /// The preflight of the greatest region id answers with the frozen unsupported body, so the
    /// frontend must return the complete ordinary result without opening any raw read.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_native_promql_cluster_remote_unsupported_preflight_falls_back() {
        common_telemetry::init_default_ut_logging();

        let (parked, _parked_signals) = mpsc::unbounded_channel();
        let (dropped, _dropped_signals) = mpsc::unbounded_channel();
        let state = Arc::new(FaultState::new(parked, dropped));
        let enabled = build_hooked_cluster("native_promql_unsupported", state.clone()).await;
        let disabled = build_cluster("native_promql_unsupported_disabled", 1, false).await;
        let enabled_frontend = enabled.fe_instance().clone();
        let disabled_frontend = disabled.fe_instance().clone();

        let leaders = seed_fixture(&enabled_frontend).await;
        execute_sql(&disabled_frontend, CREATE_TABLE).await;
        execute_sql(&disabled_frontend, INSERT_ROWS).await;
        assert_stored_rows(&enabled_frontend).await;

        // The greatest region id is the one that answers the unsupported body. The case does not
        // depend on the order in which the frontend works through its regions: that region must be
        // preflighted exactly once and then read once over the ordinary path, the other region may
        // still be preflighted before the decline is seen, so its preflight count is only bounded,
        // and neither region may be opened as a raw read.
        let unsupported = *leaders
            .keys()
            .next_back()
            .expect("one region per time partition");
        state.arm(ArmedFault::UnsupportedPreflight {
            region: unsupported,
        });
        let start = state.events().len();

        let native = query_batches_within(&enabled_frontend, EVAL, QUERY_TIMEOUT).await;
        let ordinary = query_batches_within(&disabled_frontend, EVAL, QUERY_TIMEOUT).await;

        assert_eq!(expected_columns(), columns(&native));
        assert_eq!(expected_eval_rows(), rows(&native));
        assert_eq!(columns(&native), columns(&ordinary));
        assert_eq!(native.schema(), ordinary.schema());
        assert_eq!(
            native.schema().arrow_schema(),
            ordinary.schema().arrow_schema()
        );
        assert_eq!(rows(&native), rows(&ordinary));

        let log = state.events();
        let observed = &log[start..];
        let designated = leaders[&unsupported];
        let expected = Traffic {
            prepare: 1,
            raw_get: 0,
            ordinary_get: 1,
        };
        assert_eq!(
            expected,
            Traffic::of(observed, designated),
            "the designated region {unsupported}: {observed:?}"
        );
        let sibling = Traffic::of(observed, sibling_node(&leaders, designated));
        assert!(
            sibling.prepare <= 1 && sibling.raw_get == 0 && sibling.ordinary_get == 1,
            "the sibling region must be preflighted at most once and read once: {observed:?}"
        );
        // Every preflight of both regions precedes the first read of the fallback.
        assert_prepares_first(observed);
    }

    /// The preflight of the greatest region id fails with an internal peer status that carries the
    /// marker, so the frontend must fail without a read and without a fallback.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_native_promql_cluster_preflight_error_is_terminal() {
        common_telemetry::init_default_ut_logging();

        let (parked, _parked_signals) = mpsc::unbounded_channel();
        let (dropped, _dropped_signals) = mpsc::unbounded_channel();
        let state = Arc::new(FaultState::new(parked, dropped));
        let enabled = build_hooked_cluster("native_promql_preflight_error", state.clone()).await;
        let frontend = enabled.fe_instance().clone();

        let leaders = seed_fixture(&frontend).await;
        assert_stored_rows(&frontend).await;

        // The greatest region id fails its preflight; the case does not depend on the order in
        // which the frontend preflights its regions.
        let failing = *leaders
            .keys()
            .next_back()
            .expect("one region per time partition");
        state.arm(ArmedFault::FailingPreflight {
            region: failing,
            marker: PREFLIGHT_ERROR_MARKER,
        });
        let start = state.events().len();

        let failure = eval_failure(&frontend).await;
        let log = state.events();
        let observed = &log[start..];

        assert!(
            failure.contains(PREFLIGHT_ERROR_MARKER),
            "the peer preflight failure must surface unchanged, actual: {failure}"
        );
        let designated = leaders[&failing];
        let expected = Traffic {
            prepare: 1,
            raw_get: 0,
            ordinary_get: 0,
        };
        assert_eq!(
            expected,
            Traffic::of(observed, designated),
            "the designated region {failing}: {observed:?}"
        );
        let sibling = Traffic::of(observed, sibling_node(&leaders, designated));
        assert!(
            sibling.prepare <= 1 && sibling.raw_get == 0 && sibling.ordinary_get == 0,
            "a failed preflight must not read or fall back at the sibling region: {observed:?}"
        );
    }

    /// The raw read of the greatest region id forwards the real payload prefix of its actual
    /// response and then fails late, while the read of the other region holds its actual response
    /// open behind its own real payload prefix; the cancellation that follows the error must drop
    /// both actual region-server streams.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_native_promql_cluster_late_body_error_after_real_prefix() {
        common_telemetry::init_default_ut_logging();

        let (parked, mut parked_signals) = mpsc::unbounded_channel();
        let (dropped, mut dropped_acks) = mpsc::unbounded_channel();
        let state = Arc::new(FaultState::new(parked, dropped));
        let enabled = build_hooked_cluster("native_promql_late_body_error", state.clone()).await;
        let frontend = enabled.fe_instance().clone();

        let leaders = seed_fixture(&frontend).await;
        assert_stored_rows(&frontend).await;

        // The case does not depend on the order in which the frontend reads its regions: the lower
        // region id holds its read open, and the higher one waits for that hold before it fails,
        // so the sibling response is held whichever read the frontend opens first.
        let mut regions = leaders.keys().copied();
        let sibling_region = regions.next().expect("one region per time partition");
        let selected_region = regions.next().expect("one region per time partition");
        state.arm(ArmedFault::LateRawError {
            selected: selected_region,
            sibling: sibling_region,
            marker: LATE_BODY_ERROR_MARKER,
        });
        let start = state.events().len();

        // One bound covers the planning call and the body of the faulted query.
        let failure = tokio::time::timeout(QUERY_TIMEOUT, async {
            let output = try_execute_sql(&frontend, EVAL)
                .await
                .expect("both preflights are supported, so the TQL EVAL must return its body");
            let OutputData::Stream(stream) = output.data else {
                panic!("expected the body stream of the TQL EVAL");
            };
            collect_body_error(stream).await
        })
        .await
        .unwrap_or_else(|_| {
            panic!("the late body error must be released within {QUERY_TIMEOUT:?}")
        });

        assert_eq!(
            StatusCode::Internal,
            failure.status_code(),
            "the peer error code must be preserved, actual: {}",
            error_chain(&failure)
        );
        assert!(
            error_chain(&failure).contains(LATE_BODY_ERROR_MARKER),
            "the peer status of the late body error must surface unchanged, actual: {}",
            error_chain(&failure)
        );

        // The held sibling reported itself while the error was still unreleased, and no other raw
        // read of the case parked.
        let held = parked_signals
            .try_recv()
            .expect("the held sibling must report itself before the late error is released");
        assert_eq!(
            leaders[&sibling_region], held,
            "the read of the held region must park"
        );
        assert!(
            parked_signals.try_recv().is_err(),
            "only one raw read of the case may park"
        );

        let log = state.events();
        let observed = &log[start..];
        assert_traffic(observed, &leaders, |_| Traffic {
            prepare: 1,
            raw_get: 1,
            ordinary_get: 0,
        });
        assert_prepares_first(observed);
        // Both reads forwarded the real payload prefix of their actual response before the error.
        assert_payload_prefixes(&state, nodes(&leaders));

        // The cancelled reads drop the actual region-server stream first and acknowledge after.
        let acknowledged = await_drop_acks(&mut dropped_acks, &leaders).await;
        assert_eq!(
            nodes(&leaders),
            acknowledged,
            "every raw read must acknowledge the cancellation its error caused"
        );
    }

    /// Both raw reads forward the real payload prefix of their actual response and hold it open;
    /// the frontend stream is cancelled while they are held, and the disarmed case must still
    /// serve the complete fixture and the native `TQL EVAL`.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_native_promql_cluster_frontend_cancel_drops_server_streams() {
        common_telemetry::init_default_ut_logging();

        let (parked, mut parked_signals) = mpsc::unbounded_channel();
        let (dropped, mut dropped_acks) = mpsc::unbounded_channel();
        let state = Arc::new(FaultState::new(parked, dropped));
        let enabled = build_hooked_cluster("native_promql_frontend_cancel", state.clone()).await;
        let disabled = build_cluster("native_promql_frontend_cancel_disabled", 1, false).await;
        let frontend = enabled.fe_instance().clone();
        let disabled_frontend = disabled.fe_instance().clone();

        let leaders = seed_fixture(&frontend).await;
        execute_sql(&disabled_frontend, CREATE_TABLE).await;
        execute_sql(&disabled_frontend, INSERT_ROWS).await;
        assert_stored_rows(&frontend).await;

        let regions = leaders.keys().copied().collect::<BTreeSet<_>>();
        let region_count = regions.len();
        state.arm(ArmedFault::ParkedRawStreams { regions });
        let start = state.events().len();

        // One bound covers the planning call, the body and the readiness phase below: the client
        // stream is driven until both raw reads are held, and nothing else is polled. No collector
        // holds another copy of the stream, so the drop below is the one cancellation of the
        // query.
        let (stream, held) = tokio::time::timeout(QUERY_TIMEOUT, async {
            let output = try_execute_sql(&frontend, EVAL)
                .await
                .expect("both preflights are supported, so the TQL EVAL must return its body");
            let OutputData::Stream(mut stream) = output.data else {
                panic!("expected the body stream of the TQL EVAL");
            };
            let mut held_nodes = BTreeSet::new();
            while held_nodes.len() < region_count {
                tokio::select! {
                    batch = stream.next() => match batch {
                        Some(Ok(_)) => {}
                        Some(Err(error)) => panic!("a held raw read must not fail: {error}"),
                        None => panic!("the TQL EVAL ended before both raw reads were held"),
                    },
                    signal = parked_signals.recv() => {
                        let node = signal.expect("the hold witness channel must stay open");
                        held_nodes.insert(node);
                    }
                }
            }
            (stream, held_nodes)
        })
        .await
        .unwrap_or_else(|_| panic!("both raw reads must be held within {QUERY_TIMEOUT:?}"));

        assert_eq!(
            nodes(&leaders),
            held,
            "both datanodes must hold a raw read of the fixture"
        );

        drop(stream);

        // The cancellation reaches the server-side wrappers, which drop the actual region-server
        // stream before they acknowledge it.
        let acknowledged = await_drop_acks(&mut dropped_acks, &leaders).await;
        assert_eq!(
            held, acknowledged,
            "every held raw read must observe the cancellation of its actual stream"
        );
        assert_payload_prefixes(&state, nodes(&leaders));

        // The counter window of the cancelled query ends before the follow-up queries below, which
        // are asserted on their own.
        let follow_up_start = state.events().len();

        // The same frontend must still serve the complete stored fixture through ordinary reads of
        // both datanodes, so the cancellation did not wedge the paths this case used. The disabled
        // cluster below only supplies the expected result of the native follow-up: it cannot prove
        // anything about the cancelled one.
        state.disarm();
        assert_stored_rows(&frontend).await;
        let native = query_batches_within(&frontend, EVAL, QUERY_TIMEOUT).await;
        let ordinary = query_batches_within(&disabled_frontend, EVAL, QUERY_TIMEOUT).await;
        assert_eq!(expected_columns(), columns(&native));
        assert_eq!(expected_eval_rows(), rows(&native));
        assert_eq!(columns(&native), columns(&ordinary));
        assert_eq!(native.schema(), ordinary.schema());
        assert_eq!(
            native.schema().arrow_schema(),
            ordinary.schema().arrow_schema()
        );
        assert_eq!(expected_eval_rows(), rows(&ordinary));
        assert_eq!(rows(&native), rows(&ordinary));

        // The cancelled query is the window before the follow-up queries: every region of the
        // fixture was preflighted and read once, no ordinary read happened, and nothing was
        // replayed.
        let log = state.events();
        let observed = &log[start..follow_up_start];
        assert_traffic(observed, &leaders, |_| Traffic {
            prepare: 1,
            raw_get: 1,
            ordinary_get: 0,
        });
    }
}

/// Multi-`Float64` acceptance of the whole-table native instant mapping, on a standalone frontend
/// and on the real two-datanode cluster.
///
/// Both shapes of this module declare their `Float64` fields in a deliberately non-lexical order
/// (`z_first`, `a_second`, optionally `m_third`), so a mapping that sorts or reorders field names
/// fails the fixture instead of a plan string, and the fixture makes the staleness rule of the
/// ordinary selector observable from the complete result alone: the checked first field alone
/// decides whether a sample is stale, whatever the other fields carry.
///
/// * the stale marker in the unchecked second field at 3000 must keep its exact bits in the row
///   the evaluation point returns;
/// * the stale marker in the checked first field at 4000 must suppress that evaluation point
///   without reviving the older sample of 3000;
/// * the `NULL` first field at 5000 keeps a row whose first field is `NULL` and whose second field
///   keeps its ordinary `NaN` payload.
///
/// The sparse `beta` series has one sample in each partition of the distributed shapes and none in
/// between, so the lookback of 2000, 3000 and 4000 is carried by its first sample while its 5000
/// sample lives in the region of the other datanode.
mod multifloat_acceptance {
    use api::v1::helper::{field_column_schema, tag_column_schema, time_index_column_schema};
    use api::v1::value::ValueData;
    use api::v1::{ColumnDataType, Row, RowInsertRequest, RowInsertRequests, Rows, Value};
    use common_query::prometheus::PROMETHEUS_STALE_NAN_BITS;

    use crate::standalone::GreptimeDbStandaloneBuilder;
    use crate::test_util::execute_sql;

    use super::*;

    /// One table shape: the `Float64` fields in their declared, deliberately non-lexical order.
    ///
    /// The order is the fixture of the staleness rule: a mapping that sorts field names would check
    /// `a_second` or `m_third` at 3000 instead of `z_first`.
    struct Shape {
        table: &'static str,
        fields: &'static [&'static str],
    }

    /// The two shapes of this lane: two and three `Float64` fields.
    const SHAPES: &[Shape] = &[
        Shape {
            table: "native_multifloat_two",
            fields: &["z_first", "a_second"],
        },
        Shape {
            table: "native_multifloat_three",
            fields: &["z_first", "a_second", "m_third"],
        },
    ];

    /// The exact bit patterns of the fixture and of its expectation: no cell is ever compared
    /// through `f64` equality, which would hide a `NaN` payload or a signed zero.
    const NEGATIVE_ZERO: u64 = 0x8000_0000_0000_0000;
    const POSITIVE_ZERO: u64 = 0x0000_0000_0000_0000;
    const TWO: u64 = 0x4000_0000_0000_0000;
    const THREE: u64 = 0x4008_0000_0000_0000;
    const FOUR: u64 = 0x4010_0000_0000_0000;
    const SEVEN: u64 = 0x401c_0000_0000_0000;
    const EIGHT: u64 = 0x4020_0000_0000_0000;
    const NINE: u64 = 0x4022_0000_0000_0000;
    /// An ordinary quiet `NaN` that is not the Prometheus stale marker.
    const ORDINARY_NAN_ONE: u64 = 0x7ff8_0000_0000_0001;
    /// A second ordinary quiet `NaN`, so the two planted payloads cannot be confused.
    const ORDINARY_NAN_TWO: u64 = 0x7ff8_0000_0000_0002;

    /// The planted bits of every fixture row in insertion order; `None` is a stored `NULL`.
    ///
    /// `alpha` fills the evaluation grid with the three staleness cases, and the sparse `beta`
    /// series carries the lookback across the partition at 3000. The third slot is only planted
    /// into the three-field shape.
    const FIXTURE: &[(i64, &str, [Option<u64>; 3])] = &[
        (
            1_000,
            "alpha",
            [Some(NEGATIVE_ZERO), Some(POSITIVE_ZERO), None],
        ),
        (2_000, "alpha", [Some(ORDINARY_NAN_ONE), None, Some(TWO)]),
        (
            3_000,
            "alpha",
            [
                Some(THREE),
                Some(PROMETHEUS_STALE_NAN_BITS),
                Some(NEGATIVE_ZERO),
            ],
        ),
        (
            4_000,
            "alpha",
            [Some(PROMETHEUS_STALE_NAN_BITS), Some(FOUR), Some(FOUR)],
        ),
        (
            5_000,
            "alpha",
            [None, Some(ORDINARY_NAN_TWO), Some(POSITIVE_ZERO)],
        ),
        (1_000, "beta", [Some(SEVEN), None, None]),
        (5_000, "beta", [Some(EIGHT), Some(NINE), None]),
    ];

    /// The complete result of `TQL EVAL (1, 5, '1s')` over any shape, in the only sort order the
    /// comparison uses: `(timestamp, host)`.
    ///
    /// Every cell is the exact `to_bits` of the source row its evaluation point selected, and
    /// `alpha` has no row at 4000 at all. The third slot is only compared by the three-field shape.
    const EXPECTED_RESULT: &[(i64, &str, [Option<u64>; 3])] = &[
        (
            1_000,
            "alpha",
            [Some(NEGATIVE_ZERO), Some(POSITIVE_ZERO), None],
        ),
        (1_000, "beta", [Some(SEVEN), None, None]),
        (2_000, "alpha", [Some(ORDINARY_NAN_ONE), None, Some(TWO)]),
        (2_000, "beta", [Some(SEVEN), None, None]),
        (
            3_000,
            "alpha",
            [
                Some(THREE),
                Some(PROMETHEUS_STALE_NAN_BITS),
                Some(NEGATIVE_ZERO),
            ],
        ),
        (3_000, "beta", [Some(SEVEN), None, None]),
        (4_000, "beta", [Some(SEVEN), None, None]),
        (
            5_000,
            "alpha",
            [None, Some(ORDINARY_NAN_TWO), Some(POSITIVE_ZERO)],
        ),
        (5_000, "beta", [Some(EIGHT), Some(NINE), None]),
    ];

    /// One `Float64` cell of a planted row: the exact bits, or a stored `NULL`.
    fn float_cell(bits: Option<u64>) -> Value {
        bits.map(|bits| ValueData::F64Value(f64::from_bits(bits)).into())
            .unwrap_or_default()
    }

    /// The partition clause of the distributed shapes: both regions split every series at 3000 ms,
    /// so the first samples of every series live in another region than the last ones.
    const PARTITION: &str =
        " PARTITION ON COLUMNS (ts) (ts < '1970-01-01 00:00:03', ts >= '1970-01-01 00:00:03')";

    /// The complete `DDL` of `shape`; a distributed shape appends [`PARTITION`].
    fn create_table_sql(shape: &Shape, partitioned: bool) -> String {
        let fields = shape
            .fields
            .iter()
            .map(|field| format!("{field} DOUBLE"))
            .collect::<Vec<_>>()
            .join(", ");
        let partition = if partitioned { PARTITION } else { "" };
        format!(
            "CREATE TABLE {} (ts TIMESTAMP(3), host STRING, {fields}, TIME INDEX (ts), PRIMARY \
             KEY (host)){partition} ENGINE=mito",
            shape.table
        )
    }

    /// The `TQL EVAL` of one shape: the grid evaluates every planted second from 1000 to 5000.
    fn eval_sql(shape: &Shape) -> String {
        format!("TQL EVAL (1, 5, '1s') {}", shape.table)
    }

    /// The `TQL ANALYZE VERBOSE` of one shape.
    fn analyze_sql(shape: &Shape) -> String {
        format!("TQL ANALYZE VERBOSE (1, 5, '1s') {}", shape.table)
    }

    /// The complete column list of a result of `shape`, in the declared field order.
    fn planned_columns(shape: &Shape) -> Vec<(String, DataType)> {
        let mut columns = vec![
            (
                "ts".to_string(),
                DataType::Timestamp(TimeUnit::Millisecond, None),
            ),
            ("host".to_string(), DataType::Utf8),
        ];
        columns.extend(
            shape
                .fields
                .iter()
                .map(|field| ((*field).to_string(), DataType::Float64)),
        );
        columns
    }

    /// Reads every planned field of `batches` by name and keeps the exact bits of every cell:
    /// `None` is a `NULL` and every other cell is its `to_bits`, so no `f64` equality can hide a
    /// payload. The rows are sorted only by `(timestamp, host)`, the order of every expectation.
    fn bit_rows(
        batches: &RecordBatches,
        shape: &Shape,
    ) -> Vec<(i64, Option<String>, Vec<Option<u64>>)> {
        assert_eq!(
            2 + shape.fields.len(),
            batches.schema().arrow_schema().fields().len(),
            "unexpected result column count of {}",
            shape.table
        );
        let mut rows: Vec<(i64, Option<String>, Vec<Option<u64>>)> = Vec::new();
        for batch in batches.iter() {
            let batch = batch.df_record_batch();
            let ts = batch
                .column_by_name("ts")
                .unwrap_or_else(|| panic!("the result of {} must carry ts", shape.table))
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            let host = batch
                .column_by_name("host")
                .unwrap_or_else(|| panic!("the result of {} must carry host", shape.table))
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let fields = shape
                .fields
                .iter()
                .map(|field| {
                    batch
                        .column_by_name(field)
                        .unwrap_or_else(|| {
                            panic!("the result of {} must carry {field}", shape.table)
                        })
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                })
                .collect::<Vec<_>>();
            for index in 0..batch.num_rows() {
                rows.push((
                    ts.value(index),
                    (!host.is_null(index)).then(|| host.value(index).to_string()),
                    fields
                        .iter()
                        .map(|field| (!field.is_null(index)).then(|| field.value(index).to_bits()))
                        .collect(),
                ));
            }
        }
        rows.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
        rows
    }

    /// The complete stored fixture of `shape` in the order of the stored check (`ts`, `host`).
    fn fixture_stored_rows(shape: &Shape) -> Vec<(i64, Option<String>, Vec<Option<u64>>)> {
        let mut rows = FIXTURE
            .iter()
            .map(|(ts, host, bits)| {
                (
                    *ts,
                    Some((*host).to_string()),
                    bits[..shape.fields.len()].to_vec(),
                )
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
        rows
    }

    /// The complete expected result of `shape`, with one slot per planned field.
    fn fixture_result_rows(shape: &Shape) -> Vec<(i64, Option<String>, Vec<Option<u64>>)> {
        EXPECTED_RESULT
            .iter()
            .map(|(ts, host, bits)| {
                (
                    *ts,
                    Some((*host).to_string()),
                    bits[..shape.fields.len()].to_vec(),
                )
            })
            .collect()
    }

    /// Seeds `shape` through the row-insert API of `frontend`: every planted `Float64` keeps its
    /// exact bits, which no text protocol and no `SQL` literal can guarantee.
    ///
    /// The complete `DDL` above predeclares every column, so the insert never accommodates a
    /// dynamic schema.
    async fn ingest(frontend: &Arc<Instance>, shape: &Shape) {
        let mut schema = vec![
            time_index_column_schema("ts", ColumnDataType::TimestampMillisecond),
            tag_column_schema("host", ColumnDataType::String),
        ];
        schema.extend(
            shape
                .fields
                .iter()
                .map(|field| field_column_schema(field, ColumnDataType::Float64)),
        );
        let rows = FIXTURE
            .iter()
            .map(|(ts, host, bits)| Row {
                values: std::iter::once(ValueData::TimestampMillisecondValue(*ts).into())
                    .chain(std::iter::once(
                        ValueData::StringValue((*host).to_string()).into(),
                    ))
                    .chain(bits[..shape.fields.len()].iter().copied().map(float_cell))
                    .collect(),
            })
            .collect::<Vec<_>>();
        let output = frontend
            .handle_row_inserts(
                RowInsertRequests {
                    inserts: vec![RowInsertRequest {
                        table_name: shape.table.to_string(),
                        rows: Some(Rows { schema, rows }),
                    }],
                },
                QueryContext::arc(),
                false,
                false,
            )
            .await
            .unwrap_or_else(|error| {
                panic!("inserting the fixture of {} failed: {error}", shape.table)
            });
        assert!(
            matches!(output.data, OutputData::AffectedRows(rows) if rows == FIXTURE.len()),
            "expected the row insert of {} to affect {} rows",
            shape.table,
            FIXTURE.len()
        );
    }

    /// Reads the whole stored fixture of `shape` through ordinary `SQL` before any `TQL`
    /// assertion: the exact column list, the exact `NULL`s and the exact bits of every planted
    /// float prove that the row insert reached the region(s) without normalizing a payload.
    async fn assert_stored(frontend: &Arc<Instance>, shape: &Shape) {
        let sql = format!(
            "SELECT ts, host, {} FROM {} ORDER BY ts, host",
            shape.fields.join(", "),
            shape.table
        );
        let stored = query_batches(frontend, &sql).await;
        assert_eq!(planned_columns(shape), columns(&stored));
        assert_eq!(
            fixture_stored_rows(shape),
            bit_rows(&stored, shape),
            "unexpected stored fixture of {}",
            shape.table
        );
    }

    /// Runs the native `TQL EVAL` of `shape` once, proves its complete result against the planted
    /// bits and returns it for the schema comparison with the ordinary result.
    async fn eval_native(frontend: &Arc<Instance>, shape: &Shape) -> RecordBatches {
        let native = query_batches(frontend, &eval_sql(shape)).await;
        assert_eq!(planned_columns(shape), columns(&native));
        assert_eq!(
            fixture_result_rows(shape),
            bit_rows(&native, shape),
            "unexpected native result of {}",
            shape.table
        );
        native
    }

    /// Runs one native and one ordinary eval of `shape` and proves both complete results against
    /// the planted bits: the ordinary result is the independent oracle of the literals, and the
    /// complete public schema of the two must be unchanged, nullability and metadata included.
    async fn assert_eval(enabled: &Arc<Instance>, ordinary: &Arc<Instance>, shape: &Shape) {
        let native = eval_native(enabled, shape).await;
        let normal = query_batches(ordinary, &eval_sql(shape)).await;
        assert_eq!(planned_columns(shape), columns(&normal));
        assert_eq!(
            fixture_result_rows(shape),
            bit_rows(&normal, shape),
            "unexpected ordinary result of {}",
            shape.table
        );
        assert_eq!(columns(&native), columns(&normal));
        assert_eq!(native.schema(), normal.schema());
        assert_eq!(
            native.schema().arrow_schema(),
            normal.schema().arrow_schema()
        );
    }

    /// The `ANALYZE VERBOSE` plan of `shape` on `frontend`.
    async fn analyze_plan(frontend: &Arc<Instance>, shape: &Shape) -> String {
        query_pretty(frontend, &analyze_sql(shape)).await
    }

    /// Proves the native plan of `shape`: the native instant manipulation always, and the
    /// raw-series merge only when `raw_merge` states it, because only the distributed shapes span
    /// two datanodes and must merge their regions.
    async fn assert_native_plan(frontend: &Arc<Instance>, shape: &Shape, raw_merge: Option<bool>) {
        let plan = analyze_plan(frontend, shape).await;
        assert!(
            plan.contains("NativeInstantManipulateExec"),
            "the native instant manipulation is missing for the multi-Float64 shape {}:\n{plan}",
            shape.table
        );
        if let Some(raw_merge) = raw_merge {
            assert_eq!(
                raw_merge,
                plan.contains("NativeRawMergeExec"),
                "the raw-series merge of {} must be {raw_merge} for this deployment:\n{plan}",
                shape.table
            );
        }
    }

    /// Proves that the flag-free `frontend` keeps the ordinary plan of `shape`.
    async fn assert_ordinary_plan(frontend: &Arc<Instance>, shape: &Shape) {
        let plan = analyze_plan(frontend, shape).await;
        assert!(
            !plan.contains("NativeInstantManipulateExec"),
            "the flag-free frontend unexpectedly used the native instant manipulation for {}:\n\
             {plan}",
            shape.table
        );
    }

    /// Proves that the time partition of `shape` produced two regions, one leader per datanode,
    /// and returns the leader of every region.
    async fn assert_two_datanode_regions(
        frontend: &Arc<Instance>,
        shape: &Shape,
    ) -> BTreeMap<u64, u64> {
        let leaders = region_leaders(frontend, shape.table).await;
        assert_eq!(
            2,
            leaders.len(),
            "expected one region per time partition of {}, actual region leaders: {leaders:?}",
            shape.table
        );
        assert_eq!(
            BTreeSet::from([1_u64, 2_u64]),
            leaders.values().copied().collect::<BTreeSet<u64>>(),
            "expected the time partitions of {} to be served by both datanodes, actual region \
             leaders: {leaders:?}",
            shape.table
        );
        leaders
    }

    /// The raw-series counters of every datanode that leads a region of one shape.
    fn rpc_snapshot(
        cluster: &GreptimeDbCluster,
        leaders: &BTreeMap<u64, u64>,
    ) -> BTreeMap<u64, (usize, usize)> {
        leaders
            .values()
            .map(|datanode| {
                (
                    *datanode,
                    (
                        rpc_requests(cluster, *datanode),
                        prepare_requests(cluster, *datanode),
                    ),
                )
            })
            .collect()
    }

    /// Proves that everything since `baseline` read and preflighted every region it covers
    /// exactly once: one raw read and one named preflight per datanode, so one eval of any shape
    /// costs exactly one Flight `DoGet` and one Flight `DoAction` per region leader.
    fn assert_one_raw_rpc_per_node(
        cluster: &GreptimeDbCluster,
        baseline: &BTreeMap<u64, (usize, usize)>,
    ) {
        for (datanode, (do_get, prepare)) in baseline {
            assert_eq!(
                do_get + 1,
                rpc_requests(cluster, *datanode),
                "expected exactly one raw-series read at the datanode {datanode} since the baseline"
            );
            assert_eq!(
                prepare + 1,
                prepare_requests(cluster, *datanode),
                "expected exactly one raw-series preflight at the datanode {datanode} since the \
                 baseline"
            );
        }
    }

    /// Flushes `shape` and proves that the same grid is still served by the native plan: a
    /// normalizing write or read path of a flushed value could only fail here.
    async fn flush_shape(frontend: &Arc<Instance>, shape: &Shape, raw_merge: Option<bool>) {
        execute_sql(frontend, &format!("ADMIN FLUSH_TABLE('{}')", shape.table)).await;
        assert_native_plan(frontend, shape, raw_merge).await;
    }

    /// Runs the standalone case at parallelism 1 for both shapes.
    ///
    /// The standalone table carries no partition clause, so the whole table is one region; the
    /// plan of that local route is asserted through the native instant manipulation only. The
    /// second instance keeps the flag off and supplies the ordinary result of the same fixture.
    async fn run_standalone_case(name: &str) {
        common_telemetry::init_default_ut_logging();

        let enabled = GreptimeDbStandaloneBuilder::new(&format!("{name}_enabled"))
            .with_query_options(QueryOptions {
                parallelism: 1,
                experimental_native_promql: true,
                ..Default::default()
            })
            .build()
            .await;
        let ordinary = GreptimeDbStandaloneBuilder::new(&format!("{name}_ordinary"))
            .with_query_options(QueryOptions {
                parallelism: 1,
                experimental_native_promql: false,
                ..Default::default()
            })
            .build()
            .await;
        let enabled_frontend = enabled.fe_instance().clone();
        let ordinary_frontend = ordinary.fe_instance().clone();

        for shape in SHAPES {
            execute_sql(&enabled_frontend, &create_table_sql(shape, false)).await;
            execute_sql(&ordinary_frontend, &create_table_sql(shape, false)).await;
            ingest(&enabled_frontend, shape).await;
            ingest(&ordinary_frontend, shape).await;

            // The ordinary read proves the stored bits of the whole table before any `TQL` runs.
            assert_stored(&enabled_frontend, shape).await;

            assert_native_plan(&enabled_frontend, shape, None).await;
            assert_ordinary_plan(&ordinary_frontend, shape).await;

            assert_eval(&enabled_frontend, &ordinary_frontend, shape).await;

            // The flush moves the samples of every series from the memtable into an SST; the same
            // grid must still be served natively and must still return the exact same bits.
            flush_shape(&enabled_frontend, shape, None).await;
            eval_native(&enabled_frontend, shape).await;
        }
    }

    /// Runs the two-datanode case at `parallelism` for both shapes.
    ///
    /// The frontend carries the flag while every datanode keeps its default, exactly like a
    /// deployment where the planning node opted in, and the datanodes are reachable at the
    /// addresses of the table routes, so both its named preflight and its raw-series read are
    /// counted. A second, flag-free cluster serves the same fixture and supplies the ordinary
    /// result the complete public schema is compared with.
    async fn run_cluster_case(name: &str, parallelism: usize) {
        common_telemetry::init_default_ut_logging();

        let enabled = build_cluster(&format!("{name}_enabled"), parallelism, true).await;
        let disabled = build_cluster(&format!("{name}_disabled"), parallelism, false).await;
        let enabled_frontend = enabled.fe_instance().clone();
        let disabled_frontend = disabled.fe_instance().clone();

        for shape in SHAPES {
            execute_sql(&enabled_frontend, &create_table_sql(shape, true)).await;
            execute_sql(&disabled_frontend, &create_table_sql(shape, true)).await;
            ingest(&enabled_frontend, shape).await;
            ingest(&disabled_frontend, shape).await;

            // Each shape is one region per time partition, and the two regions must be served by
            // the two different datanodes: only then does every series span both of them.
            let leaders = assert_two_datanode_regions(&enabled_frontend, shape).await;

            // The ordinary read proves the seeded rows of both datanodes are complete and
            // bit-exact before any `TQL` runs.
            assert_stored(&enabled_frontend, shape).await;

            assert_native_plan(&enabled_frontend, shape, Some(true)).await;
            assert_ordinary_plan(&disabled_frontend, shape).await;

            // Seeding and planning are done: from here on only the `TQL EVAL` below may move the
            // counters, so the baseline is taken after the analyze and before the eval.
            let baseline = rpc_snapshot(&enabled, &leaders);
            assert_eval(&enabled_frontend, &disabled_frontend, shape).await;
            assert_one_raw_rpc_per_node(&enabled, &baseline);

            // The flushed regions must still be served by the native path; a fallback below would
            // make the repeat eval vacuous.
            flush_shape(&enabled_frontend, shape, Some(true)).await;
            let flushed_baseline = rpc_snapshot(&enabled, &leaders);
            eval_native(&enabled_frontend, shape).await;
            assert_one_raw_rpc_per_node(&enabled, &flushed_baseline);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_native_promql_multifloat_standalone_p1() {
        run_standalone_case("native_promql_multifloat_standalone_p1").await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_native_promql_multifloat_cluster_p1() {
        run_cluster_case("native_promql_multifloat_cluster_p1", 1).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_native_promql_multifloat_cluster_p4() {
        run_cluster_case("native_promql_multifloat_cluster_p4", 4).await;
    }
}
