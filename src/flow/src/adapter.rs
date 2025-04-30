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

//! for getting data from source and sending results to sink
//! and communicating with other parts of the database
#![warn(unused_imports)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use api::v1::{RowDeleteRequest, RowDeleteRequests, RowInsertRequest, RowInsertRequests};
use common_config::Configurable;
use common_error::ext::BoxedError;
use common_meta::key::TableMetadataManagerRef;
use common_runtime::JoinHandle;
use common_telemetry::logging::{LoggingOptions, TracingOptions};
use common_telemetry::{debug, info, trace};
use datatypes::schema::ColumnSchema;
use datatypes::value::Value;
use greptime_proto::v1;
use itertools::{EitherOrBoth, Itertools};
use meta_client::MetaClientOptions;
use query::options::QueryOptions;
use query::QueryEngine;
use serde::{Deserialize, Serialize};
use servers::grpc::GrpcOptions;
use servers::heartbeat_options::HeartbeatOptions;
use servers::http::HttpOptions;
use session::context::QueryContext;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::storage::{ConcreteDataType, RegionId};
use table::metadata::TableId;
use tokio::sync::broadcast::error::TryRecvError;
use tokio::sync::{broadcast, watch, Mutex, RwLock};

pub(crate) use crate::adapter::node_context::FlownodeContext;
use crate::adapter::refill::RefillTask;
use crate::adapter::table_source::ManagedTableSource;
use crate::adapter::util::relation_desc_to_column_schemas_with_fallback;
pub(crate) use crate::adapter::worker::{create_worker, Worker, WorkerHandle};
use crate::compute::ErrCollector;
use crate::df_optimizer::sql_to_flow_plan;
use crate::error::{EvalSnafu, ExternalSnafu, InternalSnafu, InvalidQuerySnafu, UnexpectedSnafu};
use crate::expr::Batch;
use crate::metrics::{METRIC_FLOW_INSERT_ELAPSED, METRIC_FLOW_ROWS, METRIC_FLOW_RUN_INTERVAL_MS};
use crate::repr::{self, DiffRow, RelationDesc, Row, BATCH_SIZE};
use crate::{CreateFlowArgs, FlowId, TableName};

pub(crate) mod flownode_impl;
mod parse_expr;
pub(crate) mod refill;
mod stat;
#[cfg(test)]
mod tests;
pub(crate) mod util;
mod worker;

pub(crate) mod node_context;
pub(crate) mod table_source;

use crate::error::Error;
use crate::utils::StateReportHandler;
use crate::FrontendInvoker;

// `GREPTIME_TIMESTAMP` is not used to distinguish when table is created automatically by flow
pub const AUTO_CREATED_PLACEHOLDER_TS_COL: &str = "__ts_placeholder";

pub const AUTO_CREATED_UPDATE_AT_TS_COL: &str = "update_at";

/// Flow config that exists both in standalone&distributed mode
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct FlowConfig {
    pub num_workers: usize,
}

impl Default for FlowConfig {
    fn default() -> Self {
        Self {
            num_workers: (common_config::utils::get_cpus() / 2).max(1),
        }
    }
}

/// Options for flow node
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct FlownodeOptions {
    pub node_id: Option<u64>,
    pub flow: FlowConfig,
    pub grpc: GrpcOptions,
    pub http: HttpOptions,
    pub meta_client: Option<MetaClientOptions>,
    pub logging: LoggingOptions,
    pub tracing: TracingOptions,
    pub heartbeat: HeartbeatOptions,
    pub query: QueryOptions,
    pub user_provider: Option<String>,
}

impl Default for FlownodeOptions {
    fn default() -> Self {
        Self {
            node_id: None,
            flow: FlowConfig::default(),
            grpc: GrpcOptions::default().with_bind_addr("127.0.0.1:3004"),
            http: HttpOptions::default(),
            meta_client: None,
            logging: LoggingOptions::default(),
            tracing: TracingOptions::default(),
            heartbeat: HeartbeatOptions::default(),
            query: QueryOptions::default(),
            user_provider: None,
        }
    }
}

impl Configurable for FlownodeOptions {
    fn validate_sanitize(&mut self) -> common_config::error::Result<()> {
        if self.flow.num_workers == 0 {
            self.flow.num_workers = (common_config::utils::get_cpus() / 2).max(1);
        }
        Ok(())
    }
}

/// Arc-ed FlowNodeManager, cheaper to clone
pub type FlowStreamingEngineRef = Arc<StreamingEngine>;

/// FlowNodeManager manages the state of all tasks in the flow node, which should be run on the same thread
///
/// The choice of timestamp is just using current system timestamp for now
///
pub struct StreamingEngine {
    /// The handler to the worker that will run the dataflow
    /// which is `!Send` so a handle is used
    pub worker_handles: Vec<WorkerHandle>,
    /// The selector to select a worker to run the dataflow
    worker_selector: Mutex<usize>,
    /// The query engine that will be used to parse the query and convert it to a dataflow plan
    pub query_engine: Arc<dyn QueryEngine>,
    /// Getting table name and table schema from table info manager
    table_info_source: ManagedTableSource,
    frontend_invoker: RwLock<Option<FrontendInvoker>>,
    /// contains mapping from table name to global id, and table schema
    node_context: RwLock<FlownodeContext>,
    /// Contains all refill tasks
    refill_tasks: RwLock<BTreeMap<FlowId, RefillTask>>,
    flow_err_collectors: RwLock<BTreeMap<FlowId, ErrCollector>>,
    src_send_buf_lens: RwLock<BTreeMap<TableId, watch::Receiver<usize>>>,
    tick_manager: FlowTickManager,
    /// This node id is only available in distributed mode, on standalone mode this is guaranteed to be `None`
    pub node_id: Option<u32>,
    /// Lock for flushing, will be `read` by `handle_inserts` and `write` by `flush_flow`
    ///
    /// So that a series of event like `inserts -> flush` can be handled correctly
    flush_lock: RwLock<()>,
    /// receive a oneshot sender to send state size report
    state_report_handler: RwLock<Option<StateReportHandler>>,
}

/// Building FlownodeManager
impl StreamingEngine {
    /// set frontend invoker
    pub async fn set_frontend_invoker(&self, frontend: FrontendInvoker) {
        *self.frontend_invoker.write().await = Some(frontend);
    }

    /// Create **without** setting `frontend_invoker`
    pub fn new(
        node_id: Option<u32>,
        query_engine: Arc<dyn QueryEngine>,
        table_meta: TableMetadataManagerRef,
    ) -> Self {
        let srv_map = ManagedTableSource::new(
            table_meta.table_info_manager().clone(),
            table_meta.table_name_manager().clone(),
        );
        let node_context = FlownodeContext::new(Box::new(srv_map.clone()) as _);
        let tick_manager = FlowTickManager::new();
        let worker_handles = Vec::new();
        StreamingEngine {
            worker_handles,
            worker_selector: Mutex::new(0),
            query_engine,
            table_info_source: srv_map,
            frontend_invoker: RwLock::new(None),
            node_context: RwLock::new(node_context),
            refill_tasks: Default::default(),
            flow_err_collectors: Default::default(),
            src_send_buf_lens: Default::default(),
            tick_manager,
            node_id,
            flush_lock: RwLock::new(()),
            state_report_handler: RwLock::new(None),
        }
    }

    pub async fn with_state_report_handler(self, handler: StateReportHandler) -> Self {
        *self.state_report_handler.write().await = Some(handler);
        self
    }

    /// Create a flownode manager with one worker
    pub fn new_with_workers<'s>(
        node_id: Option<u32>,
        query_engine: Arc<dyn QueryEngine>,
        table_meta: TableMetadataManagerRef,
        num_workers: usize,
    ) -> (Self, Vec<Worker<'s>>) {
        let mut zelf = Self::new(node_id, query_engine, table_meta);

        let workers: Vec<_> = (0..num_workers)
            .map(|_| {
                let (handle, worker) = create_worker();
                zelf.add_worker_handle(handle);
                worker
            })
            .collect();
        (zelf, workers)
    }

    /// add a worker handler to manager, meaning this corresponding worker is under it's manage
    pub fn add_worker_handle(&mut self, handle: WorkerHandle) {
        self.worker_handles.push(handle);
    }
}

#[derive(Debug)]
pub enum DiffRequest {
    Insert(Vec<(Row, repr::Timestamp)>),
    Delete(Vec<(Row, repr::Timestamp)>),
}

impl DiffRequest {
    pub fn len(&self) -> usize {
        match self {
            Self::Insert(v) => v.len(),
            Self::Delete(v) => v.len(),
        }
    }
}

pub fn batches_to_rows_req(batches: Vec<Batch>) -> Result<Vec<DiffRequest>, Error> {
    let mut reqs = Vec::new();
    for batch in batches {
        let mut rows = Vec::with_capacity(batch.row_count());
        for i in 0..batch.row_count() {
            let row = batch.get_row(i).context(EvalSnafu)?;
            rows.push((Row::new(row), 0));
        }
        reqs.push(DiffRequest::Insert(rows));
    }
    Ok(reqs)
}

/// This impl block contains methods to send writeback requests to frontend
impl StreamingEngine {
    /// Return the number of requests it made
    pub async fn send_writeback_requests(&self) -> Result<usize, Error> {
        let all_reqs = self.generate_writeback_request().await?;
        if all_reqs.is_empty() || all_reqs.iter().all(|v| v.1.is_empty()) {
            return Ok(0);
        }
        let mut req_cnt = 0;
        for (table_name, reqs) in all_reqs {
            if reqs.is_empty() {
                continue;
            }
            let (catalog, schema) = (table_name[0].clone(), table_name[1].clone());
            let ctx = Arc::new(QueryContext::with(&catalog, &schema));

            let (is_ts_placeholder, proto_schema) = match self
                .try_fetch_existing_table(&table_name)
                .await?
                .context(UnexpectedSnafu {
                    reason: format!("Table not found: {}", table_name.join(".")),
                }) {
                Ok(r) => r,
                Err(e) => {
                    if self
                        .table_info_source
                        .get_opt_table_id_from_name(&table_name)
                        .await?
                        .is_none()
                    {
                        // deal with both flow&sink table no longer exists
                        // but some output is still in output buf
                        common_telemetry::warn!(e; "Table `{}` no longer exists, skip writeback", table_name.join("."));
                        continue;
                    } else {
                        return Err(e);
                    }
                }
            };
            let schema_len = proto_schema.len();

            let total_rows = reqs.iter().map(|r| r.len()).sum::<usize>();
            trace!(
                "Sending {} writeback requests to table {}, reqs total rows={}",
                reqs.len(),
                table_name.join("."),
                reqs.iter().map(|r| r.len()).sum::<usize>()
            );

            METRIC_FLOW_ROWS
                .with_label_values(&["out"])
                .inc_by(total_rows as u64);

            let now = self.tick_manager.tick();
            for req in reqs {
                match req {
                    DiffRequest::Insert(insert) => {
                        let rows_proto: Vec<v1::Row> = insert
                            .into_iter()
                            .map(|(mut row, _ts)| {
                                // extend `update_at` col if needed
                                // if schema include a millisecond timestamp here, and result row doesn't have it, add it
                                if row.len() < proto_schema.len()
                                    && proto_schema[row.len()].datatype
                                        == greptime_proto::v1::ColumnDataType::TimestampMillisecond
                                            as i32
                                {
                                    row.extend([Value::from(
                                        common_time::Timestamp::new_millisecond(now),
                                    )]);
                                }
                                // ts col, if auto create
                                if is_ts_placeholder {
                                    ensure!(
                                        row.len() == schema_len - 1,
                                        InternalSnafu {
                                            reason: format!(
                                                "Row len mismatch, expect {} got {}",
                                                schema_len - 1,
                                                row.len()
                                            )
                                        }
                                    );
                                    row.extend([Value::from(
                                        common_time::Timestamp::new_millisecond(0),
                                    )]);
                                }
                                if row.len() != proto_schema.len() {
                                    UnexpectedSnafu {
                                        reason: format!(
                                            "Flow output row length mismatch, expect {} got {}, the columns in schema are: {:?}",
                                            proto_schema.len(),
                                            row.len(),
                                            proto_schema.iter().map(|c|&c.column_name).collect_vec()
                                        ),
                                    }
                                    .fail()?;
                                }
                                Ok(row.into())
                            })
                            .collect::<Result<Vec<_>, Error>>()?;
                        let table_name = table_name.last().unwrap().clone();
                        let req = RowInsertRequest {
                            table_name,
                            rows: Some(v1::Rows {
                                schema: proto_schema.clone(),
                                rows: rows_proto,
                            }),
                        };
                        req_cnt += 1;
                        self.frontend_invoker
                            .read()
                            .await
                            .as_ref()
                            .with_context(|| UnexpectedSnafu {
                                reason: "Expect a frontend invoker for flownode to write back",
                            })?
                            .row_inserts(RowInsertRequests { inserts: vec![req] }, ctx.clone())
                            .await
                            .map_err(BoxedError::new)
                            .with_context(|_| ExternalSnafu {})?;
                    }
                    DiffRequest::Delete(remove) => {
                        info!("original remove rows={:?}", remove);
                        let rows_proto: Vec<v1::Row> = remove
                            .into_iter()
                            .map(|(mut row, _ts)| {
                                row.extend(Some(Value::from(
                                    common_time::Timestamp::new_millisecond(0),
                                )));
                                row.into()
                            })
                            .collect::<Vec<_>>();
                        let table_name = table_name.last().unwrap().clone();
                        let req = RowDeleteRequest {
                            table_name,
                            rows: Some(v1::Rows {
                                schema: proto_schema.clone(),
                                rows: rows_proto,
                            }),
                        };

                        req_cnt += 1;
                        self.frontend_invoker
                            .read()
                            .await
                            .as_ref()
                            .with_context(|| UnexpectedSnafu {
                                reason: "Expect a frontend invoker for flownode to write back",
                            })?
                            .row_deletes(RowDeleteRequests { deletes: vec![req] }, ctx.clone())
                            .await
                            .map_err(BoxedError::new)
                            .with_context(|_| ExternalSnafu {})?;
                    }
                }
            }
        }
        Ok(req_cnt)
    }

    /// Generate writeback request for all sink table
    pub async fn generate_writeback_request(
        &self,
    ) -> Result<BTreeMap<TableName, Vec<DiffRequest>>, Error> {
        trace!("Start to generate writeback request");
        let mut output = BTreeMap::new();
        let mut total_row_count = 0;
        for (name, sink_recv) in self
            .node_context
            .write()
            .await
            .sink_receiver
            .iter_mut()
            .map(|(n, (_s, r))| (n, r))
        {
            let mut batches = Vec::new();
            while let Ok(batch) = sink_recv.try_recv() {
                total_row_count += batch.row_count();
                batches.push(batch);
            }
            let reqs = batches_to_rows_req(batches)?;
            output.insert(name.clone(), reqs);
        }
        trace!("Prepare writeback req: total row count={}", total_row_count);
        Ok(output)
    }

    /// Fetch table schema and primary key from table info source, if table not exist return None
    async fn fetch_table_pk_schema(
        &self,
        table_name: &TableName,
    ) -> Result<Option<(Vec<String>, Option<usize>, Vec<ColumnSchema>)>, Error> {
        if let Some(table_id) = self
            .table_info_source
            .get_opt_table_id_from_name(table_name)
            .await?
        {
            let table_info = self
                .table_info_source
                .get_table_info_value(&table_id)
                .await?
                .unwrap();
            let meta = table_info.table_info.meta;
            let primary_keys = meta
                .primary_key_indices
                .into_iter()
                .map(|i| meta.schema.column_schemas[i].name.clone())
                .collect_vec();
            let schema = meta.schema.column_schemas;
            let time_index = meta.schema.timestamp_index;
            Ok(Some((primary_keys, time_index, schema)))
        } else {
            Ok(None)
        }
    }

    /// return (primary keys, schema and if the table have a placeholder timestamp column)
    /// schema of the table comes from flow's output plan
    ///
    /// adjust to add `update_at` column and ts placeholder if needed
    async fn adjust_auto_created_table_schema(
        &self,
        schema: &RelationDesc,
    ) -> Result<(Vec<String>, Vec<ColumnSchema>, bool), Error> {
        // TODO(discord9): condiser remove buggy auto create by schema

        // TODO(discord9): use default key from schema
        let primary_keys = schema
            .typ()
            .keys
            .first()
            .map(|v| {
                v.column_indices
                    .iter()
                    .map(|i| {
                        schema
                            .get_name(*i)
                            .clone()
                            .unwrap_or_else(|| format!("col_{i}"))
                    })
                    .collect_vec()
            })
            .unwrap_or_default();
        let update_at = ColumnSchema::new(
            AUTO_CREATED_UPDATE_AT_TS_COL,
            ConcreteDataType::timestamp_millisecond_datatype(),
            true,
        );

        let original_schema = relation_desc_to_column_schemas_with_fallback(schema);

        let mut with_auto_added_col = original_schema.clone();
        with_auto_added_col.push(update_at);

        // if no time index, add one as placeholder
        let no_time_index = schema.typ().time_index.is_none();
        if no_time_index {
            let ts_col = ColumnSchema::new(
                AUTO_CREATED_PLACEHOLDER_TS_COL,
                ConcreteDataType::timestamp_millisecond_datatype(),
                true,
            )
            .with_time_index(true);
            with_auto_added_col.push(ts_col);
        }

        Ok((primary_keys, with_auto_added_col, no_time_index))
    }
}

/// Flow Runtime related methods
impl StreamingEngine {
    /// Start state report handler, which will receive a sender from HeartbeatTask to send state size report back
    ///
    /// if heartbeat task is shutdown, this future will exit too
    async fn start_state_report_handler(self: Arc<Self>) -> Option<JoinHandle<()>> {
        let state_report_handler = self.state_report_handler.write().await.take();
        if let Some(mut handler) = state_report_handler {
            let zelf = self.clone();
            let handler = common_runtime::spawn_global(async move {
                while let Some(ret_handler) = handler.recv().await {
                    let state_report = zelf.gen_state_report().await;
                    ret_handler.send(state_report).unwrap_or_else(|err| {
                        common_telemetry::error!(err; "Send state size report error");
                    });
                }
            });
            Some(handler)
        } else {
            None
        }
    }

    /// run in common_runtime background runtime
    pub fn run_background(
        self: Arc<Self>,
        shutdown: Option<broadcast::Receiver<()>>,
    ) -> JoinHandle<()> {
        info!("Starting flownode manager's background task");
        common_runtime::spawn_global(async move {
            let _state_report_handler = self.clone().start_state_report_handler().await;
            self.run(shutdown).await;
        })
    }

    /// log all flow errors
    pub async fn log_all_errors(&self) {
        for (f_id, f_err) in self.flow_err_collectors.read().await.iter() {
            let all_errors = f_err.get_all().await;
            if !all_errors.is_empty() {
                let all_errors = all_errors
                    .into_iter()
                    .map(|i| format!("{:?}", i))
                    .join("\n");
                common_telemetry::error!("Flow {} has following errors: {}", f_id, all_errors);
            }
        }
    }

    /// Trigger dataflow running, and then send writeback request to the source sender
    ///
    /// note that this method didn't handle input mirror request, as this should be handled by grpc server
    pub async fn run(&self, mut shutdown: Option<broadcast::Receiver<()>>) {
        debug!("Starting to run");
        let default_interval = Duration::from_secs(1);
        let mut tick_interval = tokio::time::interval(default_interval);
        // burst mode, so that if we miss a tick, we will run immediately to fully utilize the cpu
        tick_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
        let mut avg_spd = 0; // rows/sec
        let mut since_last_run = tokio::time::Instant::now();
        let run_per_trace = 10;
        let mut run_cnt = 0;
        loop {
            // TODO(discord9): only run when new inputs arrive or scheduled to
            let row_cnt = self.run_available(false).await.unwrap_or_else(|err| {
                common_telemetry::error!(err;"Run available errors");
                0
            });

            if let Err(err) = self.send_writeback_requests().await {
                common_telemetry::error!(err;"Send writeback request errors");
            };
            self.log_all_errors().await;

            // determine if need to shutdown
            match &shutdown.as_mut().map(|s| s.try_recv()) {
                Some(Ok(())) => {
                    info!("Shutdown flow's main loop");
                    break;
                }
                Some(Err(TryRecvError::Empty)) => (),
                Some(Err(TryRecvError::Closed)) => {
                    common_telemetry::error!("Shutdown channel is closed");
                    break;
                }
                Some(Err(TryRecvError::Lagged(num))) => {
                    common_telemetry::error!("Shutdown channel is lagged by {}, meaning multiple shutdown cmd have been issued", num);
                    break;
                }
                None => (),
            }

            // for now we want to batch rows until there is around `BATCH_SIZE` rows in send buf
            // before trigger a run of flow's worker
            let wait_for = since_last_run.elapsed();

            // last runs insert speed
            let cur_spd = row_cnt * 1000 / wait_for.as_millis().max(1) as usize;
            // rapid increase, slow decay
            avg_spd = if cur_spd > avg_spd {
                cur_spd
            } else {
                (9 * avg_spd + cur_spd) / 10
            };
            let new_wait = BATCH_SIZE * 1000 / avg_spd.max(1); //in ms
            let new_wait = Duration::from_millis(new_wait as u64).min(default_interval);

            // print trace every `run_per_trace` times so that we can see if there is something wrong
            // but also not get flooded with trace
            if run_cnt >= run_per_trace {
                trace!("avg_spd={} r/s, cur_spd={} r/s", avg_spd, cur_spd);
                trace!("Wait for {} ms, row_cnt={}", new_wait.as_millis(), row_cnt);
                run_cnt = 0;
            } else {
                run_cnt += 1;
            }

            METRIC_FLOW_RUN_INTERVAL_MS.set(new_wait.as_millis() as i64);
            since_last_run = tokio::time::Instant::now();
            tokio::select! {
                _ = tick_interval.tick() => (),
                _ = tokio::time::sleep(new_wait) => ()
            }
        }
        // flow is now shutdown, drop frontend_invoker early so a ref cycle(in standalone mode) can be prevent:
        // FlowWorkerManager.frontend_invoker -> FrontendInvoker.inserter
        // -> Inserter.node_manager -> NodeManager.flownode -> Flownode.flow_streaming_engine.frontend_invoker
        self.frontend_invoker.write().await.take();
    }

    /// Run all available subgraph in the flow node
    /// This will try to run all dataflow in this node
    ///
    /// set `blocking` to true to wait until worker finish running
    /// false to just trigger run and return immediately
    /// return numbers of rows send to worker(Inaccuary)
    /// TODO(discord9): add flag for subgraph that have input since last run
    pub async fn run_available(&self, blocking: bool) -> Result<usize, Error> {
        let mut row_cnt = 0;

        let now = self.tick_manager.tick();
        for worker in self.worker_handles.iter() {
            // TODO(discord9): consider how to handle error in individual worker
            worker.run_available(now, blocking).await?;
        }
        // check row send and rows remain in send buf
        let flush_res = if blocking {
            let ctx = self.node_context.read().await;
            ctx.flush_all_sender().await
        } else {
            match self.node_context.try_read() {
                Ok(ctx) => ctx.flush_all_sender().await,
                Err(_) => return Ok(row_cnt),
            }
        };
        match flush_res {
            Ok(r) => {
                common_telemetry::trace!("Total flushed {} rows", r);
                row_cnt += r;
            }
            Err(err) => {
                common_telemetry::error!("Flush send buf errors: {:?}", err);
            }
        };

        Ok(row_cnt)
    }

    /// send write request to related source sender
    pub async fn handle_write_request(
        &self,
        region_id: RegionId,
        rows: Vec<DiffRow>,
        batch_datatypes: &[ConcreteDataType],
    ) -> Result<(), Error> {
        let rows_len = rows.len();
        let table_id = region_id.table_id();
        let _timer = METRIC_FLOW_INSERT_ELAPSED
            .with_label_values(&[table_id.to_string().as_str()])
            .start_timer();
        self.node_context
            .read()
            .await
            .send(table_id, rows, batch_datatypes)
            .await?;
        trace!(
            "Handling write request for table_id={} with {} rows",
            table_id,
            rows_len
        );
        Ok(())
    }
}

/// Create&Remove flow
impl StreamingEngine {
    /// remove a flow by it's id
    pub async fn remove_flow_inner(&self, flow_id: FlowId) -> Result<(), Error> {
        for handle in self.worker_handles.iter() {
            if handle.contains_flow(flow_id).await? {
                handle.remove_flow(flow_id).await?;
                break;
            }
        }
        self.node_context.write().await.remove_flow(flow_id);
        Ok(())
    }

    /// Return task id if a new task is created, otherwise return None
    ///
    /// steps to create task:
    /// 1. parse query into typed plan(and optional parse expire_after expr)
    /// 2. render source/sink with output table id and used input table id
    pub async fn create_flow_inner(&self, args: CreateFlowArgs) -> Result<Option<FlowId>, Error> {
        let CreateFlowArgs {
            flow_id,
            sink_table_name,
            source_table_ids,
            create_if_not_exists,
            or_replace,
            expire_after,
            comment,
            sql,
            flow_options,
            query_ctx,
        } = args;

        let mut node_ctx = self.node_context.write().await;
        // assign global id to source and sink table
        for source in &source_table_ids {
            node_ctx
                .assign_global_id_to_table(&self.table_info_source, None, Some(*source))
                .await?;
        }
        node_ctx
            .assign_global_id_to_table(&self.table_info_source, Some(sink_table_name.clone()), None)
            .await?;

        node_ctx.register_task_src_sink(flow_id, &source_table_ids, sink_table_name.clone());

        node_ctx.query_context = query_ctx.map(Arc::new);
        // construct a active dataflow state with it
        let flow_plan = sql_to_flow_plan(&mut node_ctx, &self.query_engine, &sql).await?;

        debug!("Flow {:?}'s Plan is {:?}", flow_id, flow_plan);

        // check schema against actual table schema if exists
        // if not exist create sink table immediately
        if let Some((_, _, real_schema)) = self.fetch_table_pk_schema(&sink_table_name).await? {
            let auto_schema = relation_desc_to_column_schemas_with_fallback(&flow_plan.schema);

            // for column schema, only `data_type` need to be check for equality
            // since one can omit flow's column name when write flow query
            // print a user friendly error message about mismatch and how to correct them
            for (idx, zipped) in auto_schema
                .iter()
                .zip_longest(real_schema.iter())
                .enumerate()
            {
                match zipped {
                    EitherOrBoth::Both(auto, real) => {
                        if auto.data_type != real.data_type {
                            InvalidQuerySnafu {
                                    reason: format!(
                                        "Column {}(name is '{}', flow inferred name is '{}')'s data type mismatch, expect {:?} got {:?}",
                                        idx,
                                        real.name,
                                        auto.name,
                                        real.data_type,
                                        auto.data_type
                                    ),
                                }
                                .fail()?;
                        }
                    }
                    EitherOrBoth::Right(real) if real.data_type.is_timestamp() => {
                        // if table is auto created, the last one or two column should be timestamp(update at and ts placeholder)
                        continue;
                    }
                    _ => InvalidQuerySnafu {
                        reason: format!(
                            "schema length mismatched, expected {} found {}",
                            real_schema.len(),
                            auto_schema.len()
                        ),
                    }
                    .fail()?,
                }
            }
        } else {
            // assign inferred schema to sink table
            // create sink table
            let did_create = self
                .create_table_from_relation(
                    &format!("flow-id={flow_id}"),
                    &sink_table_name,
                    &flow_plan.schema,
                )
                .await?;
            if !did_create {
                UnexpectedSnafu {
                    reason: format!("Failed to create table {:?}", sink_table_name),
                }
                .fail()?;
            }
        }

        node_ctx.add_flow_plan(flow_id, flow_plan.clone());

        let _ = comment;
        let _ = flow_options;

        // TODO(discord9): add more than one handles
        let sink_id = node_ctx.table_repr.get_by_name(&sink_table_name).unwrap().1;
        let sink_sender = node_ctx.get_sink_by_global_id(&sink_id)?;

        let source_ids = source_table_ids
            .iter()
            .map(|id| node_ctx.table_repr.get_by_table_id(id).unwrap().1)
            .collect_vec();
        let source_receivers = source_ids
            .iter()
            .map(|id| {
                node_ctx
                    .get_source_by_global_id(id)
                    .map(|s| s.get_receiver())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let err_collector = ErrCollector::default();
        self.flow_err_collectors
            .write()
            .await
            .insert(flow_id, err_collector.clone());
        // TODO(discord9): load balance?
        let handle = self.get_worker_handle_for_create_flow().await;
        let create_request = worker::Request::Create {
            flow_id,
            plan: flow_plan,
            sink_id,
            sink_sender,
            source_ids,
            src_recvs: source_receivers,
            expire_after,
            or_replace,
            create_if_not_exists,
            err_collector,
        };

        handle.create_flow(create_request).await?;
        info!("Successfully create flow with id={}", flow_id);
        Ok(Some(flow_id))
    }

    pub async fn flush_flow_inner(&self, flow_id: FlowId) -> Result<usize, Error> {
        debug!("Starting to flush flow_id={:?}", flow_id);
        // lock to make sure writes before flush are written to flow
        // and immediately drop to prevent following writes to be blocked
        drop(self.flush_lock.write().await);
        let flushed_input_rows = self.node_context.read().await.flush_all_sender().await?;
        let rows_send = self.run_available(true).await?;
        let row = self.send_writeback_requests().await?;
        debug!(
            "Done to flush flow_id={:?} with {} input rows flushed, {} rows sended and {} output rows flushed",
            flow_id, flushed_input_rows, rows_send, row
        );
        Ok(row)
    }

    pub async fn flow_exist_inner(&self, flow_id: FlowId) -> Result<bool, Error> {
        let mut exist = false;
        for handle in self.worker_handles.iter() {
            if handle.contains_flow(flow_id).await? {
                exist = true;
                break;
            }
        }
        Ok(exist)
    }
}

/// FlowTickManager is a manager for flow tick, which trakc flow execution progress
///
/// TODO(discord9): better way to do it, and not expose flow tick even to other flow to avoid
/// TSO coord mess
#[derive(Clone, Debug)]
pub struct FlowTickManager {
    /// The starting instant of the flow, used with `start_timestamp` to calculate the current timestamp
    start: Instant,
    /// The timestamp when the flow started
    start_timestamp: repr::Timestamp,
}

impl FlowTickManager {
    pub fn new() -> Self {
        FlowTickManager {
            start: Instant::now(),
            start_timestamp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as repr::Timestamp,
        }
    }

    /// Return the current timestamp in milliseconds
    ///
    /// TODO(discord9): reconsider since `tick()` require a monotonic clock and also need to survive recover later
    pub fn tick(&self) -> repr::Timestamp {
        let current = Instant::now();
        let since_the_epoch = current - self.start;
        since_the_epoch.as_millis() as repr::Timestamp + self.start_timestamp
    }
}
