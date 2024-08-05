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

use std::collections::{BTreeMap, HashMap};
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
use itertools::Itertools;
use meta_client::MetaClientOptions;
use query::QueryEngine;
use serde::{Deserialize, Serialize};
use servers::grpc::GrpcOptions;
use servers::heartbeat_options::HeartbeatOptions;
use servers::Mode;
use session::context::QueryContext;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::storage::{ConcreteDataType, RegionId};
use table::metadata::TableId;
use tokio::sync::broadcast::error::TryRecvError;
use tokio::sync::{broadcast, watch, Mutex, RwLock};

pub(crate) use crate::adapter::node_context::FlownodeContext;
use crate::adapter::table_source::TableSource;
use crate::adapter::util::column_schemas_to_proto;
use crate::adapter::worker::{create_worker, Worker, WorkerHandle};
use crate::compute::ErrCollector;
use crate::error::{ExternalSnafu, InternalSnafu, TableNotFoundSnafu, UnexpectedSnafu};
use crate::expr::GlobalId;
use crate::repr::{self, DiffRow, Row, BATCH_SIZE};
use crate::transform::sql_to_flow_plan;

mod flownode_impl;
mod parse_expr;
#[cfg(test)]
mod tests;
mod util;
mod worker;

pub(crate) mod node_context;
mod table_source;

use crate::error::Error;
use crate::FrontendInvoker;

// `GREPTIME_TIMESTAMP` is not used to distinguish when table is created automatically by flow
pub const AUTO_CREATED_PLACEHOLDER_TS_COL: &str = "__ts_placeholder";

pub const UPDATE_AT_TS_COL: &str = "update_at";

// TODO(discord9): refactor common types for flow to a separate module
/// FlowId is a unique identifier for a flow task
pub type FlowId = u64;
pub type TableName = [String; 3];

/// Options for flow node
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct FlownodeOptions {
    pub mode: Mode,
    pub cluster_id: Option<u64>,
    pub node_id: Option<u64>,
    pub grpc: GrpcOptions,
    pub meta_client: Option<MetaClientOptions>,
    pub logging: LoggingOptions,
    pub tracing: TracingOptions,
    pub heartbeat: HeartbeatOptions,
}

impl Default for FlownodeOptions {
    fn default() -> Self {
        Self {
            mode: servers::Mode::Standalone,
            cluster_id: None,
            node_id: None,
            grpc: GrpcOptions::default().with_addr("127.0.0.1:3004"),
            meta_client: None,
            logging: LoggingOptions::default(),
            tracing: TracingOptions::default(),
            heartbeat: HeartbeatOptions::default(),
        }
    }
}

impl Configurable for FlownodeOptions {}

/// Arc-ed FlowNodeManager, cheaper to clone
pub type FlowWorkerManagerRef = Arc<FlowWorkerManager>;

/// FlowNodeManager manages the state of all tasks in the flow node, which should be run on the same thread
///
/// The choice of timestamp is just using current system timestamp for now
pub struct FlowWorkerManager {
    /// The handler to the worker that will run the dataflow
    /// which is `!Send` so a handle is used
    pub worker_handles: Vec<Mutex<WorkerHandle>>,
    /// The query engine that will be used to parse the query and convert it to a dataflow plan
    pub query_engine: Arc<dyn QueryEngine>,
    /// Getting table name and table schema from table info manager
    table_info_source: TableSource,
    frontend_invoker: RwLock<Option<FrontendInvoker>>,
    /// contains mapping from table name to global id, and table schema
    node_context: RwLock<FlownodeContext>,
    flow_err_collectors: RwLock<BTreeMap<FlowId, ErrCollector>>,
    src_send_buf_lens: RwLock<BTreeMap<TableId, watch::Receiver<usize>>>,
    tick_manager: FlowTickManager,
    node_id: Option<u32>,
    /// Lock for flushing, will be `read` by `handle_inserts` and `write` by `flush_flow`
    ///
    /// So that a series of event like `inserts -> flush` can be handled correctly
    flush_lock: RwLock<()>,
}

/// Building FlownodeManager
impl FlowWorkerManager {
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
        let srv_map = TableSource::new(
            table_meta.table_info_manager().clone(),
            table_meta.table_name_manager().clone(),
        );
        let node_context = FlownodeContext::default();
        let tick_manager = FlowTickManager::new();
        let worker_handles = Vec::new();
        FlowWorkerManager {
            worker_handles,
            query_engine,
            table_info_source: srv_map,
            frontend_invoker: RwLock::new(None),
            node_context: RwLock::new(node_context),
            flow_err_collectors: Default::default(),
            src_send_buf_lens: Default::default(),
            tick_manager,
            node_id,
            flush_lock: RwLock::new(()),
        }
    }

    /// Create a flownode manager with one worker
    pub fn new_with_worker<'s>(
        node_id: Option<u32>,
        query_engine: Arc<dyn QueryEngine>,
        table_meta: TableMetadataManagerRef,
    ) -> (Self, Worker<'s>) {
        let mut zelf = Self::new(node_id, query_engine, table_meta);
        let (handle, worker) = create_worker();
        zelf.add_worker_handle(handle);
        (zelf, worker)
    }

    /// add a worker handler to manager, meaning this corresponding worker is under it's manage
    pub fn add_worker_handle(&mut self, handle: WorkerHandle) {
        self.worker_handles.push(Mutex::new(handle));
    }
}

#[derive(Debug)]
pub enum DiffRequest {
    Insert(Vec<(Row, repr::Timestamp)>),
    Delete(Vec<(Row, repr::Timestamp)>),
}

/// iterate through the diff row and form continuous diff row with same diff type
pub fn diff_row_to_request(rows: Vec<DiffRow>) -> Vec<DiffRequest> {
    let mut reqs = Vec::new();
    for (row, ts, diff) in rows {
        let last = reqs.last_mut();
        match (last, diff) {
            (Some(DiffRequest::Insert(rows)), 1) => {
                rows.push((row, ts));
            }
            (Some(DiffRequest::Insert(_)), -1) => reqs.push(DiffRequest::Delete(vec![(row, ts)])),
            (Some(DiffRequest::Delete(rows)), -1) => {
                rows.push((row, ts));
            }
            (Some(DiffRequest::Delete(_)), 1) => reqs.push(DiffRequest::Insert(vec![(row, ts)])),
            (None, 1) => reqs.push(DiffRequest::Insert(vec![(row, ts)])),
            (None, -1) => reqs.push(DiffRequest::Delete(vec![(row, ts)])),
            _ => {}
        }
    }
    reqs
}

/// This impl block contains methods to send writeback requests to frontend
impl FlowWorkerManager {
    /// Return the number of requests it made
    pub async fn send_writeback_requests(&self) -> Result<usize, Error> {
        let all_reqs = self.generate_writeback_request().await;
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
            // TODO(discord9): instead of auto build table from request schema, actually build table
            // before `create flow` to be able to assign pk and ts etc.
            let (primary_keys, schema, is_ts_placeholder) = if let Some(table_id) = self
                .table_info_source
                .get_table_id_from_name(&table_name)
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
                // check if the last column is the auto created timestamp column, hence the table is auto created from
                // flow's plan type
                let is_auto_create = {
                    let correct_name = schema
                        .last()
                        .map(|s| s.name == AUTO_CREATED_PLACEHOLDER_TS_COL)
                        .unwrap_or(false);
                    let correct_time_index = meta.schema.timestamp_index == Some(schema.len() - 1);
                    correct_name && correct_time_index
                };
                (primary_keys, schema, is_auto_create)
            } else {
                // TODO(discord9): condiser remove buggy auto create by schema

                let node_ctx = self.node_context.read().await;
                let gid: GlobalId = node_ctx
                    .table_repr
                    .get_by_name(&table_name)
                    .map(|x| x.1)
                    .unwrap();
                let schema = node_ctx
                    .schema
                    .get(&gid)
                    .with_context(|| TableNotFoundSnafu {
                        name: format!("Table name = {:?}", table_name),
                    })?
                    .clone();
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
                    UPDATE_AT_TS_COL,
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    true,
                );

                let original_schema = schema
                    .typ()
                    .column_types
                    .clone()
                    .into_iter()
                    .enumerate()
                    .map(|(idx, typ)| {
                        let name = schema
                            .names
                            .get(idx)
                            .cloned()
                            .flatten()
                            .unwrap_or(format!("col_{}", idx));
                        let ret = ColumnSchema::new(name, typ.scalar_type, typ.nullable);
                        if schema.typ().time_index == Some(idx) {
                            ret.with_time_index(true)
                        } else {
                            ret
                        }
                    })
                    .collect_vec();

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

                (primary_keys, with_auto_added_col, no_time_index)
            };
            let schema_len = schema.len();
            let proto_schema = column_schemas_to_proto(schema, &primary_keys)?;

            debug!(
                "Sending {} writeback requests to table {}, reqs={:?}",
                reqs.len(),
                table_name.join("."),
                reqs
            );
            let now = self.tick_manager.tick();
            for req in reqs {
                match req {
                    DiffRequest::Insert(insert) => {
                        let rows_proto: Vec<v1::Row> = insert
                            .into_iter()
                            .map(|(mut row, _ts)| {
                                // `update_at` col
                                row.extend([Value::from(common_time::Timestamp::new_millisecond(
                                    now,
                                ))]);
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
    pub async fn generate_writeback_request(&self) -> BTreeMap<TableName, Vec<DiffRequest>> {
        let mut output = BTreeMap::new();
        for (name, sink_recv) in self
            .node_context
            .write()
            .await
            .sink_receiver
            .iter_mut()
            .map(|(n, (_s, r))| (n, r))
        {
            let mut rows = Vec::new();
            while let Ok(row) = sink_recv.try_recv() {
                rows.push(row);
            }
            let reqs = diff_row_to_request(rows);
            output.insert(name.clone(), reqs);
        }
        output
    }
}

/// Flow Runtime related methods
impl FlowWorkerManager {
    /// run in common_runtime background runtime
    pub fn run_background(
        self: Arc<Self>,
        shutdown: Option<broadcast::Receiver<()>>,
    ) -> JoinHandle<()> {
        info!("Starting flownode manager's background task");
        common_runtime::spawn_global(async move {
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

    async fn get_buf_size(&self) -> usize {
        self.node_context.read().await.get_send_buf_size().await
    }

    /// Trigger dataflow running, and then send writeback request to the source sender
    ///
    /// note that this method didn't handle input mirror request, as this should be handled by grpc server
    pub async fn run(&self, mut shutdown: Option<broadcast::Receiver<()>>) {
        debug!("Starting to run");
        let default_interval = Duration::from_secs(1);
        let mut avg_spd = 0; // rows/sec
        let mut since_last_run = tokio::time::Instant::now();
        loop {
            // TODO(discord9): only run when new inputs arrive or scheduled to
            let row_cnt = self.run_available(true).await.unwrap_or_else(|err| {
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
            // (plus one for prevent div by zero)
            let wait_for = since_last_run.elapsed();

            let cur_spd = row_cnt * 1000 / wait_for.as_millis().max(1) as usize;
            // rapid increase, slow decay
            avg_spd = if cur_spd > avg_spd {
                cur_spd
            } else {
                (9 * avg_spd + cur_spd) / 10
            };
            trace!("avg_spd={} r/s, cur_spd={} r/s", avg_spd, cur_spd);
            let new_wait = BATCH_SIZE * 1000 / avg_spd.max(1); //in ms
            let new_wait = Duration::from_millis(new_wait as u64).min(default_interval);
            trace!("Wait for {} ms, row_cnt={}", new_wait.as_millis(), row_cnt);
            since_last_run = tokio::time::Instant::now();
            tokio::time::sleep(new_wait).await;
        }
        // flow is now shutdown, drop frontend_invoker early so a ref cycle(in standalone mode) can be prevent:
        // FlowWorkerManager.frontend_invoker -> FrontendInvoker.inserter
        // -> Inserter.node_manager -> NodeManager.flownode -> Flownode.flow_worker_manager.frontend_invoker
        self.frontend_invoker.write().await.take();
    }

    /// Run all available subgraph in the flow node
    /// This will try to run all dataflow in this node
    ///
    /// set `blocking` to true to wait until lock is acquired
    /// and false to return immediately if lock is not acquired
    /// return numbers of rows send to worker
    /// TODO(discord9): add flag for subgraph that have input since last run
    pub async fn run_available(&self, blocking: bool) -> Result<usize, Error> {
        let mut row_cnt = 0;
        loop {
            let now = self.tick_manager.tick();
            for worker in self.worker_handles.iter() {
                // TODO(discord9): consider how to handle error in individual worker
                if blocking {
                    worker.lock().await.run_available(now, blocking).await?;
                } else if let Ok(worker) = worker.try_lock() {
                    worker.run_available(now, blocking).await?;
                } else {
                    return Ok(row_cnt);
                }
            }
            // check row send and rows remain in send buf
            let (flush_res, buf_len) = if blocking {
                let ctx = self.node_context.read().await;
                (ctx.flush_all_sender().await, ctx.get_send_buf_size().await)
            } else {
                match self.node_context.try_read() {
                    Ok(ctx) => (ctx.flush_all_sender().await, ctx.get_send_buf_size().await),
                    Err(_) => return Ok(row_cnt),
                }
            };
            match flush_res {
                Ok(r) => row_cnt += r,
                Err(err) => {
                    common_telemetry::error!("Flush send buf errors: {:?}", err);
                    break;
                }
            };
            // if not enough rows, break
            if buf_len < BATCH_SIZE {
                break;
            }
        }

        Ok(row_cnt)
    }

    /// send write request to related source sender
    pub async fn handle_write_request(
        &self,
        region_id: RegionId,
        rows: Vec<DiffRow>,
    ) -> Result<(), Error> {
        debug!(
            "Handling write request for region_id={:?} with {} rows",
            region_id,
            rows.len()
        );
        let table_id = region_id.table_id();
        self.node_context.read().await.send(table_id, rows).await?;
        Ok(())
    }
}

/// Create&Remove flow
impl FlowWorkerManager {
    /// remove a flow by it's id
    pub async fn remove_flow(&self, flow_id: FlowId) -> Result<(), Error> {
        for handle in self.worker_handles.iter() {
            let handle = handle.lock().await;
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
    #[allow(clippy::too_many_arguments)]
    pub async fn create_flow(
        &self,
        flow_id: FlowId,
        sink_table_name: TableName,
        source_table_ids: &[TableId],
        create_if_not_exists: bool,
        expire_after: Option<i64>,
        comment: Option<String>,
        sql: String,
        flow_options: HashMap<String, String>,
        query_ctx: Option<QueryContext>,
    ) -> Result<Option<FlowId>, Error> {
        if create_if_not_exists {
            // check if the task already exists
            for handle in self.worker_handles.iter() {
                if handle.lock().await.contains_flow(flow_id).await? {
                    return Ok(None);
                }
            }
        }

        let mut node_ctx = self.node_context.write().await;
        // assign global id to source and sink table
        for source in source_table_ids {
            node_ctx
                .assign_global_id_to_table(&self.table_info_source, None, Some(*source))
                .await?;
        }
        node_ctx
            .assign_global_id_to_table(&self.table_info_source, Some(sink_table_name.clone()), None)
            .await?;

        node_ctx.register_task_src_sink(flow_id, source_table_ids, sink_table_name.clone());

        node_ctx.query_context = query_ctx.map(Arc::new);
        // construct a active dataflow state with it
        let flow_plan = sql_to_flow_plan(&mut node_ctx, &self.query_engine, &sql).await?;

        debug!("Flow {:?}'s Plan is {:?}", flow_id, flow_plan);
        node_ctx.assign_table_schema(&sink_table_name, flow_plan.schema.clone())?;

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
        let handle = &self.worker_handles[0].lock().await;
        let create_request = worker::Request::Create {
            flow_id,
            plan: flow_plan,
            sink_id,
            sink_sender,
            source_ids,
            src_recvs: source_receivers,
            expire_after,
            create_if_not_exists,
            err_collector,
        };
        handle.create_flow(create_request).await?;
        info!("Successfully create flow with id={}", flow_id);
        Ok(Some(flow_id))
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
