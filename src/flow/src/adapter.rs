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
use std::time::{Instant, SystemTime};

use api::v1::{RowDeleteRequest, RowDeleteRequests, RowInsertRequest, RowInsertRequests};
use catalog::CatalogManagerRef;
use common_base::Plugins;
use common_error::ext::BoxedError;
use common_frontend::handler::FrontendInvoker;
use common_meta::key::TableMetadataManagerRef;
use common_runtime::JoinHandle;
use common_telemetry::{debug, info};
use datatypes::schema::ColumnSchema;
use datatypes::value::Value;
use greptime_proto::v1;
use itertools::Itertools;
use query::{QueryEngine, QueryEngineFactory};
use serde::{Deserialize, Serialize};
use session::context::QueryContext;
use snafu::{OptionExt, ResultExt};
use store_api::storage::{ConcreteDataType, RegionId};
use table::metadata::TableId;
use tokio::sync::{oneshot, watch, Mutex, RwLock};

use crate::adapter::error::{ExternalSnafu, TableNotFoundSnafu, UnexpectedSnafu};
pub(crate) use crate::adapter::node_context::FlownodeContext;
use crate::adapter::parse_expr::parse_fixed;
use crate::adapter::table_source::TableSource;
use crate::adapter::util::column_schemas_to_proto;
use crate::adapter::worker::{create_worker, Worker, WorkerHandle};
use crate::compute::ErrCollector;
use crate::expr::GlobalId;
use crate::repr::{self, DiffRow, Row};
use crate::transform::{register_function_to_query_engine, sql_to_flow_plan};

pub(crate) mod error;
mod flownode_impl;
mod parse_expr;
mod server;
#[cfg(test)]
mod tests;
mod util;
mod worker;

pub(crate) mod node_context;
mod table_source;

use error::Error;

pub const PER_REQ_MAX_ROW_CNT: usize = 8192;

// TODO: refactor common types for flow to a separate module
/// FlowId is a unique identifier for a flow task
pub type FlowId = u64;
pub type TableName = [String; 3];

/// Options for flow node
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct FlownodeOptions {
    /// rpc address
    pub rpc_addr: String,
}

/// Flownode Builder
pub struct FlownodeBuilder {
    flow_node_id: u32,
    opts: FlownodeOptions,
    plugins: Plugins,
    table_meta: TableMetadataManagerRef,
    catalog_manager: CatalogManagerRef,
}

impl FlownodeBuilder {
    /// init flownode builder
    pub fn new(
        flow_node_id: u32,
        opts: FlownodeOptions,
        plugins: Plugins,
        table_meta: TableMetadataManagerRef,
        catalog_manager: CatalogManagerRef,
    ) -> Self {
        Self {
            flow_node_id,
            opts,
            plugins,
            table_meta,
            catalog_manager,
        }
    }

    /// TODO(discord9): error handling
    pub async fn build(self) -> FlownodeManager {
        let query_engine_factory = QueryEngineFactory::new_with_plugins(
            // query engine in flownode only translate plan with resolved table source.
            self.catalog_manager.clone(),
            None,
            None,
            None,
            false,
            self.plugins.clone(),
        );
        let query_engine = query_engine_factory.query_engine();

        register_function_to_query_engine(&query_engine);

        let (tx, rx) = oneshot::channel();

        let node_id = Some(self.flow_node_id);

        let _handle = std::thread::spawn(move || {
            let (flow_node_manager, mut worker) =
                FlownodeManager::new_with_worker(node_id, query_engine, self.table_meta.clone());
            let _ = tx.send(flow_node_manager);
            info!("Flow Worker started in new thread");
            worker.run();
        });
        let man = rx.await.unwrap();
        info!("Flow Node Manager started");
        man
    }
}

/// Arc-ed FlowNodeManager, cheaper to clone
pub type FlownodeManagerRef = Arc<FlownodeManager>;

/// FlowNodeManager manages the state of all tasks in the flow node, which should be run on the same thread
///
/// The choice of timestamp is just using current system timestamp for now
pub struct FlownodeManager {
    /// The handler to the worker that will run the dataflow
    /// which is `!Send` so a handle is used
    pub worker_handles: Vec<Mutex<WorkerHandle>>,
    /// The query engine that will be used to parse the query and convert it to a dataflow plan
    query_engine: Arc<dyn QueryEngine>,
    /// Getting table name and table schema from table info manager
    table_info_source: TableSource,
    frontend_invoker: RwLock<Option<Box<dyn FrontendInvoker + Send + Sync>>>,
    /// contains mapping from table name to global id, and table schema
    node_context: Mutex<FlownodeContext>,
    flow_err_collectors: RwLock<BTreeMap<FlowId, ErrCollector>>,
    src_send_buf_lens: RwLock<BTreeMap<TableId, watch::Receiver<usize>>>,
    tick_manager: FlowTickManager,
    node_id: Option<u32>,
}

/// Building FlownodeManager
impl FlownodeManager {
    /// set frontend invoker
    pub async fn set_frontend_invoker(
        self: &Arc<Self>,
        frontend: Box<dyn FrontendInvoker + Send + Sync>,
    ) {
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
        FlownodeManager {
            worker_handles,
            query_engine,
            table_info_source: srv_map,
            frontend_invoker: RwLock::new(None),
            node_context: Mutex::new(node_context),
            flow_err_collectors: Default::default(),
            src_send_buf_lens: Default::default(),
            tick_manager,
            node_id,
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
impl FlownodeManager {
    /// TODO(discord9): merge all same type of diff row into one requests
    ///
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
            let (primary_keys, schema, is_auto_create) = if let Some(table_id) = self
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
                let is_auto_create = schema
                    .last()
                    .map(|s| s.name == "__ts_placeholder")
                    .unwrap_or(false);
                (primary_keys, schema, is_auto_create)
            } else {
                // TODO(discord9): condiser remove buggy auto create by schema

                let node_ctx = self.node_context.lock().await;
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
                    .keys
                    .first()
                    .map(|v| {
                        v.column_indices
                            .iter()
                            .map(|i| format!("Col_{i}"))
                            .collect_vec()
                    })
                    .unwrap_or_default();
                let update_at = ColumnSchema::new(
                    "update_at",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    true,
                );
                // TODO(discord9): bugged so we can't infer time index from flow plan, so we have to manually set one
                let ts_col = ColumnSchema::new(
                    "__ts_placeholder",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    true,
                )
                .with_time_index(true);

                let wout_ts = schema
                    .column_types
                    .into_iter()
                    .enumerate()
                    .map(|(idx, typ)| {
                        ColumnSchema::new(format!("Col_{idx}"), typ.scalar_type, typ.nullable)
                    })
                    .collect_vec();

                let mut with_ts = wout_ts.clone();
                with_ts.push(update_at);
                with_ts.push(ts_col);

                (primary_keys, with_ts, true)
            };

            let proto_schema = column_schemas_to_proto(schema, &primary_keys)?;

            debug!(
                "Sending {} writeback requests to table {}, reqs={:?}",
                reqs.len(),
                table_name.join("."),
                reqs
            );
            let now = SystemTime::now();
            let now = now
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|s| s.as_millis() as repr::Timestamp)
                .unwrap_or_else(|_| {
                    -(SystemTime::UNIX_EPOCH
                        .duration_since(now)
                        .unwrap()
                        .as_millis() as repr::Timestamp)
                });
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
                                if is_auto_create {
                                    row.extend([Value::from(
                                        common_time::Timestamp::new_millisecond(0),
                                    )]);
                                }
                                row.into()
                            })
                            .collect::<Vec<_>>();
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
            .lock()
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
impl FlownodeManager {
    /// run in common_runtime background runtime
    pub fn run_background(self: Arc<Self>) -> JoinHandle<()> {
        info!("Starting flownode manager's background task");
        common_runtime::spawn_bg(async move {
            self.run().await;
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
    pub async fn run(&self) {
        debug!("Starting to run");
        loop {
            // TODO(discord9): only run when new inputs arrive or scheduled to
            self.run_available().await.unwrap();
            // TODO(discord9): error handling
            self.send_writeback_requests().await.unwrap();
            self.log_all_errors().await;
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }

    /// Run all available subgraph in the flow node
    /// This will try to run all dataflow in this node
    ///
    /// However this is not blocking and can sometimes return while actual computation is still running in worker thread
    /// TODO(discord9): add flag for subgraph that have input since last run
    pub async fn run_available(&self) -> Result<(), Error> {
        let now = self.tick_manager.tick();

        loop {
            for worker in self.worker_handles.iter() {
                // TODO(discord9): consider how to handle error in individual worker
                worker.lock().await.run_available(now).await.unwrap();
            }
            // first check how many inputs were sent
            let send_cnt = match self.node_context.lock().await.flush_all_sender() {
                Ok(cnt) => cnt,
                Err(err) => {
                    common_telemetry::error!("Flush send buf errors: {:?}", err);
                    break;
                }
            };
            // if no inputs
            if send_cnt == 0 {
                break;
            } else {
                debug!("FlownodeManager::run_available: send_cnt={}", send_cnt);
            }
        }

        Ok(())
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
        self.node_context.lock().await.send(table_id, rows)?;
        Ok(())
    }
}

/// Create&Remove flow
impl FlownodeManager {
    /// remove a flow by it's id
    pub async fn remove_flow(&self, flow_id: FlowId) -> Result<(), Error> {
        for handle in self.worker_handles.iter() {
            let handle = handle.lock().await;
            if handle.contains_flow(flow_id).await? {
                handle.remove_flow(flow_id).await?;
                break;
            }
        }
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
        expire_after: Option<String>,
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

        let mut node_ctx = self.node_context.lock().await;
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
        node_ctx.assign_table_schema(&sink_table_name, flow_plan.typ.clone())?;

        let expire_after = expire_after
            .and_then(|s| {
                if s.is_empty() || s.split_whitespace().join("").is_empty() {
                    None
                } else {
                    Some(s)
                }
            })
            .map(|d| {
                let d = d.as_ref();
                parse_fixed(d)
                    .map(|(_, n)| n)
                    .map_err(|err| err.to_string())
            })
            .transpose()
            .map_err(|err| UnexpectedSnafu { reason: err }.build())?;
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
#[derive(Clone)]
pub struct FlowTickManager {
    start: Instant,
}

impl std::fmt::Debug for FlowTickManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlowTickManager").finish()
    }
}

impl FlowTickManager {
    pub fn new() -> Self {
        FlowTickManager {
            start: Instant::now(),
        }
    }

    /// Return the current timestamp in milliseconds
    ///
    /// TODO(discord9): reconsider since `tick()` require a monotonic clock and also need to survive recover later
    pub fn tick(&self) -> repr::Timestamp {
        let current = Instant::now();
        let since_the_epoch = current - self.start;
        since_the_epoch.as_millis() as repr::Timestamp
    }
}
