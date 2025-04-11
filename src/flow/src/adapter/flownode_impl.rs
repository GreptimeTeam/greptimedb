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

//! impl `FlowNode` trait for FlowNodeManager so standalone can call them
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use api::v1::flow::{
    flow_request, CreateRequest, DropRequest, FlowRequest, FlowResponse, FlushFlow,
};
use api::v1::region::InsertRequests;
use common_error::ext::BoxedError;
use common_meta::ddl::create_flow::FlowType;
use common_meta::error::{Result as MetaResult, UnexpectedSnafu};
use common_runtime::JoinHandle;
use common_telemetry::{trace, warn};
use datatypes::value::Value;
use itertools::Itertools;
use snafu::{IntoError, OptionExt, ResultExt};
use store_api::storage::{RegionId, TableId};

use crate::adapter::{CreateFlowArgs, FlowWorkerManager};
use crate::batching_mode::engine::BatchingEngine;
use crate::engine::FlowEngine;
use crate::error::{CreateFlowSnafu, FlowNotFoundSnafu, InsertIntoFlowSnafu, InternalSnafu};
use crate::metrics::METRIC_FLOW_TASK_COUNT;
use crate::repr::{self, DiffRow};
use crate::{Error, FlowId};

/// Manage both streaming and batching mode engine
///
/// including create/drop/flush flow
/// and redirect insert requests to the appropriate engine
pub struct FlowDualEngine {
    streaming_engine: Arc<FlowWorkerManager>,
    batching_engine: Arc<BatchingEngine>,
    /// helper struct for faster query flow by table id or vice versa
    src_table2flow: std::sync::RwLock<SrcTableToFlow>,
}

struct SrcTableToFlow {
    /// mapping of table ids to flow ids for streaming mode
    stream: HashMap<TableId, HashSet<FlowId>>,
    /// mapping of table ids to flow ids for batching mode
    batch: HashMap<TableId, HashSet<FlowId>>,
    /// mapping of flow ids to (flow type, source table ids)
    flow_infos: HashMap<FlowId, (FlowType, Vec<TableId>)>,
}

impl SrcTableToFlow {
    fn in_stream(&self, table_id: TableId) -> bool {
        self.stream.contains_key(&table_id)
    }
    fn in_batch(&self, table_id: TableId) -> bool {
        self.batch.contains_key(&table_id)
    }
    fn add_flow(&mut self, flow_id: FlowId, flow_type: FlowType, src_table_ids: Vec<TableId>) {
        let mapping = match flow_type {
            FlowType::Streaming => &mut self.stream,
            FlowType::Batching => &mut self.batch,
        };

        for src_table in src_table_ids.clone() {
            mapping
                .entry(src_table)
                .and_modify(|flows| {
                    flows.insert(flow_id);
                })
                .or_insert_with(|| {
                    let mut set = HashSet::new();
                    set.insert(flow_id);
                    set
                });
        }
        self.flow_infos.insert(flow_id, (flow_type, src_table_ids));
    }

    fn remove_flow(&mut self, flow_id: FlowId) {
        let mapping = match self.get_flow_type(flow_id) {
            Some(FlowType::Streaming) => &mut self.stream,
            Some(FlowType::Batching) => &mut self.batch,
            None => return,
        };
        if let Some((_, src_table_ids)) = self.flow_infos.remove(&flow_id) {
            for src_table in src_table_ids {
                if let Some(flows) = mapping.get_mut(&src_table) {
                    flows.remove(&flow_id);
                }
            }
        }
    }

    fn get_flow_type(&self, flow_id: FlowId) -> Option<FlowType> {
        self.flow_infos
            .get(&flow_id)
            .map(|(flow_type, _)| flow_type)
            .cloned()
    }
}

impl FlowEngine for FlowDualEngine {
    async fn create_flow(&self, args: CreateFlowArgs) -> Result<Option<FlowId>, Error> {
        let flow_type = args
            .flow_options
            .get(FlowType::FLOW_TYPE_KEY)
            .map(|s| s.as_str());

        let flow_type = match flow_type {
            Some(FlowType::BATCHING) => FlowType::Batching,
            Some(FlowType::STREAMING) => FlowType::Streaming,
            None => FlowType::Batching,
            Some(flow_type) => {
                return InternalSnafu {
                    reason: format!("Invalid flow type: {}", flow_type),
                }
                .fail()
            }
        };

        let flow_id = args.flow_id;
        let src_table_ids = args.source_table_ids.clone();

        let res = match flow_type {
            FlowType::Batching => self.batching_engine.create_flow(args).await,
            FlowType::Streaming => self.streaming_engine.create_flow(args).await,
        }?;

        self.src_table2flow
            .write()
            .unwrap()
            .add_flow(flow_id, flow_type, src_table_ids);

        Ok(res)
    }

    async fn remove_flow(&self, flow_id: FlowId) -> Result<(), Error> {
        let flow_type = self.src_table2flow.read().unwrap().get_flow_type(flow_id);
        match flow_type {
            Some(FlowType::Batching) => self.batching_engine.remove_flow(flow_id).await,
            Some(FlowType::Streaming) => self.streaming_engine.remove_flow(flow_id).await,
            None => FlowNotFoundSnafu { id: flow_id }.fail(),
        }?;
        // remove mapping
        self.src_table2flow.write().unwrap().remove_flow(flow_id);
        Ok(())
    }

    async fn flush_flow(&self, flow_id: FlowId) -> Result<usize, Error> {
        let flow_type = self.src_table2flow.read().unwrap().get_flow_type(flow_id);
        match flow_type {
            Some(FlowType::Batching) => self.batching_engine.flush_flow(flow_id).await,
            Some(FlowType::Streaming) => self.streaming_engine.flush_flow(flow_id).await,
            None => FlowNotFoundSnafu { id: flow_id }.fail(),
        }
    }

    async fn flow_exist(&self, flow_id: FlowId) -> Result<bool, Error> {
        let flow_type = self.src_table2flow.read().unwrap().get_flow_type(flow_id);
        // not using `flow_type.is_some()` to make sure the flow is actually exist in the underlying engine
        match flow_type {
            Some(FlowType::Batching) => self.batching_engine.flow_exist(flow_id).await,
            Some(FlowType::Streaming) => self.streaming_engine.flow_exist(flow_id).await,
            None => Ok(false),
        }
    }

    async fn handle_flow_inserts(
        &self,
        request: api::v1::region::InsertRequests,
    ) -> Result<(), Error> {
        // TODO(discord9): make as little clone as possible
        let mut to_stream_engine = Vec::with_capacity(request.requests.len());
        let mut to_batch_engine = request.requests;

        {
            let src_table2flow = self.src_table2flow.read().unwrap();
            to_batch_engine.retain(|req| {
                let region_id = RegionId::from(req.region_id);
                let table_id = region_id.table_id();
                let is_in_stream = src_table2flow.in_stream(table_id);
                let is_in_batch = src_table2flow.in_batch(table_id);
                if is_in_stream {
                    to_stream_engine.push(req.clone());
                }
                if is_in_batch {
                    return true;
                }
                if !is_in_batch && !is_in_stream {
                    // TODO(discord9): also put to centralized logging for flow once it implemented
                    warn!("Table {} is not any flow's source table", table_id)
                }
                false
            });
            // drop(src_table2flow);
            // can't use drop due to https://github.com/rust-lang/rust/pull/128846
        }

        let stream_engine = self.streaming_engine.clone();
        let stream_handler: JoinHandle<Result<(), Error>> =
            common_runtime::spawn_global(async move {
                stream_engine
                    .handle_flow_inserts(api::v1::region::InsertRequests {
                        requests: to_stream_engine,
                    })
                    .await?;
                Ok(())
            });
        self.batching_engine
            .handle_flow_inserts(api::v1::region::InsertRequests {
                requests: to_batch_engine,
            })
            .await?;
        stream_handler.await.map_err(|e| {
            crate::error::UnexpectedSnafu {
                reason: format!("JoinError when handle inserts for flow stream engine: {e:?}"),
            }
            .build()
        })??;

        Ok(())
    }
}

#[async_trait::async_trait]
impl common_meta::node_manager::Flownode for FlowDualEngine {
    async fn handle(&self, request: FlowRequest) -> MetaResult<FlowResponse> {
        let query_ctx = request
            .header
            .and_then(|h| h.query_context)
            .map(|ctx| ctx.into());
        match request.body {
            Some(flow_request::Body::Create(CreateRequest {
                flow_id: Some(task_id),
                source_table_ids,
                sink_table_name: Some(sink_table_name),
                create_if_not_exists,
                expire_after,
                comment,
                sql,
                flow_options,
                or_replace,
            })) => {
                let source_table_ids = source_table_ids.into_iter().map(|id| id.id).collect_vec();
                let sink_table_name = [
                    sink_table_name.catalog_name,
                    sink_table_name.schema_name,
                    sink_table_name.table_name,
                ];
                let expire_after = expire_after.map(|e| e.value);
                let args = CreateFlowArgs {
                    flow_id: task_id.id as u64,
                    sink_table_name,
                    source_table_ids,
                    create_if_not_exists,
                    or_replace,
                    expire_after,
                    comment: Some(comment),
                    sql: sql.clone(),
                    flow_options,
                    query_ctx,
                };
                let ret = self
                    .create_flow(args)
                    .await
                    .map_err(BoxedError::new)
                    .with_context(|_| CreateFlowSnafu { sql: sql.clone() })
                    .map_err(to_meta_err(snafu::location!()))?;
                METRIC_FLOW_TASK_COUNT.inc();
                Ok(FlowResponse {
                    affected_flows: ret
                        .map(|id| greptime_proto::v1::FlowId { id: id as u32 })
                        .into_iter()
                        .collect_vec(),
                    ..Default::default()
                })
            }
            Some(flow_request::Body::Drop(DropRequest {
                flow_id: Some(flow_id),
            })) => {
                self.remove_flow(flow_id.id as u64)
                    .await
                    .map_err(to_meta_err(snafu::location!()))?;
                METRIC_FLOW_TASK_COUNT.dec();
                Ok(Default::default())
            }
            Some(flow_request::Body::Flush(FlushFlow {
                flow_id: Some(flow_id),
            })) => {
                let row = self
                    .flush_flow(flow_id.id as u64)
                    .await
                    .map_err(to_meta_err(snafu::location!()))?;
                Ok(FlowResponse {
                    affected_flows: vec![flow_id],
                    affected_rows: row as u64,
                    ..Default::default()
                })
            }
            None => UnexpectedSnafu {
                err_msg: "Missing request body",
            }
            .fail(),
            _ => UnexpectedSnafu {
                err_msg: "Invalid request body.",
            }
            .fail(),
        }
    }

    async fn handle_inserts(&self, request: InsertRequests) -> MetaResult<FlowResponse> {
        FlowEngine::handle_flow_inserts(self, request)
            .await
            .map(|_| Default::default())
            .map_err(to_meta_err(snafu::location!()))
    }
}

/// return a function to convert `crate::error::Error` to `common_meta::error::Error`
fn to_meta_err(
    location: snafu::Location,
) -> impl FnOnce(crate::error::Error) -> common_meta::error::Error {
    move |err: crate::error::Error| -> common_meta::error::Error {
        common_meta::error::Error::External {
            location,
            source: BoxedError::new(err),
        }
    }
}

#[async_trait::async_trait]
impl common_meta::node_manager::Flownode for FlowWorkerManager {
    async fn handle(&self, request: FlowRequest) -> MetaResult<FlowResponse> {
        let query_ctx = request
            .header
            .and_then(|h| h.query_context)
            .map(|ctx| ctx.into());
        match request.body {
            Some(flow_request::Body::Create(CreateRequest {
                flow_id: Some(task_id),
                source_table_ids,
                sink_table_name: Some(sink_table_name),
                create_if_not_exists,
                expire_after,
                comment,
                sql,
                flow_options,
                or_replace,
            })) => {
                let source_table_ids = source_table_ids.into_iter().map(|id| id.id).collect_vec();
                let sink_table_name = [
                    sink_table_name.catalog_name,
                    sink_table_name.schema_name,
                    sink_table_name.table_name,
                ];
                let expire_after = expire_after.map(|e| e.value);
                let args = CreateFlowArgs {
                    flow_id: task_id.id as u64,
                    sink_table_name,
                    source_table_ids,
                    create_if_not_exists,
                    or_replace,
                    expire_after,
                    comment: Some(comment),
                    sql: sql.clone(),
                    flow_options,
                    query_ctx,
                };
                let ret = self
                    .create_flow(args)
                    .await
                    .map_err(BoxedError::new)
                    .with_context(|_| CreateFlowSnafu { sql: sql.clone() })
                    .map_err(to_meta_err(snafu::location!()))?;
                METRIC_FLOW_TASK_COUNT.inc();
                Ok(FlowResponse {
                    affected_flows: ret
                        .map(|id| greptime_proto::v1::FlowId { id: id as u32 })
                        .into_iter()
                        .collect_vec(),
                    ..Default::default()
                })
            }
            Some(flow_request::Body::Drop(DropRequest {
                flow_id: Some(flow_id),
            })) => {
                self.remove_flow(flow_id.id as u64)
                    .await
                    .map_err(to_meta_err(snafu::location!()))?;
                METRIC_FLOW_TASK_COUNT.dec();
                Ok(Default::default())
            }
            Some(flow_request::Body::Flush(FlushFlow {
                flow_id: Some(flow_id),
            })) => {
                let row = self
                    .flush_flow_inner(flow_id.id as u64)
                    .await
                    .map_err(to_meta_err(snafu::location!()))?;
                Ok(FlowResponse {
                    affected_flows: vec![flow_id],
                    affected_rows: row as u64,
                    ..Default::default()
                })
            }
            None => UnexpectedSnafu {
                err_msg: "Missing request body",
            }
            .fail(),
            _ => UnexpectedSnafu {
                err_msg: "Invalid request body.",
            }
            .fail(),
        }
    }

    async fn handle_inserts(&self, request: InsertRequests) -> MetaResult<FlowResponse> {
        self.handle_inserts_inner(request)
            .await
            .map(|_| Default::default())
            .map_err(to_meta_err(snafu::location!()))
    }
}

impl FlowEngine for FlowWorkerManager {
    async fn create_flow(&self, args: CreateFlowArgs) -> Result<Option<FlowId>, Error> {
        self.create_flow_inner(args).await
    }

    async fn remove_flow(&self, flow_id: FlowId) -> Result<(), Error> {
        self.remove_flow_inner(flow_id).await
    }

    async fn flush_flow(&self, flow_id: FlowId) -> Result<usize, Error> {
        self.flush_flow_inner(flow_id).await
    }

    async fn flow_exist(&self, flow_id: FlowId) -> Result<bool, Error> {
        self.flow_exist_inner(flow_id).await
    }

    async fn handle_flow_inserts(
        &self,
        request: api::v1::region::InsertRequests,
    ) -> Result<(), Error> {
        self.handle_inserts_inner(request).await
    }
}

/// Simple helper enum for fetching value from row with default value
#[derive(Debug, Clone)]
enum FetchFromRow {
    Idx(usize),
    Default(Value),
}

impl FetchFromRow {
    /// Panic if idx is out of bound
    fn fetch(&self, row: &repr::Row) -> Value {
        match self {
            FetchFromRow::Idx(idx) => row.get(*idx).unwrap().clone(),
            FetchFromRow::Default(v) => v.clone(),
        }
    }
}

impl FlowWorkerManager {
    async fn handle_inserts_inner(
        &self,
        request: InsertRequests,
    ) -> std::result::Result<(), Error> {
        // using try_read to ensure two things:
        // 1. flush wouldn't happen until inserts before it is inserted
        // 2. inserts happening concurrently with flush wouldn't be block by flush
        let _flush_lock = self.flush_lock.try_read();
        for write_request in request.requests {
            let region_id = write_request.region_id;
            let table_id = RegionId::from(region_id).table_id();

            let (insert_schema, rows_proto) = write_request
                .rows
                .map(|r| (r.schema, r.rows))
                .unwrap_or_default();

            // TODO(discord9): reconsider time assignment mechanism
            let now = self.tick_manager.tick();

            let (table_types, fetch_order) = {
                let ctx = self.node_context.read().await;

                // TODO(discord9): also check schema version so that altered table can be reported
                let table_schema = ctx.table_source.table_from_id(&table_id).await?;
                let default_vals = table_schema
                    .default_values
                    .iter()
                    .zip(table_schema.relation_desc.typ().column_types.iter())
                    .map(|(v, ty)| {
                        v.as_ref().and_then(|v| {
                            match v.create_default(ty.scalar_type(), ty.nullable()) {
                                Ok(v) => Some(v),
                                Err(err) => {
                                    common_telemetry::error!(err; "Failed to create default value");
                                    None
                                }
                            }
                        })
                    })
                    .collect_vec();

                let table_types = table_schema
                    .relation_desc
                    .typ()
                    .column_types
                    .clone()
                    .into_iter()
                    .map(|t| t.scalar_type)
                    .collect_vec();
                let table_col_names = table_schema.relation_desc.names;
                let table_col_names = table_col_names
                    .iter().enumerate()
                    .map(|(idx,name)| match name {
                        Some(name) => Ok(name.clone()),
                        None => InternalSnafu {
                            reason: format!("Expect column {idx} of table id={table_id} to have name in table schema, found None"),
                        }
                        .fail(),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let name_to_col = HashMap::<_, _>::from_iter(
                    insert_schema
                        .iter()
                        .enumerate()
                        .map(|(i, name)| (&name.column_name, i)),
                );

                let fetch_order: Vec<FetchFromRow> = table_col_names
                    .iter()
                    .zip(default_vals.into_iter())
                    .map(|(col_name, col_default_val)| {
                        name_to_col
                            .get(col_name)
                            .copied()
                            .map(FetchFromRow::Idx)
                            .or_else(|| col_default_val.clone().map(FetchFromRow::Default))
                            .with_context(|| crate::error::UnexpectedSnafu {
                                reason: format!(
                                    "Column not found: {}, default_value: {:?}",
                                    col_name, col_default_val
                                ),
                            })
                    })
                    .try_collect()?;

                trace!("Reordering columns: {:?}", fetch_order);
                (table_types, fetch_order)
            };

            // TODO(discord9): use column instead of row
            let rows: Vec<DiffRow> = rows_proto
                .into_iter()
                .map(|r| {
                    let r = repr::Row::from(r);
                    let reordered = fetch_order.iter().map(|i| i.fetch(&r)).collect_vec();
                    repr::Row::new(reordered)
                })
                .map(|r| (r, now, 1))
                .collect_vec();
            if let Err(err) = self
                .handle_write_request(region_id.into(), rows, &table_types)
                .await
            {
                let err = BoxedError::new(err);
                let flow_ids = self
                    .node_context
                    .read()
                    .await
                    .get_flow_ids(table_id)
                    .into_iter()
                    .flatten()
                    .cloned()
                    .collect_vec();
                let err = InsertIntoFlowSnafu {
                    region_id,
                    flow_ids,
                }
                .into_error(err);
                common_telemetry::error!(err; "Failed to handle write request");
                return Err(err);
            }
        }
        Ok(())
    }
}
