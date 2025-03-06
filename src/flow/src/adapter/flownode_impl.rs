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
use std::collections::HashMap;

use api::v1::flow::{
    flow_request, CreateRequest, DropRequest, FlowRequest, FlowResponse, FlushFlow,
};
use api::v1::region::InsertRequests;
use common_error::ext::BoxedError;
use common_meta::node_manager::Flownode;
use common_telemetry::{debug, info, trace};
use datatypes::value::Value;
use futures::TryStreamExt;
use itertools::Itertools;
use session::context::QueryContextBuilder;
use snafu::{IntoError, OptionExt, ResultExt};
use store_api::storage::RegionId;

use crate::adapter::{CreateFlowArgs, FlowWorkerManager};
use crate::error::{
    CreateFlowSnafu, ExternalSnafu, FlowNotFoundSnafu, InsertIntoFlowSnafu, InternalSnafu,
    ListFlowsSnafu, UnexpectedSnafu,
};
use crate::metrics::METRIC_FLOW_TASK_COUNT;
use crate::repr::{self, DiffRow};
use crate::Error;

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
impl Flownode for FlowWorkerManager {
    async fn handle(
        &self,
        request: FlowRequest,
    ) -> Result<FlowResponse, common_meta::error::Error> {
        self.handle_inner(request)
            .await
            .map_err(to_meta_err(snafu::location!()))
    }

    #[allow(unreachable_code, unused)]
    async fn handle_inserts(
        &self,
        request: InsertRequests,
    ) -> Result<FlowResponse, common_meta::error::Error> {
        self.handle_inserts_inner(request)
            .await
            .map_err(to_meta_err(snafu::location!()))
    }
}

impl FlowWorkerManager {
    async fn handle_inner(
        &self,
        request: FlowRequest,
    ) -> Result<FlowResponse, crate::error::Error> {
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
                    .with_context(|_| CreateFlowSnafu { sql: sql.clone() })?;
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
                self.remove_flow(flow_id.id as u64).await?;
                METRIC_FLOW_TASK_COUNT.dec();
                Ok(Default::default())
            }
            Some(flow_request::Body::Flush(FlushFlow {
                flow_id: Some(flow_id),
            })) => {
                // TODO(discord9): impl individual flush
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
                if self.rule_engine.flow_exist(flow_id.id as u64).await {
                    self.rule_engine.flush_flow(flow_id.id as u64).await?;
                }
                Ok(FlowResponse {
                    affected_flows: vec![flow_id],
                    affected_rows: row as u64,
                    ..Default::default()
                })
            }
            None => UnexpectedSnafu {
                reason: "Missing request body",
            }
            .fail(),
            _ => UnexpectedSnafu {
                reason: "Invalid request body.",
            }
            .fail(),
        }
    }

    #[allow(unreachable_code, unused)]
    async fn handle_inserts_inner(
        &self,
        request: InsertRequests,
    ) -> Result<FlowResponse, crate::error::Error> {
        // TODO(discord9): make mirror request go to both engine
        return self.rule_engine.handle_inserts(request).await;
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
                            .fail().map_err(BoxedError::new).context(ExternalSnafu),
                        })
                        .collect::<Result<Vec<_>,_>>()?;
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
                            .with_context(|| UnexpectedSnafu {
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
        Ok(Default::default())
    }

    /// recover all flow tasks in this flownode in distributed mode(nodeid is Some(<num>))
    ///
    /// or recover all existing flow tasks if in standalone mode(nodeid is None)
    ///
    /// TODO(discord9): persistent flow tasks with internal state
    pub async fn recover_flows(&self) -> Result<usize, Error> {
        let nodeid = self.node_id;
        let to_be_recovered: Vec<_> = if let Some(nodeid) = nodeid {
            let to_be_recover = self
                .table_info_source
                .flow_meta
                .flownode_flow_manager()
                .flows(nodeid.into())
                .try_collect::<Vec<_>>()
                .await
                .context(ListFlowsSnafu {
                    id: Some(nodeid.into()),
                })?;
            to_be_recover.into_iter().map(|(id, _)| id).collect()
        } else {
            let all_catalogs = self
                .table_info_source
                .table_meta
                .catalog_manager()
                .catalog_names()
                .try_collect::<Vec<_>>()
                .await
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?;
            let mut all_flow_ids = vec![];
            for catalog in all_catalogs {
                let flows = self
                    .table_info_source
                    .flow_meta
                    .flow_name_manager()
                    .flow_names(&catalog)
                    .await
                    .try_collect::<Vec<_>>()
                    .await
                    .map_err(BoxedError::new)
                    .context(ExternalSnafu)?;

                all_flow_ids.extend(flows.into_iter().map(|(_, id)| id.flow_id()));
            }
            all_flow_ids
        };
        let cnt = to_be_recovered.len();

        // TODO(discord9): recover in parallel
        for flow_id in to_be_recovered {
            let info = self
                .table_info_source
                .flow_meta
                .flow_info_manager()
                .get(flow_id)
                .await
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?
                .context(FlowNotFoundSnafu { id: flow_id })?;

            let sink_table_name = [
                info.sink_table_name().catalog_name.clone(),
                info.sink_table_name().schema_name.clone(),
                info.sink_table_name().table_name.clone(),
            ];
            let args = CreateFlowArgs {
                flow_id: flow_id as _,
                sink_table_name,
                source_table_ids: info.source_table_ids().to_vec(),
                // because recover should only happen on restart the `create_if_not_exists` and `or_replace` can be arbitrary value(since flow doesn't exist)
                // but for the sake of consistency and to make sure recover of flow actually happen, we set both to true
                // (which is also fine since checks for not allow both to be true is on metasrv and we already pass that)
                create_if_not_exists: true,
                or_replace: true,
                expire_after: info.expire_after(),
                comment: Some(info.comment().clone()),
                sql: info.raw_sql().clone(),
                flow_options: info.options().clone(),
                query_ctx: Some(
                    QueryContextBuilder::default()
                        .current_catalog(info.catalog_name().clone())
                        .build(),
                ),
            };
            self.create_flow(args)
                .await
                .map_err(BoxedError::new)
                .with_context(|_| CreateFlowSnafu {
                    sql: info.raw_sql().clone(),
                })?;
        }

        info!("Recover {} flows", cnt);

        Ok(cnt)
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
