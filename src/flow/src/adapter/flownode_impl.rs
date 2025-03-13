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
use common_meta::error::{ExternalSnafu, Result, UnexpectedSnafu};
use common_meta::node_manager::Flownode;
use common_telemetry::{debug, trace};
use datatypes::value::Value;
use itertools::Itertools;
use snafu::{IntoError, OptionExt, ResultExt};
use store_api::storage::RegionId;

use crate::adapter::{CreateFlowArgs, FlowWorkerManager};
use crate::error::{CreateFlowSnafu, InsertIntoFlowSnafu, InternalSnafu};
use crate::metrics::METRIC_FLOW_TASK_COUNT;
use crate::repr::{self, DiffRow};

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
    async fn handle(&self, request: FlowRequest) -> Result<FlowResponse> {
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
                // TODO(discord9): impl individual flush
                debug!("Starting to flush flow_id={:?}", flow_id);
                // lock to make sure writes before flush are written to flow
                // and immediately drop to prevent following writes to be blocked
                drop(self.flush_lock.write().await);
                let flushed_input_rows = self
                    .node_context
                    .read()
                    .await
                    .flush_all_sender()
                    .await
                    .map_err(to_meta_err(snafu::location!()))?;
                let rows_send = self
                    .run_available(true)
                    .await
                    .map_err(to_meta_err(snafu::location!()))?;
                let row = self
                    .send_writeback_requests()
                    .await
                    .map_err(to_meta_err(snafu::location!()))?;

                debug!(
                    "Done to flush flow_id={:?} with {} input rows flushed, {} rows sended and {} output rows flushed",
                    flow_id, flushed_input_rows, rows_send, row
                );
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

    async fn handle_inserts(&self, request: InsertRequests) -> Result<FlowResponse> {
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
                let table_schema = ctx
                    .table_source
                    .table_from_id(&table_id)
                    .await
                    .map_err(to_meta_err(snafu::location!()))?;
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
                    .collect::<Result<Vec<_>>>()?;
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
                                err_msg: format!(
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
                let err = to_meta_err(snafu::location!())(err);
                return Err(err);
            }
        }
        Ok(Default::default())
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
