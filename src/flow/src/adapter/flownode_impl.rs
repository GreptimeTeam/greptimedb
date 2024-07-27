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
use common_telemetry::debug;
use itertools::Itertools;
use snafu::{OptionExt, ResultExt};
use store_api::storage::RegionId;

use crate::adapter::FlowWorkerManager;
use crate::error::InternalSnafu;
use crate::repr::{self, DiffRow};

fn to_meta_err(err: crate::error::Error) -> common_meta::error::Error {
    // TODO(discord9): refactor this
    Err::<(), _>(BoxedError::new(err))
        .with_context(|_| ExternalSnafu)
        .unwrap_err()
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
            })) => {
                let source_table_ids = source_table_ids.into_iter().map(|id| id.id).collect_vec();
                let sink_table_name = [
                    sink_table_name.catalog_name,
                    sink_table_name.schema_name,
                    sink_table_name.table_name,
                ];
                let expire_after = expire_after.map(|e| e.value);
                let ret = self
                    .create_flow(
                        task_id.id as u64,
                        sink_table_name,
                        &source_table_ids,
                        create_if_not_exists,
                        expire_after,
                        Some(comment),
                        sql,
                        flow_options,
                        query_ctx,
                    )
                    .await
                    .map_err(to_meta_err)?;
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
                    .map_err(to_meta_err)?;
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
                    .map_err(to_meta_err)?;
                let rows_send = self.run_available(true).await.map_err(to_meta_err)?;
                let row = self.send_writeback_requests().await.map_err(to_meta_err)?;

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
        // using try_read makesure two things:
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

            let fetch_order = {
                let ctx = self.node_context.read().await;
                let table_col_names = ctx
                    .table_repr
                    .get_by_table_id(&table_id)
                    .map(|r| r.1)
                    .and_then(|id| ctx.schema.get(&id))
                    .map(|desc| &desc.names)
                    .context(UnexpectedSnafu {
                        err_msg: format!("Table not found: {}", table_id),
                    })?;
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
                let fetch_order: Vec<usize> = table_col_names
                    .iter()
                    .map(|names| {
                        name_to_col.get(names).copied().context(UnexpectedSnafu {
                            err_msg: format!("Column not found: {}", names),
                        })
                    })
                    .try_collect()?;
                if !fetch_order.iter().enumerate().all(|(i, &v)| i == v) {
                    debug!("Reordering columns: {:?}", fetch_order)
                }
                fetch_order
            };

            let rows: Vec<DiffRow> = rows_proto
                .into_iter()
                .map(|r| {
                    let r = repr::Row::from(r);
                    let reordered = fetch_order
                        .iter()
                        .map(|&i| r.inner[i].clone())
                        .collect_vec();
                    repr::Row::new(reordered)
                })
                .map(|r| (r, now, 1))
                .collect_vec();
            self.handle_write_request(region_id.into(), rows)
                .await
                .map_err(to_meta_err)?;
        }
        Ok(Default::default())
    }
}
