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

use api::v1::flow::{flow_request, CreateRequest, DropRequest, FlowRequest, FlowResponse};
use api::v1::region::InsertRequests;
use common_error::ext::BoxedError;
use common_meta::error::{ExternalSnafu, Result, UnexpectedSnafu};
use common_meta::node_manager::Flownode;
use itertools::Itertools;
use snafu::ResultExt;

use crate::adapter::FlownodeManager;
use crate::repr::{self, DiffRow};

fn to_meta_err(err: crate::adapter::error::Error) -> common_meta::error::Error {
    // TODO(discord9): refactor this
    Err::<(), _>(BoxedError::new(err))
        .with_context(|_| ExternalSnafu)
        .unwrap_err()
}

#[async_trait::async_trait]
impl Flownode for FlownodeManager {
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
                let ret = self
                    .create_flow(
                        task_id.id as u64,
                        sink_table_name,
                        &source_table_ids,
                        create_if_not_exists,
                        Some(expire_after),
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
        for write_request in request.requests {
            let region_id = write_request.region_id;
            let rows_proto = write_request.rows.map(|r| r.rows).unwrap_or(vec![]);
            // TODO(discord9): reconsider time assignment mechanism
            let now = self.tick_manager.tick();
            let rows: Vec<DiffRow> = rows_proto
                .into_iter()
                .map(repr::Row::from)
                .map(|r| (r, now, 1))
                .collect_vec();
            self.handle_write_request(region_id.into(), rows)
                .await
                .map_err(to_meta_err)?;
        }
        Ok(Default::default())
    }
}
