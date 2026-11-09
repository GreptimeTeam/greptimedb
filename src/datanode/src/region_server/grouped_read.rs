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

//! Request-scoped decoding of `GroupRead`.
//!
//! The root is a `GroupRead` marker around a combine tree. Every `RegionPlan` leaf carries one
//! region id and the complete plan for that region. The query crate resolves each leaf through
//! the region resolver below, replaces it with the bound logical subtree, and the datanode runs
//! the resulting ordinary multi-input tree through the query engine. There is no physical-input
//! registry or combine-plan field: a `UnionAll` is literally a DataFusion `Union`.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use bytes::Bytes;
use common_query::logical_plan::SubstraitPlanDecoderRef;
use common_query::request::QueryRequest;
use common_recordbatch::adapter::RecordBatchMetrics;
use common_recordbatch::{OrderOption, RecordBatch, RecordBatchStream, SendableRecordBatchStream};
use datafusion_expr::LogicalPlan;
use futures::Stream;
use query::dist_plan::{GroupReadPayload, RegionPlanResolver, decode_group_read_request};
use session::context::QueryContextRef;
use snafu::ResultExt;
use store_api::storage::RegionId;
use tokio::sync::OwnedSemaphorePermit;

use crate::error::{DecodeLogicalPlanSnafu, NewPlanDecoderSnafu, Result, UnexpectedSnafu};
use crate::region_server::RegionServer;
use crate::region_server::catalog::NameAwareCatalogList;

impl RegionServer {
    /// Decodes and executes a `GroupRead` request.
    pub(super) async fn handle_grouped_read_inner(
        &self,
        request: api::v1::region::QueryRequest,
        query_ctx: QueryContextRef,
        payload: GroupReadPayload,
        permit: Option<OwnedSemaphorePermit>,
    ) -> Result<SendableRecordBatchStream> {
        if query_ctx
            .extensions()
            .keys()
            .any(|key| key.starts_with("flow."))
        {
            return UnexpectedSnafu {
                violated: "grouped reads do not support Flow query contexts",
            }
            .fail();
        }
        let anchor = RegionId::from_u64(request.region_id);
        payload
            .validate_for_request(anchor)
            .map_err(|reason| UnexpectedSnafu { violated: reason }.build())?;

        let engine_ctx = self
            .inner
            .query_engine
            .engine_context(query_ctx)
            .context(NewPlanDecoderSnafu)?;
        let query_ctx = engine_ctx.query_ctx();
        let decoder = engine_ctx.new_plan_decoder().context(NewPlanDecoderSnafu)?;
        let region_ids = payload.region_ids().to_vec();
        let resolver = DatanodeRegionPlanResolver {
            server: self.clone(),
            query_ctx: query_ctx.clone(),
            decoder,
        };
        let plan = decode_group_read_request(&request.plan, engine_ctx.state(), payload, &resolver)
            .await
            .map_err(|reason| UnexpectedSnafu { violated: reason }.build())?;

        let mut guards = Vec::with_capacity(region_ids.len());
        for region in region_ids {
            if let Some(guard) = self
                .register_initial_remote_dyn_filter_cleanup(&query_ctx, RegionId::from_u64(region))
            {
                guards.push(guard);
            }
        }

        let stream = self
            .inner
            .handle_read(
                QueryRequest {
                    header: request.header,
                    region_id: anchor,
                    plan,
                },
                query_ctx,
            )
            .await?;
        let stream = guards
            .into_iter()
            .fold(stream, super::wrap_remote_dyn_filter_guarded_stream);
        Ok(match permit {
            Some(permit) => Box::pin(GroupedReadPermitStream {
                inner: stream,
                permit: Some(permit),
            }),
            None => stream,
        })
    }
}

/// Holds the outer grouped-read permit until EOF, error or drop.
struct GroupedReadPermitStream {
    inner: SendableRecordBatchStream,
    permit: Option<OwnedSemaphorePermit>,
}

impl Stream for GroupedReadPermitStream {
    type Item = std::result::Result<RecordBatch, common_recordbatch::error::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let result = self.inner.as_mut().poll_next(cx);
        if matches!(result, Poll::Ready(None)) || matches!(result, Poll::Ready(Some(Err(_)))) {
            self.permit.take();
        }
        result
    }
}

impl RecordBatchStream for GroupedReadPermitStream {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn schema(&self) -> datatypes::schema::SchemaRef {
        self.inner.schema()
    }

    fn output_ordering(&self) -> Option<&[OrderOption]> {
        self.inner.output_ordering()
    }

    fn metrics(&self) -> Option<RecordBatchMetrics> {
        self.inner.metrics()
    }
}

struct DatanodeRegionPlanResolver {
    server: RegionServer,
    query_ctx: QueryContextRef,
    decoder: SubstraitPlanDecoderRef,
}

#[async_trait]
impl RegionPlanResolver for DatanodeRegionPlanResolver {
    async fn resolve_region_plan(
        &self,
        region_id: RegionId,
        plan: &[u8],
    ) -> std::result::Result<LogicalPlan, String> {
        let catalog_list = Arc::new(NameAwareCatalogList::new(
            self.server.clone(),
            region_id,
            self.query_ctx.clone(),
        ));
        self.decoder
            .decode(Bytes::from(plan.to_vec()), catalog_list, false)
            .await
            .context(DecodeLogicalPlanSnafu)
            .map_err(|error| error.to_string())
    }
}
