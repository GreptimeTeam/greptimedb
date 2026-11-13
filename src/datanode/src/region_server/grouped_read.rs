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

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use bytes::Bytes;
use common_error::ext::BoxedError;
use common_query::logical_plan::SubstraitPlanDecoderRef;
use common_query::request::QueryRequest;
use common_recordbatch::adapter::RecordBatchMetrics;
use common_recordbatch::{OrderOption, RecordBatch, RecordBatchStream, SendableRecordBatchStream};
use datafusion_expr::LogicalPlan;
use datatypes::schema::{Schema, SchemaRef};
use futures::Stream;
use query::dist_plan::{
    GROUP_READ_CONTEXT_EXTENSION, GroupReadError, GroupReadPayload, RegionPlanResolver,
    decode_group_read_request,
};
use session::context::QueryContextRef;
use snafu::{IntoError, ResultExt};
use store_api::storage::RegionId;

use crate::error::{
    DecodeLogicalPlanSnafu, HandleRegionRequestSnafu, InvalidQueryRequestSnafu,
    NewPlanDecoderSnafu, Result, UnexpectedSnafu,
};
use crate::region_server::RegionServer;
use crate::region_server::catalog::NameAwareCatalogList;
use crate::region_server::remote_dyn_filter::RemoteDynFilterRegistrationGuard;

type GroupedReadInitFuture =
    Pin<Box<dyn Future<Output = Result<SendableRecordBatchStream>> + Send>>;

impl RegionServer {
    /// Decodes and executes a `GroupRead` request.
    ///
    /// `plan_bytes` is the serialized `GroupRead` envelope, whether it arrived over Flight or was
    /// encoded from the in-process plan of a standalone datanode: both paths bind the region leaves
    /// through the same request-scoped consumer. The caller holds the limiter permits and wraps the
    /// returned stream with them.
    pub(super) async fn handle_grouped_read_inner(
        &self,
        header: Option<api::v1::region::RegionRequestHeader>,
        anchor: RegionId,
        plan_bytes: Bytes,
        query_ctx: QueryContextRef,
        payload: GroupReadPayload,
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
        super::validate_group_read_request(anchor, plan_bytes.len(), &payload)?;

        let mut grouped_ctx = (*query_ctx).clone();
        grouped_ctx.set_extension(GROUP_READ_CONTEXT_EXTENSION, "true");
        let query_ctx = Arc::new(grouped_ctx);
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
        let plan = decode_group_read_request(&plan_bytes, engine_ctx.state(), payload, &resolver)
            .await
            .map_err(|error| match error {
                // Keep the resolver's status code and retry hint: a region whose plan cannot be
                // resolved is a per-region failure, not a malformed request.
                GroupReadError::Resolve {
                    region_id, source, ..
                } => HandleRegionRequestSnafu { region_id }.into_error(source),
                other => InvalidQueryRequestSnafu {
                    reason: other.to_string(),
                }
                .build(),
            })?;

        let mut guards = Vec::with_capacity(region_ids.len());
        for region in region_ids {
            if let Some(guard) = self
                .register_initial_remote_dyn_filter_cleanup(&query_ctx, RegionId::from_u64(region))
            {
                guards.push(guard);
            }
        }

        let schema = SchemaRef::from(Schema::try_from(plan.schema().clone()).map_err(|error| {
            InvalidQueryRequestSnafu {
                reason: format!("GroupRead combine tree has an invalid schema: {error}"),
            }
            .build()
        })?);
        let read_query_ctx = query_ctx.clone();
        let inner = self.inner.clone();
        let init: GroupedReadInitFuture = Box::pin(async move {
            inner
                .handle_read(
                    QueryRequest {
                        header,
                        region_id: anchor,
                        plan,
                    },
                    read_query_ctx,
                )
                .await
        });
        let stream: SendableRecordBatchStream = Box::pin(GroupedReadLazyStream {
            schema,
            init: Some(init),
            inner: None,
            guards,
        });
        Ok(stream)
    }
}

/// Starts the query engine on the first poll, after the request has already returned to the
/// caller. This lets a remote dynamic-filter update that arrives before the first batch affect
/// every region scan, and keeps the scan from running during request preflight.
struct GroupedReadLazyStream {
    schema: SchemaRef,
    init: Option<GroupedReadInitFuture>,
    inner: Option<SendableRecordBatchStream>,
    guards: Vec<RemoteDynFilterRegistrationGuard>,
}

impl Stream for GroupedReadLazyStream {
    type Item = std::result::Result<RecordBatch, common_recordbatch::error::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.inner.is_none() {
            match self.init.as_mut().map(|init| init.as_mut().poll(cx)) {
                Some(Poll::Pending) => return Poll::Pending,
                Some(Poll::Ready(Ok(stream))) => {
                    self.init = None;
                    let stream = std::mem::take(&mut self.guards)
                        .into_iter()
                        .fold(stream, super::wrap_remote_dyn_filter_guarded_stream);
                    self.inner = Some(stream);
                }
                Some(Poll::Ready(Err(error))) => {
                    self.init = None;
                    self.guards.clear();
                    return Poll::Ready(Some(Err(common_recordbatch::error::ExternalSnafu
                        .into_error(common_error::ext::BoxedError::new(error)))));
                }
                None => {}
            }
        }
        match self.inner.as_mut() {
            Some(inner) => inner.as_mut().poll_next(cx),
            None => Poll::Ready(None),
        }
    }
}

impl RecordBatchStream for GroupedReadLazyStream {
    fn name(&self) -> &str {
        self.inner
            .as_ref()
            .map(|inner| inner.name())
            .unwrap_or("GroupedReadLazyStream")
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_ordering(&self) -> Option<&[OrderOption]> {
        self.inner
            .as_ref()
            .and_then(|inner| inner.output_ordering())
    }

    fn metrics(&self) -> Option<RecordBatchMetrics> {
        self.inner.as_ref().and_then(|inner| inner.metrics())
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
    ) -> std::result::Result<LogicalPlan, BoxedError> {
        let catalog_list = Arc::new(NameAwareCatalogList::new(
            self.server.clone(),
            region_id,
            self.query_ctx.clone(),
        ));
        self.decoder
            .decode(Bytes::from(plan.to_vec()), catalog_list, false)
            .await
            .map_err(|error| BoxedError::new(DecodeLogicalPlanSnafu.into_error(error)))
    }
}
