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

//! Grouped region reads: one request that names several regions of one table.
//!
//! The request is a versioned envelope (see `query::dist_plan::DatanodeGroupedReadPlan`) whose
//! nested plan is one ordinary single-table plan. Every named region decodes that plan against its
//! own region catalog, and the region plans are executed in the envelope's order, concatenating
//! their batches. Nothing is merged or reduced across regions, so the frontend sees the same rows
//! it would see from one request per region.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use api::v1::region::RegionRequestHeader;
use bytes::Bytes;
use common_error::ext::BoxedError;
use common_query::request::QueryRequest;
use common_recordbatch::adapter::RecordBatchMetrics;
use common_recordbatch::error::ExternalSnafu;
use common_recordbatch::{OrderOption, RecordBatch, RecordBatchStream, SendableRecordBatchStream};
use datatypes::schema::{Schema, SchemaRef};
use futures::{Stream, StreamExt};
use query::dist_plan::{GroupedReadPayload, validate_grouped_read_nested_plan};
use session::context::QueryContextRef;
use snafu::{IntoError, ResultExt};
use store_api::storage::RegionId;
use tokio::sync::OwnedSemaphorePermit;

use crate::error::{
    DecodeLogicalPlanSnafu, InvalidQueryContextSnafu, NewPlanDecoderSnafu, Result, UnexpectedSnafu,
};
use crate::region_server::RegionServer;
use crate::region_server::catalog::NameAwareCatalogList;
use crate::region_server::remote_dyn_filter::RemoteDynFilterRegistrationGuard;

impl RegionServer {
    /// Executes a grouped read envelope: one region plan per named region, in order.
    ///
    /// Decoding and validation happen before the stream is returned, so the stream's schema is
    /// final when the frontend receives it and a dynamic filter update can be applied while the
    /// grouped stream is still scanning. No region is skipped: a failing region fails the whole
    /// stream, and the regions already emitted stay emitted (the stream never retries a prefix).
    pub(super) async fn handle_grouped_read_inner(
        &self,
        request: api::v1::region::QueryRequest,
        query_ctx: QueryContextRef,
        payload: GroupedReadPayload,
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
            .map_err(|error| {
                InvalidQueryContextSnafu {
                    reason: error.to_string(),
                }
                .build()
            })?;
        let query_ctx = engine_ctx.query_ctx();
        let decoder = engine_ctx.new_plan_decoder().context(NewPlanDecoderSnafu)?;

        // One decode per region, against that region's own catalog, before anything runs.
        let mut plans = VecDeque::with_capacity(payload.region_ids().len());
        let mut expected_schema: Option<SchemaRef> = None;
        for region in payload.region_ids() {
            let region_id = RegionId::from_u64(*region);
            let catalog_list = Arc::new(NameAwareCatalogList::new(
                self.clone(),
                region_id,
                query_ctx.clone(),
            ));
            let plan = decoder
                .decode(
                    Bytes::from(payload.nested_plan().to_vec()),
                    catalog_list,
                    false,
                )
                .await
                .context(DecodeLogicalPlanSnafu)?;
            validate_grouped_read_nested_plan(&payload, &plan)
                .map_err(|reason| UnexpectedSnafu { violated: reason }.build())?;
            let schema = SchemaRef::from(
                Schema::try_from(plan.schema().clone()).map_err(|error| {
                    UnexpectedSnafu {
                        violated: format!(
                            "region {region_id} decodes the grouped read plan to an invalid schema: \
                             {error}"
                        ),
                    }
                    .build()
                })?,
            );
            match &expected_schema {
                Some(expected) if expected != &schema => {
                    return UnexpectedSnafu {
                        violated: format!(
                            "region {region_id} decodes the grouped read plan to a different schema"
                        ),
                    }
                    .fail();
                }
                Some(_) => {}
                None => expected_schema = Some(schema),
            }
            plans.push_back((region_id, plan));
        }
        let Some(schema) = expected_schema else {
            return UnexpectedSnafu {
                violated: "a grouped read names no region".to_string(),
            }
            .fail();
        };

        // Hold one registration guard per member region for the whole grouped stream. The initial
        // registrations arrive in the shared request header, and an update that arrives while an
        // earlier region is still scanning must find its own region's registration.
        let mut guards = Vec::with_capacity(payload.region_ids().len());
        for region in payload.region_ids() {
            if let Some(guard) = self
                .register_initial_remote_dyn_filter_cleanup(&query_ctx, RegionId::from_u64(*region))
            {
                guards.push(guard);
            }
        }

        let state = GroupedReadStreamState {
            server: self.clone(),
            query_ctx,
            header: request.header,
            plans,
            current: None,
            failed: false,
            permit,
            _guards: guards,
        };
        let inner = Box::pin(futures::stream::unfold(state, |mut state| async move {
            loop {
                if state.failed {
                    return None;
                }
                if let Some(current) = state.current.as_mut() {
                    match current.next().await {
                        Some(Ok(batch)) => return Some((Ok(batch), state)),
                        Some(Err(error)) => {
                            state.current = None;
                            state.plans.clear();
                            state.failed = true;
                            state._guards.clear();
                            state.permit.take();
                            return Some((Err(error), state));
                        }
                        None => state.current = None,
                    }
                }
                let Some((region_id, plan)) = state.plans.pop_front() else {
                    return None;
                };
                let request = QueryRequest {
                    header: state.header.clone(),
                    region_id,
                    plan,
                };
                match state
                    .server
                    .inner
                    .handle_read(request, state.query_ctx.clone())
                    .await
                {
                    Ok(stream) => state.current = Some(stream),
                    Err(error) => {
                        state.failed = true;
                        state.plans.clear();
                        state._guards.clear();
                        state.permit.take();
                        let error = ExternalSnafu.into_error(BoxedError::new(error));
                        return Some((Err(error), state));
                    }
                }
            }
        }));

        Ok(Box::pin(GroupedRecordBatchStream { schema, inner }))
    }
}

/// The lazy state of a grouped read: the region plans not started yet and the one being scanned.
struct GroupedReadStreamState {
    server: RegionServer,
    query_ctx: QueryContextRef,
    header: Option<RegionRequestHeader>,
    plans: VecDeque<(RegionId, datafusion_expr::LogicalPlan)>,
    current: Option<SendableRecordBatchStream>,
    failed: bool,
    permit: Option<OwnedSemaphorePermit>,
    /// Released when the stream finishes, fails or is dropped.
    _guards: Vec<RemoteDynFilterRegistrationGuard>,
}

/// A grouped read stream: the concatenation of the region streams, in envelope order.
struct GroupedRecordBatchStream {
    schema: SchemaRef,
    inner: Pin<
        Box<
            dyn Stream<Item = std::result::Result<RecordBatch, common_recordbatch::error::Error>>
                + Send,
        >,
    >,
}

impl Stream for GroupedRecordBatchStream {
    type Item = std::result::Result<RecordBatch, common_recordbatch::error::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl RecordBatchStream for GroupedRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_ordering(&self) -> Option<&[OrderOption]> {
        None
    }

    fn metrics(&self) -> Option<RecordBatchMetrics> {
        None
    }
}
