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

use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};

use api::region::RegionResponse;
use api::v1::region::RemoteDynFilterRequest;
use api::v1::region::remote_dyn_filter_request::Action;
use common_base::Plugins;
use common_recordbatch::adapter::RecordBatchMetrics;
use common_recordbatch::{OrderOption, RecordBatch, RecordBatchStream, SendableRecordBatchStream};
use common_telemetry::{debug, warn};
use datafusion_expr::LogicalPlan;
use futures_util::Stream;
use query::dist_plan::{
    RemoteDynFilterReceiverInjector, RemoteDynFilterReceiverInjectorRef,
    RemoteDynFilterReceiverLogicalPlan,
};
use query::options::remote_dyn_filter_pushdown_enabled_from_extensions;
use query::promql::plan_contains_promql_extension;
use session::context::QueryContextRef;
use session::query_id::QueryId;
use snafu::OptionExt;
use store_api::storage::RegionId;

use crate::error::{self, Result, UnexpectedSnafu};
use crate::region_server::registrations::{
    RemoteDynFilterId, RemoteDynFilterUpdateOutcome, apply_remote_dyn_filter_update,
    initial_dyn_filter_regs_from_query_ctx, register_initial_dyn_filter_regs,
    remote_dyn_filter_exprs_for_initial_regs, remove_initial_dyn_filter_regs,
    unregister_remote_dyn_filter,
};
use crate::region_server::{RegionServer, RegionServerInner};

fn remote_dyn_filter_receiver_plan(
    server: &Weak<RegionServerInner>,
    origin: LogicalPlan,
    ctx: QueryContextRef,
) -> LogicalPlan {
    if !remote_dyn_filter_pushdown_enabled(&ctx) {
        return origin;
    }

    if plan_contains_promql_extension(&origin) {
        return origin;
    }

    let Some(query_id) = ctx.remote_query_id_value() else {
        return origin;
    };

    let Some(initial_regs) = initial_dyn_filter_regs_from_query_ctx(&ctx) else {
        return origin;
    };

    let Some(server) = server.upgrade() else {
        return origin;
    };

    let dyn_filters = remote_dyn_filter_exprs_for_initial_regs(
        &server.initial_remote_dyn_filter_registrations,
        &query_id,
        &initial_regs,
        origin.schema().as_arrow(),
    );

    if dyn_filters.is_empty() {
        return origin;
    }

    RemoteDynFilterReceiverLogicalPlan::new(origin, dyn_filters).into_logical_plan()
}

fn remote_dyn_filter_pushdown_enabled(query_ctx: &QueryContextRef) -> bool {
    match remote_dyn_filter_pushdown_enabled_from_extensions(&query_ctx.extensions()) {
        Ok(enabled) => enabled,
        Err(error) => {
            warn!(error; "Remote dynamic filter pushdown is disabled because the query option is invalid");
            false
        }
    }
}

impl RegionServer {
    pub fn install_remote_dyn_filter_receiver_injector(&self, plugins: &Plugins) {
        let server = Arc::downgrade(&self.inner);
        plugins.insert::<RemoteDynFilterReceiverInjectorRef>(Arc::new(
            RemoteDynFilterReceiverInjector::new(move |origin, ctx| {
                remote_dyn_filter_receiver_plan(&server, origin, ctx)
            }),
        ));
    }

    pub(super) fn register_initial_remote_dyn_filter_cleanup(
        &self,
        query_ctx: &QueryContextRef,
        region_id: RegionId,
    ) -> Option<RemoteDynFilterRegistrationGuard> {
        if !remote_dyn_filter_pushdown_enabled(query_ctx) {
            return None;
        }

        let initial_dyn_filter_regs = initial_dyn_filter_regs_from_query_ctx(query_ctx);
        let query_id = query_ctx.remote_query_id_value();
        let registered_filter_ids = if let (Some(query_id), Some(regs)) =
            (query_id.as_ref(), initial_dyn_filter_regs.as_ref())
        {
            register_initial_dyn_filter_regs(
                &self.inner.initial_remote_dyn_filter_registrations,
                query_id,
                region_id,
                regs,
            )
        } else {
            Vec::new()
        };

        match (query_id, registered_filter_ids.is_empty()) {
            (Some(query_id), false) => Some(RemoteDynFilterRegistrationGuard::new(
                self.clone(),
                query_id,
                region_id,
                registered_filter_ids,
            )),
            _ => None,
        }
    }

    pub(super) async fn handle_remote_dyn_filter_request(
        &self,
        request: &RemoteDynFilterRequest,
    ) -> Result<RegionResponse> {
        if request.query_id.is_empty() {
            return error::MissingRequiredFieldSnafu { name: "query_id" }.fail();
        }

        let query_id = request.query_id.parse::<QueryId>().map_err(|_| {
            UnexpectedSnafu {
                violated: "remote dynamic filter query_id must be a valid QueryId",
            }
            .build()
        })?;

        match request
            .action
            .as_ref()
            .context(error::MissingRequiredFieldSnafu { name: "action" })?
        {
            Action::Update(update) => {
                self.handle_remote_dyn_filter_update(&query_id, update)
                    .await
            }
            Action::Unregister(unregister) => {
                self.handle_remote_dyn_filter_unregister(&query_id, unregister)
                    .await
            }
        }
    }

    async fn handle_remote_dyn_filter_update(
        &self,
        query_id: &QueryId,
        request: &api::v1::region::RemoteDynFilterUpdate,
    ) -> Result<RegionResponse> {
        if request.filter_id.is_empty() {
            return error::MissingRequiredFieldSnafu { name: "filter_id" }.fail();
        }

        if request.payload.is_empty() {
            return error::MissingRequiredFieldSnafu { name: "payload" }.fail();
        }

        let filter_id = RemoteDynFilterId::new(request.filter_id.clone());
        let outcome = apply_remote_dyn_filter_update(
            &self.inner.initial_remote_dyn_filter_registrations,
            query_id,
            &filter_id,
            &request.payload,
            request.generation,
            request.is_complete,
        );
        self.log_remote_dyn_filter_update_outcome(query_id, &filter_id, outcome);

        Ok(RegionResponse::new(0))
    }

    async fn handle_remote_dyn_filter_unregister(
        &self,
        query_id: &QueryId,
        request: &api::v1::region::RemoteDynFilterUnregister,
    ) -> Result<RegionResponse> {
        if request.filter_id.is_empty() {
            return error::MissingRequiredFieldSnafu { name: "filter_id" }.fail();
        }

        let filter_id = RemoteDynFilterId::new(request.filter_id.clone());
        let outcome = unregister_remote_dyn_filter(
            &self.inner.initial_remote_dyn_filter_registrations,
            query_id,
            &filter_id,
        );
        self.log_remote_dyn_filter_update_outcome(query_id, &filter_id, outcome);

        Ok(RegionResponse::new(0))
    }

    fn log_remote_dyn_filter_update_outcome(
        &self,
        query_id: &QueryId,
        filter_id: &RemoteDynFilterId,
        outcome: RemoteDynFilterUpdateOutcome,
    ) {
        if matches!(
            outcome,
            RemoteDynFilterUpdateOutcome::MissingRegistration
                | RemoteDynFilterUpdateOutcome::AlreadyComplete
                | RemoteDynFilterUpdateOutcome::PayloadTooLarge
                | RemoteDynFilterUpdateOutcome::DecodeFailed
        ) {
            warn!(
                "Remote dynamic filter update outcome, query_id: {}, filter_id: {}, outcome: {:?}",
                query_id, filter_id, outcome
            );
        } else {
            debug!(
                "Remote dynamic filter update outcome, query_id: {}, filter_id: {}, outcome: {:?}",
                query_id, filter_id, outcome
            );
        }
    }
}

pub(super) fn wrap_remote_dyn_filter_guarded_stream(
    stream: SendableRecordBatchStream,
    cleanup: RemoteDynFilterRegistrationGuard,
) -> SendableRecordBatchStream {
    Box::pin(RemoteDynFilterGuardedStream { stream, cleanup })
}

/// Removes query-scoped remote dynamic filter subscriptions unless ownership is moved elsewhere.
pub(super) struct RemoteDynFilterRegistrationGuard {
    server: RegionServer,
    query_id: QueryId,
    region_id: RegionId,
    filter_ids: Vec<RemoteDynFilterId>,
    cleaned: bool,
}

impl RemoteDynFilterRegistrationGuard {
    fn new(
        server: RegionServer,
        query_id: QueryId,
        region_id: RegionId,
        filter_ids: Vec<RemoteDynFilterId>,
    ) -> Self {
        Self {
            server,
            query_id,
            region_id,
            filter_ids,
            cleaned: false,
        }
    }

    fn cleanup_once(&mut self) {
        if self.cleaned {
            return;
        }

        remove_initial_dyn_filter_regs(
            &self.server.inner.initial_remote_dyn_filter_registrations,
            &self.query_id,
            self.region_id,
            &self.filter_ids,
        );
        self.cleaned = true;
    }
}

impl Drop for RemoteDynFilterRegistrationGuard {
    fn drop(&mut self) {
        self.cleanup_once();
    }
}

/// Removes query-scoped remote dynamic filter subscriptions when a remote read stream is done.
struct RemoteDynFilterGuardedStream {
    stream: SendableRecordBatchStream,
    cleanup: RemoteDynFilterRegistrationGuard,
}

impl RecordBatchStream for RemoteDynFilterGuardedStream {
    fn name(&self) -> &str {
        self.stream.name()
    }

    fn schema(&self) -> datatypes::schema::SchemaRef {
        self.stream.schema()
    }

    fn output_ordering(&self) -> Option<&[OrderOption]> {
        self.stream.output_ordering()
    }

    fn metrics(&self) -> Option<RecordBatchMetrics> {
        self.stream.metrics()
    }
}

impl Stream for RemoteDynFilterGuardedStream {
    type Item = common_recordbatch::error::Result<RecordBatch>;

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.stream).poll_next(cx) {
            Poll::Ready(None) => {
                self.cleanup.cleanup_once();
                Poll::Ready(None)
            }
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use std::time::Duration;

    use api::v1::region::{
        RemoteDynFilterRequest, RemoteDynFilterUnregister, RemoteDynFilterUpdate,
        remote_dyn_filter_request,
    };
    use common_query::request::{
        DynFilterPayload, INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY,
        InitialDynFilterReg, InitialDynFilterRegs, InitialDynFilterSnapshot,
    };
    use common_recordbatch::RecordBatches;
    use datafusion::arrow::datatypes::Schema as ArrowSchema;
    use datafusion::physical_plan::PhysicalExpr;
    use datafusion::physical_plan::expressions::{DynamicFilterPhysicalExpr, lit as physical_lit};
    use datafusion_common::DFSchema;
    use datafusion_expr::EmptyRelation;
    use datatypes::prelude::{ConcreteDataType, VectorRef};
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::Int32Vector;
    use futures_util::StreamExt;
    use query::options::QUERY_ENABLE_REMOTE_DYNAMIC_FILTER_PUSHDOWN;
    use session::context::QueryContext;
    use session::hints::REMOTE_QUERY_ID_EXTENSION_KEY;
    use session::query_id::QueryId;
    use store_api::storage::RegionId;

    use super::*;
    use crate::region_server::registrations::{
        REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES, RemoteDynFilterRegistry,
    };
    use crate::tests::mock_region_server;

    #[derive(Debug, Clone)]
    struct RegisteredDynFilterSnapshot {
        filter_id: RemoteDynFilterId,
        child_exprs_datafusion_proto: Vec<Vec<u8>>,
        subscriber_regions: HashSet<RegionId>,
    }

    fn query_regs(
        regs_by_query: &RemoteDynFilterRegistry,
        query_id: &QueryId,
    ) -> Option<HashMap<RemoteDynFilterId, RegisteredDynFilterSnapshot>> {
        regs_by_query.inspect_query(query_id, |query_regs| {
            query_regs
                .iter()
                .map(|(filter_id, registered)| {
                    (
                        filter_id.clone(),
                        RegisteredDynFilterSnapshot {
                            filter_id: registered.filter_id.clone(),
                            child_exprs_datafusion_proto: registered
                                .child_exprs_datafusion_proto
                                .clone(),
                            subscriber_regions: registered.subscriber_regions.clone(),
                        },
                    )
                })
                .collect()
        })
    }

    fn test_remote_query_id() -> QueryId {
        QueryId::new()
    }

    fn test_remote_dyn_filter_region_id() -> RegionId {
        RegionId::new(1024, 7)
    }

    fn single_value_stream() -> common_recordbatch::SendableRecordBatchStream {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "v",
            ConcreteDataType::int32_datatype(),
            false,
        )]));
        let values: VectorRef = Arc::new(Int32Vector::from_slice([1]));
        let batch = common_recordbatch::RecordBatch::new(schema.clone(), vec![values]).unwrap();
        RecordBatches::try_new(schema, vec![batch])
            .unwrap()
            .as_stream()
    }

    fn empty_logical_plan() -> LogicalPlan {
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        })
    }

    fn query_context_with_initial_regs(query_id: QueryId) -> QueryContext {
        let mut query_ctx = QueryContext::with("greptime", "public");
        query_ctx.set_extension(REMOTE_QUERY_ID_EXTENSION_KEY, query_id.to_string());
        query_ctx.set_extension(
            INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY,
            InitialDynFilterRegs::new(vec![InitialDynFilterReg::new("filter-1", vec![])])
                .to_extension_value()
                .unwrap(),
        );
        query_ctx
    }

    #[test]
    fn remote_dyn_filter_receiver_plan_respects_disabled_query_option() {
        let mock_region_server = mock_region_server();
        let query_id = test_remote_query_id();
        let mut query_ctx = query_context_with_initial_regs(query_id);
        query_ctx.set_extension(QUERY_ENABLE_REMOTE_DYNAMIC_FILTER_PUSHDOWN, "false");

        let plan = remote_dyn_filter_receiver_plan(
            &Arc::downgrade(&mock_region_server.inner),
            empty_logical_plan(),
            Arc::new(query_ctx),
        );

        assert!(matches!(plan, LogicalPlan::EmptyRelation(_)));
    }

    #[test]
    fn remote_dyn_filter_cleanup_registration_respects_disabled_query_option() {
        let mock_region_server = mock_region_server();
        let query_id = test_remote_query_id();
        let mut query_ctx = query_context_with_initial_regs(query_id);
        query_ctx.set_extension(QUERY_ENABLE_REMOTE_DYNAMIC_FILTER_PUSHDOWN, "false");
        let query_ctx = Arc::new(query_ctx);

        let cleanup = mock_region_server.register_initial_remote_dyn_filter_cleanup(
            &query_ctx,
            test_remote_dyn_filter_region_id(),
        );

        assert!(cleanup.is_none());
        assert!(
            mock_region_server
                .inner
                .initial_remote_dyn_filter_registrations
                .inspect_query(&query_id, |_| ())
                .is_none()
        );
    }

    #[test]
    fn initial_dyn_filter_regs_can_be_read_from_query_context() {
        let mut query_ctx = QueryContext::with("greptime", "public");
        query_ctx.set_extension(
            INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY,
            InitialDynFilterRegs::new(vec![InitialDynFilterReg::new(
                "filter-1",
                vec![vec![1, 2, 3]],
            )])
            .to_extension_value()
            .unwrap(),
        );

        let regs = initial_dyn_filter_regs_from_query_ctx(&Arc::new(query_ctx)).unwrap();

        assert_eq!(regs.regs.len(), 1);
        assert_eq!(regs.regs[0].filter_id, "filter-1");
    }

    #[test]
    fn initial_dyn_filter_regs_from_query_context_rejects_duplicate_filter_ids() {
        let mut query_ctx = QueryContext::with("greptime", "public");
        query_ctx.set_extension(
            INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY,
            InitialDynFilterRegs::new(vec![
                InitialDynFilterReg::new("filter-1", vec![vec![1, 2, 3]]),
                InitialDynFilterReg::new("filter-1", vec![vec![4, 5, 6]]),
            ])
            .to_extension_value()
            .unwrap(),
        );

        let regs = initial_dyn_filter_regs_from_query_ctx(&Arc::new(query_ctx));

        assert!(regs.is_none());
    }

    #[test]
    fn register_initial_dyn_filter_regs_creates_query_scoped_entries() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let regs = InitialDynFilterRegs::new(vec![
            InitialDynFilterReg::new("filter-1", vec![vec![1, 2, 3]]),
            InitialDynFilterReg::new("filter-2", vec![vec![4, 5, 6]]),
        ]);
        let query_id = test_remote_query_id();
        let region_id = test_remote_dyn_filter_region_id();

        let registered_filter_ids =
            register_initial_dyn_filter_regs(&regs_by_query, &query_id, region_id, &regs);

        let query_regs = query_regs(&regs_by_query, &query_id).unwrap();
        assert_eq!(query_regs.len(), 2);
        assert_eq!(
            registered_filter_ids,
            vec![
                RemoteDynFilterId::new("filter-1"),
                RemoteDynFilterId::new("filter-2")
            ]
        );
        let registered = query_regs.get(&RemoteDynFilterId::new("filter-1")).unwrap();
        assert_eq!(registered.filter_id, RemoteDynFilterId::new("filter-1"));
        assert_eq!(registered.child_exprs_datafusion_proto, vec![vec![1, 2, 3]]);
        assert_eq!(registered.subscriber_regions.len(), 1);
        assert!(registered.subscriber_regions.contains(&region_id));
    }

    #[test]
    fn register_initial_dyn_filter_regs_same_region_duplicate_is_idempotent() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let regs = InitialDynFilterRegs::new(vec![InitialDynFilterReg::new(
            "filter-1",
            vec![vec![1, 2, 3]],
        )]);
        let query_id = test_remote_query_id();
        let region_id = test_remote_dyn_filter_region_id();

        let first = register_initial_dyn_filter_regs(&regs_by_query, &query_id, region_id, &regs);
        let duplicate =
            register_initial_dyn_filter_regs(&regs_by_query, &query_id, region_id, &regs);

        let query_regs = query_regs(&regs_by_query, &query_id).unwrap();
        assert_eq!(query_regs.len(), 1);
        assert_eq!(first, vec![RemoteDynFilterId::new("filter-1")]);
        assert!(duplicate.is_empty());
        let registered = query_regs.get(&RemoteDynFilterId::new("filter-1")).unwrap();
        assert_eq!(registered.subscriber_regions.len(), 1);
        assert!(registered.subscriber_regions.contains(&region_id));
    }

    #[test]
    fn register_initial_dyn_filter_regs_ignores_invalid_duplicate_payload_set() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let regs = InitialDynFilterRegs::new(vec![
            InitialDynFilterReg::new("filter-1", vec![vec![1, 2, 3]]),
            InitialDynFilterReg::new("filter-1", vec![vec![4, 5, 6]]),
        ]);
        let query_id = test_remote_query_id();
        let region_id = test_remote_dyn_filter_region_id();

        register_initial_dyn_filter_regs(&regs_by_query, &query_id, region_id, &regs);

        assert!(query_regs(&regs_by_query, &query_id).is_none());
    }

    #[test]
    fn register_initial_dyn_filter_regs_tracks_different_region_subscribers_for_same_filter() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let regs = InitialDynFilterRegs::new(vec![InitialDynFilterReg::new(
            "filter-1",
            vec![vec![1, 2, 3]],
        )]);
        let query_id = test_remote_query_id();
        let first_region_id = RegionId::new(1024, 7);
        let second_region_id = RegionId::new(1024, 8);

        register_initial_dyn_filter_regs(&regs_by_query, &query_id, first_region_id, &regs);
        register_initial_dyn_filter_regs(&regs_by_query, &query_id, second_region_id, &regs);

        let query_regs = query_regs(&regs_by_query, &query_id).unwrap();
        assert_eq!(query_regs.len(), 1);
        let registered = query_regs.get(&RemoteDynFilterId::new("filter-1")).unwrap();
        assert_eq!(registered.subscriber_regions.len(), 2);
        assert!(registered.subscriber_regions.contains(&first_region_id));
        assert!(registered.subscriber_regions.contains(&second_region_id));
    }

    #[test]
    fn remove_initial_dyn_filter_regs_removes_registered_filter_entries() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let query_id = test_remote_query_id();
        let other_query_id = test_remote_query_id();
        let region_id = test_remote_dyn_filter_region_id();

        let registered_filter_ids = register_initial_dyn_filter_regs(
            &regs_by_query,
            &query_id,
            region_id,
            &InitialDynFilterRegs::new(vec![InitialDynFilterReg::new(
                "filter-1",
                vec![vec![1, 2, 3]],
            )]),
        );
        register_initial_dyn_filter_regs(
            &regs_by_query,
            &other_query_id,
            region_id,
            &InitialDynFilterRegs::new(vec![InitialDynFilterReg::new(
                "filter-2",
                vec![vec![4, 5, 6]],
            )]),
        );

        remove_initial_dyn_filter_regs(
            &regs_by_query,
            &query_id,
            region_id,
            &registered_filter_ids,
        );

        assert!(query_regs(&regs_by_query, &query_id).is_none());
        let other_query_regs = query_regs(&regs_by_query, &other_query_id).unwrap();
        assert_eq!(other_query_regs.len(), 1);
    }

    #[test]
    fn remove_initial_dyn_filter_regs_keeps_other_subscribers() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let query_id = test_remote_query_id();
        let regs = InitialDynFilterRegs::new(vec![InitialDynFilterReg::new(
            "filter-1",
            vec![vec![1, 2, 3]],
        )]);
        let first_region_id = RegionId::new(1024, 7);
        let second_region_id = RegionId::new(1024, 8);

        let first_subscription =
            register_initial_dyn_filter_regs(&regs_by_query, &query_id, first_region_id, &regs);
        register_initial_dyn_filter_regs(&regs_by_query, &query_id, second_region_id, &regs);

        remove_initial_dyn_filter_regs(
            &regs_by_query,
            &query_id,
            first_region_id,
            &first_subscription,
        );

        let query_regs = query_regs(&regs_by_query, &query_id).unwrap();
        assert_eq!(query_regs.len(), 1);
        let registered = query_regs.get(&RemoteDynFilterId::new("filter-1")).unwrap();
        assert_eq!(registered.subscriber_regions.len(), 1);
        assert!(registered.subscriber_regions.contains(&second_region_id));
    }

    #[tokio::test]
    async fn test_handle_remote_dyn_filter_request_requires_query_id() {
        let mock_region_server = mock_region_server();

        let err = mock_region_server
            .handle_remote_dyn_filter_request(&RemoteDynFilterRequest {
                query_id: String::new(),
                action: Some(remote_dyn_filter_request::Action::Unregister(
                    RemoteDynFilterUnregister {
                        filter_id: "filter-1".to_string(),
                    },
                )),
            })
            .await
            .unwrap_err();

        assert_matches!(
            err,
            crate::error::Error::MissingRequiredField { ref name, .. } if name == "query_id"
        );
    }

    #[tokio::test]
    async fn test_handle_remote_dyn_filter_request_requires_action() {
        let mock_region_server = mock_region_server();

        let err = mock_region_server
            .handle_remote_dyn_filter_request(&RemoteDynFilterRequest {
                query_id: test_remote_query_id().to_string(),
                action: None,
            })
            .await
            .unwrap_err();

        assert_matches!(
            err,
            crate::error::Error::MissingRequiredField { ref name, .. } if name == "action"
        );
    }

    #[tokio::test]
    async fn test_handle_remote_dyn_filter_update_requires_filter_id() {
        let mock_region_server = mock_region_server();

        let err = mock_region_server
            .handle_remote_dyn_filter_request(&RemoteDynFilterRequest {
                query_id: test_remote_query_id().to_string(),
                action: Some(remote_dyn_filter_request::Action::Update(
                    RemoteDynFilterUpdate {
                        filter_id: String::new(),
                        payload: vec![1],
                        generation: 1,
                        is_complete: false,
                        typed_payload: None,
                    },
                )),
            })
            .await
            .unwrap_err();

        assert_matches!(
            err,
            crate::error::Error::MissingRequiredField { ref name, .. } if name == "filter_id"
        );
    }

    #[tokio::test]
    async fn test_handle_remote_dyn_filter_update_requires_payload() {
        let mock_region_server = mock_region_server();

        let err = mock_region_server
            .handle_remote_dyn_filter_request(&RemoteDynFilterRequest {
                query_id: test_remote_query_id().to_string(),
                action: Some(remote_dyn_filter_request::Action::Update(
                    RemoteDynFilterUpdate {
                        filter_id: "filter-1".to_string(),
                        payload: Vec::new(),
                        generation: 1,
                        is_complete: false,
                        typed_payload: None,
                    },
                )),
            })
            .await
            .unwrap_err();

        assert_matches!(
            err,
            crate::error::Error::MissingRequiredField { ref name, .. } if name == "payload"
        );
    }

    #[test]
    fn test_apply_remote_dyn_filter_update_missing_registration() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let query_id = test_remote_query_id();
        let filter_id = RemoteDynFilterId::new("filter-1");

        let outcome =
            apply_remote_dyn_filter_update(&regs_by_query, &query_id, &filter_id, &[1], 1, false);
        assert_eq!(outcome, RemoteDynFilterUpdateOutcome::MissingRegistration);

        let outcome = unregister_remote_dyn_filter(&regs_by_query, &query_id, &filter_id);
        assert_eq!(outcome, RemoteDynFilterUpdateOutcome::MissingRegistration);
    }

    #[test]
    fn test_apply_remote_dyn_filter_update_buffering_before_scan() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let regs = InitialDynFilterRegs::new(vec![InitialDynFilterReg::new(
            "filter-1",
            vec![vec![1, 2, 3]],
        )]);
        let query_id = test_remote_query_id();
        let filter_id = RemoteDynFilterId::new("filter-1");

        register_initial_dyn_filter_regs(
            &regs_by_query,
            &query_id,
            test_remote_dyn_filter_region_id(),
            &regs,
        );

        // First update with generation 1: should be Buffered (no runtime installed yet)
        let outcome =
            apply_remote_dyn_filter_update(&regs_by_query, &query_id, &filter_id, &[1], 1, false);
        assert_eq!(outcome, RemoteDynFilterUpdateOutcome::Buffered);

        // Update with generation 0: should be Stale (older than pending generation 1)
        let outcome =
            apply_remote_dyn_filter_update(&regs_by_query, &query_id, &filter_id, &[2], 0, false);
        assert_eq!(outcome, RemoteDynFilterUpdateOutcome::Stale);

        // Update with generation 1 again: should be Idempotent (same generation)
        let outcome =
            apply_remote_dyn_filter_update(&regs_by_query, &query_id, &filter_id, &[3], 1, false);
        assert_eq!(outcome, RemoteDynFilterUpdateOutcome::Idempotent);
    }

    fn datafusion_payload_bytes(expr: Arc<dyn PhysicalExpr>) -> Vec<u8> {
        match DynFilterPayload::from_datafusion_expr(&expr, REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES)
            .unwrap()
        {
            DynFilterPayload::Datafusion(bytes) => bytes,
            _ => unreachable!(
                "DynFilterPayload::from_datafusion_expr only returns datafusion payloads"
            ),
        }
    }

    fn empty_arrow_schema() -> ArrowSchema {
        ArrowSchema::empty()
    }

    fn register_empty_remote_dyn_filter(
        regs_by_query: &RemoteDynFilterRegistry,
        query_id: &QueryId,
    ) -> Vec<RemoteDynFilterId> {
        register_empty_remote_dyn_filter_for_region(
            regs_by_query,
            query_id,
            test_remote_dyn_filter_region_id(),
        )
    }

    fn register_empty_remote_dyn_filter_for_region(
        regs_by_query: &RemoteDynFilterRegistry,
        query_id: &QueryId,
        region_id: RegionId,
    ) -> Vec<RemoteDynFilterId> {
        register_initial_dyn_filter_regs(
            regs_by_query,
            query_id,
            region_id,
            &InitialDynFilterRegs::new(vec![InitialDynFilterReg::new("filter-1", vec![])]),
        )
    }

    #[test]
    fn initial_remote_dyn_filter_snapshot_initializes_runtime_filter() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let query_id = test_remote_query_id();
        let payload = DynFilterPayload::Datafusion(datafusion_payload_bytes(physical_lit(false)));
        let regs = InitialDynFilterRegs::new(vec![
            InitialDynFilterReg::new("filter-1", vec![])
                .with_initial_snapshot(InitialDynFilterSnapshot::new(payload, 7, false)),
        ]);

        register_initial_dyn_filter_regs(
            &regs_by_query,
            &query_id,
            test_remote_dyn_filter_region_id(),
            &regs,
        );
        let exprs = remote_dyn_filter_exprs_for_initial_regs(
            &regs_by_query,
            &query_id,
            &regs,
            &empty_arrow_schema(),
        );

        assert_eq!(exprs.len(), 1);
        assert_eq!(format!("{}", exprs[0]), "DynamicFilter [ false ]");
    }

    fn only_remote_dyn_filter(
        regs_by_query: &RemoteDynFilterRegistry,
        query_id: &QueryId,
    ) -> Arc<DynamicFilterPhysicalExpr> {
        let initial_regs =
            InitialDynFilterRegs::new(vec![InitialDynFilterReg::new("filter-1", vec![])]);
        let exprs = remote_dyn_filter_exprs_for_initial_regs(
            regs_by_query,
            query_id,
            &initial_regs,
            &empty_arrow_schema(),
        );
        assert_eq!(1, exprs.len());
        let expr = exprs.into_iter().next().unwrap();
        let expr = expr as Arc<dyn std::any::Any + Send + Sync>;
        expr.downcast::<DynamicFilterPhysicalExpr>().unwrap()
    }

    #[test]
    fn test_remote_dyn_filter_rejects_oversized_payload_before_buffering() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let query_id = test_remote_query_id();
        let filter_id = RemoteDynFilterId::new("filter-1");
        register_empty_remote_dyn_filter(&regs_by_query, &query_id);

        let oversized = vec![0; REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES + 1];
        let outcome = apply_remote_dyn_filter_update(
            &regs_by_query,
            &query_id,
            &filter_id,
            &oversized,
            1,
            false,
        );
        assert_eq!(outcome, RemoteDynFilterUpdateOutcome::PayloadTooLarge);

        // The rejected generation must not become the pending generation.
        let outcome =
            apply_remote_dyn_filter_update(&regs_by_query, &query_id, &filter_id, &[1], 0, false);
        assert_eq!(outcome, RemoteDynFilterUpdateOutcome::Buffered);
    }

    #[test]
    fn test_remote_dyn_filter_generation_zero_applies_as_first_update() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let query_id = test_remote_query_id();
        let filter_id = RemoteDynFilterId::new("filter-1");
        register_empty_remote_dyn_filter(&regs_by_query, &query_id);

        // Installing the wrapper before the update exercises the runtime apply path directly.
        let dyn_filter = only_remote_dyn_filter(&regs_by_query, &query_id);
        let payload = datafusion_payload_bytes(physical_lit(false));
        let outcome = apply_remote_dyn_filter_update(
            &regs_by_query,
            &query_id,
            &filter_id,
            &payload,
            0,
            false,
        );

        assert_eq!(outcome, RemoteDynFilterUpdateOutcome::Applied);
        assert!(dyn_filter.snapshot_generation() > 1);
        assert_eq!(format!("{}", dyn_filter.current().unwrap()), "false");
    }

    #[test]
    fn test_buffered_update_applies_when_scan_installs_wrapper() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let query_id = test_remote_query_id();
        let filter_id = RemoteDynFilterId::new("filter-1");
        register_empty_remote_dyn_filter(&regs_by_query, &query_id);

        let payload = datafusion_payload_bytes(physical_lit(false));
        let outcome = apply_remote_dyn_filter_update(
            &regs_by_query,
            &query_id,
            &filter_id,
            &payload,
            1,
            false,
        );
        assert_eq!(outcome, RemoteDynFilterUpdateOutcome::Buffered);

        let dyn_filter = only_remote_dyn_filter(&regs_by_query, &query_id);
        assert!(dyn_filter.snapshot_generation() > 1);
        assert_eq!(format!("{}", dyn_filter.current().unwrap()), "false");
    }

    #[tokio::test]
    async fn test_unregister_completes_installed_remote_dyn_filter_without_relaxing() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let query_id = test_remote_query_id();
        let filter_id = RemoteDynFilterId::new("filter-1");
        register_empty_remote_dyn_filter(&regs_by_query, &query_id);

        let dyn_filter = only_remote_dyn_filter(&regs_by_query, &query_id);
        let payload = datafusion_payload_bytes(physical_lit(false));
        let outcome = apply_remote_dyn_filter_update(
            &regs_by_query,
            &query_id,
            &filter_id,
            &payload,
            1,
            false,
        );
        assert_eq!(outcome, RemoteDynFilterUpdateOutcome::Applied);
        assert_eq!(format!("{}", dyn_filter.current().unwrap()), "false");

        let outcome = unregister_remote_dyn_filter(&regs_by_query, &query_id, &filter_id);
        assert_eq!(outcome, RemoteDynFilterUpdateOutcome::Applied);
        assert!(query_regs(&regs_by_query, &query_id).is_none());
        tokio::time::timeout(Duration::from_secs(1), dyn_filter.wait_complete())
            .await
            .unwrap();
        assert_eq!(format!("{}", dyn_filter.current().unwrap()), "false");
    }

    #[tokio::test]
    async fn test_remote_dyn_filter_unregister_removes_all_region_subscribers_for_filter() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let query_id = test_remote_query_id();
        let filter_id = RemoteDynFilterId::new("filter-1");
        let first_region_id = RegionId::new(1024, 7);
        let second_region_id = RegionId::new(1024, 8);
        let regs = InitialDynFilterRegs::new(vec![InitialDynFilterReg::new("filter-1", vec![])]);

        let first_subscription =
            register_initial_dyn_filter_regs(&regs_by_query, &query_id, first_region_id, &regs);
        let second_subscription =
            register_initial_dyn_filter_regs(&regs_by_query, &query_id, second_region_id, &regs);
        let dyn_filter = only_remote_dyn_filter(&regs_by_query, &query_id);

        let payload = datafusion_payload_bytes(physical_lit(false));
        let outcome = apply_remote_dyn_filter_update(
            &regs_by_query,
            &query_id,
            &filter_id,
            &payload,
            1,
            false,
        );
        assert_eq!(outcome, RemoteDynFilterUpdateOutcome::Applied);

        let outcome = unregister_remote_dyn_filter(&regs_by_query, &query_id, &filter_id);
        assert_eq!(outcome, RemoteDynFilterUpdateOutcome::Applied);
        assert!(query_regs(&regs_by_query, &query_id).is_none());
        tokio::time::timeout(Duration::from_secs(1), dyn_filter.wait_complete())
            .await
            .unwrap();
        assert_eq!(format!("{}", dyn_filter.current().unwrap()), "false");

        // Stream-local cleanup may run after FE's peer-deduplicated unregister. It should be benign.
        remove_initial_dyn_filter_regs(
            &regs_by_query,
            &query_id,
            first_region_id,
            &first_subscription,
        );
        remove_initial_dyn_filter_regs(
            &regs_by_query,
            &query_id,
            second_region_id,
            &second_subscription,
        );
        assert!(query_regs(&regs_by_query, &query_id).is_none());
    }

    #[test]
    fn test_remote_dyn_filter_unregister_keeps_other_filters_for_same_query() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let query_id = test_remote_query_id();
        let region_id = test_remote_dyn_filter_region_id();

        register_initial_dyn_filter_regs(
            &regs_by_query,
            &query_id,
            region_id,
            &InitialDynFilterRegs::new(vec![InitialDynFilterReg::new("filter-1", vec![])]),
        );
        register_initial_dyn_filter_regs(
            &regs_by_query,
            &query_id,
            region_id,
            &InitialDynFilterRegs::new(vec![InitialDynFilterReg::new("filter-2", vec![])]),
        );

        let outcome = unregister_remote_dyn_filter(
            &regs_by_query,
            &query_id,
            &RemoteDynFilterId::new("filter-1"),
        );
        assert_eq!(outcome, RemoteDynFilterUpdateOutcome::Applied);

        let query_regs = query_regs(&regs_by_query, &query_id).unwrap();
        assert!(!query_regs.contains_key(&RemoteDynFilterId::new("filter-1")));
        assert!(query_regs.contains_key(&RemoteDynFilterId::new("filter-2")));
    }

    #[tokio::test]
    async fn test_remote_dyn_filter_guarded_stream_removes_on_eof() {
        let mock_region_server = mock_region_server();
        let query_id = test_remote_query_id();
        let region_id = test_remote_dyn_filter_region_id();
        let registered_filter_ids = register_empty_remote_dyn_filter(
            &mock_region_server
                .inner
                .initial_remote_dyn_filter_registrations,
            &query_id,
        );
        let cleanup = RemoteDynFilterRegistrationGuard::new(
            mock_region_server.clone(),
            query_id,
            region_id,
            registered_filter_ids,
        );

        let stream = wrap_remote_dyn_filter_guarded_stream(single_value_stream(), cleanup);
        let mut pinned = Box::pin(stream);
        while pinned.next().await.is_some() {}

        assert!(
            mock_region_server
                .inner
                .initial_remote_dyn_filter_registrations
                .inspect_query(&query_id, |_| ())
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_remote_dyn_filter_multi_region_subscription_cleanup() {
        let regs_by_query = RemoteDynFilterRegistry::new();
        let regs = InitialDynFilterRegs::new(vec![InitialDynFilterReg::new("filter-1", vec![])]);
        let query_id = test_remote_query_id();
        let first_region_id = RegionId::new(1024, 7);
        let second_region_id = RegionId::new(1024, 8);

        // Register the same logical filter for two regions on the same datanode.
        let first_subscription =
            register_initial_dyn_filter_regs(&regs_by_query, &query_id, first_region_id, &regs);
        let second_subscription =
            register_initial_dyn_filter_regs(&regs_by_query, &query_id, second_region_id, &regs);
        let dyn_filter = only_remote_dyn_filter(&regs_by_query, &query_id);

        // Verify only one filter entry exists and both region subscribers are tracked explicitly.
        {
            let query_regs = query_regs(&regs_by_query, &query_id).unwrap();
            assert_eq!(query_regs.len(), 1);
            let registered = query_regs.get(&RemoteDynFilterId::new("filter-1")).unwrap();
            assert_eq!(registered.subscriber_regions.len(), 2);
            assert!(registered.subscriber_regions.contains(&first_region_id));
            assert!(registered.subscriber_regions.contains(&second_region_id));
        }

        // One region cleanup should not drop the entry while another region is subscribed.
        remove_initial_dyn_filter_regs(
            &regs_by_query,
            &query_id,
            first_region_id,
            &first_subscription,
        );
        assert!(query_regs(&regs_by_query, &query_id).is_some());
        assert!(
            tokio::time::timeout(Duration::from_millis(50), dyn_filter.wait_complete())
                .await
                .is_err()
        );
        {
            let query_regs = query_regs(&regs_by_query, &query_id).unwrap();
            let registered = query_regs.get(&RemoteDynFilterId::new("filter-1")).unwrap();
            assert_eq!(registered.subscriber_regions.len(), 1);
            assert!(registered.subscriber_regions.contains(&second_region_id));
        }

        // Last region cleanup should drop the entry.
        remove_initial_dyn_filter_regs(
            &regs_by_query,
            &query_id,
            second_region_id,
            &second_subscription,
        );
        assert!(query_regs(&regs_by_query, &query_id).is_none());
        tokio::time::timeout(Duration::from_secs(1), dyn_filter.wait_complete())
            .await
            .unwrap();
    }
}
