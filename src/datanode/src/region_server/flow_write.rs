// Copyright 2023-2026 GrepTime Inc.
//
// This file is part of the GreptimeDB Enterprise Edition and is licensed under
// the GreptimeDB Enterprise License. You may not use this file except in
// compliance with that license. A copy of the license is available at the root
// of this repository in the file LICENSE-ENTERPRISE.
//
// Unless required by applicable law or agreed to in writing, this software is
// distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied.

//! Datanode receiver for tracked Flow writes.
//!
//! A single immutable operation per receiver-issued capability: the first
//! admitted `Write` becomes the owner and executes the Inserts exactly once on
//! the region engines; concurrent or retried identical writers join the
//! owner's completion. Dropping a waiter (for example a lost RPC response)
//! never cancels the engine work, and closing a capability rejects later
//! writes with that capability. If the owner's outcome becomes unknown (a
//! child ingest worker was lost), no positive terminal completion is
//! published: waiters observe `WriterLost`; the capability admits no second
//! owner, so no overlapping engine write can be produced.

use std::sync::Arc;

use api::v1::region::RegionRequest as RegionRequestV1;
use api::v1::region::region_request;
use common_error::ext::ErrorExt;
use common_grpc::flow_write::{Admission, COMMAND_KEY, Command, Reply, TerminalError};
use common_runtime::runtime::RuntimeTrait;
use common_telemetry::tracing_context::{FutureExt, TracingContext};
use futures_util::future::join_all;
use prost::Message;
use servers::error::{InvalidQuerySnafu, Result as ServerResult};
use snafu::OptionExt;

use crate::region_server::RegionServer;

/// Payload of a tracked write: encoded Inserts rows (exact bytes, so floats
/// keep bit equality even for NaN), schemas compared by protobuf equality, and
/// semantic header identity (dbname and command-free query context). Retrying
/// with different business data or context is rejected instead of joining.
pub(crate) type TrackedPayload = (
    Vec<u8>,
    Vec<Option<Vec<api::v1::ColumnSchema>>>,
    String,
    Option<api::v1::QueryContext>,
);

fn tracked_payload(request: &RegionRequestV1) -> ServerResult<TrackedPayload> {
    let body = request.body.as_ref().context(InvalidQuerySnafu {
        reason: "Expecting non-empty region request body.",
    })?;
    if !matches!(body, region_request::Body::Inserts(_)) {
        return Err(servers::error::Error::InvalidQuery {
            reason: "Tracked Flow write supports Inserts requests only.".to_string(),
            location: snafu::Location::new(file!(), line!(), column!()),
        });
    }
    // Oneof is not `prost::Message`; the variant message itself is. Schema
    // options contain unordered maps, so compare schemas separately rather than
    // letting their wire ordering affect retries; retain encoded rows for NaN.
    let (bytes, schemas) = match body {
        region_request::Body::Inserts(inserts) => {
            let mut inserts = inserts.clone();
            let schemas = inserts
                .requests
                .iter_mut()
                .map(|request| {
                    request
                        .rows
                        .as_mut()
                        .map(|rows| std::mem::take(&mut rows.schema))
                })
                .collect();
            (inserts.encode_to_vec(), schemas)
        }
        _ => unreachable!("checked above"),
    };
    let header = request.header.as_ref();
    let mut query_context = header.and_then(|header| header.query_context.clone());
    if let Some(context) = query_context.as_mut() {
        // The command extension differs per control request, not per business.
        context.extensions.remove(COMMAND_KEY);
    }
    Ok((
        bytes,
        schemas,
        header
            .map(|header| header.dbname.clone())
            .unwrap_or_default(),
        query_context,
    ))
}

fn ensure_empty_control_body(request: &RegionRequestV1) -> ServerResult<()> {
    match request.body.as_ref() {
        Some(region_request::Body::Inserts(inserts)) if inserts.requests.is_empty() => Ok(()),
        _ => Err(servers::error::Error::InvalidQuery {
            reason: "Tracked Flow write control requests must carry an empty Inserts body."
                .to_string(),
            location: snafu::Location::new(file!(), line!(), column!()),
        }),
    }
}

fn registry_error(error: common_grpc::flow_write::RegistryError) -> servers::error::Error {
    servers::error::Error::UnexpectedResult {
        reason: format!("Tracked Flow write registry rejected request: {error:?}"),
        location: snafu::Location::new(file!(), line!(), column!()),
    }
}

fn response(reply: Reply) -> ServerResult<api::v1::region::RegionResponse> {
    let encoded =
        serde_json::to_vec(&reply).map_err(|error| servers::error::Error::UnexpectedResult {
            reason: format!("Failed to encode tracked Flow write reply: {error}"),
            location: snafu::Location::new(file!(), line!(), column!()),
        })?;
    let mut extensions = std::collections::HashMap::new();
    extensions.insert(COMMAND_KEY.to_string(), encoded);
    Ok(api::v1::region::RegionResponse {
        header: Some(api::v1::ResponseHeader {
            status: Some(api::v1::Status {
                status_code: common_error::status_code::StatusCode::Success as _,
                ..Default::default()
            }),
        }),
        affected_rows: 0,
        extensions,
        metadata: Vec::new(),
        failures: Vec::new(),
    })
}

impl RegionServer {
    pub(crate) async fn handle_flow_write_request(
        &self,
        request: RegionRequestV1,
    ) -> ServerResult<api::v1::region::RegionResponse> {
        let command = request
            .header
            .as_ref()
            .and_then(|header| header.query_context.as_ref())
            .and_then(|context| context.extensions.get(COMMAND_KEY))
            .and_then(|value| Command::decode(value).ok());
        let command = match command {
            Some(command) => command,
            None => {
                // The extension exists but is not a decodable command: reject
                // before any ordinary DML dispatch.
                return Err(InvalidQuerySnafu {
                    reason: "Malformed tracked Flow write command.",
                }
                .build());
            }
        };

        match command {
            Command::Open { nonce } => {
                ensure_empty_control_body(&request)?;
                let capability = self
                    .flow_write_registry
                    .open(&nonce)
                    .map_err(registry_error)?;
                response(Reply::Opened { capability })
            }
            Command::Close { capability } => {
                ensure_empty_control_body(&request)?;
                self.flow_write_registry
                    .close(&capability)
                    .await
                    .map_err(registry_error)?;
                response(Reply::Closed)
            }
            Command::Write { capability } => {
                let payload = tracked_payload(&request)?;
                let admission = self
                    .flow_write_registry
                    .admit(&capability, payload)
                    .map_err(registry_error)?;
                let reply = match admission {
                    Admission::Start(owner) => {
                        // Subscribe before the owner sender moves into the
                        // spawned task: this caller stays a plain waiter whose
                        // drop cannot cancel the engine work.
                        let receiver = owner.subscribe();
                        let server = self.clone();
                        let body = request.body;
                        let header = request.header;
                        self.inner.runtime.spawn(async move {
                            let completion = server
                                .run_tracked_write(header, body)
                                .trace(TracingContext::from_current_span().attach(
                                    common_telemetry::tracing::info_span!(
                                        "RegionServer::tracked_flow_write"
                                    ),
                                ))
                                .await;
                            match completion {
                                Some(reply) => {
                                    let _ = owner.send(Some(Arc::new(reply)));
                                }
                                None => {
                                    // Unknown worker outcome: no positive terminal
                                    // result is published. Dropping the sender lets
                                    // waiters observe `WriterLost`.
                                    drop(owner);
                                }
                            }
                        });
                        common_grpc::flow_write::wait_completion(receiver)
                            .await
                            .map_err(registry_error)?
                    }
                    Admission::Join(waiter) => common_grpc::flow_write::wait_completion(waiter)
                        .await
                        .map_err(registry_error)?,
                };
                response((*reply).clone())
            }
        }
    }

    /// Executes one admitted tracked write. Returns `None` only when the
    /// outcome is unknown (a child worker was lost); malformed request bodies
    /// are terminal because no child was ever dispatched.
    async fn run_tracked_write(
        &self,
        header: Option<api::v1::region::RegionRequestHeader>,
        body: Option<region_request::Body>,
    ) -> Option<Reply> {
        let requests =
            match body.map(store_api::region_request::RegionRequest::try_from_request_body) {
                Some(Ok(requests)) => requests,
                Some(Err(error)) => {
                    return Some(Reply::Complete {
                        rows: 0,
                        cost: 0,
                        metrics: None,
                        error: Some(TerminalError {
                            code: error.status_code() as u32,
                            message: error.output_msg(),
                        }),
                    });
                }
                // `None` is unreachable here: the Write path already rejected an
                // empty body before admission. Treat it as unknown rather than
                // inventing a terminal result.
                None => return None,
            };
        let tracing = header
            .as_ref()
            .map(|header| TracingContext::from_w3c(&header.tracing_context))
            .unwrap_or_default();

        let mut unknown_worker = false;
        let mut first_error: Option<crate::error::Error> = None;
        let mut affected_rows = 0usize;

        let tasks = requests.into_iter().map(|(region_id, request)| {
            let span = tracing.attach(common_telemetry::tracing::info_span!(
                "RegionServer::tracked_region_write",
                region_id = region_id.to_string()
            ));
            async move { self.handle_request(region_id, request).trace(span).await }
        });
        // Drain every per-region child before any terminal decision. A lost
        // ingest worker (`RuntimeJoin`) is unknown and outranks any ordinary
        // application error observed during the same drain.
        for result in join_all(tasks).await {
            match result {
                Ok(response) => affected_rows += response.affected_rows,
                Err(crate::error::Error::RuntimeJoin { .. }) => {
                    unknown_worker = true;
                }
                Err(error) if first_error.is_none() => first_error = Some(error),
                Err(_) => {}
            }
        }

        if unknown_worker {
            return None;
        }

        Some(match first_error {
            None => Reply::Complete {
                rows: affected_rows,
                cost: affected_rows,
                metrics: None,
                error: None,
            },
            Some(error) => Reply::Complete {
                rows: 0,
                cost: 0,
                metrics: None,
                error: Some(TerminalError {
                    code: error.status_code() as u32,
                    message: error.output_msg(),
                }),
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use api::region::RegionResponse;
    use api::v1::region::region_request::Body;
    use api::v1::region::{
        InsertRequest, InsertRequests, RegionRequest as Request, RegionRequestHeader,
    };
    use api::v1::{ColumnDataType, ColumnSchema, QueryContext, Row, Rows, SemanticType, Value};
    use common_grpc::flow_write::{self, Command, Reply};
    use servers::grpc::region_server::RegionServerHandler;
    use store_api::region_engine::{RegionEngine, RegionEngineRef};
    use store_api::storage::RegionId;
    use tokio::sync::Notify;
    use tokio::time::{Duration, timeout};

    use super::*;
    use crate::tests::{MockRegionEngine, mock_region_server};

    #[derive(Clone, Default)]
    struct Gate {
        entered: Arc<AtomicUsize>,
        notified: Arc<Notify>,
        open: Arc<AtomicUsize>,
        fail: bool,
    }

    impl Gate {
        async fn wait_entered(&self, count: usize) {
            timeout(Duration::from_secs(5), async {
                loop {
                    let notified = self.notified.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    if self.entered.load(Ordering::SeqCst) >= count {
                        break;
                    }
                    notified.await;
                }
            })
            .await
            .unwrap();
        }
        fn release(&self) {
            self.open.store(1, Ordering::SeqCst);
            self.notified.notify_waiters();
        }
        async fn wait_open(&self) {
            loop {
                let notified = self.notified.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.open.load(Ordering::SeqCst) != 0 {
                    break;
                }
                notified.await;
            }
        }
    }

    struct GatedEngine {
        gate: Gate,
        inner: Arc<MockRegionEngine>,
        panic_id: Option<u64>,
    }
    #[async_trait::async_trait]
    impl RegionEngine for GatedEngine {
        fn name(&self) -> &str {
            self.inner.name()
        }
        async fn handle_request(
            &self,
            id: RegionId,
            _request: store_api::region_request::RegionRequest,
        ) -> Result<RegionResponse, common_error::ext::BoxedError> {
            self.gate.entered.fetch_add(1, Ordering::SeqCst);
            self.gate.notified.notify_waiters();
            if self.panic_id == Some(id.as_u64()) {
                panic!("test ingest panic");
            }
            if self.gate.fail && id.as_u64() == 1 {
                return Err(common_error::ext::BoxedError::new(
                    crate::error::RegionNotFoundSnafu { region_id: id }.build(),
                ));
            }
            self.gate.wait_open().await;
            Ok(RegionResponse::new(1))
        }
        async fn handle_query(
            &self,
            id: RegionId,
            req: store_api::storage::ScanRequest,
        ) -> Result<store_api::region_engine::RegionScannerRef, common_error::ext::BoxedError>
        {
            self.inner.handle_query(id, req).await
        }
        async fn get_committed_sequence(
            &self,
            id: RegionId,
        ) -> Result<store_api::storage::SequenceNumber, common_error::ext::BoxedError> {
            self.inner.get_committed_sequence(id).await
        }
        async fn get_metadata(
            &self,
            id: RegionId,
        ) -> Result<store_api::metadata::RegionMetadataRef, common_error::ext::BoxedError> {
            self.inner.get_metadata(id).await
        }
        fn region_statistic(
            &self,
            id: RegionId,
        ) -> Option<store_api::region_engine::RegionStatistic> {
            self.inner.region_statistic(id)
        }
        async fn stop(&self) -> Result<(), common_error::ext::BoxedError> {
            self.inner.stop().await
        }
        fn set_region_role(
            &self,
            id: RegionId,
            role: store_api::region_engine::RegionRole,
        ) -> Result<(), common_error::ext::BoxedError> {
            self.inner.set_region_role(id, role)
        }

        fn set_region_role_with_lease(
            &self,
            id: RegionId,
            role: store_api::region_engine::RegionRole,
            sent_at: std::time::Instant,
            deadline: std::time::Instant,
        ) -> Result<bool, common_error::ext::BoxedError> {
            self.inner
                .set_region_role_with_lease(id, role, sent_at, deadline)
        }
        async fn sync_region(
            &self,
            id: RegionId,
            req: store_api::region_engine::SyncRegionFromRequest,
        ) -> Result<store_api::region_engine::SyncRegionFromResponse, common_error::ext::BoxedError>
        {
            self.inner.sync_region(id, req).await
        }
        async fn remap_manifests(
            &self,
            req: store_api::region_engine::RemapManifestsRequest,
        ) -> Result<store_api::region_engine::RemapManifestsResponse, common_error::ext::BoxedError>
        {
            self.inner.remap_manifests(req).await
        }
        async fn set_region_role_state_gracefully(
            &self,
            id: RegionId,
            state: store_api::region_engine::SettableRegionRoleState,
        ) -> Result<
            store_api::region_engine::SetRegionRoleStateResponse,
            common_error::ext::BoxedError,
        > {
            self.inner.set_region_role_state_gracefully(id, state).await
        }
        fn role(&self, id: RegionId) -> Option<store_api::region_engine::RegionRole> {
            self.inner.role(id)
        }
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    fn body(ids: &[u64], nan: bool) -> Body {
        Body::Inserts(InsertRequests {
            requests: ids
                .iter()
                .map(|id| InsertRequest {
                    region_id: *id,
                    rows: Some(Rows {
                        schema: vec![
                            ColumnSchema {
                                column_name: "ts".into(),
                                datatype: ColumnDataType::TimestampMillisecond as i32,
                                semantic_type: SemanticType::Timestamp as i32,
                                datatype_extension: None,
                                options: None,
                            },
                            ColumnSchema {
                                column_name: "v".into(),
                                datatype: ColumnDataType::Float64 as i32,
                                semantic_type: SemanticType::Field as i32,
                                datatype_extension: None,
                                options: None,
                            },
                        ],
                        rows: vec![Row {
                            values: vec![
                                Value {
                                    value_data: Some(
                                        api::v1::value::ValueData::TimestampMillisecondValue(1),
                                    ),
                                },
                                Value {
                                    value_data: Some(api::v1::value::ValueData::F64Value(if nan {
                                        f64::NAN
                                    } else {
                                        7.
                                    })),
                                },
                            ],
                        }],
                    }),
                    partition_expr_version: None,
                    skip_wal: false,
                })
                .collect(),
        })
    }

    fn request(cmd: Command, body: Option<Body>) -> Request {
        let mut extensions = HashMap::new();
        extensions.insert(flow_write::COMMAND_KEY.into(), cmd.encode().unwrap());
        Request {
            header: Some(RegionRequestHeader {
                dbname: "db".into(),
                tracing_context: Default::default(),
                query_context: Some(QueryContext {
                    extensions,
                    ..Default::default()
                }),
            }),
            body,
        }
    }
    fn decode(resp: &api::v1::region::RegionResponse) -> Reply {
        assert_eq!(
            resp.header
                .as_ref()
                .unwrap()
                .status
                .as_ref()
                .unwrap()
                .status_code,
            common_error::status_code::StatusCode::Success as u32
        );
        serde_json::from_slice(resp.extensions.get(flow_write::COMMAND_KEY).unwrap()).unwrap()
    }
    async fn open(s: &RegionServer) -> String {
        match decode(
            &s.handle_region_request(request(
                Command::Open { nonce: "n".into() },
                Some(Body::Inserts(InsertRequests { requests: vec![] })),
            ))
            .await
            .unwrap(),
        ) {
            Reply::Opened { capability } => capability,
            x => panic!("{x:?}"),
        }
    }
    fn setup(gate: Gate, panic_id: Option<u64>, ids: &[u64]) -> RegionServer {
        let s = mock_region_server();
        let (inner, _) = MockRegionEngine::new("mock");
        let engine: RegionEngineRef = Arc::new(GatedEngine {
            gate,
            inner,
            panic_id,
        });
        for id in ids {
            s.inner.region_map.insert(
                RegionId::from_u64(*id),
                super::super::RegionEngineWithStatus::Ready(engine.clone()),
            );
        }
        s
    }
    fn write(cap: &str, ids: &[u64], nan: bool) -> Request {
        request(
            Command::Write {
                capability: cap.into(),
            },
            Some(body(ids, nan)),
        )
    }

    #[tokio::test]
    async fn dropped_rpc_retry_and_close_drain_exactly_once() {
        let gate = Gate::default();
        let s = setup(gate.clone(), None, &[1, 2]);
        let cap = open(&s).await;
        let w = write(&cap, &[1, 2], false);
        let first = tokio::spawn({
            let s = s.clone();
            let w = w.clone();
            async move { s.handle_region_request(w).await }
        });
        gate.wait_entered(2).await;
        first.abort();
        // Exercise the same actual registry admission used by retry, before release.
        let payload = tracked_payload(&w).unwrap();
        let Admission::Join(waiter) = s.flow_write_registry.admit(&cap, payload).unwrap() else {
            panic!("retry did not join")
        };
        let mut retry_waiter = tokio::spawn(common_grpc::flow_write::wait_completion(waiter));
        assert!(
            timeout(Duration::from_millis(20), &mut retry_waiter)
                .await
                .is_err()
        );
        let mut closer = tokio::spawn({
            let s = s.clone();
            let cap = cap.clone();
            async move {
                s.handle_region_request(request(
                    Command::Close { capability: cap },
                    Some(Body::Inserts(InsertRequests { requests: vec![] })),
                ))
                .await
            }
        });
        assert!(
            timeout(Duration::from_millis(20), &mut closer)
                .await
                .is_err()
        );
        gate.release();
        let joined = timeout(Duration::from_secs(5), &mut retry_waiter)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(
            *joined,
            Reply::Complete {
                rows: 2,
                error: None,
                ..
            }
        ));
        assert!(matches!(
            decode(
                &timeout(Duration::from_secs(5), &mut closer)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap()
            ),
            Reply::Closed
        ));
        assert!(
            s.handle_region_request(write(&cap, &[1], false))
                .await
                .is_err()
        );
        let cap2 = open(&s).await;
        gate.release();
        let r = s
            .handle_region_request(write(&cap2, &[1], false))
            .await
            .unwrap();
        assert!(matches!(decode(&r), Reply::Complete { rows: 1, .. }));
        assert!(
            s.handle_region_request(write(&cap, &[1], false))
                .await
                .is_err()
        );
        assert_eq!(gate.entered.load(Ordering::SeqCst), 3);
        let close2 = request(
            Command::Close { capability: cap2 },
            Some(Body::Inserts(InsertRequests { requests: vec![] })),
        );
        assert!(matches!(
            decode(&s.handle_region_request(close2).await.unwrap()),
            Reply::Closed
        ));
    }

    #[tokio::test]
    async fn application_error_waits_for_sibling_and_panic_overrides_it() {
        let gate = Gate {
            fail: true,
            ..Default::default()
        };
        let s = setup(gate.clone(), None, &[1, 2]);
        let cap = open(&s).await;
        let task = tokio::spawn({
            let s = s.clone();
            async move { s.handle_region_request(write(&cap, &[1, 2], false)).await }
        });
        gate.wait_entered(2).await;
        // The ordinary error is immediate for region 1, but region 2 remains gated.
        assert!(!task.is_finished());
        gate.release();
        let r = timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(decode(&r), Reply::Complete { error: Some(_), .. }));

        let gate = Gate {
            fail: true,
            ..Default::default()
        };
        let s = setup(gate.clone(), Some(2), &[1, 2, 3]);
        let cap = open(&s).await;
        let task = tokio::spawn({
            let s = s.clone();
            let cap = cap.clone();
            async move {
                s.handle_region_request(write(&cap, &[1, 2, 3], false))
                    .await
            }
        });
        gate.wait_entered(3).await;
        assert!(!task.is_finished());
        gate.release();
        assert!(
            timeout(Duration::from_secs(5), task)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
        assert!(
            s.handle_region_request(write(&cap, &[1, 2, 3], false))
                .await
                .is_err()
        );
        assert_eq!(gate.entered.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn decoded_schema_option_maps_join_live_nan_write() {
        let gate = Gate::default();
        let s = setup(gate.clone(), None, &[1]);
        let cap = open(&s).await;
        let mut original = write(&cap, &[1], true);
        let Body::Inserts(inserts) = original.body.as_mut().unwrap() else {
            unreachable!()
        };
        inserts.requests[0].rows.as_mut().unwrap().schema[1].options =
            Some(api::v1::ColumnOptions {
                options: [
                    ("alpha".to_string(), "one".to_string()),
                    ("beta".to_string(), "two".to_string()),
                    ("gamma".to_string(), "three".to_string()),
                ]
                .into_iter()
                .collect(),
            });
        let encoded = original.encode_to_vec();
        let Body::Inserts(original_inserts) = original.body.as_ref().unwrap() else {
            unreachable!()
        };
        let original_rows = original_inserts.requests[0].rows.clone();
        let original_body_bytes = original_inserts.encode_to_vec();
        let mut retries = Vec::new();
        let mut found_different_wire_order = false;
        for _ in 0..128 {
            let decoded = Request::decode(encoded.as_slice()).unwrap();
            let Body::Inserts(decoded_inserts) = decoded.body.as_ref().unwrap() else {
                unreachable!()
            };
            assert_eq!(
                decoded_inserts.requests[0].rows.as_ref().unwrap().schema,
                original_rows.as_ref().unwrap().schema
            );
            assert_eq!(
                tracked_payload(&decoded).unwrap(),
                tracked_payload(&original).unwrap()
            );
            found_different_wire_order |= decoded_inserts.encode_to_vec() != original_body_bytes;
            retries.push(decoded);
            if found_different_wire_order && retries.len() >= 16 {
                break;
            }
        }
        assert!(found_different_wire_order);

        let owner = tokio::spawn({
            let s = s.clone();
            async move { s.handle_region_request(original).await }
        });
        gate.wait_entered(1).await;
        let retry_tasks: Vec<_> = retries
            .into_iter()
            .map(|retry| {
                let s = s.clone();
                tokio::spawn(async move { s.handle_region_request(retry).await })
            })
            .collect();
        tokio::task::yield_now().await;
        assert!(retry_tasks.iter().all(|retry| !retry.is_finished()));
        assert_eq!(gate.entered.load(Ordering::SeqCst), 1);
        gate.release();
        let owner_reply = decode(
            &timeout(Duration::from_secs(5), owner)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
        );
        assert!(matches!(
            &owner_reply,
            Reply::Complete {
                rows: 1,
                error: None,
                ..
            }
        ));
        let owner_reply_json = serde_json::to_string(&owner_reply).unwrap();
        for retry in retry_tasks {
            let retry_reply = decode(
                &timeout(Duration::from_secs(5), retry)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap(),
            );
            assert!(matches!(
                &retry_reply,
                Reply::Complete {
                    rows: 1,
                    error: None,
                    ..
                }
            ));
            assert_eq!(
                serde_json::to_string(&retry_reply).unwrap(),
                owner_reply_json
            );
        }
        assert_eq!(gate.entered.load(Ordering::SeqCst), 1);

        let replay = Request::decode(encoded.as_slice()).unwrap();
        assert!(matches!(
            decode(&s.handle_region_request(replay.clone()).await.unwrap()),
            Reply::Complete { rows: 1, .. }
        ));
        let mut changed_schema = replay.clone();
        let Body::Inserts(inserts) = changed_schema.body.as_mut().unwrap() else {
            unreachable!()
        };
        inserts.requests[0].rows.as_mut().unwrap().schema[1]
            .options
            .as_mut()
            .unwrap()
            .options
            .insert("gamma".into(), "changed".into());
        assert!(s.handle_region_request(changed_schema).await.is_err());
        let close = request(
            Command::Close {
                capability: cap.clone(),
            },
            Some(Body::Inserts(InsertRequests { requests: vec![] })),
        );
        assert!(matches!(
            decode(&s.handle_region_request(close).await.unwrap()),
            Reply::Closed
        ));
        assert!(s.handle_region_request(replay).await.is_err());
        assert_eq!(gate.entered.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn payload_identity_nan_semantics_and_pre_admission_rejection() {
        let s = mock_region_server();
        let cap = open(&s).await;
        let a = write(&cap, &[1], true);
        let b = write(&cap, &[1], true);
        assert_eq!(tracked_payload(&a).unwrap(), tracked_payload(&b).unwrap());
        let owner = match s
            .flow_write_registry
            .admit(&cap, tracked_payload(&a).unwrap())
            .unwrap()
        {
            Admission::Start(owner) => owner,
            _ => panic!(),
        };
        let mut semantic_mismatch = b.clone();
        semantic_mismatch.header.as_mut().unwrap().dbname = "other_db".into();
        assert_eq!(
            s.flow_write_registry
                .admit(&cap, tracked_payload(&semantic_mismatch).unwrap())
                .unwrap_err(),
            flow_write::RegistryError::PayloadMismatch
        );
        semantic_mismatch = b.clone();
        semantic_mismatch
            .header
            .as_mut()
            .unwrap()
            .query_context
            .as_mut()
            .unwrap()
            .extensions
            .insert("flow_lane".into(), "changed".into());
        assert_eq!(
            s.flow_write_registry
                .admit(&cap, tracked_payload(&semantic_mismatch).unwrap())
                .unwrap_err(),
            flow_write::RegistryError::PayloadMismatch
        );
        let Admission::Join(waiter) = s
            .flow_write_registry
            .admit(&cap, tracked_payload(&b).unwrap())
            .unwrap()
        else {
            panic!()
        };
        let mut joined = tokio::spawn(common_grpc::flow_write::wait_completion(waiter));
        assert!(
            timeout(Duration::from_millis(20), &mut joined)
                .await
                .is_err()
        );
        owner
            .send(Some(Arc::new(Reply::Complete {
                rows: 1,
                cost: 1,
                metrics: None,
                error: None,
            })))
            .unwrap();
        assert!(matches!(
            *timeout(Duration::from_secs(5), &mut joined)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            Reply::Complete { rows: 1, .. }
        ));
        s.flow_write_registry.close(&cap).await.unwrap();
        assert_ne!(
            tracked_payload(&a).unwrap(),
            tracked_payload(&write(&cap, &[1], false)).unwrap()
        );
        assert!(
            s.handle_region_request(request(
                Command::Write {
                    capability: cap.clone()
                },
                Some(Body::Deletes(Default::default()))
            ))
            .await
            .is_err()
        );
        let fresh = open(&s).await;
        let mut bad = request(
            Command::Write {
                capability: fresh.clone(),
            },
            Some(body(&[1], false)),
        );
        bad.header
            .as_mut()
            .unwrap()
            .query_context
            .as_mut()
            .unwrap()
            .extensions
            .insert(flow_write::COMMAND_KEY.into(), "not-json".into());
        assert!(s.handle_region_request(bad).await.is_err());
        s.flow_write_registry.close(&fresh).await.unwrap();
    }
}
