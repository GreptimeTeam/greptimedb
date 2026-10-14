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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use api::v1::query_request::Query;
use api::v1::{QueryRequest, greptime_request::Request};
use client::OutputWithMetrics;
use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_grpc::flow_write::{self, COMMAND_KEY, Command, Reply, TerminalError};
use common_query::{Output, OutputData, OutputMeta};
use common_telemetry::warn;
use session::context::{Channel, QueryContextBuilder, generate_remote_query_id};
use snafu::OptionExt;
use tokio::time::sleep;

use crate::Error;
use crate::batching_mode::frontend_client::{DatabaseWithPeer, FrontendClient, PeerDesc};
use crate::error::UnexpectedSnafu;

const RETRY_PACE: Duration = Duration::from_secs(1);

pub(crate) async fn tracked_sql_write(
    frontend: &FrontendClient,
    catalog: &str,
    schema: &str,
    sql: &str,
) -> Result<OutputWithMetrics, Error> {
    tracked_query(
        frontend,
        catalog,
        schema,
        QueryRequest {
            query: Some(Query::Sql(sql.to_string())),
        },
        &[],
        &HashMap::new(),
        &mut None,
    )
    .await
}

pub(crate) async fn tracked_query(
    frontend: &FrontendClient,
    catalog: &str,
    schema: &str,
    request: QueryRequest,
    extensions: &[(&str, &str)],
    snapshot_seqs: &HashMap<u64, u64>,
    peer_desc: &mut Option<PeerDesc>,
) -> Result<OutputWithMetrics, Error> {
    let (selected, handler, parallelism, read_preference, timeout) = match frontend {
        FrontendClient::Distributed {
            query, batch_opts, ..
        } => (
            Some(
                frontend
                    .get_random_active_frontend(catalog, schema)
                    .await
                    .map_err(prewrite_error)?,
            ),
            None,
            query.parallelism.to_string(),
            Some(batch_opts.read_preference.as_ref().to_string()),
            batch_opts.experimental_flight_do_get_timeout,
        ),
        FrontendClient::Standalone {
            database_client,
            query,
            ..
        } => {
            let handler = database_client
                .handler
                .lock()
                .map_err(|e| {
                    prewrite_reason(
                        format!("Failed to lock database client: {e}"),
                        StatusCode::Internal,
                    )
                })?
                .as_ref()
                .context(UnexpectedSnafu {
                    reason: "Standalone's frontend instance is not set",
                })
                .map_err(prewrite_error)?
                .upgrade()
                .context(UnexpectedSnafu {
                    reason: "Failed to upgrade database client",
                })
                .map_err(prewrite_error)?;
            (
                None,
                Some(handler),
                query.parallelism.to_string(),
                None,
                RETRY_PACE,
            )
        }
    };
    *peer_desc = Some(match &selected {
        Some(DatabaseWithPeer { peer, .. }) => PeerDesc::Dist { peer: peer.clone() },
        None => PeerDesc::Standalone,
    });

    let nonce = generate_remote_query_id();
    let mut command = Command::Open { nonce };
    let mut completion: Option<(
        usize,
        usize,
        Option<common_recordbatch::adapter::RecordBatchMetrics>,
        Option<TerminalError>,
    )> = None;

    loop {
        let encoded = command.encode().map_err(|error| {
            UnexpectedSnafu {
                reason: format!("Failed to encode tracked Flow command: {error}"),
            }
            .build()
        })?;
        let mut operation_extensions: Vec<(String, String)> = extensions
            .iter()
            .filter(|(key, _)| *key != COMMAND_KEY)
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        operation_extensions.push((COMMAND_KEY.to_string(), encoded));
        let extension_refs: Vec<(&str, &str)> = operation_extensions
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let phase_request = match &command {
            Command::Write { .. } => request.clone(),
            Command::Open { .. } | Command::Close { .. } => QueryRequest {
                query: Some(Query::Sql(String::new())),
            },
        };
        let response = if let Some(DatabaseWithPeer { database, .. }) = &selected {
            let mut hints = vec![(
                query::datafusion::QUERY_PARALLELISM_HINT,
                parallelism.as_str(),
            )];
            if let Some(preference) = read_preference.as_deref() {
                hints.push((session::hints::READ_PREFERENCE_HINT, preference));
            }
            database
                .flight_request()
                .with_hints(&hints)
                .with_flow_extensions(&extension_refs)
                .with_snapshot_seqs(snapshot_seqs)
                .with_timeout(timeout)
                .query_with_terminal_metrics(phase_request)
                .await
                .map(|output| output.output)
                .map_err(|e| (e.status_code(), e.to_string()))
        } else {
            let mut ctx_extensions: HashMap<String, String> =
                operation_extensions.into_iter().collect();
            ctx_extensions.insert(
                query::datafusion::QUERY_PARALLELISM_HINT.to_string(),
                parallelism.clone(),
            );
            let ctx = QueryContextBuilder::default()
                .current_catalog(catalog.to_string())
                .current_schema(schema.to_string())
                .channel(Channel::Internal)
                .extensions(ctx_extensions)
                .snapshot_seqs(Arc::new(std::sync::RwLock::new(snapshot_seqs.clone())))
                .build();
            let handler = handler.as_ref().ok_or_else(|| {
                UnexpectedSnafu {
                    reason: "Standalone handler was not pinned",
                }
                .build()
            })?;
            handler
                .do_query(Request::Query(phase_request), Arc::new(ctx))
                .await
                .map_err(|e| (e.status_code(), e.to_string()))
        };
        let reply = match response {
            Ok(output) => match decode_output(output).await {
                Ok(reply) => Some(reply),
                Err(e) if matches!(&command, Command::Open { .. }) => {
                    return Err(prewrite_reason(
                        format!("Frontend does not support tracked Flow writes: {e}"),
                        StatusCode::Unsupported,
                    ));
                }
                Err(e) => {
                    warn!("Malformed tracked Flow {:?} reply: {}", command, e);
                    None
                }
            },
            Err((code, msg)) => {
                // Classify application-level rejections that will never succeed on
                // retry against this frontend. This fail-fast is applied ONLY to
                // `Open`, which is always pre-admission (no write has been
                // dispatched yet), so a definitive rejection (an older frontend
                // treats Open as empty SQL -> InvalidArguments; a non-internal
                // channel rejects the extension -> PermissionDenied/Unsupported)
                // can never have committed anything — safe to surface a terminal
                // pre-write error instead of looping forever on a pinned frontend.
                //
                // `Write`/`Close` rejections are NOT classified this way: once a
                // write may have been admitted, a rejection (e.g. WriterLost, or an
                // UnknownCapability after a VIP moved Open->A/Write->B) leaves the
                // outcome ambiguous. Treating it as "provably not dispatched" would
                // let the caller replay and duplicate rows, so those fall through
                // to the same-capability retry below like any ambiguous transport
                // failure.
                if matches!(&command, Command::Open { .. }) && is_definitive_rejection(code) {
                    return Err(prewrite_reason(
                        format!("Frontend rejected tracked Flow open: {msg}"),
                        code,
                    ));
                }
                warn!(
                    "Tracked Flow {:?} transport failure (code={:?}): {}",
                    command, code, msg
                );
                None
            }
        };
        match (command.clone(), reply) {
            (Command::Open { .. }, Some(Reply::Opened { capability: cap })) if !cap.is_empty() => {
                command = Command::Write { capability: cap };
                continue;
            }
            (Command::Open { .. }, Some(other)) => {
                return Err(prewrite_reason(
                    format!("Invalid tracked Flow open reply: {other:?}"),
                    StatusCode::InvalidArguments,
                ));
            }
            (
                Command::Write { capability },
                Some(Reply::Complete {
                    rows,
                    cost,
                    metrics,
                    error,
                }),
            ) => {
                completion = Some((rows, cost, metrics, error));
                command = Command::Close { capability };
                continue;
            }
            (Command::Write { .. }, Some(other)) => {
                warn!("Unexpected tracked Flow write reply: {other:?}")
            }
            (Command::Close { .. }, Some(Reply::Closed)) => {
                let Some((rows, cost, metrics, error)) = completion.take() else {
                    return UnexpectedSnafu {
                        reason: "Close completed without positive write result",
                    }
                    .fail();
                };
                if let Some(error) = error {
                    return Err(Error::External {
                        source: BoxedError::new(error),
                        location: snafu::Location::new(file!(), line!(), column!()),
                    });
                }
                let output = OutputWithMetrics::from_output(Output::new(
                    OutputData::AffectedRows(rows),
                    OutputMeta {
                        cost,
                        ..Default::default()
                    },
                ));
                output.metrics.update(metrics);
                return Ok(output);
            }
            (Command::Close { .. }, Some(other)) => {
                warn!("Unexpected tracked Flow close reply: {other:?}")
            }
            (_, None) => {}
        }
        sleep(RETRY_PACE).await;
    }
}

fn prewrite_error(error: Error) -> Error {
    let status = error.status_code();
    prewrite_reason(error.to_string(), status)
}

/// Returns true when a tracked Flow command error is a definitive, non-retryable
/// rejection from the frontend rather than an ambiguous transport failure. These
/// codes indicate the frontend itself refused the command (unsupported protocol,
/// malformed/empty command, or wrong channel), so retrying the same pinned
/// frontend can never succeed; the caller must fail fast instead of looping.
fn is_definitive_rejection(code: StatusCode) -> bool {
    matches!(
        code,
        StatusCode::Unsupported | StatusCode::InvalidArguments | StatusCode::PermissionDenied
    )
}

fn prewrite_reason(message: String, status: StatusCode) -> Error {
    Error::External {
        source: BoxedError::new(TerminalError {
            code: status as u32,
            message,
        }),
        location: snafu::Location::new(file!(), line!(), column!()),
    }
}

async fn decode_output(output: Output) -> Result<Reply, String> {
    let batches = match output.data {
        OutputData::RecordBatches(batches) => batches,
        OutputData::Stream(stream) => common_recordbatch::util::collect_batches(stream)
            .await
            .map_err(|e| e.to_string())?,
        _ => return Err("tracked control request returned a non-record-batch result".to_string()),
    };
    flow_write::decode_reply(&batches).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use common_error::ext::{BoxedError, PlainError};
    use common_error::status_code::StatusCode;
    use session::context::QueryContextRef;

    #[derive(Debug, Default)]
    struct LostReplyHandler {
        opens: AtomicUsize,
        writes: AtomicUsize,
        closes: AtomicUsize,
        executions: AtomicUsize,
        registry: common_grpc::flow_write::FlowWriteRegistry<String, Reply>,
        capabilities: std::sync::Mutex<HashMap<String, String>>,
    }

    #[async_trait::async_trait]
    impl super::super::GrpcQueryHandlerWithBoxedError for LostReplyHandler {
        async fn do_query(
            &self,
            request: Request,
            ctx: QueryContextRef,
        ) -> Result<Output, BoxedError> {
            let encoded = ctx.extensions().get(COMMAND_KEY).cloned().ok_or_else(|| {
                BoxedError::new(PlainError::new(
                    "missing command".into(),
                    StatusCode::Unexpected,
                ))
            })?;
            match Command::decode(&encoded).map_err(|error| {
                BoxedError::new(PlainError::new(
                    error.to_string(),
                    StatusCode::InvalidArguments,
                ))
            })? {
                Command::Open { nonce } => {
                    let call = self.opens.fetch_add(1, Ordering::SeqCst);
                    let capability = self.registry.open(&nonce).unwrap();
                    let previous = self
                        .capabilities
                        .lock()
                        .unwrap()
                        .insert(nonce, capability.clone());
                    if let Some(previous) = previous {
                        assert_eq!(previous, capability);
                    }
                    if call == 0 {
                        return Err(transport_error("lost Open reply"));
                    }
                    assert_empty_sql(&request);
                    response(Reply::Opened { capability })
                }
                Command::Write { capability } => {
                    let sql = request_sql(&request);
                    let call = self.writes.fetch_add(1, Ordering::SeqCst);
                    let admission = self.registry.admit(&capability, sql.clone()).unwrap();
                    let result = match admission {
                        common_grpc::flow_write::Admission::Start(sender) => {
                            self.executions.fetch_add(1, Ordering::SeqCst);
                            let reply = Reply::Complete {
                                rows: 1,
                                cost: 9,
                                metrics: Some(common_recordbatch::adapter::RecordBatchMetrics {
                                    region_watermarks: vec![
                                        common_recordbatch::adapter::RegionWatermarkEntry {
                                            region_id: 41,
                                            watermark: Some(17),
                                        },
                                    ],
                                    ..Default::default()
                                }),
                                error: None,
                            };
                            sender.send(Some(Arc::new(reply.clone()))).unwrap();
                            reply
                        }
                        common_grpc::flow_write::Admission::Join(receiver) => {
                            common_grpc::flow_write::wait_completion(receiver)
                                .await
                                .unwrap()
                                .as_ref()
                                .clone()
                        }
                    };
                    if call == 0 {
                        return Err(transport_error("lost Write reply"));
                    }
                    response(result)
                }
                Command::Close { capability } => {
                    let call = self.closes.fetch_add(1, Ordering::SeqCst);
                    self.registry.close(&capability).await.unwrap();
                    if call == 0 {
                        return Err(transport_error("lost Close reply"));
                    }
                    response(Reply::Closed)
                }
            }
        }
    }

    fn request_sql(request: &Request) -> String {
        match request {
            Request::Query(query) => match query.query.as_ref() {
                Some(Query::Sql(sql)) => sql.clone(),
                other => panic!("expected SQL INSERT, got {other:?}"),
            },
            other => panic!("expected Query request, got {other:?}"),
        }
    }

    fn assert_empty_sql(request: &Request) {
        assert!(
            matches!(request, Request::Query(QueryRequest { query: Some(Query::Sql(sql)) }) if sql.is_empty())
        );
    }

    fn transport_error(message: &str) -> BoxedError {
        BoxedError::new(PlainError::new(
            message.to_string(),
            StatusCode::StorageUnavailable,
        ))
    }

    fn response(reply: Reply) -> Result<Output, BoxedError> {
        let batches = flow_write::reply_batches(&reply).map_err(BoxedError::new)?;
        Ok(Output::new_with_record_batches(batches))
    }

    #[tokio::test]
    async fn retries_same_open_write_and_close_capability_and_executes_once() {
        let handler = Arc::new(LostReplyHandler::default());
        let trait_handler: Arc<dyn super::super::GrpcQueryHandlerWithBoxedError> = handler.clone();
        let weak = Arc::downgrade(&trait_handler);
        let frontend =
            FrontendClient::from_grpc_handler(weak, query::options::QueryOptions::default())
                .with_tracked_writes();
        let output = frontend
            .tracked_sql_write("greptime", "public", "INSERT INTO t VALUES (7)")
            .await
            .unwrap();
        assert!(matches!(output.output.data, OutputData::AffectedRows(1)));
        assert_eq!(output.output.meta.cost, 9);
        assert_eq!(
            output.metrics.region_watermark_map(),
            Some(HashMap::from([(41, 17)]))
        );
        assert_eq!(handler.opens.load(Ordering::SeqCst), 2);
        assert_eq!(handler.writes.load(Ordering::SeqCst), 2);
        assert_eq!(handler.closes.load(Ordering::SeqCst), 2);
        assert_eq!(handler.executions.load(Ordering::SeqCst), 1);
        let second = frontend
            .tracked_sql_write("greptime", "public", "INSERT INTO t VALUES (8)")
            .await
            .unwrap();
        assert!(matches!(second.output.data, OutputData::AffectedRows(1)));
        assert_eq!(handler.executions.load(Ordering::SeqCst), 2);
    }

    /// A legacy frontend that does not understand the tracked-write extension
    /// processes `Open` as an ordinary empty-SQL query and rejects it with
    /// `InvalidArguments`. That is a definitive application rejection, not an
    /// ambiguous transport failure: the client must fail fast instead of
    /// retrying the same pinned frontend forever.
    #[derive(Debug, Default)]
    struct LegacyRejectHandler {
        opens: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl super::super::GrpcQueryHandlerWithBoxedError for LegacyRejectHandler {
        async fn do_query(
            &self,
            _request: Request,
            _ctx: QueryContextRef,
        ) -> Result<Output, BoxedError> {
            self.opens.fetch_add(1, Ordering::SeqCst);
            Err(BoxedError::new(PlainError::new(
                "empty statements".to_string(),
                StatusCode::InvalidArguments,
            )))
        }
    }

    #[tokio::test]
    async fn fails_fast_on_definitive_open_rejection() {
        let handler = Arc::new(LegacyRejectHandler::default());
        let trait_handler: Arc<dyn super::super::GrpcQueryHandlerWithBoxedError> = handler.clone();
        let weak = Arc::downgrade(&trait_handler);
        let frontend =
            FrontendClient::from_grpc_handler(weak, query::options::QueryOptions::default())
                .with_tracked_writes();
        let result = frontend
            .tracked_sql_write("greptime", "public", "INSERT INTO t VALUES (7)")
            .await;
        let err = result.expect_err("definitive Open rejection must not be retried forever");
        assert_eq!(err.status_code(), StatusCode::InvalidArguments);
        // The durable-execution layer keys repair off a TerminalError inside
        // Error::External; pin that exact shape so a refactor cannot silently
        // downgrade the rejection to an unresolved (permanently stalled) error.
        assert!(
            matches!(&err, Error::External { source, .. } if source.as_any().is::<common_grpc::flow_write::TerminalError>()),
            "expected Error::External carrying TerminalError, got {err:?}"
        );
        assert_eq!(
            handler.opens.load(Ordering::SeqCst),
            1,
            "Open must not be retried after a definitive rejection"
        );
    }
}
