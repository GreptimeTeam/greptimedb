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

use std::time::Duration;

use api::v1::region::RegionRequest;
use common_error::ext::BoxedError;
use common_grpc::flow_write::{self, Command, Reply};
use common_meta::node_manager::{DatanodeRef, NodeManagerRef};
use common_meta::peer::Peer;
use session::context::generate_remote_query_id;

use crate::error::{Error, Result};

const RETRY_DELAY: Duration = Duration::from_secs(1);

fn error(message: impl Into<String>) -> Error {
    Error::Unexpected {
        violated: message.into(),
        location: snafu::Location::new(file!(), line!(), column!()),
    }
}

fn command_request(
    request: &RegionRequest,
    command: &Command,
    control: bool,
) -> Result<RegionRequest> {
    let mut request = request.clone();
    if control {
        request.body = Some(api::v1::region::region_request::Body::Inserts(
            Default::default(),
        ));
    }
    let header = request.header.get_or_insert_with(Default::default);
    let query_context = header
        .query_context
        .get_or_insert_with(api::v1::QueryContext::default);
    query_context.extensions.insert(
        flow_write::COMMAND_KEY.to_string(),
        command.encode().map_err(|e| error(e.to_string()))?,
    );
    Ok(request)
}

fn reply(response: &api::region::RegionResponse) -> Option<Reply> {
    response
        .extensions
        .get(flow_write::COMMAND_KEY)
        .and_then(|value| std::str::from_utf8(value).ok())
        .and_then(|value| serde_json::from_str(value).ok())
}

pub(crate) async fn dispatch(
    node_manager: &NodeManagerRef,
    peer: &Peer,
    request: RegionRequest,
) -> Result<api::region::RegionResponse> {
    dispatch_datanode(node_manager.datanode(peer).await, request).await
}

async fn dispatch_datanode(
    datanode: DatanodeRef,
    request: RegionRequest,
) -> Result<api::region::RegionResponse> {
    let nonce = generate_remote_query_id();
    let open = command_request(&request, &Command::Open { nonce }, true)?;
    let capability = loop {
        match datanode.handle(open.clone()).await {
            Ok(response) => match reply(&response) {
                Some(Reply::Opened { capability }) if !capability.is_empty() => break capability,
                _ => return Err(error("Datanode does not support tracked Flow writes")),
            },
            Err(_) => tokio::time::sleep(RETRY_DELAY).await,
        }
    };

    let write = command_request(
        &request,
        &Command::Write {
            capability: capability.clone(),
        },
        false,
    )?;
    let complete = loop {
        match datanode.handle(write.clone()).await {
            Ok(response) => match reply(&response) {
                Some(Reply::Complete {
                    rows,
                    cost: _,
                    metrics: _,
                    error,
                }) => break (response, rows, error),
                _ => tokio::time::sleep(RETRY_DELAY).await,
            },
            Err(_) => tokio::time::sleep(RETRY_DELAY).await,
        }
    };

    let close = command_request(
        &request,
        &Command::Close {
            capability: capability.clone(),
        },
        true,
    )?;
    loop {
        match datanode.handle(close.clone()).await {
            Ok(response) if matches!(reply(&response), Some(Reply::Closed)) => break,
            _ => tokio::time::sleep(RETRY_DELAY).await,
        }
    }

    let (mut response, rows, terminal_error) = complete;
    if let Some(terminal_error) = terminal_error {
        return Err(Error::External {
            source: BoxedError::new(terminal_error),
            location: snafu::location!(),
        });
    }
    response.affected_rows = rows as _;
    Ok(response)
}

pub(crate) fn is_child(ctx: &session::context::QueryContextRef) -> bool {
    ctx.channel() == common_base::protocol::Channel::Internal
        && ctx.extension(flow_write::CHILD_KEY) == Some("1")
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use api::region::RegionResponse;
    use api::v1::region::{RegionRequest, region_request::Body};
    use common_error::ext::BoxedError;
    use common_error::ext::ErrorExt;
    use common_grpc::flow_write::{self, Command, Reply};
    use common_meta::node_manager::{Datanode, DatanodeRef};
    use common_query::request::QueryRequest;
    use common_recordbatch::SendableRecordBatchStream;
    use futures_util::future::join_all;
    use tokio::sync::Notify;

    use super::dispatch_datanode;

    #[derive(Default)]
    struct MockDatanode {
        capabilities: Mutex<HashMap<String, String>>,
        business_write_attempts: AtomicUsize,
        write_commands: Mutex<Vec<String>>,
        requests: Mutex<Vec<RegionRequest>>,
        drop_open_reply: AtomicUsize,
        drop_write_reply: AtomicUsize,
        drop_close_reply: AtomicUsize,
        latch_write: bool,
        terminal_error: bool,
        write_started: Notify,
        release_write: Notify,
        close_started: Notify,
    }

    impl MockDatanode {
        fn response(reply: Reply) -> RegionResponse {
            let mut response = RegionResponse::new(0);
            response.extensions.insert(
                flow_write::COMMAND_KEY.to_string(),
                serde_json::to_vec(&reply).unwrap(),
            );
            response
        }

        fn command(request: &RegionRequest) -> Command {
            let serialized = request
                .header
                .as_ref()
                .and_then(|header| header.query_context.as_ref())
                .and_then(|context| context.extensions.get(flow_write::COMMAND_KEY))
                .unwrap();
            Command::decode(serialized).unwrap()
        }
    }

    #[async_trait::async_trait]
    impl Datanode for MockDatanode {
        async fn handle(
            &self,
            request: RegionRequest,
        ) -> common_meta::error::Result<RegionResponse> {
            self.requests.lock().unwrap().push(request.clone());
            match Self::command(&request) {
                Command::Open { nonce } => {
                    if !matches!(request.body, Some(Body::Inserts(ref inserts)) if inserts.requests.is_empty())
                    {
                        return Err(common_meta::error::Error::External {
                            source: BoxedError::new(common_error::mock::MockError::new(
                                common_error::status_code::StatusCode::InvalidArguments,
                            )),
                            location: snafu::location!(),
                        });
                    }
                    let mut capabilities = self.capabilities.lock().unwrap();
                    let capability = capabilities
                        .entry(nonce)
                        .or_insert_with(|| "cap-1".into())
                        .clone();
                    if self
                        .drop_open_reply
                        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                        .is_ok()
                    {
                        return Err(common_meta::error::Error::External {
                            source: BoxedError::new(common_error::mock::MockError::new(
                                common_error::status_code::StatusCode::TableUnavailable,
                            )),
                            location: snafu::location!(),
                        });
                    }
                    Ok(Self::response(Reply::Opened { capability }))
                }
                Command::Write { capability } => {
                    self.write_commands.lock().unwrap().push(capability.clone());
                    if matches!(request.body, Some(Body::Inserts(ref inserts)) if !inserts.requests.is_empty())
                        && self.business_write_attempts.fetch_add(1, Ordering::SeqCst) == 0
                        && self.latch_write
                    {
                        self.write_started.notify_one();
                        self.release_write.notified().await;
                    }
                    if self
                        .drop_write_reply
                        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                        .is_ok()
                    {
                        return Err(common_meta::error::Error::External {
                            source: BoxedError::new(common_error::mock::MockError::new(
                                common_error::status_code::StatusCode::TableUnavailable,
                            )),
                            location: snafu::location!(),
                        });
                    }
                    Ok(Self::response(Reply::Complete {
                        rows: 7,
                        cost: 0,
                        metrics: None,
                        error: self.terminal_error.then(|| flow_write::TerminalError {
                            code: common_error::status_code::StatusCode::TableNotFound as u32,
                            message: "terminal child error".to_string(),
                        }),
                    }))
                }
                Command::Close { .. } => {
                    self.close_started.notify_one();
                    if self
                        .drop_close_reply
                        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                        .is_ok()
                    {
                        return Err(common_meta::error::Error::External {
                            source: BoxedError::new(common_error::mock::MockError::new(
                                common_error::status_code::StatusCode::TableUnavailable,
                            )),
                            location: snafu::location!(),
                        });
                    }
                    Ok(Self::response(Reply::Closed))
                }
            }
        }

        async fn handle_query(
            &self,
            _: QueryRequest,
        ) -> common_meta::error::Result<SendableRecordBatchStream> {
            unreachable!()
        }
    }

    fn request() -> RegionRequest {
        RegionRequest {
            header: None,
            body: Some(Body::Inserts(api::v1::region::InsertRequests {
                requests: vec![Default::default()],
            })),
        }
    }

    #[tokio::test]
    async fn lost_control_and_write_replies_reuse_capability_and_exact_request() {
        let datanode = Arc::new(MockDatanode::default());
        datanode.drop_open_reply.store(1, Ordering::SeqCst);
        datanode.drop_write_reply.store(1, Ordering::SeqCst);
        datanode.drop_close_reply.store(1, Ordering::SeqCst);
        let response = dispatch_datanode(datanode.clone(), request())
            .await
            .unwrap();
        assert_eq!(response.affected_rows, 7);
        assert_eq!(datanode.business_write_attempts.load(Ordering::SeqCst), 2);
        let writes = datanode.write_commands.lock().unwrap();
        assert_eq!(writes.len(), 2);
        assert!(writes.iter().all(|capability| capability == "cap-1"));
        let requests = datanode.requests.lock().unwrap();
        let writes: Vec<_> = requests
            .iter()
            .filter(|request| matches!(MockDatanode::command(request), Command::Write { .. }))
            .collect();
        assert_eq!(writes.len(), 2);
        assert_eq!(writes[0].body, writes[1].body);
        assert_eq!(writes[0].header, writes[1].header);
        assert_eq!(
            requests
                .iter()
                .filter(|request| matches!(
                    MockDatanode::command(request),
                    Command::Open { .. } | Command::Close { .. }
                ))
                .count(),
            4
        );
        assert!(requests
            .iter()
            .filter(|request| matches!(MockDatanode::command(request), Command::Open { .. } | Command::Close { .. }))
            .all(|request| matches!(request.body, Some(Body::Inserts(ref inserts)) if inserts.requests.is_empty())));
    }

    #[tokio::test]
    async fn unsupported_open_never_submits_business_request() {
        struct Unsupported;
        #[async_trait::async_trait]
        impl Datanode for Unsupported {
            async fn handle(&self, _: RegionRequest) -> common_meta::error::Result<RegionResponse> {
                Ok(RegionResponse::new(0))
            }
            async fn handle_query(
                &self,
                _: QueryRequest,
            ) -> common_meta::error::Result<SendableRecordBatchStream> {
                unreachable!()
            }
        }
        let datanode: DatanodeRef = Arc::new(Unsupported);
        let error = dispatch_datanode(datanode, request()).await.unwrap_err();
        assert_eq!(
            error.status_code(),
            common_error::status_code::StatusCode::Unexpected
        );
    }

    #[tokio::test]
    async fn all_peer_writes_drain_before_terminal_error_returns() {
        let failing = Arc::new(MockDatanode {
            terminal_error: true,
            ..Default::default()
        });
        let latched = Arc::new(MockDatanode {
            latch_write: true,
            ..Default::default()
        });
        let first_datanode: DatanodeRef = failing.clone();
        let second_datanode: DatanodeRef = latched.clone();
        let aggregate = tokio::spawn(async move {
            join_all([
                dispatch_datanode(first_datanode, request()),
                dispatch_datanode(second_datanode, request()),
            ])
            .await
        });
        latched.write_started.notified().await;
        failing.close_started.notified().await;
        assert!(!aggregate.is_finished());
        latched.release_write.notify_one();
        let results = aggregate.await.unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(failing.business_write_attempts.load(Ordering::SeqCst), 1);
        assert_eq!(latched.business_write_attempts.load(Ordering::SeqCst), 1);
        assert!(results.into_iter().any(|result| result.is_err()));
    }
}
