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

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use api::v1::meta::procedure_service_client::ProcedureServiceClient;
use api::v1::meta::{
    DdlTaskRequest, DdlTaskResponse, MigrateRegionRequest, MigrateRegionResponse,
    ProcedureDetailRequest, ProcedureDetailResponse, ProcedureId, ProcedureStateResponse,
    QueryProcedureRequest, ReconcileRequest, ReconcileResponse, ResponseHeader, Role,
};
use common_grpc::channel_manager::ChannelManager;
use common_telemetry::tracing_context::TracingContext;
use common_telemetry::{error, info, warn};
use snafu::{ResultExt, ensure};
use tokio::sync::RwLock;
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;
use tonic::{Request, Status};

use crate::client::{Id, LeaderProviderRef, util};
use crate::error;
use crate::error::Result;

#[derive(Clone, Debug)]
pub struct Client {
    inner: Arc<RwLock<Inner>>,
}

impl Client {
    pub fn new(
        id: Id,
        role: Role,
        channel_manager: ChannelManager,
        max_retry: usize,
        timeout: Duration,
    ) -> Self {
        let inner = Arc::new(RwLock::new(Inner {
            id,
            role,
            channel_manager,
            leader_provider: None,
            max_retry,
            timeout,
        }));

        Self { inner }
    }

    /// Start the client with a [LeaderProvider].
    pub(crate) async fn start_with(&self, leader_provider: LeaderProviderRef) -> Result<()> {
        let mut inner = self.inner.write().await;
        inner.start_with(leader_provider)
    }

    pub async fn submit_ddl_task(&self, req: DdlTaskRequest) -> Result<DdlTaskResponse> {
        let inner = self.inner.read().await;
        inner.submit_ddl_task(req).await
    }

    /// Query the procedure' state by its id
    pub async fn query_procedure_state(&self, pid: &str) -> Result<ProcedureStateResponse> {
        let inner = self.inner.read().await;
        inner.query_procedure_state(pid).await
    }

    /// Migrate the region from one datanode to the other datanode:
    /// - `region_id`:  the migrated region id
    /// - `from_peer`:  the source datanode id
    /// - `to_peer`:  the target datanode id
    /// - `timeout`: timeout for downgrading region and upgrading region operations
    pub async fn migrate_region(
        &self,
        region_id: u64,
        from_peer: u64,
        to_peer: u64,
        timeout: Duration,
    ) -> Result<MigrateRegionResponse> {
        let inner = self.inner.read().await;
        inner
            .migrate_region(region_id, from_peer, to_peer, timeout)
            .await
    }

    /// Reconcile the procedure state.
    pub async fn reconcile(&self, request: ReconcileRequest) -> Result<ReconcileResponse> {
        let inner = self.inner.read().await;
        inner.reconcile(request).await
    }

    pub async fn list_procedures(&self) -> Result<ProcedureDetailResponse> {
        let inner = self.inner.read().await;
        inner.list_procedures().await
    }
}

#[derive(Debug)]
struct Inner {
    id: Id,
    role: Role,
    channel_manager: ChannelManager,
    leader_provider: Option<LeaderProviderRef>,
    max_retry: usize,
    /// Request timeout.
    timeout: Duration,
}

impl Inner {
    fn start_with(&mut self, leader_provider: LeaderProviderRef) -> Result<()> {
        ensure!(
            !self.is_started(),
            error::IllegalGrpcClientStateSnafu {
                err_msg: "DDL client already started",
            }
        );
        self.leader_provider = Some(leader_provider);
        Ok(())
    }

    fn make_client(&self, addr: impl AsRef<str>) -> Result<ProcedureServiceClient<Channel>> {
        let channel = self
            .channel_manager
            .get(addr)
            .context(error::CreateChannelSnafu)?;

        Ok(ProcedureServiceClient::new(channel)
            .accept_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Zstd))
    }

    #[inline]
    fn is_started(&self) -> bool {
        self.leader_provider.is_some()
    }

    async fn with_retry<T, F, R, H>(&self, task: &str, body_fn: F, get_header: H) -> Result<T>
    where
        R: Future<Output = std::result::Result<T, Status>>,
        F: Fn(ProcedureServiceClient<Channel>) -> R,
        H: Fn(&T) -> &Option<ResponseHeader>,
    {
        let Some(leader_provider) = self.leader_provider.as_ref() else {
            return error::IllegalGrpcClientStateSnafu {
                err_msg: "not started",
            }
            .fail();
        };

        let mut times = 0;
        let mut last_error = None;

        while times < self.max_retry {
            if let Some(leader) = &leader_provider.leader() {
                let client = self.make_client(leader)?;
                match body_fn(client).await {
                    Ok(res) => {
                        if util::is_not_leader(get_header(&res)) {
                            last_error = Some(format!("{leader} is not a leader"));
                            warn!("Failed to {task} to {leader}, not a leader");
                            let leader = leader_provider.ask_leader().await?;
                            info!("DDL client updated to new leader addr: {leader}");
                            times += 1;
                            continue;
                        }
                        return Ok(res);
                    }
                    Err(status) => {
                        // The leader may be unreachable.
                        if util::is_unreachable(&status) {
                            last_error = Some(status.to_string());
                            warn!("Failed to {task} to {leader}, source: {status}");
                            let leader = leader_provider.ask_leader().await?;
                            info!("Procedure client updated to new leader addr: {leader}");
                            times += 1;
                            continue;
                        } else {
                            error!("An error occurred in gRPC, status: {status:?}");
                            return Err(error::Error::from(status));
                        }
                    }
                }
            } else if let Err(err) = leader_provider.ask_leader().await {
                return Err(err);
            }
        }

        error::RetryTimesExceededSnafu {
            msg: format!("Failed to {task}, last error: {:?}", last_error),
            times: self.max_retry,
        }
        .fail()
    }

    async fn migrate_region(
        &self,
        region_id: u64,
        from_peer: u64,
        to_peer: u64,
        timeout: Duration,
    ) -> Result<MigrateRegionResponse> {
        let mut req = MigrateRegionRequest {
            region_id,
            from_peer,
            to_peer,
            timeout_secs: timeout.as_secs() as u32,
            ..Default::default()
        };

        req.set_header(
            self.id,
            self.role,
            TracingContext::from_current_span().to_w3c(),
        );

        self.with_retry(
            "migrate region",
            move |mut client| {
                let mut req = Request::new(req.clone());
                req.set_timeout(self.timeout);

                async move { client.migrate(req).await.map(|res| res.into_inner()) }
            },
            |resp: &MigrateRegionResponse| &resp.header,
        )
        .await
    }

    async fn reconcile(&self, request: ReconcileRequest) -> Result<ReconcileResponse> {
        let mut req = request;
        req.set_header(
            self.id,
            self.role,
            TracingContext::from_current_span().to_w3c(),
        );

        self.with_retry(
            "reconcile",
            move |mut client| {
                let mut req = Request::new(req.clone());
                req.set_timeout(self.timeout);

                async move { client.reconcile(req).await.map(|res| res.into_inner()) }
            },
            |resp: &ReconcileResponse| &resp.header,
        )
        .await
    }

    async fn query_procedure_state(&self, pid: &str) -> Result<ProcedureStateResponse> {
        let mut req = QueryProcedureRequest {
            pid: Some(ProcedureId { key: pid.into() }),
            ..Default::default()
        };

        req.set_header(
            self.id,
            self.role,
            TracingContext::from_current_span().to_w3c(),
        );

        self.with_retry(
            "query procedure state",
            move |mut client| {
                let mut req = Request::new(req.clone());
                req.set_timeout(self.timeout);

                async move { client.query(req).await.map(|res| res.into_inner()) }
            },
            |resp: &ProcedureStateResponse| &resp.header,
        )
        .await
    }

    async fn submit_ddl_task(&self, mut req: DdlTaskRequest) -> Result<DdlTaskResponse> {
        req.set_header(
            self.id,
            self.role,
            TracingContext::from_current_span().to_w3c(),
        );
        let timeout = Duration::from_secs(req.timeout_secs.into());

        self.with_retry(
            "submit ddl task",
            move |mut client| {
                let mut req = Request::new(req.clone());
                req.set_timeout(timeout);
                async move { client.ddl(req).await.map(|res| res.into_inner()) }
            },
            |resp: &DdlTaskResponse| &resp.header,
        )
        .await
    }

    async fn list_procedures(&self) -> Result<ProcedureDetailResponse> {
        let mut req = ProcedureDetailRequest::default();
        req.set_header(
            self.id,
            self.role,
            TracingContext::from_current_span().to_w3c(),
        );

        self.with_retry(
            "list procedure",
            move |mut client| {
                let mut req = Request::new(req.clone());
                req.set_timeout(self.timeout);
                async move { client.details(req).await.map(|res| res.into_inner()) }
            },
            |resp: &ProcedureDetailResponse| &resp.header,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use api::v1::meta::heartbeat_server::{Heartbeat, HeartbeatServer};
    use api::v1::meta::procedure_service_server::{ProcedureService, ProcedureServiceServer};
    use api::v1::meta::{
        AskLeaderRequest, AskLeaderResponse, DdlTaskRequest, DdlTaskResponse, HeartbeatRequest,
        HeartbeatResponse, MigrateRegionRequest, MigrateRegionResponse, Peer,
        ProcedureDetailRequest, ProcedureDetailResponse, ProcedureStateResponse,
        QueryProcedureRequest, ReconcileRequest, ReconcileResponse, ResponseHeader, Role,
    };
    use async_trait::async_trait;
    use common_error::status_code::StatusCode;
    use common_meta::rpc::ddl::{CommentObjectType, CommentOnTask, DdlTask, SubmitDdlTaskRequest};
    use common_telemetry::common_error::ext::ErrorExt;
    use common_telemetry::info;
    use session::context::QueryContext;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
    use tonic::codec::CompressionEncoding;
    use tonic::{Request, Response, Status};

    use crate::client::MetaClientBuilder;

    #[derive(Clone)]
    struct MockHeartbeat {
        leader_addr: String,
    }

    #[async_trait]
    impl Heartbeat for MockHeartbeat {
        type HeartbeatStream = ReceiverStream<Result<HeartbeatResponse, Status>>;

        async fn heartbeat(
            &self,
            _request: Request<tonic::Streaming<HeartbeatRequest>>,
        ) -> Result<Response<Self::HeartbeatStream>, Status> {
            Err(Status::unimplemented(
                "heartbeat stream is not used in this test",
            ))
        }

        async fn ask_leader(
            &self,
            _request: Request<AskLeaderRequest>,
        ) -> Result<Response<AskLeaderResponse>, Status> {
            Ok(Response::new(AskLeaderResponse {
                header: Some(ResponseHeader {
                    protocol_version: 0,
                    error: None,
                }),
                leader: Some(Peer {
                    id: 1,
                    addr: self.leader_addr.clone(),
                }),
            }))
        }
    }

    #[derive(Clone)]
    struct MockProcedure {
        delay: Duration,
    }

    #[async_trait]
    impl ProcedureService for MockProcedure {
        async fn query(
            &self,
            _request: Request<QueryProcedureRequest>,
        ) -> Result<Response<ProcedureStateResponse>, Status> {
            Err(Status::unimplemented("query is not used in this test"))
        }

        async fn ddl(
            &self,
            _request: Request<DdlTaskRequest>,
        ) -> Result<Response<DdlTaskResponse>, Status> {
            tokio::time::sleep(self.delay).await;
            Ok(Response::new(DdlTaskResponse {
                header: Some(ResponseHeader {
                    protocol_version: 0,
                    error: None,
                }),
                ..Default::default()
            }))
        }

        async fn reconcile(
            &self,
            _request: Request<ReconcileRequest>,
        ) -> Result<Response<ReconcileResponse>, Status> {
            Err(Status::unimplemented("reconcile is not used in this test"))
        }

        async fn migrate(
            &self,
            _request: Request<MigrateRegionRequest>,
        ) -> Result<Response<MigrateRegionResponse>, Status> {
            Err(Status::unimplemented("migrate is not used in this test"))
        }

        async fn details(
            &self,
            _request: Request<ProcedureDetailRequest>,
        ) -> Result<Response<ProcedureDetailResponse>, Status> {
            Err(Status::unimplemented("details is not used in this test"))
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_meta_client_ddl_request_timeout() {
        common_telemetry::init_default_ut_logging();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let addr_str = addr.to_string();

        let heartbeat = MockHeartbeat {
            leader_addr: addr_str.clone(),
        };
        let procedure = MockProcedure {
            delay: Duration::from_secs(2),
        };

        let server = tonic::transport::Server::builder()
            .add_service(
                HeartbeatServer::new(heartbeat).accept_compressed(CompressionEncoding::Zstd),
            )
            .add_service(
                ProcedureServiceServer::new(procedure).accept_compressed(CompressionEncoding::Zstd),
            )
            .serve_with_incoming(TcpListenerStream::new(listener));
        let server_handle = tokio::spawn(server);

        let mut client = MetaClientBuilder::new(0, Role::Frontend)
            .enable_heartbeat()
            .enable_procedure()
            .build();
        client.start(&[addr_str.as_str()]).await.unwrap();

        let mut request = SubmitDdlTaskRequest::new(
            QueryContext::arc(),
            DdlTask::new_comment_on(CommentOnTask {
                catalog_name: "greptime".to_string(),
                schema_name: "public".to_string(),
                object_type: CommentObjectType::Table,
                object_name: "test_table".to_string(),
                column_name: None,
                object_id: None,
                comment: Some("timeout".to_string()),
            }),
        );
        request.timeout = Duration::from_secs(1);

        let err = client.submit_ddl_task(request).await.unwrap_err();
        info!("err: {err:?}, code: {}", err.status_code());
        assert_eq!(err.status_code(), StatusCode::Cancelled);
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("Timeout expired"),
            "unexpected error: {err_msg}"
        );

        server_handle.abort();
    }
}
