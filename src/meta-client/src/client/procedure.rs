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

use api::v1::meta::ddl_task_request::Task;
use api::v1::meta::procedure_service_client::ProcedureServiceClient;
use api::v1::meta::{
    DdlTaskRequest, DdlTaskResponse, GcRegionsRequest, GcRegionsResponse, GcTableRequest,
    GcTableResponse, MigrateRegionRequest, MigrateRegionResponse, ProcedureActor,
    ProcedureDetailRequest, ProcedureDetailResponse, ProcedureEventContext, ProcedureId,
    ProcedureStateResponse, QueryProcedureRequest, ReconcileRequest, ReconcileResponse,
    RequestHeader, ResponseHeader, Role,
};
use common_grpc::channel_manager::ChannelManager;
use common_meta::procedure_executor::ExecutorContext;
use common_meta::rpc::ddl::{
    CREATE_DATABASE_CREATOR_EXTENSION_KEY, CREATE_DATABASE_CREATOR_METADATA_KEY,
};
use common_meta::rpc::procedure::{
    GcRegionsRequest as MetaGcRegionsRequest, GcResponse as MetaGcResponse,
    GcTableRequest as MetaGcTableRequest,
};
use common_telemetry::tracing_context::TracingContext;
use common_telemetry::{error, info, warn};
use snafu::{ResultExt, ensure};
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tonic::{Request, Status};

use crate::client::{Id, LeaderProviderRef, util};
use crate::error;
use crate::error::Result;

/// Builds the event context transported by a procedure RPC.
///
/// The caller can only supply reason/extensions. Protocol is derived here from
/// the trusted, typed query channel held locally in the executor context.
pub(crate) fn procedure_event_context(context: &ExecutorContext) -> Option<ProcedureEventContext> {
    context.event_input.as_ref().map(|input| {
        let mut event_context = ProcedureEventContext::from(input);
        event_context.protocol = context
            .query_context
            .as_ref()
            .and_then(|query_context| query_context.protocol())
            .unwrap_or_default();
        event_context
    })
}

/// Builds the optional procedure actor transported by a procedure RPC.
pub(crate) fn procedure_actor(context: &ExecutorContext) -> Option<ProcedureActor> {
    context
        .actor
        .as_deref()
        .filter(|username| !username.is_empty())
        .map(|username| ProcedureActor {
            username: username.to_string(),
        })
}

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
        context: &ExecutorContext,
        region_id: u64,
        from_peer: u64,
        to_peer: u64,
        timeout: Duration,
    ) -> Result<MigrateRegionResponse> {
        let inner = self.inner.read().await;
        inner
            .migrate_region(context, region_id, from_peer, to_peer, timeout)
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

    pub async fn gc_regions(
        &self,
        context: &ExecutorContext,
        request: MetaGcRegionsRequest,
    ) -> Result<MetaGcResponse> {
        let inner = self.inner.read().await;
        inner.gc_regions(context, request).await
    }

    pub async fn gc_table(
        &self,
        context: &ExecutorContext,
        request: MetaGcTableRequest,
    ) -> Result<MetaGcResponse> {
        let inner = self.inner.read().await;
        inner.gc_table(context, request).await
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

        Ok(common_grpc::configure_tonic_client!(
            ProcedureServiceClient::new(channel),
            self.channel_manager,
        ))
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
            } else {
                leader_provider.ask_leader().await?;
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
        context: &ExecutorContext,
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
            event_context: procedure_event_context(context),
            actor: procedure_actor(context),
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

    async fn gc_regions(
        &self,
        context: &ExecutorContext,
        request: MetaGcRegionsRequest,
    ) -> Result<MetaGcResponse> {
        let timeout = request.timeout;
        let req = GcRegionsRequest {
            header: Some(RequestHeader {
                protocol_version: 0,
                member_id: self.id,
                role: self.role as i32,
                tracing_context: TracingContext::from_current_span().to_w3c(),
            }),
            region_ids: request.region_ids,
            full_file_listing: request.full_file_listing,
            timeout_secs: gc_timeout_secs(timeout),
            event_context: procedure_event_context(context),
            actor: procedure_actor(context),
        };

        let resp: GcRegionsResponse = self
            .with_retry(
                "gc_regions",
                move |mut client| {
                    let mut req = Request::new(req.clone());
                    if let Some(timeout) = timeout {
                        req.set_timeout(timeout);
                    }
                    async move { client.gc_regions(req).await.map(|res| res.into_inner()) }
                },
                |resp: &GcRegionsResponse| &resp.header,
            )
            .await?;

        let stats = resp.stats.unwrap_or_default();
        Ok(MetaGcResponse {
            processed_regions: stats.processed_regions,
            need_retry_regions: stats.need_retry_regions,
            deleted_files: stats.deleted_files,
            deleted_indexes: stats.deleted_indexes,
        })
    }

    async fn gc_table(
        &self,
        context: &ExecutorContext,
        request: MetaGcTableRequest,
    ) -> Result<MetaGcResponse> {
        let timeout = request.timeout;
        let req = GcTableRequest {
            header: Some(RequestHeader {
                protocol_version: 0,
                member_id: self.id,
                role: self.role as i32,
                tracing_context: TracingContext::from_current_span().to_w3c(),
            }),
            catalog_name: request.catalog_name,
            schema_name: request.schema_name,
            table_name: request.table_name,
            full_file_listing: request.full_file_listing,
            timeout_secs: gc_timeout_secs(timeout),
            event_context: procedure_event_context(context),
            actor: procedure_actor(context),
        };

        let resp: GcTableResponse = self
            .with_retry(
                "gc_table",
                move |mut client| {
                    let mut req = Request::new(req.clone());
                    if let Some(timeout) = timeout {
                        req.set_timeout(timeout);
                    }
                    async move { client.gc_table(req).await.map(|res| res.into_inner()) }
                },
                |resp: &GcTableResponse| &resp.header,
            )
            .await?;

        let stats = resp.stats.unwrap_or_default();
        Ok(MetaGcResponse {
            processed_regions: stats.processed_regions,
            need_retry_regions: stats.need_retry_regions,
            deleted_files: stats.deleted_files,
            deleted_indexes: stats.deleted_indexes,
        })
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
        let creator = create_database_creator_metadata_value(&req);
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
                if let Some(value) = creator.as_deref() {
                    req.metadata_mut().insert_bin(
                        CREATE_DATABASE_CREATOR_METADATA_KEY,
                        tonic::metadata::MetadataValue::from_bytes(value.as_bytes()),
                    );
                }
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

fn create_database_creator_metadata_value(req: &DdlTaskRequest) -> Option<String> {
    // StatementExecutor removes client values before attaching the authenticated creator.
    if !matches!(req.task, Some(Task::CreateDatabaseTask(_))) {
        return None;
    }

    req.query_context
        .as_ref()?
        .extensions
        .get(CREATE_DATABASE_CREATOR_EXTENSION_KEY)
        .cloned()
}

fn gc_timeout_secs(timeout: Option<Duration>) -> u32 {
    timeout
        .map(|timeout| timeout.as_secs().max(1).try_into().unwrap_or(u32::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use api::v1::meta::heartbeat_server::{Heartbeat, HeartbeatServer};
    use api::v1::meta::procedure_service_server::{ProcedureService, ProcedureServiceServer};
    use api::v1::meta::{
        AskLeaderRequest, AskLeaderResponse, DdlTaskRequest, DdlTaskResponse, GcRegionsRequest,
        GcRegionsResponse, GcTableRequest, GcTableResponse, HeartbeatRequest, HeartbeatResponse,
        MigrateRegionRequest, MigrateRegionResponse, Peer, ProcedureDetailRequest,
        ProcedureDetailResponse, ProcedureStateResponse, QueryProcedureRequest, ReconcileRequest,
        ReconcileResponse, ResponseHeader, Role,
    };
    use async_trait::async_trait;
    use common_base::protocol::Channel;
    use common_error::status_code::StatusCode;
    use common_event_recorder::{PersistentEventContext, ProcedureEventInput};
    use common_meta::procedure_executor::{ExecutorContext, ProcedureExecutor};
    use common_meta::rpc::ddl::{
        CREATE_DATABASE_CREATOR_EXTENSION_KEY, CREATE_DATABASE_CREATOR_METADATA_KEY,
        CommentObjectType, CommentOnTask, CreatorGrantIntent, DdlTask, QueryContext,
        SubmitDdlTaskRequest, TriggerReason,
    };
    use common_telemetry::common_error::ext::ErrorExt;
    use common_telemetry::info;
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
    use tonic::codec::CompressionEncoding;
    use tonic::{Request, Response, Status};

    use crate::client::MetaClientBuilder;
    use crate::client::procedure::{gc_timeout_secs, procedure_actor, procedure_event_context};

    #[test]
    fn test_gc_timeout_secs() {
        assert_eq!(gc_timeout_secs(None), 0);
        assert_eq!(gc_timeout_secs(Some(Duration::from_millis(1))), 1);
        assert_eq!(gc_timeout_secs(Some(Duration::from_millis(999))), 1);
        assert_eq!(gc_timeout_secs(Some(Duration::from_secs(1))), 1);
        assert_eq!(gc_timeout_secs(Some(Duration::from_secs(10))), 10);
    }

    #[test]
    fn test_procedure_event_context_derives_protocol_from_query_context() {
        let context = ExecutorContext {
            query_context: Some(QueryContext {
                channel: Channel::Postgres as u8,
                ..Default::default()
            }),
            event_input: Some(ProcedureEventInput::new(TriggerReason::Manual)),
            ..Default::default()
        };

        assert_eq!(
            procedure_event_context(&context).map(PersistentEventContext::from),
            Some(PersistentEventContext::new(TriggerReason::Manual).with_protocol("postgres"))
        );

        let automatic_context = ExecutorContext {
            event_input: Some(ProcedureEventInput::new(TriggerReason::ScheduledGc)),
            ..Default::default()
        };
        assert_eq!(
            procedure_event_context(&automatic_context).map(PersistentEventContext::from),
            Some(PersistentEventContext::new(TriggerReason::ScheduledGc))
        );

        let unknown_channel_context = ExecutorContext {
            query_context: Some(QueryContext::default()),
            event_input: Some(ProcedureEventInput::new(TriggerReason::Manual)),
            ..Default::default()
        };
        assert_eq!(
            procedure_event_context(&unknown_channel_context).map(PersistentEventContext::from),
            Some(PersistentEventContext::new(TriggerReason::Manual))
        );
    }

    #[test]
    fn test_procedure_actor() {
        let context = ExecutorContext {
            actor: Some(String::new()),
            ..Default::default()
        };
        assert!(procedure_actor(&context).is_none());

        let context = ExecutorContext {
            actor: Some("alice".to_string()),
            ..Default::default()
        };
        assert_eq!(procedure_actor(&context).unwrap().username, "alice");
    }

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
        request_tx: Option<mpsc::UnboundedSender<Request<DdlTaskRequest>>>,
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
            request: Request<DdlTaskRequest>,
        ) -> Result<Response<DdlTaskResponse>, Status> {
            if let Some(request_tx) = &self.request_tx {
                request_tx.send(request).unwrap();
            }
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

        async fn gc_regions(
            &self,
            _request: Request<GcRegionsRequest>,
        ) -> Result<Response<GcRegionsResponse>, Status> {
            Err(Status::unimplemented("gc_regions is not used in this test"))
        }

        async fn gc_table(
            &self,
            _request: Request<GcTableRequest>,
        ) -> Result<Response<GcTableResponse>, Status> {
            Err(Status::unimplemented("gc_table is not used in this test"))
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_meta_client_forwards_create_database_creator_metadata() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr_str = listener.local_addr().unwrap().to_string();
        let (request_tx, mut request_rx) = mpsc::unbounded_channel();
        let heartbeat = MockHeartbeat {
            leader_addr: addr_str.clone(),
        };
        let procedure = MockProcedure {
            delay: Duration::ZERO,
            request_tx: Some(request_tx),
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

        let creator = CreatorGrantIntent {
            username: "alice".to_string(),
            created_at_ns: 42,
        };
        let executor_context = |actor: String| ExecutorContext {
            query_context: Some(QueryContext {
                channel: Channel::Postgres as u8,
                ..Default::default()
            }),
            actor: Some(actor),
            event_input: Some(ProcedureEventInput::new(TriggerReason::Manual)),
            ..Default::default()
        };
        ProcedureExecutor::submit_ddl_task(
            &client,
            executor_context("effective-user".to_string()),
            SubmitDdlTaskRequest::new(DdlTask::new_create_database(
                "greptime".to_string(),
                "metrics".to_string(),
                false,
                Default::default(),
                Some(creator.clone()),
            )),
        )
        .await
        .unwrap();

        let request = request_rx.recv().await.unwrap();
        let encoded = serde_json::to_string(&creator).unwrap();
        assert_eq!(
            request
                .metadata()
                .get_bin(CREATE_DATABASE_CREATOR_METADATA_KEY)
                .unwrap()
                .to_bytes()
                .unwrap()
                .as_ref(),
            encoded.as_bytes()
        );
        let request = request.into_inner();
        assert_eq!(request.actor.unwrap().username, "effective-user");
        assert_eq!(
            PersistentEventContext::from(request.event_context.unwrap()),
            PersistentEventContext::new(TriggerReason::Manual).with_protocol("postgres")
        );
        let extensions = &request.query_context.unwrap().extensions;
        assert_eq!(extensions[CREATE_DATABASE_CREATOR_EXTENSION_KEY], encoded);

        ProcedureExecutor::submit_ddl_task(
            &client,
            executor_context(String::new()),
            SubmitDdlTaskRequest::new(DdlTask::new_drop_database(
                "greptime".to_string(),
                "metrics".to_string(),
                false,
            )),
        )
        .await
        .unwrap();
        assert!(
            request_rx
                .recv()
                .await
                .unwrap()
                .into_inner()
                .actor
                .is_none()
        );

        server_handle.abort();
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
            delay: Duration::from_secs(4),
            request_tx: None,
        };

        let server = tonic::transport::Server::builder()
            .add_service(
                HeartbeatServer::new(heartbeat)
                    .accept_compressed(CompressionEncoding::Gzip)
                    .accept_compressed(CompressionEncoding::Zstd),
            )
            .add_service(
                ProcedureServiceServer::new(procedure)
                    .accept_compressed(CompressionEncoding::Gzip)
                    .accept_compressed(CompressionEncoding::Zstd),
            )
            .serve_with_incoming(TcpListenerStream::new(listener));
        let server_handle = tokio::spawn(server);

        let mut client = MetaClientBuilder::new(0, Role::Frontend)
            .enable_heartbeat()
            .enable_procedure()
            .build();
        client.start(&[addr_str.as_str()]).await.unwrap();

        let mut request = SubmitDdlTaskRequest::new(DdlTask::new_comment_on(CommentOnTask {
            catalog_name: "greptime".to_string(),
            schema_name: "public".to_string(),
            object_type: CommentObjectType::Table,
            object_name: "test_table".to_string(),
            column_name: None,
            object_id: None,
            comment: Some("timeout".to_string()),
        }));
        request.timeout = Duration::from_secs(1);

        let now = Instant::now();
        let err = client
            .submit_ddl_task(
                ExecutorContext {
                    query_context: Some(QueryContext::default()),
                    ..Default::default()
                },
                request,
            )
            .await
            .unwrap_err();
        let elapsed = now.elapsed();
        // The request should be cancelled within 1 second.
        assert!(elapsed < Duration::from_secs(2));
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
