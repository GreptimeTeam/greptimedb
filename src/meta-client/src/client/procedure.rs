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
                            error!("An error occurred in gRPC, status: {status}");
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
