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

use std::any::Any;
use std::future::Future;
use std::sync::Arc;

use api::greptime_proto::v1;
use api::v1::meta::cluster_client::ClusterClient;
use api::v1::meta::{MetasrvNodeInfo, MetasrvPeersRequest, ResponseHeader, Role};
use common_error::ext::BoxedError;
use common_grpc::channel_manager::ChannelManager;
use common_meta::error::{
    Error as MetaError, ExternalSnafu, ResponseExceededSizeLimitSnafu, Result as MetaResult,
};
use common_meta::kv_backend::{KvBackend, TxnService};
use common_meta::rpc::store::{
    BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse, BatchPutRequest,
    BatchPutResponse, DeleteRangeRequest, DeleteRangeResponse, PutRequest, PutResponse,
    RangeRequest, RangeResponse,
};
use common_telemetry::{info, warn};
use snafu::{ensure, ResultExt};
use tokio::sync::RwLock;
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;
use tonic::Status;

use crate::client::ask_leader::AskLeader;
use crate::client::{util, Id};
use crate::error::{
    ConvertMetaResponseSnafu, CreateChannelSnafu, Error, IllegalGrpcClientStateSnafu, Result,
    RetryTimesExceededSnafu,
};

#[derive(Clone, Debug)]
pub struct Client {
    inner: Arc<RwLock<Inner>>,
}

impl Client {
    pub fn new(id: Id, role: Role, channel_manager: ChannelManager, max_retry: usize) -> Self {
        let inner = Arc::new(RwLock::new(Inner {
            id,
            role,
            channel_manager,
            ask_leader: None,
            max_retry,
        }));

        Self { inner }
    }

    pub async fn start<U, A>(&mut self, urls: A) -> Result<()>
    where
        U: AsRef<str>,
        A: AsRef<[U]>,
    {
        let mut inner = self.inner.write().await;
        inner.start(urls).await
    }

    pub async fn range(&self, req: RangeRequest) -> Result<RangeResponse> {
        let inner = self.inner.read().await;
        inner.range(req).await
    }

    #[allow(dead_code)]
    pub async fn batch_get(&self, req: BatchGetRequest) -> Result<BatchGetResponse> {
        let inner = self.inner.read().await;
        inner.batch_get(req).await
    }

    pub async fn get_metasrv_peers(
        &self,
    ) -> Result<(Option<MetasrvNodeInfo>, Vec<MetasrvNodeInfo>)> {
        let inner = self.inner.read().await;
        inner.get_metasrv_peers().await
    }
}

impl TxnService for Client {
    type Error = MetaError;
}

#[async_trait::async_trait]
impl KvBackend for Client {
    fn name(&self) -> &str {
        "ClusterClientKvBackend"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn range(&self, req: RangeRequest) -> MetaResult<RangeResponse> {
        let resp = self.range(req).await;
        match resp {
            Ok(resp) => Ok(resp),
            Err(err) if err.is_exceeded_size_limit() => {
                Err(BoxedError::new(err)).context(ResponseExceededSizeLimitSnafu)
            }
            Err(err) => Err(BoxedError::new(err)).context(ExternalSnafu),
        }
    }

    async fn put(&self, _: PutRequest) -> MetaResult<PutResponse> {
        unimplemented!("`put` is not supported in cluster client kv backend")
    }

    async fn batch_put(&self, _: BatchPutRequest) -> MetaResult<BatchPutResponse> {
        unimplemented!("`batch_put` is not supported in cluster client kv backend")
    }

    async fn batch_get(&self, req: BatchGetRequest) -> MetaResult<BatchGetResponse> {
        self.batch_get(req)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)
    }

    async fn delete_range(&self, _: DeleteRangeRequest) -> MetaResult<DeleteRangeResponse> {
        unimplemented!("`delete_range` is not supported in cluster client kv backend")
    }

    async fn batch_delete(&self, _: BatchDeleteRequest) -> MetaResult<BatchDeleteResponse> {
        unimplemented!("`batch_delete` is not supported in cluster client kv backend")
    }
}

#[derive(Debug)]
struct Inner {
    id: Id,
    role: Role,
    channel_manager: ChannelManager,
    ask_leader: Option<AskLeader>,
    max_retry: usize,
}

impl Inner {
    async fn start<U, A>(&mut self, urls: A) -> Result<()>
    where
        U: AsRef<str>,
        A: AsRef<[U]>,
    {
        ensure!(
            !self.is_started(),
            IllegalGrpcClientStateSnafu {
                err_msg: "Cluster client already started",
            }
        );

        let peers = urls
            .as_ref()
            .iter()
            .map(|url| url.as_ref().to_string())
            .collect::<Vec<_>>();
        self.ask_leader = Some(AskLeader::new(
            self.id,
            self.role,
            peers,
            self.channel_manager.clone(),
            self.max_retry,
        ));

        Ok(())
    }

    fn make_client(&self, addr: impl AsRef<str>) -> Result<ClusterClient<Channel>> {
        let channel = self.channel_manager.get(addr).context(CreateChannelSnafu)?;

        Ok(ClusterClient::new(channel)
            .accept_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Zstd))
    }

    #[inline]
    fn is_started(&self) -> bool {
        self.ask_leader.is_some()
    }

    fn ask_leader(&self) -> Result<&AskLeader> {
        ensure!(
            self.is_started(),
            IllegalGrpcClientStateSnafu {
                err_msg: "Cluster client not start"
            }
        );

        Ok(self.ask_leader.as_ref().unwrap())
    }

    async fn with_retry<T, F, R, H>(&self, task: &str, body_fn: F, get_header: H) -> Result<T>
    where
        R: Future<Output = std::result::Result<T, Status>>,
        F: Fn(ClusterClient<Channel>) -> R,
        H: Fn(&T) -> &Option<ResponseHeader>,
    {
        let ask_leader = self.ask_leader()?;
        let mut times = 0;
        let mut last_error = None;

        while times < self.max_retry {
            if let Some(leader) = &ask_leader.get_leader() {
                let client = self.make_client(leader)?;
                match body_fn(client).await {
                    Ok(res) => {
                        if util::is_not_leader(get_header(&res)) {
                            last_error = Some(format!("{leader} is not a leader"));
                            warn!("Failed to {task} to {leader}, not a leader");
                            let leader = ask_leader.ask_leader().await?;
                            info!("Cluster client updated to new leader addr: {leader}");
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
                            let leader = ask_leader.ask_leader().await?;
                            info!("Cluster client updated to new leader addr: {leader}");
                            times += 1;
                            continue;
                        } else {
                            return Err(Error::from(status));
                        }
                    }
                }
            } else if let Err(err) = ask_leader.ask_leader().await {
                return Err(err);
            }
        }

        RetryTimesExceededSnafu {
            msg: format!("Failed to {task}, last error: {:?}", last_error),
            times: self.max_retry,
        }
        .fail()
    }

    async fn range(&self, request: RangeRequest) -> Result<RangeResponse> {
        self.with_retry(
            "range",
            move |mut client| {
                let inner_req = tonic::Request::new(v1::meta::RangeRequest::from(request.clone()));

                async move { client.range(inner_req).await.map(|res| res.into_inner()) }
            },
            |res| &res.header,
        )
        .await?
        .try_into()
        .context(ConvertMetaResponseSnafu)
    }

    async fn batch_get(&self, request: BatchGetRequest) -> Result<BatchGetResponse> {
        self.with_retry(
            "batch_get",
            move |mut client| {
                let inner_req =
                    tonic::Request::new(v1::meta::BatchGetRequest::from(request.clone()));

                async move {
                    client
                        .batch_get(inner_req)
                        .await
                        .map(|res| res.into_inner())
                }
            },
            |res| &res.header,
        )
        .await?
        .try_into()
        .context(ConvertMetaResponseSnafu)
    }

    async fn get_metasrv_peers(&self) -> Result<(Option<MetasrvNodeInfo>, Vec<MetasrvNodeInfo>)> {
        self.with_retry(
            "get_metasrv_peers",
            move |mut client| {
                let inner_req = tonic::Request::new(MetasrvPeersRequest::default());

                async move {
                    client
                        .metasrv_peers(inner_req)
                        .await
                        .map(|res| res.into_inner())
                }
            },
            |res| &res.header,
        )
        .await
        .map(|res| (res.leader, res.followers))
    }
}
