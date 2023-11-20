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

use std::collections::HashSet;
use std::sync::Arc;

use api::v1::meta::lock_client::LockClient;
use api::v1::meta::{LockRequest, LockResponse, Role, UnlockRequest, UnlockResponse};
use common_grpc::channel_manager::ChannelManager;
use common_telemetry::tracing_context::TracingContext;
use snafu::{ensure, OptionExt, ResultExt};
use tokio::sync::RwLock;
use tonic::transport::Channel;

use crate::client::{load_balance, Id};
use crate::error;
use crate::error::Result;

#[derive(Clone, Debug)]
pub struct Client {
    inner: Arc<RwLock<Inner>>,
}

impl Client {
    pub fn new(id: Id, role: Role, channel_manager: ChannelManager) -> Self {
        let inner = Arc::new(RwLock::new(Inner {
            id,
            role,
            channel_manager,
            peers: vec![],
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

    pub async fn is_started(&self) -> bool {
        let inner = self.inner.read().await;
        inner.is_started()
    }

    pub async fn lock(&self, req: LockRequest) -> Result<LockResponse> {
        let inner = self.inner.read().await;
        inner.lock(req).await
    }

    pub async fn unlock(&self, req: UnlockRequest) -> Result<UnlockResponse> {
        let inner = self.inner.read().await;
        inner.unlock(req).await
    }
}

#[derive(Debug)]
struct Inner {
    id: Id,
    role: Role,
    channel_manager: ChannelManager,
    peers: Vec<String>,
}

impl Inner {
    async fn start<U, A>(&mut self, urls: A) -> Result<()>
    where
        U: AsRef<str>,
        A: AsRef<[U]>,
    {
        ensure!(
            !self.is_started(),
            error::IllegalGrpcClientStateSnafu {
                err_msg: "Lock client already started",
            }
        );

        self.peers = urls
            .as_ref()
            .iter()
            .map(|url| url.as_ref().to_string())
            .collect::<HashSet<_>>()
            .drain()
            .collect::<Vec<_>>();

        Ok(())
    }

    fn random_client(&self) -> Result<LockClient<Channel>> {
        let len = self.peers.len();
        let peer = load_balance::random_get(len, |i| Some(&self.peers[i])).context(
            error::IllegalGrpcClientStateSnafu {
                err_msg: "Empty peers, lock client may not start yet",
            },
        )?;

        self.make_client(peer)
    }

    fn make_client(&self, addr: impl AsRef<str>) -> Result<LockClient<Channel>> {
        let channel = self
            .channel_manager
            .get(addr)
            .context(error::CreateChannelSnafu)?;

        Ok(LockClient::new(channel))
    }

    #[inline]
    fn is_started(&self) -> bool {
        !self.peers.is_empty()
    }

    async fn lock(&self, mut req: LockRequest) -> Result<LockResponse> {
        let mut client = self.random_client()?;
        req.set_header(
            self.id,
            self.role,
            TracingContext::from_current_span().to_w3c(),
        );
        let res = client.lock(req).await.map_err(error::Error::from)?;

        Ok(res.into_inner())
    }

    async fn unlock(&self, mut req: UnlockRequest) -> Result<UnlockResponse> {
        let mut client = self.random_client()?;
        req.set_header(
            self.id,
            self.role,
            TracingContext::from_current_span().to_w3c(),
        );
        let res = client.unlock(req).await.map_err(error::Error::from)?;

        Ok(res.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_start_client() {
        let mut client = Client::new((0, 0), Role::Datanode, ChannelManager::default());
        assert!(!client.is_started().await);
        client
            .start(&["127.0.0.1:1000", "127.0.0.1:1001"])
            .await
            .unwrap();
        assert!(client.is_started().await);
    }

    #[tokio::test]
    async fn test_already_start() {
        let mut client = Client::new((0, 0), Role::Datanode, ChannelManager::default());
        client
            .start(&["127.0.0.1:1000", "127.0.0.1:1001"])
            .await
            .unwrap();
        assert!(client.is_started().await);
        let res = client.start(&["127.0.0.1:1002"]).await;
        assert!(res.is_err());
        assert!(matches!(
            res.err(),
            Some(error::Error::IllegalGrpcClientState { .. })
        ));
    }

    #[tokio::test]
    async fn test_start_with_duplicate_peers() {
        let mut client = Client::new((0, 0), Role::Datanode, ChannelManager::default());
        client
            .start(&["127.0.0.1:1000", "127.0.0.1:1000", "127.0.0.1:1000"])
            .await
            .unwrap();

        assert_eq!(1, client.inner.write().await.peers.len());
    }
}
