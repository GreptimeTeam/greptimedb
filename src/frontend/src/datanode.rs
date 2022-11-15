use std::time::Duration;

use client::Client;
use common_grpc::channel_manager::ChannelManager;
use meta_client::rpc::Peer;
use moka::future::{Cache, CacheBuilder};

pub(crate) struct DatanodeClients {
    channel_manager: ChannelManager,
    clients: Cache<Peer, Client>,
}

impl DatanodeClients {
    pub(crate) fn new() -> Self {
        Self {
            channel_manager: ChannelManager::new(),
            clients: CacheBuilder::new(1024)
                .time_to_live(Duration::from_secs(30 * 60))
                .time_to_idle(Duration::from_secs(5 * 60))
                .build(),
        }
    }

    pub(crate) async fn get_client(&self, datanode: &Peer) -> Client {
        self.clients
            .get_with_by_ref(datanode, async move {
                Client::with_manager_and_urls(
                    self.channel_manager.clone(),
                    vec![datanode.addr.clone()],
                )
            })
            .await
    }

    #[cfg(test)]
    pub(crate) async fn insert_client(&self, datanode: Peer, client: Client) {
        self.clients.insert(datanode, client).await
    }
}
