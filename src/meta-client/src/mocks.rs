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

use std::sync::Arc;

use common_base::readable_size::ReadableSize;
use common_grpc::channel_manager::{ChannelConfig, ChannelManager};
use common_meta::kv_backend::memory::MemoryKvBackend;
use common_meta::kv_backend::{KvBackendRef, ResettableKvBackendRef};
use meta_srv::metasrv::MetasrvOptions;
use meta_srv::mocks as server_mock;
use meta_srv::mocks::MockInfo;

use crate::client::{MetaClient, MetaClientBuilder};

pub struct MockMetaContext {
    pub kv_backend: KvBackendRef,
    pub in_memory: Option<ResettableKvBackendRef>,
}

pub async fn mock_client_with_memstore() -> (MetaClient, MockMetaContext) {
    let MockInfo {
        server_addr,
        channel_manager,
        kv_backend,
        in_memory,
        ..
    } = server_mock::mock_with_memstore().await;
    (
        mock_client_by(server_addr, channel_manager).await,
        MockMetaContext {
            kv_backend,
            in_memory,
        },
    )
}

pub async fn mock_client_with_memstore_and_grpc_message_sizes(
    server_max_recv_message_size: ReadableSize,
    server_max_send_message_size: ReadableSize,
    client_max_recv_message_size: ReadableSize,
    client_max_send_message_size: ReadableSize,
) -> (MetaClient, MockMetaContext) {
    let mut opts = MetasrvOptions::default();
    opts.grpc.server_addr = "127.0.0.1:3002".to_string();
    opts.grpc.max_recv_message_size = server_max_recv_message_size;
    opts.grpc.max_send_message_size = server_max_send_message_size;

    let client_channel_config = ChannelConfig {
        max_recv_message_size: client_max_recv_message_size,
        max_send_message_size: client_max_send_message_size,
        ..ChannelConfig::new()
    };

    let kv_backend = Arc::new(MemoryKvBackend::new());
    let in_memory = Arc::new(MemoryKvBackend::new());
    let MockInfo {
        server_addr,
        channel_manager,
        kv_backend,
        in_memory,
        ..
    } = server_mock::mock_with_client_channel_config(
        opts,
        kv_backend,
        None,
        None,
        Some(in_memory),
        client_channel_config,
    )
    .await;

    (
        mock_client_by(server_addr, channel_manager).await,
        MockMetaContext {
            kv_backend,
            in_memory,
        },
    )
}

#[allow(dead_code)]
pub async fn mock_client_with_etcdstore(addr: &str) -> (MetaClient, MockMetaContext) {
    let MockInfo {
        server_addr,
        channel_manager,
        kv_backend,
        in_memory,
        ..
    } = server_mock::mock_with_etcdstore(addr).await;
    (
        mock_client_by(server_addr, channel_manager).await,
        MockMetaContext {
            kv_backend,
            in_memory,
        },
    )
}

pub async fn mock_client_by(server_addr: String, channel_manager: ChannelManager) -> MetaClient {
    let id = 2000u64;
    let mut meta_client = MetaClientBuilder::datanode_default_options(id)
        .enable_access_cluster_info()
        .enable_direct_store_writes_for_test()
        .channel_manager(channel_manager)
        .build();
    meta_client.start(&[&server_addr]).await.unwrap();
    // required only when the heartbeat_client is enabled
    meta_client.ask_leader().await.unwrap();

    meta_client
}
