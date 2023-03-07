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

use meta_client::client::{MetaClient, MetaClientBuilder};
use meta_srv::mocks::MockInfo;
use storage::compaction::noop::NoopCompactionScheduler;

use crate::datanode::DatanodeOptions;
use crate::error::Result;
use crate::instance::Instance;

impl Instance {
    pub async fn with_mock_meta_client(opts: &DatanodeOptions) -> Result<Self> {
        let mock_info = meta_srv::mocks::mock_with_memstore().await;
        Self::with_mock_meta_server(opts, mock_info).await
    }

    pub async fn with_mock_meta_server(opts: &DatanodeOptions, meta_srv: MockInfo) -> Result<Self> {
        let meta_client = Arc::new(mock_meta_client(meta_srv, opts.node_id.unwrap_or(42)).await);
        let compaction_scheduler = Arc::new(NoopCompactionScheduler::default());
        Instance::new_with(opts, Some(meta_client), compaction_scheduler).await
    }
}

async fn mock_meta_client(mock_info: MockInfo, node_id: u64) -> MetaClient {
    let MockInfo {
        server_addr,
        channel_manager,
    } = mock_info;

    let id = (1000u64, 2000u64);
    let mut meta_client = MetaClientBuilder::new(id.0, node_id)
        .enable_heartbeat()
        .enable_router()
        .enable_store()
        .channel_manager(channel_manager)
        .build();
    meta_client.start(&[&server_addr]).await.unwrap();
    // // required only when the heartbeat_client is enabled
    meta_client.ask_leader().await.unwrap();

    meta_client
}
