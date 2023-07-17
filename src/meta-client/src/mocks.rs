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

use api::v1::meta::Role;
use meta_srv::mocks as server_mock;
use meta_srv::mocks::MockInfo;

use crate::client::{MetaClient, MetaClientBuilder};

pub async fn mock_client_with_memstore() -> MetaClient {
    let mock_info = server_mock::mock_with_memstore().await;
    mock_client_by(mock_info).await
}

#[allow(dead_code)]
pub async fn mock_client_with_etcdstore(addr: &str) -> MetaClient {
    let mock_info = server_mock::mock_with_etcdstore(addr).await;
    mock_client_by(mock_info).await
}

pub async fn mock_client_by(mock_info: MockInfo) -> MetaClient {
    let MockInfo {
        server_addr,
        channel_manager,
        ..
    } = mock_info;

    let id = (1000u64, 2000u64);
    let mut meta_client = MetaClientBuilder::new(id.0, id.1, Role::Datanode)
        .enable_heartbeat()
        .enable_router()
        .enable_store()
        .channel_manager(channel_manager)
        .build();
    meta_client.start(&[&server_addr]).await.unwrap();
    // required only when the heartbeat_client is enabled
    meta_client.ask_leader().await.unwrap();

    meta_client
}
