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

use std::time::Duration;

use api::v1::meta::{HeartbeatRequest, Peer};
use common_grpc::channel_manager::{ChannelConfig, ChannelManager};
use common_meta::rpc::store::{BatchGetRequest, RangeRequest};
use meta_client::client::MetaClientBuilder;
use tracing::{Level, event, subscriber};
use tracing_subscriber::FmtSubscriber;

fn main() {
    subscriber::set_global_default(FmtSubscriber::builder().finish()).unwrap();
    run();
}

#[tokio::main]
async fn run() {
    let id = 2000u64;
    let config = ChannelConfig::new()
        .timeout(Some(Duration::from_secs(3)))
        .connect_timeout(Duration::from_secs(5))
        .tcp_nodelay(true);
    let channel_manager = ChannelManager::with_config(config, None);
    let mut meta_client = MetaClientBuilder::datanode_default_options(id)
        .channel_manager(channel_manager)
        .build();
    meta_client.start(&["127.0.0.1:3002"]).await.unwrap();
    // required only when the heartbeat_client is enabled
    meta_client.ask_leader().await.unwrap();

    let (sender, mut receiver, _config) = meta_client.heartbeat().await.unwrap();

    // send heartbeats
    let _handle = tokio::spawn(async move {
        for _ in 0..5 {
            let req = HeartbeatRequest {
                peer: Some(Peer {
                    id: 1,
                    addr: "meta_client_peer".to_string(),
                }),
                ..Default::default()
            };
            sender.send(req).await.unwrap();
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    });

    let _handle = tokio::spawn(async move {
        while let Some(res) = receiver.message().await.unwrap() {
            event!(Level::TRACE, "heartbeat response: {:#?}", res);
        }
    });

    // get
    let range = RangeRequest::new().with_key(b"key1".to_vec());
    let res = meta_client.range(range.clone()).await.unwrap();
    event!(Level::INFO, "get range result: {:#?}", res);

    // get prefix
    let range2 = RangeRequest::new().with_prefix(b"key1".to_vec());
    let res = meta_client.range(range2.clone()).await.unwrap();
    event!(Level::INFO, "get prefix result: {:#?}", res);

    // batch get
    let batch_get = BatchGetRequest::new()
        .add_key(b"key1".to_vec())
        .add_key(b"key2".to_vec());

    let res = meta_client.batch_get(batch_get).await.unwrap();
    event!(Level::INFO, "batch get result: {:#?}", res);
}
