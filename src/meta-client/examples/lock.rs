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

use api::v1::meta::Role;
use common_grpc::channel_manager::{ChannelConfig, ChannelManager};
use common_meta::rpc::lock::{LockRequest, UnlockRequest};
use meta_client::client::{MetaClient, MetaClientBuilder};
use tracing::{info, subscriber};
use tracing_subscriber::FmtSubscriber;

fn main() {
    subscriber::set_global_default(FmtSubscriber::builder().finish()).unwrap();
    run();
}

#[tokio::main]
async fn run() {
    let id = (1000u64, 2000u64);
    let config = ChannelConfig::new()
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(5))
        .tcp_nodelay(true);
    let channel_manager = ChannelManager::with_config(config);
    let mut meta_client = MetaClientBuilder::new(id.0, id.1, Role::Datanode)
        .enable_lock()
        .channel_manager(channel_manager)
        .build();
    meta_client.start(&["127.0.0.1:3002"]).await.unwrap();

    run_normal(meta_client.clone()).await;

    run_multi_thread(meta_client.clone()).await;

    run_multi_thread_with_one_timeout(meta_client).await;
}

async fn run_normal(meta_client: MetaClient) {
    let name = "lock_name".as_bytes().to_vec();
    let expire_secs = 60;

    let lock_req = LockRequest { name, expire_secs };

    let lock_result = meta_client.lock(lock_req).await.unwrap();
    let key = lock_result.key;
    info!(
        "lock success! Returned key: {}",
        String::from_utf8(key.clone()).unwrap()
    );

    // It is recommended that time of holding lock is less than the timeout of the grpc channel
    info!("do some work, take 3 seconds");
    tokio::time::sleep(Duration::from_secs(3)).await;

    let unlock_req = UnlockRequest { key };

    meta_client.unlock(unlock_req).await.unwrap();
    info!("unlock success!");
}

async fn run_multi_thread(meta_client: MetaClient) {
    let meta_client_clone = meta_client.clone();
    let join1 = tokio::spawn(async move {
        run_normal(meta_client_clone.clone()).await;
    });

    tokio::time::sleep(Duration::from_secs(1)).await;

    let join2 = tokio::spawn(async move {
        run_normal(meta_client).await;
    });

    join1.await.unwrap();
    join2.await.unwrap();
}

async fn run_multi_thread_with_one_timeout(meta_client: MetaClient) {
    let meta_client_clone = meta_client.clone();
    let join1 = tokio::spawn(async move {
        run_with_timeout(meta_client_clone.clone()).await;
    });

    tokio::time::sleep(Duration::from_secs(1)).await;

    let join2 = tokio::spawn(async move {
        run_normal(meta_client).await;
    });

    join1.await.unwrap();
    join2.await.unwrap();
}

async fn run_with_timeout(meta_client: MetaClient) {
    let name = "lock_name".as_bytes().to_vec();
    let expire_secs = 5;

    let lock_req = LockRequest { name, expire_secs };

    let lock_result = meta_client.lock(lock_req).await.unwrap();
    let key = lock_result.key;
    info!(
        "lock success! Returned key: {}",
        String::from_utf8(key.clone()).unwrap()
    );

    // It is recommended that time of holding lock is less than the timeout of the grpc channel
    info!("do some work, take 20 seconds");
    tokio::time::sleep(Duration::from_secs(20)).await;

    let unlock_req = UnlockRequest { key };

    meta_client.unlock(unlock_req).await.unwrap();
    info!("unlock success!");
}
