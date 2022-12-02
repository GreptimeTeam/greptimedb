// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use api::v1::meta::{DeleteRangeRequest, PutRequest, RangeRequest};
use meta_srv::service::store::etcd::EtcdStore;
use tracing::{event, subscriber, Level};
use tracing_subscriber::FmtSubscriber;

fn main() {
    subscriber::set_global_default(FmtSubscriber::builder().finish()).unwrap();
    run();
}

#[tokio::main]
async fn run() {
    let kv_store = EtcdStore::with_endpoints(["127.0.0.1:2380"]).await.unwrap();

    // put
    let put_req = PutRequest {
        key: b"key1".to_vec(),
        value: b"value1".to_vec(),
        prev_kv: true,
        ..Default::default()
    };
    let res = kv_store.put(put_req).await;
    event!(Level::INFO, "put result: {:#?}", res);

    // get
    let range_req = RangeRequest {
        key: b"key1".to_vec(),
        ..Default::default()
    };
    let res = kv_store.range(range_req.clone()).await;
    event!(Level::INFO, "get range result: {:#?}", res);

    // delete
    let delete_range_req = DeleteRangeRequest {
        key: b"key1".to_vec(),
        ..Default::default()
    };
    let res = kv_store.delete_range(delete_range_req).await;
    event!(Level::INFO, "delete range result: {:#?}", res);

    // get none
    let res = kv_store.range(range_req).await;
    event!(Level::INFO, "get range result: {:#?}", res);
}
