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

use async_trait::async_trait;
use common_greptimedb_telemetry::{
    Collector, GreptimeDBTelemetry, GreptimeDBTelemetryTask, Mode as VersionReporterMode,
    TELEMETRY_INTERVAL, TELEMETRY_UUID_KEY,
};
use common_meta::rpc::store::{PutRequest, RangeRequest};

use crate::cluster::MetaPeerClientRef;
use crate::service::store::kv::KvStoreRef;

struct DistributedGreptimeDBTelemetryCollector {
    meta_peer_client: MetaPeerClientRef,
    kv_store: KvStoreRef,
    uuid: Option<String>,
    retry: i32,
    uuid_key_name: Vec<u8>,
}

async fn get_uuid(key: &Vec<u8>, kv_store: KvStoreRef) -> Option<String> {
    let req = RangeRequest::new().with_key((*key).clone()).with_limit(1);
    let kv_res = kv_store.range(req).await;
    match kv_res {
        Ok(mut res) => {
            if !res.kvs.is_empty() {
                res.kvs
                    .pop()
                    .and_then(|kv| String::from_utf8(kv.value).ok())
            } else {
                let uuid = uuid::Uuid::new_v4().to_string();
                let req = PutRequest {
                    key: (*key).clone(),
                    value: uuid.clone().into_bytes(),
                    ..Default::default()
                };
                let put_result = kv_store.put(req.clone()).await;
                put_result.ok().map(|_| uuid)
            }
        }
        Err(_) => None,
    }
}

#[async_trait]
impl Collector for DistributedGreptimeDBTelemetryCollector {
    fn get_mode(&self) -> VersionReporterMode {
        VersionReporterMode::Distributed
    }
    async fn get_nodes(&self) -> i32 {
        self.meta_peer_client
            .get_node_cnt()
            .await
            .ok()
            .unwrap_or(-1)
    }
    async fn get_uuid(&mut self) -> String {
        if let Some(uuid) = self.uuid.clone() {
            return uuid;
        } else if self.retry > 3 {
            return "".to_string();
        } else {
            let uuid = get_uuid(&self.uuid_key_name, self.kv_store.clone()).await;
            if let Some(_uuid) = uuid {
                self.uuid = Some(_uuid.clone());
                return _uuid;
            } else {
                self.retry += 1;
                return "".to_string();
            }
        }
    }
}

pub async fn get_greptimedb_telemetry_task(
    meta_peer_client: MetaPeerClientRef,
    kv_store: KvStoreRef,
) -> Arc<GreptimeDBTelemetryTask> {
    if cfg!(feature = "greptimedb-telemetry") {
        Arc::new(GreptimeDBTelemetryTask::enable(
            TELEMETRY_INTERVAL,
            Box::new(GreptimeDBTelemetry::new(Box::new(
                DistributedGreptimeDBTelemetryCollector {
                    meta_peer_client,
                    kv_store,
                    uuid: None,
                    retry: 0,
                    uuid_key_name: TELEMETRY_UUID_KEY.as_bytes().to_vec(),
                },
            ))),
        ))
    } else {
        Arc::new(GreptimeDBTelemetryTask::disable())
    }
}
