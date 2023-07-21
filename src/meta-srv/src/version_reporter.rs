use std::sync::Arc;

use async_trait::async_trait;
use common_meta::rpc::store::{PutRequest, RangeRequest};
use common_meta::version_reporter::{
    GreptimeVersionReport, Mode as VersionReporterMode, Reporter, VersionReportTask,
    VERSION_REPORT_INTERVAL, VERSION_UUID_KEY,
};

use crate::cluster::MetaPeerClientRef;
use crate::service::store::kv::KvStoreRef;

struct MetaVersionReport {
    meta_peer_client: MetaPeerClientRef,
    kv_store: KvStoreRef,
    uuid: Option<String>,
    retry: i32,
    uuid_key_name: Vec<u8>,
}

fn get_uuid_key_name() -> Vec<u8> {
    VERSION_UUID_KEY.as_bytes().to_vec()
}

async fn get_uuid(key: &Vec<u8>, kv_store: KvStoreRef) -> Option<String> {
    let req = RangeRequest::new().with_key((*key).clone()).with_limit(1);
    let kv_res = kv_store.range(req).await;
    match kv_res {
        Ok(mut res) => {
            if res.kvs.len() > 0 {
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
impl Reporter for MetaVersionReport {
    async fn get_mode(&self) -> VersionReporterMode {
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
        if let Some(_uuid) = self.uuid.clone() {
            return _uuid;
        } else {
            if self.retry > 3 {
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
}

pub async fn get_version_reporter_task(
    meta_peer_client: MetaPeerClientRef,
    kv_store: KvStoreRef,
) -> Arc<VersionReportTask> {
    Arc::new(VersionReportTask::new(
        *VERSION_REPORT_INTERVAL,
        Box::new(GreptimeVersionReport::new(Box::new(MetaVersionReport {
            meta_peer_client,
            kv_store,
            uuid: None,
            retry: 0,
            uuid_key_name: get_uuid_key_name(),
        }))),
    ))
}
