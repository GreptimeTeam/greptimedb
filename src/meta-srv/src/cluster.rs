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

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use api::v1::meta::cluster_client::ClusterClient;
use api::v1::meta::{
    BatchGetRequest as PbBatchGetRequest, BatchGetResponse as PbBatchGetResponse,
    RangeRequest as PbRangeRequest, RangeResponse as PbRangeResponse, ResponseHeader,
};
use common_grpc::channel_manager::ChannelManager;
use common_meta::datanode::{DatanodeStatKey, DatanodeStatValue};
use common_meta::kv_backend::{KvBackend, ResettableKvBackendRef, TxnService};
use common_meta::rpc::store::{
    BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse, BatchPutRequest,
    BatchPutResponse, CompareAndPutRequest, CompareAndPutResponse, DeleteRangeRequest,
    DeleteRangeResponse, PutRequest, PutResponse, RangeRequest, RangeResponse,
};
use common_meta::rpc::KeyValue;
use common_meta::utils;
use common_telemetry::warn;
use derive_builder::Builder;
use snafu::{ensure, OptionExt, ResultExt};

use crate::error;
use crate::error::{match_for_io_error, Result};
use crate::metasrv::ElectionRef;

pub type MetaPeerClientRef = Arc<MetaPeerClient>;

#[derive(Builder)]
pub struct MetaPeerClient {
    election: Option<ElectionRef>,
    in_memory: ResettableKvBackendRef,
    #[builder(default = "ChannelManager::default()")]
    channel_manager: ChannelManager,
    #[builder(default = "3")]
    max_retry_count: usize,
    #[builder(default = "1000")]
    retry_interval_ms: u64,
}

#[async_trait::async_trait]
impl TxnService for MetaPeerClient {
    type Error = error::Error;
}

#[async_trait::async_trait]
impl KvBackend for MetaPeerClient {
    fn name(&self) -> &str {
        "MetaPeerClient"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn range(&self, req: RangeRequest) -> Result<RangeResponse> {
        if self.is_leader() {
            return self
                .in_memory
                .range(req)
                .await
                .context(error::KvBackendSnafu);
        }

        let max_retry_count = self.max_retry_count;
        let retry_interval_ms = self.retry_interval_ms;

        for _ in 0..max_retry_count {
            match self
                .remote_range(req.key.clone(), req.range_end.clone(), req.keys_only)
                .await
            {
                Ok(res) => return Ok(res),
                Err(e) => {
                    if need_retry(&e) {
                        warn!(e; "Encountered an error that need to retry");
                        tokio::time::sleep(Duration::from_millis(retry_interval_ms)).await;
                    } else {
                        return Err(e);
                    }
                }
            }
        }

        error::ExceededRetryLimitSnafu {
            func_name: "range",
            retry_num: max_retry_count,
        }
        .fail()
    }

    // MetaPeerClient does not support mutable methods listed below.
    async fn put(&self, _req: PutRequest) -> Result<PutResponse> {
        error::UnsupportedSnafu {
            operation: "put".to_string(),
        }
        .fail()
    }

    async fn batch_put(&self, _req: BatchPutRequest) -> Result<BatchPutResponse> {
        error::UnsupportedSnafu {
            operation: "batch put".to_string(),
        }
        .fail()
    }

    // Get kv information from the leader's in_mem kv store
    async fn batch_get(&self, req: BatchGetRequest) -> Result<BatchGetResponse> {
        if self.is_leader() {
            return self
                .in_memory
                .batch_get(req)
                .await
                .context(error::KvBackendSnafu);
        }

        let max_retry_count = self.max_retry_count;
        let retry_interval_ms = self.retry_interval_ms;

        for _ in 0..max_retry_count {
            match self.remote_batch_get(req.keys.clone()).await {
                Ok(res) => return Ok(res),
                Err(e) => {
                    if need_retry(&e) {
                        warn!(e; "Encountered an error that need to retry");
                        tokio::time::sleep(Duration::from_millis(retry_interval_ms)).await;
                    } else {
                        return Err(e);
                    }
                }
            }
        }

        error::ExceededRetryLimitSnafu {
            func_name: "batch_get",
            retry_num: max_retry_count,
        }
        .fail()
    }

    async fn delete_range(&self, _req: DeleteRangeRequest) -> Result<DeleteRangeResponse> {
        error::UnsupportedSnafu {
            operation: "delete range".to_string(),
        }
        .fail()
    }

    async fn batch_delete(&self, _req: BatchDeleteRequest) -> Result<BatchDeleteResponse> {
        error::UnsupportedSnafu {
            operation: "batch delete".to_string(),
        }
        .fail()
    }

    async fn compare_and_put(&self, _req: CompareAndPutRequest) -> Result<CompareAndPutResponse> {
        error::UnsupportedSnafu {
            operation: "compare and put".to_string(),
        }
        .fail()
    }

    async fn put_conditionally(
        &self,
        _key: Vec<u8>,
        _value: Vec<u8>,
        _if_not_exists: bool,
    ) -> Result<bool> {
        error::UnsupportedSnafu {
            operation: "put conditionally".to_string(),
        }
        .fail()
    }

    async fn delete(&self, _key: &[u8], _prev_kv: bool) -> Result<Option<KeyValue>> {
        error::UnsupportedSnafu {
            operation: "delete".to_string(),
        }
        .fail()
    }
}

impl MetaPeerClient {
    async fn get_dn_key_value(&self, keys_only: bool) -> Result<Vec<KeyValue>> {
        let key = DatanodeStatKey::prefix_key();
        let range_end = utils::get_prefix_end_key(&key);
        let range_request = RangeRequest {
            key,
            range_end,
            keys_only,
            ..Default::default()
        };
        self.range(range_request).await.map(|res| res.kvs)
    }

    // Get all datanode stat kvs from leader meta.
    pub async fn get_all_dn_stat_kvs(&self) -> Result<HashMap<DatanodeStatKey, DatanodeStatValue>> {
        let kvs = self.get_dn_key_value(false).await?;
        to_stat_kv_map(kvs)
    }

    pub async fn get_node_cnt(&self) -> Result<i32> {
        let kvs = self.get_dn_key_value(true).await?;
        kvs.into_iter()
            .map(|kv| {
                kv.key
                    .try_into()
                    .context(error::InvalidDatanodeStatFormatSnafu {})
            })
            .collect::<Result<HashSet<DatanodeStatKey>>>()
            .map(|hash_set| hash_set.len() as i32)
    }

    // Get datanode stat kvs from leader meta by input keys.
    pub async fn get_dn_stat_kvs(
        &self,
        keys: Vec<DatanodeStatKey>,
    ) -> Result<HashMap<DatanodeStatKey, DatanodeStatValue>> {
        let stat_keys = keys.into_iter().map(|key| key.into()).collect();
        let batch_get_req = BatchGetRequest { keys: stat_keys };

        let res = self.batch_get(batch_get_req).await?;

        to_stat_kv_map(res.kvs)
    }

    async fn remote_range(
        &self,
        key: Vec<u8>,
        range_end: Vec<u8>,
        keys_only: bool,
    ) -> Result<RangeResponse> {
        // Safety: when self.is_leader() == false, election must not empty.
        let election = self.election.as_ref().unwrap();

        let leader_addr = election.leader().await?.0;

        let channel = self
            .channel_manager
            .get(&leader_addr)
            .context(error::CreateChannelSnafu)?;

        let request = tonic::Request::new(PbRangeRequest {
            key,
            range_end,
            keys_only,
            ..Default::default()
        });

        let response: PbRangeResponse = ClusterClient::new(channel)
            .range(request)
            .await
            .context(error::RangeSnafu)?
            .into_inner();

        check_resp_header(&response.header, Context { addr: &leader_addr })?;

        Ok(RangeResponse {
            kvs: response.kvs.into_iter().map(KeyValue::new).collect(),
            more: response.more,
        })
    }

    async fn remote_batch_get(&self, keys: Vec<Vec<u8>>) -> Result<BatchGetResponse> {
        // Safety: when self.is_leader() == false, election must not empty.
        let election = self.election.as_ref().unwrap();

        let leader_addr = election.leader().await?.0;

        let channel = self
            .channel_manager
            .get(&leader_addr)
            .context(error::CreateChannelSnafu)?;

        let request = tonic::Request::new(PbBatchGetRequest {
            keys,
            ..Default::default()
        });

        let response: PbBatchGetResponse = ClusterClient::new(channel)
            .batch_get(request)
            .await
            .context(error::BatchGetSnafu)?
            .into_inner();

        check_resp_header(&response.header, Context { addr: &leader_addr })?;

        Ok(BatchGetResponse {
            kvs: response.kvs.into_iter().map(KeyValue::new).collect(),
        })
    }

    // Check if the meta node is a leader node.
    // Note: when self.election is None, we also consider the meta node is leader
    pub(crate) fn is_leader(&self) -> bool {
        self.election
            .as_ref()
            .map(|election| election.is_leader())
            .unwrap_or(true)
    }

    #[cfg(test)]
    pub(crate) fn memory_backend(&self) -> ResettableKvBackendRef {
        self.in_memory.clone()
    }
}

fn to_stat_kv_map(kvs: Vec<KeyValue>) -> Result<HashMap<DatanodeStatKey, DatanodeStatValue>> {
    let mut map = HashMap::with_capacity(kvs.len());
    for kv in kvs {
        let _ = map.insert(
            kv.key
                .try_into()
                .context(error::InvalidDatanodeStatFormatSnafu {})?,
            kv.value
                .try_into()
                .context(error::InvalidDatanodeStatFormatSnafu {})?,
        );
    }
    Ok(map)
}

struct Context<'a> {
    addr: &'a str,
}

fn check_resp_header(header: &Option<ResponseHeader>, ctx: Context) -> Result<()> {
    let header = header
        .as_ref()
        .context(error::ResponseHeaderNotFoundSnafu)?;

    ensure!(
        !header.is_not_leader(),
        error::IsNotLeaderSnafu {
            node_addr: ctx.addr
        }
    );

    Ok(())
}

fn need_retry(error: &error::Error) -> bool {
    match error {
        error::Error::IsNotLeader { .. } => true,
        error::Error::Range { error, .. } | error::Error::BatchGet { error, .. } => {
            match_for_io_error(error).is_some()
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use api::v1::meta::{Error, ErrorCode, ResponseHeader};
    use common_meta::datanode::{DatanodeStatKey, DatanodeStatValue, Stat};
    use common_meta::rpc::KeyValue;

    use super::{check_resp_header, to_stat_kv_map, Context};
    use crate::error;

    #[test]
    fn test_to_stat_kv_map() {
        let stat_key = DatanodeStatKey {
            cluster_id: 0,
            node_id: 100,
        };

        let stat = Stat {
            cluster_id: 0,
            id: 100,
            addr: "127.0.0.1:3001".to_string(),
            ..Default::default()
        };
        let stat_val = DatanodeStatValue { stats: vec![stat] }.try_into().unwrap();

        let kv = KeyValue {
            key: stat_key.into(),
            value: stat_val,
        };

        let kv_map = to_stat_kv_map(vec![kv]).unwrap();
        assert_eq!(1, kv_map.len());
        let _ = kv_map.get(&stat_key).unwrap();

        let stat_val = kv_map.get(&stat_key).unwrap();
        let stat = stat_val.stats.first().unwrap();

        assert_eq!(0, stat.cluster_id);
        assert_eq!(100, stat.id);
        assert_eq!("127.0.0.1:3001", stat.addr);
    }

    #[test]
    fn test_check_resp_header() {
        let header = Some(ResponseHeader {
            error: None,
            ..Default::default()
        });
        check_resp_header(&header, mock_ctx()).unwrap();

        let result = check_resp_header(&None, mock_ctx());
        assert!(result.is_err());
        assert!(matches!(
            result.err().unwrap(),
            error::Error::ResponseHeaderNotFound { .. }
        ));

        let header = Some(ResponseHeader {
            error: Some(Error {
                code: ErrorCode::NotLeader as i32,
                err_msg: "The current meta is not leader".to_string(),
            }),
            ..Default::default()
        });
        let result = check_resp_header(&header, mock_ctx());
        assert!(result.is_err());
        assert!(matches!(
            result.err().unwrap(),
            error::Error::IsNotLeader { .. }
        ));
    }

    fn mock_ctx<'a>() -> Context<'a> {
        Context { addr: "addr" }
    }
}
