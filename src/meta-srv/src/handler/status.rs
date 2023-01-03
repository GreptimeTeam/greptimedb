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

use api::v1::meta::{HeartbeatRequest, PutRequest};

use super::{HeartbeatAccumulator, HeartbeatHandler};
use crate::error::Result;
use crate::keys::{StatusKey, StatusValue};
use crate::metasrv::Context;

pub struct StatusHandler;

#[async_trait::async_trait]
impl HeartbeatHandler for StatusHandler {
    async fn handle(
        &self,
        req: &HeartbeatRequest,
        ctx: &Context,
        _acc: &mut HeartbeatAccumulator,
    ) -> Result<()> {
        if ctx.is_skip_all() {
            return Ok(());
        }

        let HeartbeatRequest {
            header,
            peer,
            node_stat,
            ..
        } = req;

        let peer = if let Some(peer) = peer {
            peer
        } else {
            return Ok(());
        };

        let node_stat = if let Some(node_stat) = node_stat {
            node_stat
        } else {
            return Ok(());
        };

        let key = StatusKey {
            cluster_id: header.as_ref().map_or(0, |h| h.cluster_id),
            node_id: peer.id,
        }
        .into();

        let value = StatusValue {
            region_number: node_stat.region_num,
        }
        .try_into()?;

        let put = PutRequest {
            key,
            value,
            ..Default::default()
        };

        ctx.kv_store.put(put).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    use api::v1::meta::{NodeStat, Peer, RangeRequest, RequestHeader};

    use super::*;
    use crate::service::store::memory::MemStore;

    #[tokio::test]
    async fn test_handle_datanode_status() {
        let kv_store = Arc::new(MemStore::new());
        let ctx = Context {
            datanode_lease_secs: 30,
            server_addr: "127.0.0.1:0000".to_string(),
            kv_store,
            election: None,
            skip_all: Arc::new(AtomicBool::new(false)),
        };

        let req = HeartbeatRequest {
            header: Some(RequestHeader::new((1, 2))),
            peer: Some(Peer {
                id: 3,
                addr: "127.0.0.1:1111".to_string(),
            }),
            node_stat: Some(NodeStat {
                region_num: 3,
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut acc = HeartbeatAccumulator::default();

        let status_handler = StatusHandler;
        status_handler.handle(&req, &ctx, &mut acc).await.unwrap();

        let key = StatusKey {
            cluster_id: 1,
            node_id: 3,
        };

        let req = RangeRequest {
            key: key.try_into().unwrap(),
            ..Default::default()
        };

        let res = ctx.kv_store.range(req).await.unwrap();

        assert_eq!(1, res.kvs.len());
    }
}
