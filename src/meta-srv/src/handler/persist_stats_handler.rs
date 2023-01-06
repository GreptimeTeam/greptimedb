// Copyright 2023 Greptime Team
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

use crate::error::Result;
use crate::handler::{HeartbeatAccumulator, HeartbeatHandler};
use crate::keys::{StatKey, StatValue};
use crate::metasrv::Context;

#[derive(Default)]
pub struct PersistStatsHandler;

#[async_trait::async_trait]
impl HeartbeatHandler for PersistStatsHandler {
    async fn handle(
        &self,
        _req: &HeartbeatRequest,
        ctx: &Context,
        acc: &mut HeartbeatAccumulator,
    ) -> Result<()> {
        if ctx.is_skip_all() || acc.stats.is_empty() {
            return Ok(());
        }

        let stat = match acc.stats.get(0) {
            Some(stat) => stat,
            None => return Ok(()),
        };

        let key: StatKey = stat.into();
        let value: StatValue = stat.into();

        let put = PutRequest {
            key: key.into(),
            value: value.try_into()?,
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

    use api::v1::meta::RangeRequest;

    use super::*;
    use crate::handler::node_stat::Stat;
    use crate::service::store::memory::MemStore;

    #[tokio::test]
    async fn test_handle_datanode_stats() {
        let kv_store = Arc::new(MemStore::new());
        let ctx = Context {
            datanode_lease_secs: 30,
            server_addr: "127.0.0.1:0000".to_string(),
            kv_store,
            election: None,
            skip_all: Arc::new(AtomicBool::new(false)),
        };

        let req = HeartbeatRequest::default();
        let mut acc = HeartbeatAccumulator {
            stats: vec![Stat {
                cluster_id: 3,
                id: 101,
                region_num: 100,
                ..Default::default()
            }],
            ..Default::default()
        };

        let stats_handler = PersistStatsHandler;
        stats_handler.handle(&req, &ctx, &mut acc).await.unwrap();

        let key = StatKey {
            cluster_id: 3,
            node_id: 101,
        };

        let req = RangeRequest {
            key: key.try_into().unwrap(),
            ..Default::default()
        };

        let res = ctx.kv_store.range(req).await.unwrap();

        assert_eq!(1, res.kvs.len());

        let kv = &res.kvs[0];

        let key: StatKey = kv.key.clone().try_into().unwrap();
        assert_eq!(3, key.cluster_id);
        assert_eq!(101, key.node_id);

        let val: StatValue = kv.value.clone().try_into().unwrap();
        assert_eq!(100, val.region_num);
    }
}
