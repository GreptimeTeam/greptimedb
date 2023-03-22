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
use crate::keys::StatValue;
use crate::metasrv::Context;

#[derive(Default)]
pub struct PersistStatsHandler;

#[async_trait::async_trait]
impl HeartbeatHandler for PersistStatsHandler {
    async fn handle(
        &self,
        _req: &HeartbeatRequest,
        ctx: &mut Context,
        acc: &mut HeartbeatAccumulator,
    ) -> Result<()> {
        if ctx.is_skip_all() || acc.stats.is_empty() {
            return Ok(());
        }

        let stats = &mut acc.stats;
        let key = match stats.get(0) {
            Some(stat) => stat.stat_key(),
            None => return Ok(()),
        };

        // take stats from &mut acc.stats, avoid clone of vec
        let stats = std::mem::take(stats);

        let val = StatValue { stats };

        let put = PutRequest {
            key: key.into(),
            value: val.try_into()?,
            ..Default::default()
        };

        ctx.in_memory.put(put).await?;

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
    use crate::keys::StatKey;
    use crate::service::store::memory::MemStore;

    #[tokio::test]
    async fn test_handle_datanode_stats() {
        let in_memory = Arc::new(MemStore::new());
        let kv_store = Arc::new(MemStore::new());
        let mut ctx = Context {
            datanode_lease_secs: 30,
            server_addr: "127.0.0.1:0000".to_string(),
            in_memory,
            kv_store,
            election: None,
            skip_all: Arc::new(AtomicBool::new(false)),
            catalog: None,
            schema: None,
            table: None,
        };

        let req = HeartbeatRequest::default();
        let mut acc = HeartbeatAccumulator {
            stats: vec![Arc::new(Stat {
                cluster_id: 3,
                id: 101,
                region_num: Some(100),
                ..Default::default()
            })],
            ..Default::default()
        };

        let stats_handler = PersistStatsHandler;
        stats_handler
            .handle(&req, &mut ctx, &mut acc)
            .await
            .unwrap();

        let key = StatKey {
            cluster_id: 3,
            node_id: 101,
        };

        let req = RangeRequest {
            key: key.try_into().unwrap(),
            ..Default::default()
        };

        let res = ctx.in_memory.range(req).await.unwrap();

        assert_eq!(1, res.kvs.len());

        let kv = &res.kvs[0];

        let key: StatKey = kv.key.clone().try_into().unwrap();
        assert_eq!(3, key.cluster_id);
        assert_eq!(101, key.node_id);

        let val: StatValue = kv.value.clone().try_into().unwrap();

        assert_eq!(1, val.stats.len());
        assert_eq!(Some(100), val.stats[0].region_num);
    }
}
