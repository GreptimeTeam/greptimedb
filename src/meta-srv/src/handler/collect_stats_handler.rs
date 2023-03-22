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

use std::collections::VecDeque;

use api::v1::meta::HeartbeatRequest;
use common_telemetry::debug;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;

use super::node_stat::Stat;
use crate::error::Result;
use crate::handler::{HeartbeatAccumulator, HeartbeatHandler};
use crate::metasrv::Context;

type StatKey = (u64, u64);

pub struct CollectStatsHandler {
    max_cached_stats_per_key: usize,
    cache: DashMap<StatKey, VecDeque<Stat>>,
}

impl Default for CollectStatsHandler {
    fn default() -> Self {
        Self::new(10)
    }
}

impl CollectStatsHandler {
    pub fn new(max_cached_stats_per_key: usize) -> Self {
        Self {
            max_cached_stats_per_key,
            cache: DashMap::new(),
        }
    }
}

#[async_trait::async_trait]
impl HeartbeatHandler for CollectStatsHandler {
    async fn handle(
        &self,
        req: &HeartbeatRequest,
        ctx: &mut Context,
        acc: &mut HeartbeatAccumulator,
    ) -> Result<()> {
        if ctx.is_skip_all() {
            return Ok(());
        }

        match Stat::try_from(req.clone()) {
            Ok(stat) => {
                acc.stat = stat.clone();

                let key = (stat.cluster_id, stat.id);
                match self.cache.entry(key) {
                    Entry::Occupied(mut e) => {
                        let deque = e.get_mut();
                        deque.push_front(stat);
                        if deque.len() >= self.max_cached_stats_per_key {
                            acc.stats = deque.drain(..).collect();
                        }
                    }
                    Entry::Vacant(e) => {
                        let mut stat_vec = VecDeque::with_capacity(self.max_cached_stats_per_key);
                        stat_vec.push_front(stat);
                        e.insert(stat_vec);
                    }
                }
            }
            Err(_) => {
                debug!("Incomplete heartbeat data: {:?}", req);
            }
        };

        Ok(())
    }
}
