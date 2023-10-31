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

use api::v1::meta::{HeartbeatRequest, RegionLease, Role};
use async_trait::async_trait;

use crate::error::Result;
use crate::handler::{HeartbeatAccumulator, HeartbeatHandler};
use crate::inactive_region_manager::InactiveRegionManager;
use crate::metasrv::Context;

pub struct RegionLeaseHandler {
    region_lease_seconds: u64,
}

impl RegionLeaseHandler {
    pub fn new(region_lease_seconds: u64) -> Self {
        Self {
            region_lease_seconds,
        }
    }
}

#[async_trait]
impl HeartbeatHandler for RegionLeaseHandler {
    fn is_acceptable(&self, role: Role) -> bool {
        role == Role::Datanode
    }

    async fn handle(
        &self,
        req: &HeartbeatRequest,
        ctx: &mut Context,
        acc: &mut HeartbeatAccumulator,
    ) -> Result<()> {
        let Some(stat) = acc.stat.as_ref() else {
            return Ok(());
        };

        let mut region_ids = stat.region_ids();

        let inactive_region_manager = InactiveRegionManager::new(&ctx.in_memory);
        let inactive_region_ids = inactive_region_manager
            .retain_active_regions(stat.cluster_id, stat.id, &mut region_ids)
            .await?;

        acc.inactive_region_ids = inactive_region_ids;
        acc.region_lease = Some(RegionLease {
            region_ids,
            duration_since_epoch: req.duration_since_epoch,
            lease_seconds: self.region_lease_seconds,
        });

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use common_meta::key::TableMetadataManager;
    use common_meta::{distributed_time_constants, RegionIdent};
    use store_api::storage::{RegionId, RegionNumber};

    use super::*;
    use crate::handler::node_stat::{RegionStat, Stat};
    use crate::metasrv::builder::MetaSrvBuilder;
    use crate::service::store::kv::KvBackendAdapter;
    use crate::test_util;

    #[tokio::test]
    async fn test_handle_region_lease() {
        let region_failover_manager = test_util::create_region_failover_manager();
        let kv_store = region_failover_manager
            .create_context()
            .selector_ctx
            .kv_store
            .clone();

        let table_id = 1;
        let table_name = "my_table";
        let table_metadata_manager = Arc::new(TableMetadataManager::new(KvBackendAdapter::wrap(
            kv_store.clone(),
        )));
        test_util::prepare_table_region_and_info_value(&table_metadata_manager, table_name).await;

        let req = HeartbeatRequest {
            duration_since_epoch: 1234,
            ..Default::default()
        };

        let builder = MetaSrvBuilder::new();
        let metasrv = builder.build().await.unwrap();
        let ctx = &mut metasrv.new_ctx();

        let acc = &mut HeartbeatAccumulator::default();
        let new_region_stat = |region_number: RegionNumber| -> RegionStat {
            let region_id = RegionId::new(table_id, region_number);
            RegionStat {
                id: region_id.as_u64(),
                ..Default::default()
            }
        };
        acc.stat = Some(Stat {
            cluster_id: 1,
            id: 1,
            region_stats: vec![new_region_stat(1), new_region_stat(2), new_region_stat(3)],
            ..Default::default()
        });

        let inactive_region_manager = InactiveRegionManager::new(&ctx.in_memory);
        inactive_region_manager
            .register_inactive_region(&RegionIdent {
                cluster_id: 1,
                datanode_id: 1,
                table_id: 1,
                region_number: 1,
                engine: "mito2".to_string(),
            })
            .await
            .unwrap();
        inactive_region_manager
            .register_inactive_region(&RegionIdent {
                cluster_id: 1,
                datanode_id: 1,
                table_id: 1,
                region_number: 3,
                engine: "mito2".to_string(),
            })
            .await
            .unwrap();

        RegionLeaseHandler::new(distributed_time_constants::REGION_LEASE_SECS)
            .handle(&req, ctx, acc)
            .await
            .unwrap();

        assert!(acc.region_lease.is_some());
        let lease = acc.region_lease.as_ref().unwrap();
        assert_eq!(lease.region_ids, vec![RegionId::new(table_id, 2).as_u64()]);
        assert_eq!(lease.duration_since_epoch, 1234);
        assert_eq!(
            lease.lease_seconds,
            distributed_time_constants::REGION_LEASE_SECS
        );
    }
}
