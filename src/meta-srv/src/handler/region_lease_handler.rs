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

use std::collections::HashMap;
use std::sync::Arc;

use api::v1::meta::{HeartbeatRequest, RegionLease, Role};
use async_trait::async_trait;
use catalog::helper::TableGlobalKey;
use common_meta::ident::TableIdent;
use common_meta::ClusterId;
use store_api::storage::RegionNumber;
use table::engine::TableReference;

use crate::error::Result;
use crate::handler::{HeartbeatAccumulator, HeartbeatHandler};
use crate::metasrv::Context;
use crate::procedure::region_failover::{RegionFailoverKey, RegionFailoverManager};
use crate::service::store::kv::KvStoreRef;
use crate::table_routes;

/// The lease seconds of a region. It's set by two default heartbeat intervals (5 second × 2) plus
/// two roundtrip time (2 second × 2 × 2), plus some extra buffer (2 second).
// TODO(LFC): Make region lease seconds calculated from Datanode heartbeat configuration.
pub(crate) const REGION_LEASE_SECONDS: u64 = 20;

pub(crate) struct RegionLeaseHandler {
    kv_store: KvStoreRef,
    region_failover_manager: Option<Arc<RegionFailoverManager>>,
}

impl RegionLeaseHandler {
    pub(crate) fn new(
        kv_store: KvStoreRef,
        region_failover_manager: Option<Arc<RegionFailoverManager>>,
    ) -> Self {
        Self {
            kv_store,
            region_failover_manager,
        }
    }

    /// Filter out the regions that are currently in failover.
    /// It's meaningless to extend the lease of a region if it is in failover.
    fn filter_failover_regions(
        &self,
        cluster_id: ClusterId,
        table_ident: &TableIdent,
        regions: Vec<RegionNumber>,
    ) -> Vec<RegionNumber> {
        if let Some(region_failover_manager) = &self.region_failover_manager {
            let mut region_failover_key = RegionFailoverKey {
                cluster_id,
                table_ident: table_ident.clone(),
                region_number: 0,
            };

            regions
                .into_iter()
                .filter(|region| {
                    region_failover_key.region_number = *region;
                    !region_failover_manager.is_region_failover_running(&region_failover_key)
                })
                .collect()
        } else {
            regions
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
        _: &mut Context,
        acc: &mut HeartbeatAccumulator,
    ) -> Result<()> {
        let Some(stat) = acc.stat.as_ref() else { return Ok(()) };

        let mut datanode_regions = HashMap::new();
        stat.region_stats.iter().for_each(|x| {
            let table_ref = TableReference::full(&x.catalog, &x.schema, &x.table);
            datanode_regions
                .entry(table_ref)
                .or_insert_with(Vec::new)
                .push(table::engine::region_number(x.id));
        });

        let mut region_leases = Vec::with_capacity(datanode_regions.len());
        for (table_ref, local_regions) in datanode_regions {
            let table_global_key = TableGlobalKey {
                catalog_name: table_ref.catalog.to_string(),
                schema_name: table_ref.schema.to_string(),
                table_name: table_ref.table.to_string(),
            };
            let Some(table_global_value) = table_routes::get_table_global_value(&self.kv_store, &table_global_key).await? else { continue };

            let Some(global_regions) = table_global_value.regions_id_map.get(&stat.id) else { continue };

            // Filter out the designated regions from table global metadata for the given table on the given Datanode.
            let designated_regions = local_regions
                .into_iter()
                .filter(|x| global_regions.contains(x))
                .collect::<Vec<_>>();

            let table_ident = TableIdent {
                catalog: table_ref.catalog.to_string(),
                schema: table_ref.schema.to_string(),
                table: table_ref.table.to_string(),
                table_id: table_global_value.table_id(),
                engine: table_global_value.engine().to_string(),
            };
            let designated_regions =
                self.filter_failover_regions(stat.cluster_id, &table_ident, designated_regions);

            region_leases.push(RegionLease {
                table_ident: Some(table_ident.into()),
                regions: designated_regions,
                duration_since_epoch: req.duration_since_epoch,
                lease_seconds: REGION_LEASE_SECONDS,
            });
        }
        acc.region_leases = region_leases;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};

    use super::*;
    use crate::handler::node_stat::{RegionStat, Stat};
    use crate::metasrv::builder::MetaSrvBuilder;
    use crate::test_util;

    #[tokio::test]
    async fn test_handle_region_lease() {
        let region_failover_manager = test_util::create_region_failover_manager();
        let kv_store = region_failover_manager
            .create_context()
            .selector_ctx
            .kv_store
            .clone();

        let table_name = "my_table";
        let _ = table_routes::tests::prepare_table_global_value(&kv_store, table_name).await;

        let table_ident = TableIdent {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table: table_name.to_string(),
            table_id: 1,
            engine: "mito".to_string(),
        };
        region_failover_manager
            .running_procedures()
            .write()
            .unwrap()
            .insert(RegionFailoverKey {
                cluster_id: 1,
                table_ident: table_ident.clone(),
                region_number: 1,
            });

        let handler = RegionLeaseHandler::new(kv_store, Some(region_failover_manager));

        let req = HeartbeatRequest {
            duration_since_epoch: 1234,
            ..Default::default()
        };

        let builder = MetaSrvBuilder::new();
        let metasrv = builder.build().await.unwrap();
        let ctx = &mut metasrv.new_ctx();

        let acc = &mut HeartbeatAccumulator::default();
        let new_region_stat = |region_id: u64| -> RegionStat {
            RegionStat {
                id: region_id,
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table: table_name.to_string(),
                ..Default::default()
            }
        };
        acc.stat = Some(Stat {
            cluster_id: 1,
            id: 1,
            region_stats: vec![new_region_stat(1), new_region_stat(2), new_region_stat(3)],
            ..Default::default()
        });

        handler.handle(&req, ctx, acc).await.unwrap();

        // region 1 is during failover and region 3 is not in table global value,
        // so only region 2's lease is extended.
        assert_eq!(acc.region_leases.len(), 1);
        let lease = acc.region_leases.remove(0);
        assert_eq!(lease.table_ident.unwrap(), table_ident.into());
        assert_eq!(lease.regions, vec![2]);
        assert_eq!(lease.duration_since_epoch, 1234);
        assert_eq!(lease.lease_seconds, REGION_LEASE_SECONDS);
    }
}
