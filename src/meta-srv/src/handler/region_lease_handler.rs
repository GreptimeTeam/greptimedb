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
use common_meta::ident::TableIdent;
use common_meta::key::TableMetadataManagerRef;
use common_meta::table_name::TableName;
use common_meta::ClusterId;
use common_telemetry::warn;
use snafu::ResultExt;
use store_api::storage::{RegionId, RegionNumber};
use table::metadata::TableId;

use crate::error::{Result, TableMetadataManagerSnafu};
use crate::handler::{HeartbeatAccumulator, HeartbeatHandler};
use crate::metasrv::Context;
use crate::procedure::region_failover::{RegionFailoverKey, RegionFailoverManager};

/// The lease seconds of a region. It's set by two default heartbeat intervals (5 second × 2) plus
/// two roundtrip time (2 second × 2 × 2), plus some extra buffer (2 second).
// TODO(LFC): Make region lease seconds calculated from Datanode heartbeat configuration.
pub(crate) const REGION_LEASE_SECONDS: u64 = 20;

pub(crate) struct RegionLeaseHandler {
    region_failover_manager: Option<Arc<RegionFailoverManager>>,
    table_metadata_manager: TableMetadataManagerRef,
}

impl RegionLeaseHandler {
    pub(crate) fn new(
        region_failover_manager: Option<Arc<RegionFailoverManager>>,
        table_metadata_manager: TableMetadataManagerRef,
    ) -> Self {
        Self {
            region_failover_manager,
            table_metadata_manager,
        }
    }

    async fn find_table_ident(
        &self,
        table_id: TableId,
        table_name: &TableName,
    ) -> Result<Option<TableIdent>> {
        let value = self
            .table_metadata_manager
            .table_info_manager()
            .get_old(table_name)
            .await
            .context(TableMetadataManagerSnafu)?;
        Ok(value.map(|x| {
            let table_info = &x.table_info;
            TableIdent {
                catalog: table_info.catalog_name.clone(),
                schema: table_info.schema_name.clone(),
                table: table_info.name.clone(),
                table_id,
                engine: table_info.meta.engine.clone(),
            }
        }))
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
        let datanode_id = stat.id;

        let mut datanode_regions = HashMap::new();
        stat.region_stats.iter().for_each(|x| {
            let region_id: RegionId = x.id.into();
            let table_id = region_id.table_id();
            let table_name = TableName::new(
                x.catalog.to_string(),
                x.schema.to_string(),
                x.table.to_string(),
            );
            datanode_regions
                .entry((table_id, table_name))
                .or_insert_with(Vec::new)
                .push(RegionId::from(x.id).region_number());
        });

        let mut region_leases = Vec::with_capacity(datanode_regions.len());
        for ((table_id, table_name), local_regions) in datanode_regions {
            let Some(table_ident) = self.find_table_ident(table_id, &table_name).await? else {
                warn!("Reject region lease request from Datanode {datanode_id} for table id {table_id}. \
                       Reason: table not found.");
                continue;
            };

            let Some(table_region_value) = self
                .table_metadata_manager
                .table_region_manager()
                .get_old(&table_name)
                .await
                .context(TableMetadataManagerSnafu)? else {
                warn!("Reject region lease request from Datanode {datanode_id} for table id {table_id}. \
                       Reason: table region value not found.");
                continue;
            };
            let Some(global_regions) = table_region_value
                .region_distribution
                .get(&datanode_id) else {
                warn!("Reject region lease request from Datanode {datanode_id} for table id {table_id}. \
                       Reason: not expected to place the region on it.");
                continue;
            };

            // Filter out the designated regions from table info value for the given table on the given Datanode.
            let designated_regions = local_regions
                .into_iter()
                .filter(|x| global_regions.contains(x))
                .collect::<Vec<_>>();

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
    use common_meta::key::TableMetadataManager;

    use super::*;
    use crate::handler::node_stat::{RegionStat, Stat};
    use crate::metasrv::builder::MetaSrvBuilder;
    use crate::service::store::kv::KvBackendAdapter;
    use crate::{table_routes, test_util};

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
        table_routes::tests::prepare_table_region_and_info_value(
            &table_metadata_manager,
            table_name,
        )
        .await;

        let table_ident = TableIdent {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table: table_name.to_string(),
            table_id,
            engine: "mito".to_string(),
        };
        let _ = region_failover_manager
            .running_procedures()
            .write()
            .unwrap()
            .insert(RegionFailoverKey {
                cluster_id: 1,
                table_ident: table_ident.clone(),
                region_number: 1,
            });

        let handler =
            RegionLeaseHandler::new(Some(region_failover_manager), table_metadata_manager);

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

        // region 1 is during failover and region 3 is not in table region value,
        // so only region 2's lease is extended.
        assert_eq!(acc.region_leases.len(), 1);
        let lease = acc.region_leases.remove(0);
        assert_eq!(lease.table_ident.unwrap(), table_ident.into());
        assert_eq!(lease.regions, vec![2]);
        assert_eq!(lease.duration_since_epoch, 1234);
        assert_eq!(lease.lease_seconds, REGION_LEASE_SECONDS);
    }
}
