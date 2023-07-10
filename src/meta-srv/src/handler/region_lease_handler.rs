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

use api::v1::meta::{HeartbeatRequest, RegionLease, Role};
use async_trait::async_trait;
use store_api::storage::RegionId;

use crate::error::Result;
use crate::handler::{HeartbeatAccumulator, HeartbeatHandler};
use crate::metasrv::Context;

/// The lease seconds of a region. It's set by two default heartbeat intervals (5 second × 2) plus
/// two roundtrip time (2 second × 2 × 2), plus some extra buffer (2 second).
// TODO(LFC): Make region lease seconds calculated from Datanode heartbeat configuration.
pub(crate) const REGION_LEASE_SECONDS: u64 = 20;

#[derive(Default)]
pub(crate) struct RegionLeaseHandler;

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

        let mut table_region_leases = HashMap::new();
        stat.region_stats.iter().for_each(|region_stat| {
            let table_ident = region_stat.table_ident.clone();
            table_region_leases
                .entry(table_ident)
                .or_insert_with(Vec::new)
                .push(RegionId::from(region_stat.id).region_number());
        });

        acc.region_leases = table_region_leases
            .into_iter()
            .map(|(table_ident, regions)| RegionLease {
                table_ident: Some(table_ident.into()),
                regions,
                duration_since_epoch: req.duration_since_epoch,
                lease_seconds: REGION_LEASE_SECONDS,
            })
            .collect();

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_meta::ident::TableIdent;

    use super::*;
    use crate::handler::node_stat::{RegionStat, Stat};
    use crate::metasrv::builder::MetaSrvBuilder;

    #[tokio::test]
    async fn test_handle_region_lease() {
        let table_name = "my_table";
        let table_ident = TableIdent {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table: table_name.to_string(),
            table_id: 1,
            engine: "mito".to_string(),
        };

        let handler = RegionLeaseHandler::default();

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
                table_ident: TableIdent {
                    catalog: DEFAULT_CATALOG_NAME.to_string(),
                    schema: DEFAULT_SCHEMA_NAME.to_string(),
                    table: table_name.to_string(),
                    table_id: 1,
                    engine: "mito".to_string(),
                },
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

        assert_eq!(acc.region_leases.len(), 1);
        let lease = acc.region_leases.remove(0);
        assert_eq!(lease.table_ident.unwrap(), table_ident.into());
        assert_eq!(lease.regions, vec![1, 2, 3]);
        assert_eq!(lease.duration_since_epoch, 1234);
        assert_eq!(lease.lease_seconds, REGION_LEASE_SECONDS);
    }
}
