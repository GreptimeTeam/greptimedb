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

use common_catalog::consts::METRIC_ENGINE;
use common_error::ext::BoxedError;
use common_meta::ddl::{TableMetadata, TableMetadataAllocator, TableMetadataAllocatorContext};
use common_meta::error::{ExternalSnafu, Result as MetaResult};
use common_meta::key::table_route::{
    LogicalTableRouteValue, PhysicalTableRouteValue, TableRouteValue,
};
use common_meta::rpc::ddl::CreateTableTask;
use common_meta::rpc::router::{Region, RegionRoute};
use common_meta::sequence::SequenceRef;
use common_meta::wal::{allocate_region_wal_options, WalOptionsAllocatorRef};
use common_meta::ClusterId;
use common_telemetry::debug;
use snafu::{ensure, ResultExt};
use store_api::storage::{RegionId, RegionNumber, TableId, MAX_REGION_SEQ};

use crate::error::{self, Result, TooManyPartitionsSnafu};
use crate::metasrv::{SelectorContext, SelectorRef};
use crate::selector::SelectorOptions;

pub struct MetaSrvTableMetadataAllocator {
    ctx: SelectorContext,
    selector: SelectorRef,
    table_id_sequence: SequenceRef,
    wal_options_allocator: WalOptionsAllocatorRef,
}

impl MetaSrvTableMetadataAllocator {
    pub fn new(
        ctx: SelectorContext,
        selector: SelectorRef,
        table_id_sequence: SequenceRef,
        wal_options_allocator: WalOptionsAllocatorRef,
    ) -> Self {
        Self {
            ctx,
            selector,
            table_id_sequence,
            wal_options_allocator,
        }
    }

    async fn create_table_route(
        &self,
        cluster_id: ClusterId,
        table_id: TableId,
        task: &CreateTableTask,
    ) -> Result<TableRouteValue> {
        let table_route = if task.create_table.engine == METRIC_ENGINE {
            TableRouteValue::Logical(LogicalTableRouteValue {})
        } else {
            let regions = task.partitions.len();

            ensure!(regions <= MAX_REGION_SEQ as usize, TooManyPartitionsSnafu);

            let mut peers = self
                .selector
                .select(
                    cluster_id,
                    &self.ctx,
                    SelectorOptions {
                        min_required_items: regions,
                        allow_duplication: true,
                    },
                )
                .await?;

            ensure!(
                peers.len() >= regions,
                error::NoEnoughAvailableDatanodeSnafu {
                    required: regions,
                    available: peers.len(),
                }
            );

            peers.truncate(regions);

            let region_routes = task
                .partitions
                .iter()
                .enumerate()
                .map(|(i, partition)| {
                    let region = Region {
                        id: RegionId::new(table_id, i as RegionNumber),
                        partition: Some(partition.clone().into()),
                        ..Default::default()
                    };

                    let peer = peers[i % peers.len()].clone();

                    RegionRoute {
                        region,
                        leader_peer: Some(peer.into()),
                        ..Default::default()
                    }
                })
                .collect::<Vec<_>>();
            TableRouteValue::Physical(PhysicalTableRouteValue::new(region_routes))
        };
        Ok(table_route)
    }

    fn create_wal_options(
        &self,
        table_route: &TableRouteValue,
    ) -> MetaResult<HashMap<RegionNumber, String>> {
        match table_route {
            TableRouteValue::Physical(x) => {
                let region_numbers = x
                    .region_routes
                    .iter()
                    .map(|route| route.region.id.region_number())
                    .collect();
                allocate_region_wal_options(region_numbers, &self.wal_options_allocator)
            }
            TableRouteValue::Logical(_) => Ok(HashMap::new()),
        }
    }
}

#[async_trait::async_trait]
impl TableMetadataAllocator for MetaSrvTableMetadataAllocator {
    async fn create(
        &self,
        ctx: &TableMetadataAllocatorContext,
        task: &CreateTableTask,
    ) -> MetaResult<TableMetadata> {
        let table_id = self.table_id_sequence.next().await? as TableId;

        let table_route = self
            .create_table_route(ctx.cluster_id, table_id, task)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;

        let region_wal_options = self.create_wal_options(&table_route)?;

        debug!(
            "Allocated region wal options {:?} for table {}",
            region_wal_options, table_id
        );

        Ok(TableMetadata {
            table_id,
            table_route,
            region_wal_options,
        })
    }
}
