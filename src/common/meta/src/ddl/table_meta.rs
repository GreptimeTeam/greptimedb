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

use common_telemetry::{debug, info};
use snafu::ensure;
use store_api::storage::{RegionNumber, TableId};

use crate::ddl::TableMetadata;
use crate::ddl::allocator::region_routes::RegionRoutesAllocatorRef;
use crate::ddl::allocator::resource_id::ResourceIdAllocatorRef;
use crate::ddl::allocator::wal_options::WalOptionsAllocatorRef;
use crate::error::{Result, UnsupportedSnafu};
use crate::key::table_route::PhysicalTableRouteValue;
use crate::peer::{NoopPeerAllocator, PeerAllocatorRef};
use crate::rpc::ddl::CreateTableTask;

pub type TableMetadataAllocatorRef = Arc<TableMetadataAllocator>;

#[derive(Clone)]
pub struct TableMetadataAllocator {
    table_id_allocator: ResourceIdAllocatorRef,
    wal_options_allocator: WalOptionsAllocatorRef,
    region_routes_allocator: RegionRoutesAllocatorRef,
}

impl TableMetadataAllocator {
    pub fn new(
        table_id_allocator: ResourceIdAllocatorRef,
        wal_options_allocator: WalOptionsAllocatorRef,
    ) -> Self {
        Self::with_peer_allocator(
            table_id_allocator,
            wal_options_allocator,
            Arc::new(NoopPeerAllocator),
        )
    }

    pub fn with_peer_allocator(
        table_id_allocator: ResourceIdAllocatorRef,
        wal_options_allocator: WalOptionsAllocatorRef,
        peer_allocator: PeerAllocatorRef,
    ) -> Self {
        Self {
            table_id_allocator,
            wal_options_allocator,
            region_routes_allocator: Arc::new(peer_allocator) as _,
        }
    }

    pub(crate) async fn allocate_table_id(
        &self,
        table_id: &Option<api::v1::TableId>,
    ) -> Result<TableId> {
        let table_id = if let Some(table_id) = table_id {
            let table_id = table_id.id;

            ensure!(
                !self
                    .table_id_allocator
                    .min_max()
                    .await
                    .contains(&(table_id as u64)),
                UnsupportedSnafu {
                    operation: format!(
                        "create table by id {} that is reserved in this node",
                        table_id
                    )
                }
            );

            info!(
                "Received explicitly allocated table id {}, will use it directly.",
                table_id
            );

            table_id
        } else {
            self.table_id_allocator.next().await? as TableId
        };
        Ok(table_id)
    }

    async fn create_wal_options(
        &self,
        region_numbers: &[RegionNumber],
        skip_wal: bool,
    ) -> Result<HashMap<RegionNumber, String>> {
        self.wal_options_allocator
            .allocate(region_numbers, skip_wal)
            .await
    }

    async fn create_table_route(
        &self,
        table_id: TableId,
        partition_exprs: &[&str],
        next_region_number: u32,
    ) -> Result<PhysicalTableRouteValue> {
        let region_routes = self
            .region_routes_allocator
            .allocate(table_id, partition_exprs, next_region_number)
            .await?;

        Ok(PhysicalTableRouteValue::new(region_routes))
    }

    /// Create VIEW metadata
    pub async fn create_view(&self, table_id: &Option<api::v1::TableId>) -> Result<TableMetadata> {
        let table_id = self.allocate_table_id(table_id).await?;

        Ok(TableMetadata {
            table_id,
            ..Default::default()
        })
    }

    pub async fn create(&self, task: &CreateTableTask) -> Result<TableMetadata> {
        let table_id = self.allocate_table_id(&task.create_table.table_id).await?;
        let partition_exprs = task
            .partitions
            .iter()
            .map(|p| p.expression.as_str())
            .collect::<Vec<_>>();
        let table_route = self
            .create_table_route(table_id, &partition_exprs, 0)
            .await?;
        let region_numbers = table_route
            .region_routes
            .iter()
            .map(|route| route.region.id.region_number())
            .collect::<Vec<_>>();
        let region_wal_options = self
            .create_wal_options(&region_numbers, task.table_info.meta.options.skip_wal)
            .await?;

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

    pub fn table_id_allocator(&self) -> ResourceIdAllocatorRef {
        self.table_id_allocator.clone()
    }
}
