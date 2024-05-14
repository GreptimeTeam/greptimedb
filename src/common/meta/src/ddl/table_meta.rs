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

use async_trait::async_trait;
use common_telemetry::{debug, info};
use snafu::ensure;
use store_api::storage::{RegionId, RegionNumber, TableId};

use crate::ddl::{TableMetadata, TableMetadataAllocatorContext};
use crate::error::{self, Result, UnsupportedSnafu};
use crate::key::table_route::PhysicalTableRouteValue;
use crate::peer::Peer;
use crate::rpc::ddl::CreateTableTask;
use crate::rpc::router::{Region, RegionRoute};
use crate::sequence::SequenceRef;
use crate::wal_options_allocator::{allocate_region_wal_options, WalOptionsAllocatorRef};

pub type TableMetadataAllocatorRef = Arc<TableMetadataAllocator>;

#[derive(Clone)]
pub struct TableMetadataAllocator {
    table_id_sequence: SequenceRef,
    wal_options_allocator: WalOptionsAllocatorRef,
    peer_allocator: PeerAllocatorRef,
}

impl TableMetadataAllocator {
    pub fn new(
        table_id_sequence: SequenceRef,
        wal_options_allocator: WalOptionsAllocatorRef,
    ) -> Self {
        Self::with_peer_allocator(
            table_id_sequence,
            wal_options_allocator,
            Arc::new(NoopPeerAllocator),
        )
    }

    pub fn with_peer_allocator(
        table_id_sequence: SequenceRef,
        wal_options_allocator: WalOptionsAllocatorRef,
        peer_allocator: PeerAllocatorRef,
    ) -> Self {
        Self {
            table_id_sequence,
            wal_options_allocator,
            peer_allocator,
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
                    .table_id_sequence
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
            self.table_id_sequence.next().await? as TableId
        };
        Ok(table_id)
    }

    fn create_wal_options(
        &self,
        table_route: &PhysicalTableRouteValue,
    ) -> Result<HashMap<RegionNumber, String>> {
        let region_numbers = table_route
            .region_routes
            .iter()
            .map(|route| route.region.id.region_number())
            .collect();
        allocate_region_wal_options(region_numbers, &self.wal_options_allocator)
    }

    async fn create_table_route(
        &self,
        ctx: &TableMetadataAllocatorContext,
        table_id: TableId,
        task: &CreateTableTask,
    ) -> Result<PhysicalTableRouteValue> {
        let regions = task.partitions.len();
        ensure!(
            regions > 0,
            error::UnexpectedSnafu {
                err_msg: "The number of partitions must be greater than 0"
            }
        );

        let peers = self.peer_allocator.alloc(ctx, regions).await?;
        let region_routes = task
            .partitions
            .iter()
            .enumerate()
            .map(|(i, partition)| {
                let region = Region {
                    id: RegionId::new(table_id, i as u32),
                    partition: Some(partition.clone().into()),
                    ..Default::default()
                };

                let peer = peers[i % peers.len()].clone();

                RegionRoute {
                    region,
                    leader_peer: Some(peer),
                    ..Default::default()
                }
            })
            .collect::<Vec<_>>();

        Ok(PhysicalTableRouteValue::new(region_routes))
    }

    /// Create VIEW metadata
    pub async fn create_view(
        &self,
        _ctx: &TableMetadataAllocatorContext,
        table_id: &Option<api::v1::TableId>,
    ) -> Result<TableMetadata> {
        let table_id = self.allocate_table_id(table_id).await?;

        Ok(TableMetadata {
            table_id,
            ..Default::default()
        })
    }

    pub async fn create(
        &self,
        ctx: &TableMetadataAllocatorContext,
        task: &CreateTableTask,
    ) -> Result<TableMetadata> {
        let table_id = self.allocate_table_id(&task.create_table.table_id).await?;
        let table_route = self.create_table_route(ctx, table_id, task).await?;
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

pub type PeerAllocatorRef = Arc<dyn PeerAllocator>;

/// [PeerAllocator] allocates [Peer]s for creating regions.
#[async_trait]
pub trait PeerAllocator: Send + Sync {
    /// Allocates `regions` size [Peer]s.
    async fn alloc(&self, ctx: &TableMetadataAllocatorContext, regions: usize)
        -> Result<Vec<Peer>>;
}

struct NoopPeerAllocator;

#[async_trait]
impl PeerAllocator for NoopPeerAllocator {
    async fn alloc(
        &self,
        _ctx: &TableMetadataAllocatorContext,
        regions: usize,
    ) -> Result<Vec<Peer>> {
        Ok(vec![Peer::default(); regions])
    }
}
