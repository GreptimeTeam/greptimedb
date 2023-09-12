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

use api::v1::meta::Partition;
use common_catalog::format_full_table_name;
use common_error::ext::BoxedError;
use common_meta::ddl::{TableMetadataAllocator, TableMetadataAllocatorContext};
use common_meta::error::{self as meta_error, Result as MetaResult};
use common_meta::rpc::router::{Region, RegionRoute};
use common_meta::sequence::SequenceRef;
use common_telemetry::warn;
use snafu::{ensure, ResultExt};
use store_api::storage::{RegionId, TableId, MAX_REGION_SEQ};
use table::metadata::RawTableInfo;

use crate::error::{self, Result, TooManyPartitionsSnafu};
use crate::metasrv::{SelectorContext, SelectorRef};

pub struct MetaSrvTableMetadataAllocator {
    ctx: SelectorContext,
    selector: SelectorRef,
    table_id_sequence: SequenceRef,
}

impl MetaSrvTableMetadataAllocator {
    pub fn new(
        ctx: SelectorContext,
        selector: SelectorRef,
        table_id_sequence: SequenceRef,
    ) -> Self {
        Self {
            ctx,
            selector,
            table_id_sequence,
        }
    }
}

#[async_trait::async_trait]
impl TableMetadataAllocator for MetaSrvTableMetadataAllocator {
    async fn create(
        &self,
        ctx: &TableMetadataAllocatorContext,
        raw_table_info: &mut RawTableInfo,
        partitions: &[Partition],
    ) -> MetaResult<(TableId, Vec<RegionRoute>)> {
        handle_create_region_routes(
            ctx.cluster_id,
            raw_table_info,
            partitions,
            &self.ctx,
            &self.selector,
            &self.table_id_sequence,
        )
        .await
        .map_err(BoxedError::new)
        .context(meta_error::ExternalSnafu)
    }
}

/// pre-allocates create table's table id and region routes.
async fn handle_create_region_routes(
    cluster_id: u64,
    table_info: &mut RawTableInfo,
    partitions: &[Partition],
    ctx: &SelectorContext,
    selector: &SelectorRef,
    table_id_sequence: &SequenceRef,
) -> Result<(TableId, Vec<RegionRoute>)> {
    let mut peers = selector.select(cluster_id, ctx).await?;

    if peers.len() < partitions.len() {
        warn!("Create table failed due to no enough available datanodes, table: {}, partition number: {}, datanode number: {}", format_full_table_name(&table_info.catalog_name,&table_info.schema_name,&table_info.name), partitions.len(), peers.len());
        return error::NoEnoughAvailableDatanodeSnafu {
            expected: partitions.len(),
            available: peers.len(),
        }
        .fail();
    }

    // We don't need to keep all peers, just truncate it to the number of partitions.
    // If the peers are not enough, some peers will be used for multiple partitions.
    peers.truncate(partitions.len());

    let table_id = table_id_sequence
        .next()
        .await
        .context(error::NextSequenceSnafu)? as u32;
    table_info.ident.table_id = table_id;

    ensure!(
        partitions.len() <= MAX_REGION_SEQ as usize,
        TooManyPartitionsSnafu
    );

    let region_routes = partitions
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
                leader_peer: Some(peer.into()),
                follower_peers: vec![], // follower_peers is not supported at the moment
            }
        })
        .collect::<Vec<_>>();

    Ok((table_id, region_routes))
}
