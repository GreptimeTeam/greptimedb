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

pub mod manager;

pub mod add_region_follower;
mod create;
mod remove;
pub mod remove_region_follower;
#[cfg(test)]
mod test_util;

use common_error::ext::BoxedError;
use common_meta::cache_invalidator::CacheInvalidatorRef;
use common_meta::key::datanode_table::{DatanodeTableKey, DatanodeTableValue, RegionInfo};
use common_meta::key::table_route::{PhysicalTableRouteValue, TableRouteValue};
use common_meta::key::{DeserializedValueWithBytes, TableMetadataManagerRef};
use common_meta::lock_key::{CatalogLock, RegionLock, SchemaLock, TableLock};
use common_meta::peer::Peer;
use common_meta::{distributed_time_constants, DatanodeId};
use common_procedure::StringKey;
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};
use store_api::storage::RegionId;
use strum::AsRefStr;

use crate::cluster::MetaPeerClientRef;
use crate::error::{self, Result};
use crate::lease::lookup_datanode_peer;
use crate::service::mailbox::MailboxRef;

#[derive(Clone)]
/// The context of add/remove region follower procedure.
pub struct Context {
    /// The table metadata manager.
    pub table_metadata_manager: TableMetadataManagerRef,
    /// The mailbox.
    pub mailbox: MailboxRef,
    /// The metasrv's address.
    pub server_addr: String,
    /// The cache invalidator.
    pub cache_invalidator: CacheInvalidatorRef,
    /// The meta peer client.
    pub meta_peer_client: MetaPeerClientRef,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AlterRegionFollowerData {
    /// The catalog name.
    pub(crate) catalog: String,
    /// The schema name.
    pub(crate) schema: String,
    /// The region id.
    pub(crate) region_id: RegionId,
    /// The peer id of the datanode to add region follower.
    pub(crate) peer_id: u64,
    /// The peer of the datanode to add region follower.
    pub(crate) peer: Option<Peer>,
    /// The datanode table value of the region.
    pub(crate) datanode_table_value: Option<DatanodeTableValue>,
    /// The physical table route of the region.
    pub(crate) table_route: Option<(
        DeserializedValueWithBytes<TableRouteValue>,
        PhysicalTableRouteValue,
    )>,
    /// The state.
    pub(crate) state: AlterRegionFollowerState,
}

impl AlterRegionFollowerData {
    pub fn lock_key(&self) -> Vec<StringKey> {
        let region_id = self.region_id;
        let lock_key = vec![
            CatalogLock::Read(&self.catalog).into(),
            SchemaLock::read(&self.catalog, &self.schema).into(),
            // The optimistic updating of table route is not working very well,
            // so we need to use the write lock here.
            TableLock::Write(region_id.table_id()).into(),
            RegionLock::Write(region_id).into(),
        ];

        lock_key
    }

    pub(crate) fn datanode_peer(&self) -> Option<&Peer> {
        self.peer.as_ref()
    }

    pub(crate) fn physical_table_route(&self) -> Option<&PhysicalTableRouteValue> {
        self.table_route
            .as_ref()
            .map(|(_, table_route)| table_route)
    }

    /// Returns the region info of the region.
    pub(crate) fn region_info(&self) -> Option<RegionInfo> {
        self.datanode_table_value
            .as_ref()
            .map(|datanode_table_value| datanode_table_value.region_info.clone())
    }

    /// Loads the datanode peer.
    pub(crate) async fn load_datanode_peer(&self, ctx: &Context) -> Result<Option<Peer>> {
        let peer = lookup_datanode_peer(
            self.peer_id,
            &ctx.meta_peer_client,
            distributed_time_constants::DATANODE_LEASE_SECS,
        )
        .await?
        .context(error::PeerUnavailableSnafu {
            peer_id: self.peer_id,
        })?;

        Ok(Some(peer))
    }

    /// Loads the datanode table value of the region.
    pub(crate) async fn load_datanode_table_value(
        &self,
        ctx: &Context,
        datanode_id: DatanodeId,
    ) -> Result<Option<DatanodeTableValue>> {
        let table_id = self.region_id.table_id();
        let datanode_table_key = DatanodeTableKey {
            datanode_id,
            table_id,
        };

        let datanode_table_value = ctx
            .table_metadata_manager
            .datanode_table_manager()
            .get(&datanode_table_key)
            .await
            .context(error::TableMetadataManagerSnafu)
            .map_err(BoxedError::new)
            .with_context(|_| error::RetryLaterWithSourceSnafu {
                reason: format!("Failed to get DatanodeTable: ({datanode_id},{table_id})"),
            })?
            .context(error::DatanodeTableNotFoundSnafu {
                table_id,
                datanode_id,
            })?;

        Ok(Some(datanode_table_value))
    }

    /// Loads the table route of the region, returns the physical table id.
    pub(crate) async fn load_table_route(
        &self,
        ctx: &Context,
    ) -> Result<
        Option<(
            DeserializedValueWithBytes<TableRouteValue>,
            PhysicalTableRouteValue,
        )>,
    > {
        let table_id = self.region_id.table_id();
        let raw_table_route = ctx
            .table_metadata_manager
            .table_route_manager()
            .table_route_storage()
            .get_with_raw_bytes(table_id)
            .await
            .context(error::TableMetadataManagerSnafu)
            .map_err(BoxedError::new)
            .with_context(|_| error::RetryLaterWithSourceSnafu {
                reason: format!("Failed to get TableRoute: {table_id}"),
            })?
            .context(error::TableRouteNotFoundSnafu { table_id })?;
        let table_route = raw_table_route.clone().into_inner();

        ensure!(
            table_route.is_physical(),
            error::LogicalTableCannotAddFollowerSnafu { table_id }
        );

        Ok(Some((
            raw_table_route,
            table_route.into_physical_table_route(),
        )))
    }
}

#[derive(Debug, Serialize, Deserialize, AsRefStr)]
pub enum AlterRegionFollowerState {
    /// Prepares to alter region follower.
    Prepare,
    /// Sends alter region follower request to Datanode.
    SubmitRequest,
    /// Updates table metadata.
    UpdateMetadata,
    /// Broadcasts the invalidate table route cache message.
    InvalidateTableCache,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_serialization() {
        let data = AlterRegionFollowerData {
            catalog: "test_catalog".to_string(),
            schema: "test_schema".to_string(),
            region_id: RegionId::new(1, 1),
            peer_id: 1,
            peer: None,
            datanode_table_value: None,
            table_route: None,
            state: AlterRegionFollowerState::Prepare,
        };

        assert_eq!(data.region_id.as_u64(), 4294967297);
        let serialized = serde_json::to_string(&data).unwrap();
        let expected = r#"{"catalog":"test_catalog","schema":"test_schema","region_id":4294967297,"peer_id":1,"peer":null,"datanode_table_value":null,"table_route":null,"state":"Prepare"}"#;
        assert_eq!(expected, serialized);
    }
}
