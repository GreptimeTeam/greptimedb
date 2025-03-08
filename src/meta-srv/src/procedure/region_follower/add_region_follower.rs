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

use common_error::ext::BoxedError;
use common_meta::distributed_time_constants;
use common_meta::key::datanode_table::{DatanodeTableKey, DatanodeTableValue, RegionInfo};
use common_meta::key::table_route::{PhysicalTableRouteValue, TableRouteValue};
use common_meta::key::DeserializedValueWithBytes;
use common_meta::lock_key::{CatalogLock, RegionLock, SchemaLock, TableLock};
use common_meta::peer::Peer;
use common_procedure::error::ToJsonSnafu;
use common_procedure::{
    Context as ProcedureContext, LockKey, Procedure, Result as ProcedureResult, Status, StringKey,
};
use common_telemetry::info;
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};
use store_api::storage::RegionId;

use super::create::CreateFollower;
use super::Context;
use crate::error::{self, Result};
use crate::lease::lookup_datanode_peer;
pub struct AddRegionFollowerProcedure {
    pub data: AddRegionFollowerData,
    pub context: Context,
}

impl AddRegionFollowerProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::AddRegionFollower";

    pub fn new(
        catalog: String,
        schema: String,
        region_id: RegionId,
        peer_id: u64,
        context: Context,
    ) -> Self {
        Self {
            data: AddRegionFollowerData {
                catalog,
                schema,
                region_id,
                peer_id,
                peer: None,
                datanode_table_value: None,
                table_route: None,
                state: AddRegionFollowerState::Prepare,
            },
            context,
        }
    }

    pub fn from_json(json: &str, context: Context) -> ProcedureResult<Self> {
        let data: AddRegionFollowerData = serde_json::from_str(json).unwrap();
        Ok(Self { data, context })
    }

    pub async fn on_prepare(&mut self) -> Result<Status> {
        // loads the datanode peer and check peer is alive
        let datanode_peer = self.data.load_datanode_peer(&self.context).await?;

        // loads the datanode table value
        let _ = self.data.load_datanode_table_value(&self.context).await?;

        // loads the table route of the region
        let table_route = self.data.load_table_route(&self.context).await?;

        // check if the destination peer is already a leader/follower of the region
        for region_route in &table_route.region_routes {
            if region_route.region.id != self.data.region_id {
                continue;
            }
            let Some(leader_peer) = &region_route.leader_peer else {
                continue;
            };

            // check if the destination peer is already a leader of the region
            if leader_peer.id == datanode_peer.id {
                return error::RegionFollowerLeaderConflictSnafu {
                    region_id: self.data.region_id,
                    peer_id: datanode_peer.id,
                }
                .fail();
            }

            // check if the destination peer is already a follower of the region
            if region_route
                .follower_peers
                .iter()
                .any(|peer| peer.id == datanode_peer.id)
            {
                return error::MultipleRegionFollowersOnSameNodeSnafu {
                    region_id: self.data.region_id,
                    peer_id: datanode_peer.id,
                }
                .fail();
            }
        }

        info!(
            "Add region({}) follower procedure is preparing, peer: {datanode_peer:?}",
            self.data.region_id
        );

        Ok(Status::executing(true))
    }

    pub async fn on_submit_request(&mut self) -> Result<Status> {
        let region_id = self.data.region_id;
        // Safety: we have already set the peer in `on_prepare``.
        let peer = self.data.peer.clone().unwrap();
        let create_follower = CreateFollower::new(region_id, peer);
        let instruction = create_follower
            .build_open_region_instruction(self.data.region_info().unwrap())
            .await?;
        create_follower
            .send_open_region_instruction(&self.context, instruction)
            .await?;

        Ok(Status::executing(true))
    }

    pub async fn on_update_metadata(&mut self) -> Result<Status> {
        // Safety: we have already load the table route in `on_prepare``.
        let (current_table_route_value, phy_table_route) = self.data.table_route.as_ref().unwrap();

        let mut new_region_routes = phy_table_route.region_routes.clone();
        for region_route in &mut new_region_routes {
            if region_route.region.id != self.data.region_id {
                continue;
            }
            region_route
                .follower_peers
                .push(self.data.peer.clone().unwrap());
        }

        // Safety: we have already load the region info in `on_prepare`.
        let region_info = self.data.region_info().unwrap();
        let new_region_options = region_info.region_options.clone();
        let new_region_wal_options = region_info.region_wal_options.clone();

        self.context
            .table_metadata_manager
            .update_table_route(
                self.data.region_id.table_id(),
                region_info,
                current_table_route_value,
                new_region_routes,
                &new_region_options,
                &new_region_wal_options,
            )
            .await
            .context(error::TableMetadataManagerSnafu)?;

        Ok(Status::executing(true))
    }

    pub async fn on_broadcast(&mut self) -> Result<Status> {
        Ok(Status::executing(true))
    }
}

#[async_trait::async_trait]
impl Procedure for AddRegionFollowerProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        Ok(Status::executing(true))
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        LockKey::new(self.data.lock_key())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AddRegionFollowerData {
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
    pub(crate) state: AddRegionFollowerState,
}

impl AddRegionFollowerData {
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

    /// Returns the region info of the region.
    pub fn region_info(&self) -> Option<RegionInfo> {
        self.datanode_table_value
            .as_ref()
            .map(|datanode_table_value| datanode_table_value.region_info.clone())
    }

    /// Loads the datanode peer.
    pub async fn load_datanode_peer(&mut self, ctx: &Context) -> Result<Peer> {
        let peer = lookup_datanode_peer(
            self.peer_id,
            &ctx.meta_peer_client,
            distributed_time_constants::DATANODE_LEASE_SECS,
        )
        .await?
        .context(error::PeerUnavailableSnafu {
            peer_id: self.peer_id,
        })?;

        self.peer = Some(peer);

        Ok(self.peer.clone().unwrap())
    }

    /// Loads the datanode table value of the region.
    pub async fn load_datanode_table_value(
        &mut self,
        ctx: &Context,
    ) -> Result<&DatanodeTableValue> {
        let table_id = self.region_id.table_id();
        let datanode_id = self.peer_id;
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

        self.datanode_table_value = Some(datanode_table_value);

        Ok(self.datanode_table_value.as_ref().unwrap())
    }

    /// Loads the table route of the region, returns the physical table id.
    pub async fn load_table_route(&mut self, ctx: &Context) -> Result<PhysicalTableRouteValue> {
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

        self.table_route = Some((raw_table_route, table_route.into_physical_table_route()));

        Ok(self.table_route.clone().unwrap().1)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum AddRegionFollowerState {
    /// Prepares to add region follower.
    Prepare,
    /// Sends add region follower request to Datanode.
    SubmitRequest,
    /// Updates table metadata.
    UpdateMetadata,
    /// Broadcasts the invalidate table route cache message.
    InvalidateTableCache,
}
