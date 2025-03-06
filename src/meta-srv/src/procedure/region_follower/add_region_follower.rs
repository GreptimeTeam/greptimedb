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

use common_meta::distributed_time_constants;
use common_meta::lock_key::{CatalogLock, RegionLock, SchemaLock, TableLock};
use common_meta::peer::Peer;
use common_procedure::error::ToJsonSnafu;
use common_procedure::{
    Context as ProcedureContext, LockKey, Procedure, Result as ProcedureResult, Status, StringKey,
};
use common_telemetry::info;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
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
        // check peer is alive
        let datanode_peer = lookup_datanode_peer(
            self.data.peer_id,
            &self.context.meta_peer_client,
            distributed_time_constants::DATANODE_LEASE_SECS,
        )
        .await?
        .context(error::PeerUnavailableSnafu {
            peer_id: self.data.peer_id,
        })?;

        info!(
            "Add region({}) follower procedure is preparing, peer: {datanode_peer:?}",
            self.data.region_id
        );

        self.data.peer = Some(datanode_peer);

        Ok(Status::executing(true))
    }

    pub async fn on_submit_request(&mut self) -> Result<Status> {
        let region_id = self.data.region_id;
        // Safety: we have already set the peer in `on_prepare``.
        let peer = self.data.peer.clone().unwrap();
        let create_follower = CreateFollower::new(region_id, peer);
        let instruction = create_follower
            .build_open_region_instruction(&self.context)
            .await?;
        create_follower
            .send_open_region_instruction(&self.context, instruction)
            .await?;

        Ok(Status::executing(true))
    }

    pub async fn on_update_metadata(&mut self) -> Result<Status> {
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
