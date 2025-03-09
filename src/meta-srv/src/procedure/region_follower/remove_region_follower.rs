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

use common_meta::instruction::CacheIdent;
use common_procedure::error::ToJsonSnafu;
use common_procedure::{
    Context as ProcedureContext, Error as ProcedureError, LockKey, Procedure,
    Result as ProcedureResult, Status,
};
use common_telemetry::info;
use snafu::{ensure, ResultExt};
use store_api::storage::RegionId;

use super::remove::RemoveFollower;
use super::{AlterRegionFollowerData, AlterRegionFollowerState, Context};
use crate::error::{self, Result};
use crate::metrics;
pub struct RemoveRegionFollowerProcedure {
    pub data: AlterRegionFollowerData,
    pub context: Context,
}

impl RemoveRegionFollowerProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::RemoveRegionFollower";

    pub fn new(
        catalog: String,
        schema: String,
        region_id: RegionId,
        peer_id: u64,
        context: Context,
    ) -> Self {
        Self {
            data: AlterRegionFollowerData {
                catalog,
                schema,
                region_id,
                peer_id,
                peer: None,
                datanode_table_value: None,
                table_route: None,
                state: AlterRegionFollowerState::Prepare,
            },
            context,
        }
    }

    pub fn from_json(json: &str, context: Context) -> ProcedureResult<Self> {
        let data: AlterRegionFollowerData = serde_json::from_str(json).unwrap();
        Ok(Self { data, context })
    }

    pub async fn on_prepare(&mut self) -> Result<Status> {
        // loads the datanode peer and check peer is alive
        let datanode_peer = self.data.load_datanode_peer(&self.context).await?;

        // loads the datanode table value
        let _ = self.data.load_datanode_table_value(&self.context).await?;

        // loads the table route of the region
        let table_route = self.data.load_table_route(&self.context).await?;

        // check if the destination peer has this region
        for region_route in &table_route.region_routes {
            if region_route.region.id != self.data.region_id {
                continue;
            }
            ensure!(
                !region_route
                    .follower_peers
                    .iter()
                    .any(|peer| peer.id == self.data.peer_id),
                error::RegionFollowerNotExistsSnafu {
                    region_id: self.data.region_id,
                    peer_id: self.data.peer_id,
                }
            );
        }

        info!(
            "Remove region({}) follower procedure is preparing, peer: {datanode_peer:?}",
            self.data.region_id
        );

        Ok(Status::executing(true))
    }

    pub async fn on_submit_request(&mut self) -> Result<Status> {
        let region_id = self.data.region_id;
        // Safety: we have already set the peer in `on_prepare``.
        let peer = self.data.peer.clone().unwrap();
        let remove_follower = RemoveFollower::new(region_id, peer);
        let instruction = remove_follower
            .build_close_region_instruction(self.data.region_info().unwrap())
            .await?;
        remove_follower
            .send_close_region_instruction(&self.context, instruction)
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
            // remove the follower peer from the region route
            region_route
                .follower_peers
                .retain(|peer| peer.id != self.data.peer_id);
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
        let table_id = self.data.region_id.table_id();
        // ignore the result
        let ctx = common_meta::cache_invalidator::Context::default();
        let _ = self
            .context
            .cache_invalidator
            .invalidate(&ctx, &[CacheIdent::TableId(table_id)])
            .await;
        Ok(Status::executing(true))
    }
}

#[async_trait::async_trait]
impl Procedure for RemoveRegionFollowerProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let state = &self.data.state;

        let _timer = metrics::METRIC_META_REMOVE_REGION_FOLLOWER_EXECUTE
            .with_label_values(&[state.as_ref()])
            .start_timer();

        match state {
            AlterRegionFollowerState::Prepare => self.on_prepare().await,
            AlterRegionFollowerState::SubmitRequest => self.on_submit_request().await,
            AlterRegionFollowerState::UpdateMetadata => self.on_update_metadata().await,
            AlterRegionFollowerState::InvalidateTableCache => self.on_broadcast().await,
        }
        .map_err(|e| {
            if e.is_retryable() {
                ProcedureError::retry_later(e)
            } else {
                ProcedureError::external(e)
            }
        })
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        LockKey::new(self.data.lock_key())
    }
}

#[cfg(test)]
mod tests {
    use common_meta::lock_key::{CatalogLock, RegionLock, SchemaLock, TableLock};

    use super::*;
    use crate::procedure::region_follower::test_util::TestingEnv;

    #[tokio::test]
    async fn test_lock_key() {
        let env = TestingEnv::new();
        let context = env.new_context();

        let procedure = RemoveRegionFollowerProcedure::new(
            "test_catalog".to_string(),
            "test_schema".to_string(),
            RegionId::new(1, 1),
            1,
            context,
        );

        let key = procedure.lock_key();
        let keys = key.keys_to_lock().cloned().collect::<Vec<_>>();

        assert_eq!(keys.len(), 4);
        assert!(keys.contains(&CatalogLock::Read("test_catalog").into()));
        assert!(keys.contains(&SchemaLock::read("test_catalog", "test_schema").into()));
        assert!(keys.contains(&TableLock::Write(1).into()));
        assert!(keys.contains(&RegionLock::Write(RegionId::new(1, 1)).into()));
    }
}
