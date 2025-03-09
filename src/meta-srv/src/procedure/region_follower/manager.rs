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

use common_meta::rpc::procedure::{AddRegionFollowerRequest, RemoveRegionFollowerRequest};
use common_procedure::{watcher, Output, ProcedureId, ProcedureManagerRef, ProcedureWithId};
use common_telemetry::info;
use snafu::{OptionExt, ResultExt};
use store_api::storage::RegionId;
use table::table_name::TableName;

use super::remove_region_follower::RemoveRegionFollowerProcedure;
use crate::error::{self, Result};
use crate::procedure::region_follower::add_region_follower::AddRegionFollowerProcedure;
use crate::procedure::region_follower::Context;

pub struct RegionFollowerManager {
    procedure_manager: ProcedureManagerRef,
    default_context: Context,
}

impl RegionFollowerManager {
    pub fn new(procedure_manager: ProcedureManagerRef, default_context: Context) -> Self {
        Self {
            procedure_manager,
            default_context,
        }
    }

    pub fn new_context(&self) -> Context {
        self.default_context.clone()
    }

    pub(crate) fn try_start(&self) -> Result<()> {
        // register add region follower procedure
        let context = self.new_context();
        let type_name = AddRegionFollowerProcedure::TYPE_NAME;
        self.procedure_manager
            .register_loader(
                type_name,
                Box::new(move |json| {
                    let context = context.clone();
                    AddRegionFollowerProcedure::from_json(json, context).map(|p| Box::new(p) as _)
                }),
            )
            .context(error::RegisterProcedureLoaderSnafu { type_name })?;

        // register remove region follower procedure
        let context = self.new_context();
        let type_name = RemoveRegionFollowerProcedure::TYPE_NAME;
        self.procedure_manager
            .register_loader(
                type_name,
                Box::new(move |json| {
                    let context = context.clone();
                    RemoveRegionFollowerProcedure::from_json(json, context)
                        .map(|p| Box::new(p) as _)
                }),
            )
            .context(error::RegisterProcedureLoaderSnafu { type_name })?;
        Ok(())
    }

    pub async fn submit_add_follower_procedure(
        &self,
        req: AddRegionFollowerRequest,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let AddRegionFollowerRequest { region_id, peer_id } = req;
        let region_id = RegionId::from_u64(region_id);
        let table_id = region_id.table_id();
        let ctx = self.new_context();

        // get the table info
        let table_info = ctx
            .table_metadata_manager
            .table_info_manager()
            .get(table_id)
            .await
            .context(error::TableMetadataManagerSnafu)?
            .context(error::TableInfoNotFoundSnafu { table_id })?
            .into_inner();

        let TableName {
            catalog_name,
            schema_name,
            ..
        } = table_info.table_name();

        let procedure =
            AddRegionFollowerProcedure::new(catalog_name, schema_name, region_id, peer_id, ctx);

        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));
        let procedure_id = procedure_with_id.id;
        info!("Starting add region follower procedure {procedure_id} for {req:?}");
        let mut watcher = self
            .procedure_manager
            .submit(procedure_with_id)
            .await
            .context(error::SubmitProcedureSnafu)?;
        let output = watcher::wait(&mut watcher)
            .await
            .context(error::WaitProcedureSnafu)?;

        Ok((procedure_id, output))
    }

    pub async fn submit_remove_follower_procedure(
        &self,
        req: RemoveRegionFollowerRequest,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let RemoveRegionFollowerRequest { region_id, peer_id } = req;
        let region_id = RegionId::from_u64(region_id);
        let table_id = region_id.table_id();
        let ctx = self.new_context();

        // get the table info
        let table_info = ctx
            .table_metadata_manager
            .table_info_manager()
            .get(table_id)
            .await
            .context(error::TableMetadataManagerSnafu)?
            .context(error::TableInfoNotFoundSnafu { table_id })?
            .into_inner();

        let TableName {
            catalog_name,
            schema_name,
            ..
        } = table_info.table_name();

        let procedure =
            RemoveRegionFollowerProcedure::new(catalog_name, schema_name, region_id, peer_id, ctx);

        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));
        let procedure_id = procedure_with_id.id;
        info!("Starting remove region follower procedure {procedure_id} for {req:?}");
        let mut watcher = self
            .procedure_manager
            .submit(procedure_with_id)
            .await
            .context(error::SubmitProcedureSnafu)?;
        let output = watcher::wait(&mut watcher)
            .await
            .context(error::WaitProcedureSnafu)?;

        Ok((procedure_id, output))
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use super::*;
    use crate::procedure::region_follower::test_util::TestingEnv;

    #[tokio::test]
    async fn test_submit_add_follower_procedure_table_not_found() {
        let env = TestingEnv::new();
        let ctx = env.new_context();
        let region_follower_manager = RegionFollowerManager::new(env.procedure_manager(), ctx);
        let req = AddRegionFollowerRequest {
            region_id: 1,
            peer_id: 2,
        };
        let err = region_follower_manager
            .submit_add_follower_procedure(req)
            .await
            .unwrap_err();
        assert_matches!(err, error::Error::TableInfoNotFound { .. });
    }
}
