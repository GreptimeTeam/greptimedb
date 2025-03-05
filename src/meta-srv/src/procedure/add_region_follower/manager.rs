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

use common_meta::cache_invalidator::CacheInvalidatorRef;
use common_meta::key::TableMetadataManagerRef;
use common_meta::rpc::procedure::AddRegionFollowerRequest;
use common_procedure::{watcher, Output, ProcedureId, ProcedureManagerRef, ProcedureWithId};
use common_telemetry::info;
use snafu::{OptionExt, ResultExt};
use store_api::storage::RegionId;
use table::table_name::TableName;

use super::AddRegionFollowerProcedure;
use crate::cluster::MetaPeerClientRef;
use crate::error::{self, Result};
use crate::service::mailbox::MailboxRef;

#[derive(Clone)]
/// The context of add region follower procedure.
pub struct Context {
    pub table_metadata_manager: TableMetadataManagerRef,
    pub mailbox: MailboxRef,
    pub server_addr: String,
    pub cache_invalidator: CacheInvalidatorRef,
    pub meta_peer_client: MetaPeerClientRef,
}

pub struct AddRegionFollowerManager {
    procedure_manager: ProcedureManagerRef,
    default_context: Context,
}

impl AddRegionFollowerManager {
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
            .context(error::RegisterProcedureLoaderSnafu { type_name })
    }

    pub async fn submit_procedure(
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
}
