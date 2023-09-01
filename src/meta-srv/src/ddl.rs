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

use std::sync::Arc;

use client::client_manager::DatanodeClients;
use common_meta::cache_invalidator::CacheInvalidatorRef;
use common_meta::datanode_manager::DatanodeManagerRef;
use common_meta::key::table_info::TableInfoValue;
use common_meta::key::table_route::TableRouteValue;
use common_meta::key::TableMetadataManagerRef;
use common_meta::rpc::ddl::{AlterTableTask, CreateTableTask, DropTableTask, TruncateTableTask};
use common_meta::rpc::router::RegionRoute;
use common_procedure::{watcher, ProcedureId, ProcedureManagerRef, ProcedureWithId};
use common_telemetry::error;
use snafu::ResultExt;
use table::requests::AlterTableRequest;

use crate::error::{
    RegisterProcedureLoaderSnafu, Result, SubmitProcedureSnafu, UnsupportedSnafu,
    WaitProcedureSnafu,
};
use crate::procedure::alter_table::AlterTableProcedure;
use crate::procedure::create_table::CreateTableProcedure;
use crate::procedure::drop_table::DropTableProcedure;

pub type DdlManagerRef = Arc<DdlManager>;

pub struct DdlManager {
    procedure_manager: ProcedureManagerRef,
    datanode_clients: Arc<DatanodeClients>,

    pub(crate) cache_invalidator: CacheInvalidatorRef,
    pub(crate) table_metadata_manager: TableMetadataManagerRef,
}

#[derive(Clone)]
pub(crate) struct DdlContext {
    // TODO(weny): removes it
    pub(crate) datanode_clients: Arc<DatanodeClients>,

    pub(crate) datanode_manager: DatanodeManagerRef,
    pub(crate) cache_invalidator: CacheInvalidatorRef,
    pub(crate) table_metadata_manager: TableMetadataManagerRef,
}

impl DdlManager {
    pub(crate) fn new(
        procedure_manager: ProcedureManagerRef,
        datanode_clients: Arc<DatanodeClients>,
        cache_invalidator: CacheInvalidatorRef,
        table_metadata_manager: TableMetadataManagerRef,
    ) -> Self {
        Self {
            procedure_manager,
            datanode_clients,
            cache_invalidator,
            table_metadata_manager,
        }
    }

    pub(crate) fn create_context(&self) -> DdlContext {
        DdlContext {
            datanode_manager: self.datanode_clients.clone(),
            datanode_clients: self.datanode_clients.clone(),
            cache_invalidator: self.cache_invalidator.clone(),

            table_metadata_manager: self.table_metadata_manager.clone(),
        }
    }

    pub(crate) fn try_start(&self) -> Result<()> {
        let context = self.create_context();

        self.procedure_manager
            .register_loader(
                CreateTableProcedure::TYPE_NAME,
                Box::new(move |json| {
                    let context = context.clone();
                    CreateTableProcedure::from_json(json, context).map(|p| Box::new(p) as _)
                }),
            )
            .context(RegisterProcedureLoaderSnafu {
                type_name: CreateTableProcedure::TYPE_NAME,
            })?;

        let context = self.create_context();

        self.procedure_manager
            .register_loader(
                DropTableProcedure::TYPE_NAME,
                Box::new(move |json| {
                    let context = context.clone();
                    DropTableProcedure::from_json(json, context).map(|p| Box::new(p) as _)
                }),
            )
            .context(RegisterProcedureLoaderSnafu {
                type_name: DropTableProcedure::TYPE_NAME,
            })?;

        let context = self.create_context();

        self.procedure_manager
            .register_loader(
                AlterTableProcedure::TYPE_NAME,
                Box::new(move |json| {
                    let context = context.clone();
                    AlterTableProcedure::from_json(json, context).map(|p| Box::new(p) as _)
                }),
            )
            .context(RegisterProcedureLoaderSnafu {
                type_name: AlterTableProcedure::TYPE_NAME,
            })
    }

    pub async fn submit_alter_table_task(
        &self,
        cluster_id: u64,
        alter_table_task: AlterTableTask,
        alter_table_request: AlterTableRequest,
        table_info_value: TableInfoValue,
    ) -> Result<ProcedureId> {
        let context = self.create_context();

        let procedure = AlterTableProcedure::new(
            cluster_id,
            alter_table_task,
            alter_table_request,
            table_info_value,
            context,
        );

        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));

        self.submit_procedure(procedure_with_id).await
    }

    pub async fn submit_create_table_task(
        &self,
        cluster_id: u64,
        create_table_task: CreateTableTask,
        region_routes: Vec<RegionRoute>,
    ) -> Result<ProcedureId> {
        let context = self.create_context();

        let procedure =
            CreateTableProcedure::new(cluster_id, create_table_task, region_routes, context);

        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));

        self.submit_procedure(procedure_with_id).await
    }

    pub async fn submit_drop_table_task(
        &self,
        cluster_id: u64,
        drop_table_task: DropTableTask,
        table_info_value: TableInfoValue,
        table_route_value: TableRouteValue,
    ) -> Result<ProcedureId> {
        let context = self.create_context();

        let procedure = DropTableProcedure::new(
            cluster_id,
            drop_table_task,
            table_route_value,
            table_info_value,
            context,
        );

        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));

        self.submit_procedure(procedure_with_id).await
    }

    pub async fn submit_truncate_table_task(
        &self,
        cluster_id: u64,
        truncate_table_task: TruncateTableTask,
        region_routes: Vec<RegionRoute>,
    ) -> Result<ProcedureId> {
        error!("truncate table procedure is not supported, cluster_id = {}, truncate_table_task = {:?}, region_routes = {:?}",
            cluster_id, truncate_table_task, region_routes);

        UnsupportedSnafu {
            operation: "TRUNCATE TABLE",
        }
        .fail()
    }

    async fn submit_procedure(&self, procedure_with_id: ProcedureWithId) -> Result<ProcedureId> {
        let procedure_id = procedure_with_id.id;

        let mut watcher = self
            .procedure_manager
            .submit(procedure_with_id)
            .await
            .context(SubmitProcedureSnafu)?;

        watcher::wait(&mut watcher)
            .await
            .context(WaitProcedureSnafu)?;

        Ok(procedure_id)
    }
}
