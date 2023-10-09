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

use common_procedure::{watcher, ProcedureId, ProcedureManagerRef, ProcedureWithId};
use common_telemetry::info;
use snafu::{OptionExt, ResultExt};

use crate::cache_invalidator::CacheInvalidatorRef;
use crate::datanode_manager::DatanodeManagerRef;
use crate::ddl::alter_table::AlterTableProcedure;
use crate::ddl::create_table::CreateTableProcedure;
use crate::ddl::drop_table::DropTableProcedure;
use crate::ddl::truncate_table::TruncateTableProcedure;
use crate::ddl::{
    DdlContext, DdlTaskExecutor, ExecutorContext, TableMetadataAllocatorContext,
    TableMetadataAllocatorRef,
};
use crate::error::{
    self, RegisterProcedureLoaderSnafu, Result, SubmitProcedureSnafu, TableNotFoundSnafu,
    WaitProcedureSnafu,
};
use crate::key::table_info::TableInfoValue;
use crate::key::table_name::TableNameKey;
use crate::key::table_route::TableRouteValue;
use crate::key::{DeserializedValueWithBytes, TableMetadataManagerRef};
use crate::rpc::ddl::DdlTask::{AlterTable, CreateTable, DropTable, TruncateTable};
use crate::rpc::ddl::{
    AlterTableTask, CreateTableTask, DropTableTask, SubmitDdlTaskRequest, SubmitDdlTaskResponse,
    TruncateTableTask,
};
use crate::rpc::router::RegionRoute;

pub type DdlManagerRef = Arc<DdlManager>;

pub struct DdlManager {
    procedure_manager: ProcedureManagerRef,
    datanode_manager: DatanodeManagerRef,
    cache_invalidator: CacheInvalidatorRef,
    table_metadata_manager: TableMetadataManagerRef,
    table_meta_allocator: TableMetadataAllocatorRef,
}

impl DdlManager {
    pub fn new(
        procedure_manager: ProcedureManagerRef,
        datanode_clients: DatanodeManagerRef,
        cache_invalidator: CacheInvalidatorRef,
        table_metadata_manager: TableMetadataManagerRef,
        table_meta_allocator: TableMetadataAllocatorRef,
    ) -> Self {
        Self {
            procedure_manager,
            datanode_manager: datanode_clients,
            cache_invalidator,
            table_metadata_manager,
            table_meta_allocator,
        }
    }

    pub fn table_metadata_manager(&self) -> &TableMetadataManagerRef {
        &self.table_metadata_manager
    }

    pub fn create_context(&self) -> DdlContext {
        DdlContext {
            datanode_manager: self.datanode_manager.clone(),
            cache_invalidator: self.cache_invalidator.clone(),
            table_metadata_manager: self.table_metadata_manager.clone(),
        }
    }

    pub fn try_start(&self) -> Result<()> {
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
            })?;

        let context = self.create_context();

        self.procedure_manager
            .register_loader(
                TruncateTableProcedure::TYPE_NAME,
                Box::new(move |json| {
                    let context = context.clone();
                    TruncateTableProcedure::from_json(json, context).map(|p| Box::new(p) as _)
                }),
            )
            .context(RegisterProcedureLoaderSnafu {
                type_name: TruncateTableProcedure::TYPE_NAME,
            })
    }

    pub async fn submit_alter_table_task(
        &self,
        cluster_id: u64,
        alter_table_task: AlterTableTask,
        table_info_value: DeserializedValueWithBytes<TableInfoValue>,
    ) -> Result<ProcedureId> {
        let context = self.create_context();

        let procedure =
            AlterTableProcedure::new(cluster_id, alter_table_task, table_info_value, context)?;

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
        table_info_value: DeserializedValueWithBytes<TableInfoValue>,
        table_route_value: DeserializedValueWithBytes<TableRouteValue>,
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
        table_info_value: DeserializedValueWithBytes<TableInfoValue>,
        region_routes: Vec<RegionRoute>,
    ) -> Result<ProcedureId> {
        let context = self.create_context();
        let procedure = TruncateTableProcedure::new(
            cluster_id,
            truncate_table_task,
            table_info_value,
            region_routes,
            context,
        );

        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));

        self.submit_procedure(procedure_with_id).await
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

async fn handle_truncate_table_task(
    ddl_manager: &DdlManager,
    cluster_id: u64,
    truncate_table_task: TruncateTableTask,
) -> Result<SubmitDdlTaskResponse> {
    let table_id = truncate_table_task.table_id;
    let table_metadata_manager = &ddl_manager.table_metadata_manager();
    let table_ref = truncate_table_task.table_ref();

    let (table_info_value, table_route_value) =
        table_metadata_manager.get_full_table_info(table_id).await?;

    let table_info_value = table_info_value.with_context(|| error::TableInfoNotFoundSnafu {
        table_name: table_ref.to_string(),
    })?;

    let table_route_value = table_route_value.with_context(|| error::TableRouteNotFoundSnafu {
        table_name: table_ref.to_string(),
    })?;

    let table_route = table_route_value.into_inner().region_routes;

    let id = ddl_manager
        .submit_truncate_table_task(
            cluster_id,
            truncate_table_task,
            table_info_value,
            table_route,
        )
        .await?;

    info!("Table: {table_id} is truncated via procedure_id {id:?}");

    Ok(SubmitDdlTaskResponse {
        key: id.to_string().into(),
        ..Default::default()
    })
}

async fn handle_alter_table_task(
    ddl_manager: &DdlManager,
    cluster_id: u64,
    alter_table_task: AlterTableTask,
) -> Result<SubmitDdlTaskResponse> {
    let table_ref = alter_table_task.table_ref();

    let table_id = ddl_manager
        .table_metadata_manager
        .table_name_manager()
        .get(TableNameKey::new(
            table_ref.catalog,
            table_ref.schema,
            table_ref.table,
        ))
        .await?
        .with_context(|| TableNotFoundSnafu {
            table_name: table_ref.to_string(),
        })?
        .table_id();

    let table_info_value = ddl_manager
        .table_metadata_manager()
        .table_info_manager()
        .get(table_id)
        .await?
        .with_context(|| error::TableInfoNotFoundSnafu {
            table_name: table_ref.to_string(),
        })?;

    let id = ddl_manager
        .submit_alter_table_task(cluster_id, alter_table_task, table_info_value)
        .await?;

    info!("Table: {table_id} is altered via procedure_id {id:?}");

    Ok(SubmitDdlTaskResponse {
        key: id.to_string().into(),
        ..Default::default()
    })
}

async fn handle_drop_table_task(
    ddl_manager: &DdlManager,
    cluster_id: u64,
    drop_table_task: DropTableTask,
) -> Result<SubmitDdlTaskResponse> {
    let table_id = drop_table_task.table_id;
    let table_metadata_manager = &ddl_manager.table_metadata_manager();
    let table_ref = drop_table_task.table_ref();

    let (table_info_value, table_route_value) =
        table_metadata_manager.get_full_table_info(table_id).await?;

    let table_info_value = table_info_value.with_context(|| error::TableInfoNotFoundSnafu {
        table_name: table_ref.to_string(),
    })?;

    let table_route_value = table_route_value.with_context(|| error::TableRouteNotFoundSnafu {
        table_name: table_ref.to_string(),
    })?;

    let id = ddl_manager
        .submit_drop_table_task(
            cluster_id,
            drop_table_task,
            table_info_value,
            table_route_value,
        )
        .await?;

    info!("Table: {table_id} is dropped via procedure_id {id:?}");

    Ok(SubmitDdlTaskResponse {
        key: id.to_string().into(),
        ..Default::default()
    })
}

async fn handle_create_table_task(
    ddl_manager: &DdlManager,
    cluster_id: u64,
    mut create_table_task: CreateTableTask,
) -> Result<SubmitDdlTaskResponse> {
    let (table_id, region_routes) = ddl_manager
        .table_meta_allocator
        .create(
            &TableMetadataAllocatorContext { cluster_id },
            &mut create_table_task.table_info,
            &create_table_task.partitions,
        )
        .await?;

    let id = ddl_manager
        .submit_create_table_task(cluster_id, create_table_task, region_routes)
        .await?;

    info!("Table: {table_id:?} is created via procedure_id {id:?}");

    Ok(SubmitDdlTaskResponse {
        key: id.to_string().into(),
        table_id: Some(table_id),
    })
}

#[async_trait::async_trait]
impl DdlTaskExecutor for DdlManager {
    async fn submit_ddl_task(
        &self,
        ctx: &ExecutorContext,
        request: SubmitDdlTaskRequest,
    ) -> Result<SubmitDdlTaskResponse> {
        let cluster_id = ctx.cluster_id.unwrap_or_default();
        info!("Submitting Ddl task: {:?}", request.task);
        match request.task {
            CreateTable(create_table_task) => {
                handle_create_table_task(self, cluster_id, create_table_task).await
            }
            DropTable(drop_table_task) => {
                handle_drop_table_task(self, cluster_id, drop_table_task).await
            }
            AlterTable(alter_table_task) => {
                handle_alter_table_task(self, cluster_id, alter_table_task).await
            }
            TruncateTable(truncate_table_task) => {
                handle_truncate_table_task(self, cluster_id, truncate_table_task).await
            }
        }
    }
}
