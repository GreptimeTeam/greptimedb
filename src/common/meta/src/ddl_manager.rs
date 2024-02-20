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

use common_procedure::{watcher, Output, ProcedureId, ProcedureManagerRef, ProcedureWithId};
use common_telemetry::tracing_context::{FutureExt, TracingContext};
use common_telemetry::{info, tracing};
use snafu::{ensure, OptionExt, ResultExt};
use store_api::storage::TableId;

use crate::cache_invalidator::CacheInvalidatorRef;
use crate::datanode_manager::DatanodeManagerRef;
use crate::ddl::alter_table::AlterTableProcedure;
use crate::ddl::create_logical_tables::CreateLogicalTablesProcedure;
use crate::ddl::create_table::CreateTableProcedure;
use crate::ddl::drop_table::DropTableProcedure;
use crate::ddl::table_meta::TableMetadataAllocatorRef;
use crate::ddl::truncate_table::TruncateTableProcedure;
use crate::ddl::{
    utils, DdlContext, ExecutorContext, ProcedureExecutor, TableMetadata,
    TableMetadataAllocatorContext,
};
use crate::error::{
    self, EmptyCreateTableTasksSnafu, ProcedureOutputSnafu, RegisterProcedureLoaderSnafu, Result,
    SubmitProcedureSnafu, TableNotFoundSnafu, UnsupportedSnafu, WaitProcedureSnafu,
};
use crate::key::table_info::TableInfoValue;
use crate::key::table_name::TableNameKey;
use crate::key::table_route::TableRouteValue;
use crate::key::{DeserializedValueWithBytes, TableMetadataManagerRef};
use crate::region_keeper::MemoryRegionKeeperRef;
use crate::rpc::ddl::DdlTask::{
    AlterLogicalTables, AlterTable, CreateLogicalTables, CreateTable, DropLogicalTables, DropTable,
    TruncateTable,
};
use crate::rpc::ddl::{
    AlterTableTask, CreateTableTask, DropTableTask, SubmitDdlTaskRequest, SubmitDdlTaskResponse,
    TruncateTableTask,
};
use crate::rpc::procedure;
use crate::rpc::procedure::{MigrateRegionRequest, MigrateRegionResponse, ProcedureStateResponse};
use crate::rpc::router::RegionRoute;
use crate::table_name::TableName;
use crate::ClusterId;

pub type DdlManagerRef = Arc<DdlManager>;

/// The [DdlManager] provides the ability to execute Ddl.
pub struct DdlManager {
    procedure_manager: ProcedureManagerRef,
    datanode_manager: DatanodeManagerRef,
    cache_invalidator: CacheInvalidatorRef,
    table_metadata_manager: TableMetadataManagerRef,
    table_metadata_allocator: TableMetadataAllocatorRef,
    memory_region_keeper: MemoryRegionKeeperRef,
}

impl DdlManager {
    /// Returns a new [DdlManager] with all Ddl [BoxedProcedureLoader](common_procedure::procedure::BoxedProcedureLoader)s registered.
    pub fn try_new(
        procedure_manager: ProcedureManagerRef,
        datanode_clients: DatanodeManagerRef,
        cache_invalidator: CacheInvalidatorRef,
        table_metadata_manager: TableMetadataManagerRef,
        table_metadata_allocator: TableMetadataAllocatorRef,
        memory_region_keeper: MemoryRegionKeeperRef,
    ) -> Result<Self> {
        let manager = Self {
            procedure_manager,
            datanode_manager: datanode_clients,
            cache_invalidator,
            table_metadata_manager,
            table_metadata_allocator,
            memory_region_keeper,
        };
        manager.register_loaders()?;
        Ok(manager)
    }

    /// Returns the [TableMetadataManagerRef].
    pub fn table_metadata_manager(&self) -> &TableMetadataManagerRef {
        &self.table_metadata_manager
    }

    /// Returns the [DdlContext]
    pub fn create_context(&self) -> DdlContext {
        DdlContext {
            datanode_manager: self.datanode_manager.clone(),
            cache_invalidator: self.cache_invalidator.clone(),
            table_metadata_manager: self.table_metadata_manager.clone(),
            memory_region_keeper: self.memory_region_keeper.clone(),
            table_metadata_allocator: self.table_metadata_allocator.clone(),
        }
    }

    fn register_loaders(&self) -> Result<()> {
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
                CreateLogicalTablesProcedure::TYPE_NAME,
                Box::new(move |json| {
                    let context = context.clone();
                    CreateLogicalTablesProcedure::from_json(json, context).map(|p| Box::new(p) as _)
                }),
            )
            .context(RegisterProcedureLoaderSnafu {
                type_name: CreateLogicalTablesProcedure::TYPE_NAME,
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

    #[tracing::instrument(skip_all)]
    /// Submits and executes an alter table task.
    pub async fn submit_alter_table_task(
        &self,
        cluster_id: ClusterId,
        alter_table_task: AlterTableTask,
        table_info_value: DeserializedValueWithBytes<TableInfoValue>,
        physical_table_info: Option<(TableId, TableName)>,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let context = self.create_context();

        let procedure = AlterTableProcedure::new(
            cluster_id,
            alter_table_task,
            table_info_value,
            physical_table_info,
            context,
        )?;

        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));

        self.submit_procedure(procedure_with_id).await
    }

    #[tracing::instrument(skip_all)]
    /// Submits and executes a create table task.
    pub async fn submit_create_table_task(
        &self,
        cluster_id: ClusterId,
        create_table_task: CreateTableTask,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let context = self.create_context();

        let procedure = CreateTableProcedure::new(cluster_id, create_table_task, context);

        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));

        self.submit_procedure(procedure_with_id).await
    }

    #[tracing::instrument(skip_all)]
    /// Submits and executes a create table task.
    pub async fn submit_create_logical_table_tasks(
        &self,
        cluster_id: ClusterId,
        create_table_tasks: Vec<CreateTableTask>,
        physical_table_id: TableId,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let context = self.create_context();

        let procedure = CreateLogicalTablesProcedure::new(
            cluster_id,
            create_table_tasks,
            physical_table_id,
            context,
        );

        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));

        self.submit_procedure(procedure_with_id).await
    }

    #[tracing::instrument(skip_all)]
    /// Submits and executes a drop table task.
    pub async fn submit_drop_table_task(
        &self,
        cluster_id: ClusterId,
        drop_table_task: DropTableTask,
        table_info_value: DeserializedValueWithBytes<TableInfoValue>,
        table_route_value: DeserializedValueWithBytes<TableRouteValue>,
    ) -> Result<(ProcedureId, Option<Output>)> {
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

    #[tracing::instrument(skip_all)]
    /// Submits and executes a truncate table task.
    pub async fn submit_truncate_table_task(
        &self,
        cluster_id: ClusterId,
        truncate_table_task: TruncateTableTask,
        table_info_value: DeserializedValueWithBytes<TableInfoValue>,
        region_routes: Vec<RegionRoute>,
    ) -> Result<(ProcedureId, Option<Output>)> {
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

    async fn submit_procedure(
        &self,
        procedure_with_id: ProcedureWithId,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let procedure_id = procedure_with_id.id;

        let mut watcher = self
            .procedure_manager
            .submit(procedure_with_id)
            .await
            .context(SubmitProcedureSnafu)?;

        let output = watcher::wait(&mut watcher)
            .await
            .context(WaitProcedureSnafu)?;

        Ok((procedure_id, output))
    }
}

async fn handle_truncate_table_task(
    ddl_manager: &DdlManager,
    cluster_id: ClusterId,
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

    let table_route_value =
        table_route_value.context(error::TableRouteNotFoundSnafu { table_id })?;

    let table_route = table_route_value.into_inner().region_routes()?.clone();

    let (id, _) = ddl_manager
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
    cluster_id: ClusterId,
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

    let physical_table_id = ddl_manager
        .table_metadata_manager()
        .table_route_manager()
        .get_physical_table_id(table_id)
        .await?;

    let physical_table_info = if physical_table_id == table_id {
        None
    } else {
        let physical_table_info = &ddl_manager
            .table_metadata_manager()
            .table_info_manager()
            .get(physical_table_id)
            .await?
            .with_context(|| error::TableInfoNotFoundSnafu {
                table_name: table_ref.to_string(),
            })?
            .table_info;
        Some((
            physical_table_id,
            TableName {
                catalog_name: physical_table_info.catalog_name.clone(),
                schema_name: physical_table_info.schema_name.clone(),
                table_name: physical_table_info.name.clone(),
            },
        ))
    };

    let (id, _) = ddl_manager
        .submit_alter_table_task(
            cluster_id,
            alter_table_task,
            table_info_value,
            physical_table_info,
        )
        .await?;

    info!("Table: {table_id} is altered via procedure_id {id:?}");

    Ok(SubmitDdlTaskResponse {
        key: id.to_string().into(),
        ..Default::default()
    })
}

async fn handle_drop_table_task(
    ddl_manager: &DdlManager,
    cluster_id: ClusterId,
    drop_table_task: DropTableTask,
) -> Result<SubmitDdlTaskResponse> {
    let table_id = drop_table_task.table_id;
    let table_metadata_manager = &ddl_manager.table_metadata_manager();
    let table_ref = drop_table_task.table_ref();

    let table_info_value = table_metadata_manager
        .table_info_manager()
        .get(table_id)
        .await?;
    let (_, table_route_value) = table_metadata_manager
        .table_route_manager()
        .get_physical_table_route(table_id)
        .await?;

    let table_info_value = table_info_value.with_context(|| error::TableInfoNotFoundSnafu {
        table_name: table_ref.to_string(),
    })?;

    let table_route_value =
        DeserializedValueWithBytes::from_inner(TableRouteValue::Physical(table_route_value));

    let (id, _) = ddl_manager
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
    cluster_id: ClusterId,
    create_table_task: CreateTableTask,
) -> Result<SubmitDdlTaskResponse> {
    let (id, output) = ddl_manager
        .submit_create_table_task(cluster_id, create_table_task)
        .await?;

    let procedure_id = id.to_string();
    let output = output.context(ProcedureOutputSnafu {
        procedure_id: &procedure_id,
        err_msg: "empty output",
    })?;
    let table_id = *(output.downcast_ref::<u32>().context(ProcedureOutputSnafu {
        procedure_id: &procedure_id,
        err_msg: "downcast to `u32`",
    })?);
    info!("Table: {table_id} is created via procedure_id {id:?}");

    Ok(SubmitDdlTaskResponse {
        key: procedure_id.into(),
        table_id: Some(table_id),
        ..Default::default()
    })
}

async fn handle_create_logical_table_tasks(
    ddl_manager: &DdlManager,
    cluster_id: ClusterId,
    mut create_table_tasks: Vec<CreateTableTask>,
) -> Result<SubmitDdlTaskResponse> {
    ensure!(!create_table_tasks.is_empty(), EmptyCreateTableTasksSnafu);
    let physical_table_id = utils::check_and_get_physical_table_id(
        &ddl_manager.table_metadata_manager,
        &create_table_tasks,
    )
    .await?;
    // Sets table_ids on create_table_tasks
    ddl_manager
        .table_metadata_allocator
        .set_table_ids_on_logic_create(&mut create_table_tasks)
        .await?;
    let num_logical_tables = create_table_tasks.len();

    let (id, output) = ddl_manager
        .submit_create_logical_table_tasks(cluster_id, create_table_tasks, physical_table_id)
        .await?;

    info!("{num_logical_tables} logical tables on physical table: {physical_table_id:?} is created via procedure_id {id:?}");

    let procedure_id = id.to_string();
    let output = output.context(ProcedureOutputSnafu {
        procedure_id: &procedure_id,
        err_msg: "empty output",
    })?;
    let table_ids = output
        .downcast_ref::<Vec<TableId>>()
        .context(ProcedureOutputSnafu {
            procedure_id: &procedure_id,
            err_msg: "downcast to `Vec<TableId>`",
        })?
        .clone();

    Ok(SubmitDdlTaskResponse {
        key: procedure_id.into(),
        table_ids,
        ..Default::default()
    })
}

/// TODO(dennis): let [`DdlManager`] implement [`ProcedureExecutor`] looks weird, find some way to refactor it.
#[async_trait::async_trait]
impl ProcedureExecutor for DdlManager {
    async fn submit_ddl_task(
        &self,
        ctx: &ExecutorContext,
        request: SubmitDdlTaskRequest,
    ) -> Result<SubmitDdlTaskResponse> {
        let span = ctx
            .tracing_context
            .as_ref()
            .map(TracingContext::from_w3c)
            .unwrap_or(TracingContext::from_current_span())
            .attach(tracing::info_span!("DdlManager::submit_ddl_task"));
        async move {
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
                CreateLogicalTables(create_table_tasks) => {
                    handle_create_logical_table_tasks(self, cluster_id, create_table_tasks).await
                }
                DropLogicalTables(_) => todo!(),
                AlterLogicalTables(_) => todo!(),
            }
        }
        .trace(span)
        .await
    }

    async fn migrate_region(
        &self,
        _ctx: &ExecutorContext,
        _request: MigrateRegionRequest,
    ) -> Result<MigrateRegionResponse> {
        UnsupportedSnafu {
            operation: "migrate_region",
        }
        .fail()
    }

    async fn query_procedure_state(
        &self,
        _ctx: &ExecutorContext,
        pid: &str,
    ) -> Result<ProcedureStateResponse> {
        let pid = ProcedureId::parse_str(pid)
            .with_context(|_| error::ParseProcedureIdSnafu { key: pid })?;

        let state = self
            .procedure_manager
            .procedure_state(pid)
            .await
            .context(error::QueryProcedureSnafu)?
            .context(error::ProcedureNotFoundSnafu {
                pid: pid.to_string(),
            })?;

        Ok(procedure::procedure_state_to_pb_response(&state))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_procedure::local::LocalManager;

    use super::DdlManager;
    use crate::cache_invalidator::DummyCacheInvalidator;
    use crate::datanode_manager::{DatanodeManager, DatanodeRef};
    use crate::ddl::alter_table::AlterTableProcedure;
    use crate::ddl::create_table::CreateTableProcedure;
    use crate::ddl::drop_table::DropTableProcedure;
    use crate::ddl::table_meta::TableMetadataAllocator;
    use crate::ddl::truncate_table::TruncateTableProcedure;
    use crate::key::TableMetadataManager;
    use crate::kv_backend::memory::MemoryKvBackend;
    use crate::peer::Peer;
    use crate::region_keeper::MemoryRegionKeeper;
    use crate::sequence::SequenceBuilder;
    use crate::state_store::KvStateStore;
    use crate::wal_options_allocator::WalOptionsAllocator;

    /// A dummy implemented [DatanodeManager].
    pub struct DummyDatanodeManager;

    #[async_trait::async_trait]
    impl DatanodeManager for DummyDatanodeManager {
        async fn datanode(&self, _datanode: &Peer) -> DatanodeRef {
            unimplemented!()
        }
    }

    #[test]
    fn test_try_new() {
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let table_metadata_manager = Arc::new(TableMetadataManager::new(kv_backend.clone()));

        let state_store = Arc::new(KvStateStore::new(kv_backend.clone()));
        let procedure_manager = Arc::new(LocalManager::new(Default::default(), state_store));

        let _ = DdlManager::try_new(
            procedure_manager.clone(),
            Arc::new(DummyDatanodeManager),
            Arc::new(DummyCacheInvalidator),
            table_metadata_manager.clone(),
            Arc::new(TableMetadataAllocator::new(
                Arc::new(SequenceBuilder::new("test", kv_backend.clone()).build()),
                Arc::new(WalOptionsAllocator::default()),
                table_metadata_manager.table_name_manager().clone(),
            )),
            Arc::new(MemoryRegionKeeper::default()),
        );

        let expected_loaders = vec![
            CreateTableProcedure::TYPE_NAME,
            AlterTableProcedure::TYPE_NAME,
            DropTableProcedure::TYPE_NAME,
            TruncateTableProcedure::TYPE_NAME,
        ];

        for loader in expected_loaders {
            assert!(procedure_manager.contains_loader(loader));
        }
    }
}
