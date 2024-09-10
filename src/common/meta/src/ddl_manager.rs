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

use common_procedure::{
    watcher, BoxedProcedureLoader, Output, ProcedureId, ProcedureManagerRef, ProcedureWithId,
};
use common_telemetry::tracing_context::{FutureExt, TracingContext};
use common_telemetry::{debug, info, tracing};
use derive_builder::Builder;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::storage::TableId;

use crate::ddl::alter_logical_tables::AlterLogicalTablesProcedure;
use crate::ddl::alter_table::AlterTableProcedure;
use crate::ddl::create_database::CreateDatabaseProcedure;
use crate::ddl::create_flow::CreateFlowProcedure;
use crate::ddl::create_logical_tables::CreateLogicalTablesProcedure;
use crate::ddl::create_table::CreateTableProcedure;
use crate::ddl::create_view::CreateViewProcedure;
use crate::ddl::drop_database::DropDatabaseProcedure;
use crate::ddl::drop_flow::DropFlowProcedure;
use crate::ddl::drop_table::DropTableProcedure;
use crate::ddl::drop_view::DropViewProcedure;
use crate::ddl::truncate_table::TruncateTableProcedure;
use crate::ddl::{utils, DdlContext, ExecutorContext, ProcedureExecutor};
use crate::error::{
    EmptyDdlTasksSnafu, ParseProcedureIdSnafu, ProcedureNotFoundSnafu, ProcedureOutputSnafu,
    QueryProcedureSnafu, RegisterProcedureLoaderSnafu, Result, SubmitProcedureSnafu,
    TableInfoNotFoundSnafu, TableNotFoundSnafu, TableRouteNotFoundSnafu,
    UnexpectedLogicalRouteTableSnafu, UnsupportedSnafu, WaitProcedureSnafu,
};
use crate::key::table_info::TableInfoValue;
use crate::key::table_name::TableNameKey;
use crate::key::{DeserializedValueWithBytes, TableMetadataManagerRef};
use crate::rpc::ddl::DdlTask::{
    AlterLogicalTables, AlterTable, CreateDatabase, CreateFlow, CreateLogicalTables, CreateTable,
    CreateView, DropDatabase, DropFlow, DropLogicalTables, DropTable, DropView, TruncateTable,
};
use crate::rpc::ddl::{
    AlterTableTask, CreateDatabaseTask, CreateFlowTask, CreateTableTask, CreateViewTask,
    DropDatabaseTask, DropFlowTask, DropTableTask, DropViewTask, QueryContext,
    SubmitDdlTaskRequest, SubmitDdlTaskResponse, TruncateTableTask,
};
use crate::rpc::procedure;
use crate::rpc::procedure::{MigrateRegionRequest, MigrateRegionResponse, ProcedureStateResponse};
use crate::rpc::router::RegionRoute;
use crate::ClusterId;

pub type DdlManagerRef = Arc<DdlManager>;

pub type BoxedProcedureLoaderFactory = dyn Fn(DdlContext) -> BoxedProcedureLoader;

/// The [DdlManager] provides the ability to execute Ddl.
#[derive(Builder)]
pub struct DdlManager {
    ddl_context: DdlContext,
    procedure_manager: ProcedureManagerRef,
}

macro_rules! procedure_loader_entry {
    ($procedure:ident) => {
        (
            $procedure::TYPE_NAME,
            &|context: DdlContext| -> BoxedProcedureLoader {
                Box::new(move |json: &str| {
                    let context = context.clone();
                    $procedure::from_json(json, context).map(|p| Box::new(p) as _)
                })
            },
        )
    };
}

macro_rules! procedure_loader {
    ($($procedure:ident),*) => {
        vec![
            $(procedure_loader_entry!($procedure)),*
        ]
    };
}

impl DdlManager {
    /// Returns a new [DdlManager] with all Ddl [BoxedProcedureLoader](common_procedure::procedure::BoxedProcedureLoader)s registered.
    pub fn try_new(
        ddl_context: DdlContext,
        procedure_manager: ProcedureManagerRef,
        register_loaders: bool,
    ) -> Result<Self> {
        let manager = Self {
            ddl_context,
            procedure_manager,
        };
        if register_loaders {
            manager.register_loaders()?;
        }
        Ok(manager)
    }

    /// Returns the [TableMetadataManagerRef].
    pub fn table_metadata_manager(&self) -> &TableMetadataManagerRef {
        &self.ddl_context.table_metadata_manager
    }

    /// Returns the [DdlContext]
    pub fn create_context(&self) -> DdlContext {
        self.ddl_context.clone()
    }

    /// Registers all Ddl loaders.
    pub fn register_loaders(&self) -> Result<()> {
        let loaders: Vec<(&str, &BoxedProcedureLoaderFactory)> = procedure_loader!(
            CreateTableProcedure,
            CreateLogicalTablesProcedure,
            CreateViewProcedure,
            CreateFlowProcedure,
            AlterTableProcedure,
            AlterLogicalTablesProcedure,
            DropTableProcedure,
            DropFlowProcedure,
            TruncateTableProcedure,
            CreateDatabaseProcedure,
            DropDatabaseProcedure,
            DropViewProcedure
        );

        for (type_name, loader_factory) in loaders {
            let context = self.create_context();
            self.procedure_manager
                .register_loader(type_name, loader_factory(context))
                .context(RegisterProcedureLoaderSnafu { type_name })?;
        }

        Ok(())
    }

    /// Submits and executes an alter table task.
    #[tracing::instrument(skip_all)]
    pub async fn submit_alter_table_task(
        &self,
        cluster_id: ClusterId,
        table_id: TableId,
        alter_table_task: AlterTableTask,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let context = self.create_context();

        let procedure = AlterTableProcedure::new(cluster_id, table_id, alter_table_task, context)?;

        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));

        self.submit_procedure(procedure_with_id).await
    }

    /// Submits and executes a create table task.
    #[tracing::instrument(skip_all)]
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

    /// Submits and executes a `[CreateViewTask]`.
    #[tracing::instrument(skip_all)]
    pub async fn submit_create_view_task(
        &self,
        cluster_id: ClusterId,
        create_view_task: CreateViewTask,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let context = self.create_context();

        let procedure = CreateViewProcedure::new(cluster_id, create_view_task, context);

        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));

        self.submit_procedure(procedure_with_id).await
    }

    /// Submits and executes a create multiple logical table tasks.
    #[tracing::instrument(skip_all)]
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

    /// Submits and executes alter multiple table tasks.
    #[tracing::instrument(skip_all)]
    pub async fn submit_alter_logical_table_tasks(
        &self,
        cluster_id: ClusterId,
        alter_table_tasks: Vec<AlterTableTask>,
        physical_table_id: TableId,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let context = self.create_context();

        let procedure = AlterLogicalTablesProcedure::new(
            cluster_id,
            alter_table_tasks,
            physical_table_id,
            context,
        );

        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));

        self.submit_procedure(procedure_with_id).await
    }

    /// Submits and executes a drop table task.
    #[tracing::instrument(skip_all)]
    pub async fn submit_drop_table_task(
        &self,
        cluster_id: ClusterId,
        drop_table_task: DropTableTask,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let context = self.create_context();

        let procedure = DropTableProcedure::new(cluster_id, drop_table_task, context);

        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));

        self.submit_procedure(procedure_with_id).await
    }

    /// Submits and executes a create database task.
    #[tracing::instrument(skip_all)]
    pub async fn submit_create_database(
        &self,
        _cluster_id: ClusterId,
        CreateDatabaseTask {
            catalog,
            schema,
            create_if_not_exists,
            options,
        }: CreateDatabaseTask,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let context = self.create_context();
        let procedure =
            CreateDatabaseProcedure::new(catalog, schema, create_if_not_exists, options, context);
        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));

        self.submit_procedure(procedure_with_id).await
    }

    /// Submits and executes a drop table task.
    #[tracing::instrument(skip_all)]
    pub async fn submit_drop_database(
        &self,
        _cluster_id: ClusterId,
        DropDatabaseTask {
            catalog,
            schema,
            drop_if_exists,
        }: DropDatabaseTask,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let context = self.create_context();
        let procedure = DropDatabaseProcedure::new(catalog, schema, drop_if_exists, context);
        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));

        self.submit_procedure(procedure_with_id).await
    }

    /// Submits and executes a create flow task.
    #[tracing::instrument(skip_all)]
    pub async fn submit_create_flow_task(
        &self,
        cluster_id: ClusterId,
        create_flow: CreateFlowTask,
        query_context: QueryContext,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let context = self.create_context();
        let procedure = CreateFlowProcedure::new(cluster_id, create_flow, query_context, context);
        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));

        self.submit_procedure(procedure_with_id).await
    }

    /// Submits and executes a drop flow task.
    #[tracing::instrument(skip_all)]
    pub async fn submit_drop_flow_task(
        &self,
        cluster_id: ClusterId,
        drop_flow: DropFlowTask,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let context = self.create_context();
        let procedure = DropFlowProcedure::new(cluster_id, drop_flow, context);
        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));

        self.submit_procedure(procedure_with_id).await
    }

    /// Submits and executes a drop view task.
    #[tracing::instrument(skip_all)]
    pub async fn submit_drop_view_task(
        &self,
        cluster_id: ClusterId,
        drop_view: DropViewTask,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let context = self.create_context();
        let procedure = DropViewProcedure::new(cluster_id, drop_view, context);
        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));

        self.submit_procedure(procedure_with_id).await
    }

    /// Submits and executes a truncate table task.
    #[tracing::instrument(skip_all)]
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

    let table_info_value = table_info_value.with_context(|| TableInfoNotFoundSnafu {
        table: table_ref.to_string(),
    })?;

    let table_route_value = table_route_value.context(TableRouteNotFoundSnafu { table_id })?;

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
        .table_metadata_manager()
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

    let table_route_value = ddl_manager
        .table_metadata_manager()
        .table_route_manager()
        .table_route_storage()
        .get(table_id)
        .await?
        .context(TableRouteNotFoundSnafu { table_id })?;
    ensure!(
        table_route_value.is_physical(),
        UnexpectedLogicalRouteTableSnafu {
            err_msg: format!("{:?} is a non-physical TableRouteValue.", table_ref),
        }
    );

    let (id, _) = ddl_manager
        .submit_alter_table_task(cluster_id, table_id, alter_table_task)
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
    let (id, _) = ddl_manager
        .submit_drop_table_task(cluster_id, drop_table_task)
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
        table_ids: vec![table_id],
    })
}

async fn handle_create_logical_table_tasks(
    ddl_manager: &DdlManager,
    cluster_id: ClusterId,
    create_table_tasks: Vec<CreateTableTask>,
) -> Result<SubmitDdlTaskResponse> {
    ensure!(
        !create_table_tasks.is_empty(),
        EmptyDdlTasksSnafu {
            name: "create logical tables"
        }
    );
    let physical_table_id = utils::check_and_get_physical_table_id(
        ddl_manager.table_metadata_manager(),
        &create_table_tasks,
    )
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
    })
}

async fn handle_create_database_task(
    ddl_manager: &DdlManager,
    cluster_id: ClusterId,
    create_database_task: CreateDatabaseTask,
) -> Result<SubmitDdlTaskResponse> {
    let (id, _) = ddl_manager
        .submit_create_database(cluster_id, create_database_task.clone())
        .await?;

    let procedure_id = id.to_string();
    info!(
        "Database {}.{} is created via procedure_id {id:?}",
        create_database_task.catalog, create_database_task.schema
    );

    Ok(SubmitDdlTaskResponse {
        key: procedure_id.into(),
        ..Default::default()
    })
}

async fn handle_drop_database_task(
    ddl_manager: &DdlManager,
    cluster_id: ClusterId,
    drop_database_task: DropDatabaseTask,
) -> Result<SubmitDdlTaskResponse> {
    let (id, _) = ddl_manager
        .submit_drop_database(cluster_id, drop_database_task.clone())
        .await?;

    let procedure_id = id.to_string();
    info!(
        "Database {}.{} is dropped via procedure_id {id:?}",
        drop_database_task.catalog, drop_database_task.schema
    );

    Ok(SubmitDdlTaskResponse {
        key: procedure_id.into(),
        ..Default::default()
    })
}

async fn handle_drop_flow_task(
    ddl_manager: &DdlManager,
    cluster_id: ClusterId,
    drop_flow_task: DropFlowTask,
) -> Result<SubmitDdlTaskResponse> {
    let (id, _) = ddl_manager
        .submit_drop_flow_task(cluster_id, drop_flow_task.clone())
        .await?;

    let procedure_id = id.to_string();
    info!(
        "Flow {}.{}({}) is dropped via procedure_id {id:?}",
        drop_flow_task.catalog_name, drop_flow_task.flow_name, drop_flow_task.flow_id,
    );

    Ok(SubmitDdlTaskResponse {
        key: procedure_id.into(),
        ..Default::default()
    })
}

async fn handle_drop_view_task(
    ddl_manager: &DdlManager,
    cluster_id: ClusterId,
    drop_view_task: DropViewTask,
) -> Result<SubmitDdlTaskResponse> {
    let (id, _) = ddl_manager
        .submit_drop_view_task(cluster_id, drop_view_task.clone())
        .await?;

    let procedure_id = id.to_string();
    info!(
        "View {}({}) is dropped via procedure_id {id:?}",
        drop_view_task.table_ref(),
        drop_view_task.view_id,
    );

    Ok(SubmitDdlTaskResponse {
        key: procedure_id.into(),
        ..Default::default()
    })
}

async fn handle_create_flow_task(
    ddl_manager: &DdlManager,
    cluster_id: ClusterId,
    create_flow_task: CreateFlowTask,
    query_context: QueryContext,
) -> Result<SubmitDdlTaskResponse> {
    let (id, output) = ddl_manager
        .submit_create_flow_task(cluster_id, create_flow_task.clone(), query_context)
        .await?;

    let procedure_id = id.to_string();
    let output = output.context(ProcedureOutputSnafu {
        procedure_id: &procedure_id,
        err_msg: "empty output",
    })?;
    let flow_id = *(output.downcast_ref::<u32>().context(ProcedureOutputSnafu {
        procedure_id: &procedure_id,
        err_msg: "downcast to `u32`",
    })?);
    info!(
        "Flow {}.{}({flow_id}) is created via procedure_id {id:?}",
        create_flow_task.catalog_name, create_flow_task.flow_name,
    );

    Ok(SubmitDdlTaskResponse {
        key: procedure_id.into(),
        ..Default::default()
    })
}

async fn handle_alter_logical_table_tasks(
    ddl_manager: &DdlManager,
    cluster_id: ClusterId,
    alter_table_tasks: Vec<AlterTableTask>,
) -> Result<SubmitDdlTaskResponse> {
    ensure!(
        !alter_table_tasks.is_empty(),
        EmptyDdlTasksSnafu {
            name: "alter logical tables"
        }
    );

    // Use the physical table id in the first logical table, then it will be checked in the procedure.
    let first_table = TableNameKey {
        catalog: &alter_table_tasks[0].alter_table.catalog_name,
        schema: &alter_table_tasks[0].alter_table.schema_name,
        table: &alter_table_tasks[0].alter_table.table_name,
    };
    let physical_table_id =
        utils::get_physical_table_id(ddl_manager.table_metadata_manager(), first_table).await?;
    let num_logical_tables = alter_table_tasks.len();

    let (id, _) = ddl_manager
        .submit_alter_logical_table_tasks(cluster_id, alter_table_tasks, physical_table_id)
        .await?;

    info!("{num_logical_tables} logical tables on physical table: {physical_table_id:?} is altered via procedure_id {id:?}");

    let procedure_id = id.to_string();

    Ok(SubmitDdlTaskResponse {
        key: procedure_id.into(),
        ..Default::default()
    })
}

/// Handle the `[CreateViewTask]` and returns the DDL response when success.
async fn handle_create_view_task(
    ddl_manager: &DdlManager,
    cluster_id: ClusterId,
    create_view_task: CreateViewTask,
) -> Result<SubmitDdlTaskResponse> {
    let (id, output) = ddl_manager
        .submit_create_view_task(cluster_id, create_view_task)
        .await?;

    let procedure_id = id.to_string();
    let output = output.context(ProcedureOutputSnafu {
        procedure_id: &procedure_id,
        err_msg: "empty output",
    })?;
    let view_id = *(output.downcast_ref::<u32>().context(ProcedureOutputSnafu {
        procedure_id: &procedure_id,
        err_msg: "downcast to `u32`",
    })?);
    info!("View: {view_id} is created via procedure_id {id:?}");

    Ok(SubmitDdlTaskResponse {
        key: procedure_id.into(),
        table_ids: vec![view_id],
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
            debug!("Submitting Ddl task: {:?}", request.task);
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
                AlterLogicalTables(alter_table_tasks) => {
                    handle_alter_logical_table_tasks(self, cluster_id, alter_table_tasks).await
                }
                DropLogicalTables(_) => todo!(),
                CreateDatabase(create_database_task) => {
                    handle_create_database_task(self, cluster_id, create_database_task).await
                }
                DropDatabase(drop_database_task) => {
                    handle_drop_database_task(self, cluster_id, drop_database_task).await
                }
                CreateFlow(create_flow_task) => {
                    handle_create_flow_task(
                        self,
                        cluster_id,
                        create_flow_task,
                        request.query_context.into(),
                    )
                    .await
                }
                DropFlow(drop_flow_task) => {
                    handle_drop_flow_task(self, cluster_id, drop_flow_task).await
                }
                CreateView(create_view_task) => {
                    handle_create_view_task(self, cluster_id, create_view_task).await
                }
                DropView(drop_view_task) => {
                    handle_drop_view_task(self, cluster_id, drop_view_task).await
                }
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
        let pid =
            ProcedureId::parse_str(pid).with_context(|_| ParseProcedureIdSnafu { key: pid })?;

        let state = self
            .procedure_manager
            .procedure_state(pid)
            .await
            .context(QueryProcedureSnafu)?
            .context(ProcedureNotFoundSnafu {
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
    use crate::ddl::alter_table::AlterTableProcedure;
    use crate::ddl::create_table::CreateTableProcedure;
    use crate::ddl::drop_table::DropTableProcedure;
    use crate::ddl::flow_meta::FlowMetadataAllocator;
    use crate::ddl::table_meta::TableMetadataAllocator;
    use crate::ddl::truncate_table::TruncateTableProcedure;
    use crate::ddl::{DdlContext, NoopRegionFailureDetectorControl};
    use crate::key::flow::FlowMetadataManager;
    use crate::key::TableMetadataManager;
    use crate::kv_backend::memory::MemoryKvBackend;
    use crate::node_manager::{DatanodeRef, FlownodeRef, NodeManager};
    use crate::peer::Peer;
    use crate::region_keeper::MemoryRegionKeeper;
    use crate::sequence::SequenceBuilder;
    use crate::state_store::KvStateStore;
    use crate::wal_options_allocator::WalOptionsAllocator;

    /// A dummy implemented [NodeManager].
    pub struct DummyDatanodeManager;

    #[async_trait::async_trait]
    impl NodeManager for DummyDatanodeManager {
        async fn datanode(&self, _datanode: &Peer) -> DatanodeRef {
            unimplemented!()
        }

        async fn flownode(&self, _node: &Peer) -> FlownodeRef {
            unimplemented!()
        }
    }

    #[test]
    fn test_try_new() {
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let table_metadata_manager = Arc::new(TableMetadataManager::new(kv_backend.clone()));
        let table_metadata_allocator = Arc::new(TableMetadataAllocator::new(
            Arc::new(SequenceBuilder::new("test", kv_backend.clone()).build()),
            Arc::new(WalOptionsAllocator::default()),
        ));
        let flow_metadata_manager = Arc::new(FlowMetadataManager::new(kv_backend.clone()));
        let flow_metadata_allocator = Arc::new(FlowMetadataAllocator::with_noop_peer_allocator(
            Arc::new(SequenceBuilder::new("flow-test", kv_backend.clone()).build()),
        ));

        let state_store = Arc::new(KvStateStore::new(kv_backend.clone()));
        let procedure_manager = Arc::new(LocalManager::new(Default::default(), state_store));

        let _ = DdlManager::try_new(
            DdlContext {
                node_manager: Arc::new(DummyDatanodeManager),
                cache_invalidator: Arc::new(DummyCacheInvalidator),
                table_metadata_manager,
                table_metadata_allocator,
                flow_metadata_manager,
                flow_metadata_allocator,
                memory_region_keeper: Arc::new(MemoryRegionKeeper::default()),
                region_failure_detector_controller: Arc::new(NoopRegionFailureDetectorControl),
            },
            procedure_manager.clone(),
            true,
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
