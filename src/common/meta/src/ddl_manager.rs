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
use std::time::Duration;

use api::v1::Repartition;
use api::v1::alter_table_expr::Kind;
use common_error::ext::BoxedError;
use common_procedure::{
    BoxedProcedure, BoxedProcedureLoader, Output, ProcedureId, ProcedureManagerRef,
    ProcedureWithId, watcher,
};
use common_telemetry::tracing_context::{FutureExt, TracingContext};
use common_telemetry::{debug, info, tracing};
use derive_builder::Builder;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::storage::TableId;
use table::table_name::TableName;

use crate::ddl::alter_database::AlterDatabaseProcedure;
use crate::ddl::alter_logical_tables::AlterLogicalTablesProcedure;
use crate::ddl::alter_table::AlterTableProcedure;
use crate::ddl::comment_on::CommentOnProcedure;
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
use crate::ddl::{DdlContext, utils};
use crate::error::{
    CreateRepartitionProcedureSnafu, EmptyDdlTasksSnafu, ProcedureOutputSnafu,
    RegisterProcedureLoaderSnafu, RegisterRepartitionProcedureLoaderSnafu, Result,
    SubmitProcedureSnafu, TableInfoNotFoundSnafu, TableNotFoundSnafu, TableRouteNotFoundSnafu,
    UnexpectedLogicalRouteTableSnafu, WaitProcedureSnafu,
};
use crate::key::table_info::TableInfoValue;
use crate::key::table_name::TableNameKey;
use crate::key::{DeserializedValueWithBytes, TableMetadataManagerRef};
use crate::procedure_executor::ExecutorContext;
#[cfg(feature = "enterprise")]
use crate::rpc::ddl::DdlTask::CreateTrigger;
#[cfg(feature = "enterprise")]
use crate::rpc::ddl::DdlTask::DropTrigger;
use crate::rpc::ddl::DdlTask::{
    AlterDatabase, AlterLogicalTables, AlterTable, CommentOn, CreateDatabase, CreateFlow,
    CreateLogicalTables, CreateTable, CreateView, DropDatabase, DropFlow, DropLogicalTables,
    DropTable, DropView, TruncateTable,
};
#[cfg(feature = "enterprise")]
use crate::rpc::ddl::trigger::CreateTriggerTask;
#[cfg(feature = "enterprise")]
use crate::rpc::ddl::trigger::DropTriggerTask;
use crate::rpc::ddl::{
    AlterDatabaseTask, AlterTableTask, CommentOnTask, CreateDatabaseTask, CreateFlowTask,
    CreateTableTask, CreateViewTask, DropDatabaseTask, DropFlowTask, DropTableTask, DropViewTask,
    QueryContext, SubmitDdlTaskRequest, SubmitDdlTaskResponse, TruncateTableTask,
};
use crate::rpc::router::RegionRoute;

/// A configurator that customizes or enhances a [`DdlManager`].
#[async_trait::async_trait]
pub trait DdlManagerConfigurator<C>: Send + Sync {
    /// Configures the given [`DdlManager`] using the provided [`DdlManagerConfigureContext`].
    async fn configure(
        &self,
        ddl_manager: DdlManager,
        ctx: C,
    ) -> std::result::Result<DdlManager, BoxedError>;
}

pub type DdlManagerConfiguratorRef<C> = Arc<dyn DdlManagerConfigurator<C>>;

pub type DdlManagerRef = Arc<DdlManager>;

pub type BoxedProcedureLoaderFactory = dyn Fn(DdlContext) -> BoxedProcedureLoader;

/// The [DdlManager] provides the ability to execute Ddl.
#[derive(Builder)]
pub struct DdlManager {
    ddl_context: DdlContext,
    procedure_manager: ProcedureManagerRef,
    repartition_procedure_factory: RepartitionProcedureFactoryRef,
    #[cfg(feature = "enterprise")]
    trigger_ddl_manager: Option<TriggerDdlManagerRef>,
}

/// This trait is responsible for handling DDL tasks about triggers. e.g.,
/// create trigger, drop trigger, etc.
#[cfg(feature = "enterprise")]
#[async_trait::async_trait]
pub trait TriggerDdlManager: Send + Sync {
    async fn create_trigger(
        &self,
        create_trigger_task: CreateTriggerTask,
        procedure_manager: ProcedureManagerRef,
        ddl_context: DdlContext,
        query_context: QueryContext,
    ) -> Result<SubmitDdlTaskResponse>;

    async fn drop_trigger(
        &self,
        drop_trigger_task: DropTriggerTask,
        procedure_manager: ProcedureManagerRef,
        ddl_context: DdlContext,
        query_context: QueryContext,
    ) -> Result<SubmitDdlTaskResponse>;

    fn as_any(&self) -> &dyn std::any::Any;
}

#[cfg(feature = "enterprise")]
pub type TriggerDdlManagerRef = Arc<dyn TriggerDdlManager>;

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

pub type RepartitionProcedureFactoryRef = Arc<dyn RepartitionProcedureFactory>;

pub trait RepartitionProcedureFactory: Send + Sync {
    fn create(
        &self,
        ddl_ctx: &DdlContext,
        table_name: TableName,
        table_id: TableId,
        from_exprs: Vec<String>,
        to_exprs: Vec<String>,
        timeout: Option<Duration>,
    ) -> std::result::Result<BoxedProcedure, BoxedError>;

    fn register_loaders(
        &self,
        ddl_ctx: &DdlContext,
        procedure_manager: &ProcedureManagerRef,
    ) -> std::result::Result<(), BoxedError>;
}

/// The options for DDL tasks.
#[derive(Debug, Clone, Copy)]
pub struct DdlOptions {
    /// Timeout for the task.
    pub timeout: Duration,
    /// Waits for the task to complete.
    pub wait: bool,
}

impl DdlManager {
    /// Returns a new [DdlManager] with all Ddl [BoxedProcedureLoader](common_procedure::procedure::BoxedProcedureLoader)s registered.
    pub fn try_new(
        ddl_context: DdlContext,
        procedure_manager: ProcedureManagerRef,
        repartition_procedure_factory: RepartitionProcedureFactoryRef,
        register_loaders: bool,
    ) -> Result<Self> {
        let manager = Self {
            ddl_context,
            procedure_manager,
            repartition_procedure_factory,
            #[cfg(feature = "enterprise")]
            trigger_ddl_manager: None,
        };
        if register_loaders {
            manager.register_loaders()?;
        }
        Ok(manager)
    }

    #[cfg(feature = "enterprise")]
    pub fn with_trigger_ddl_manager(mut self, trigger_ddl_manager: TriggerDdlManagerRef) -> Self {
        self.trigger_ddl_manager = Some(trigger_ddl_manager);
        self
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
            AlterDatabaseProcedure,
            DropTableProcedure,
            DropFlowProcedure,
            TruncateTableProcedure,
            CreateDatabaseProcedure,
            DropDatabaseProcedure,
            DropViewProcedure,
            CommentOnProcedure
        );

        for (type_name, loader_factory) in loaders {
            let context = self.create_context();
            self.procedure_manager
                .register_loader(type_name, loader_factory(context))
                .context(RegisterProcedureLoaderSnafu { type_name })?;
        }

        self.repartition_procedure_factory
            .register_loaders(&self.ddl_context, &self.procedure_manager)
            .context(RegisterRepartitionProcedureLoaderSnafu)?;

        Ok(())
    }

    /// Submits a repartition procedure for the specified table.
    ///
    /// This creates a repartition procedure using the provided `table_id`,
    /// `table_name`, and `Repartition` configuration, and then either executes it
    /// to completion or just submits it for asynchronous execution.
    ///
    /// The `Repartition` argument contains the original (`from_partition_exprs`)
    /// and target (`into_partition_exprs`) partition expressions that define how
    /// the table should be repartitioned.
    ///
    /// The `wait` flag controls whether this method waits for the repartition
    /// procedure to finish:
    /// - If `wait` is `true`, the procedure is executed and this method awaits
    ///   its completion, returning both the generated `ProcedureId` and the
    ///   final `Output` of the procedure.
    /// - If `wait` is `false`, the procedure is only submitted to the procedure
    ///   manager for asynchronous execution, and this method returns the
    ///   `ProcedureId` along with `None` as the output.
    async fn submit_repartition_task(
        &self,
        table_id: TableId,
        table_name: TableName,
        Repartition {
            from_partition_exprs,
            into_partition_exprs,
        }: Repartition,
        wait: bool,
        timeout: Duration,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let context = self.create_context();

        let procedure = self
            .repartition_procedure_factory
            .create(
                &context,
                table_name,
                table_id,
                from_partition_exprs,
                into_partition_exprs,
                Some(timeout),
            )
            .context(CreateRepartitionProcedureSnafu)?;
        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));
        if wait {
            self.execute_procedure_and_wait(procedure_with_id).await
        } else {
            self.submit_procedure(procedure_with_id)
                .await
                .map(|p| (p, None))
        }
    }

    /// Submits and executes an alter table task.
    #[tracing::instrument(skip_all)]
    pub async fn submit_alter_table_task(
        &self,
        table_id: TableId,
        alter_table_task: AlterTableTask,
        ddl_options: DdlOptions,
    ) -> Result<(ProcedureId, Option<Output>)> {
        // make alter_table_task mutable so we can call .take() on its field
        let mut alter_table_task = alter_table_task;
        if let Some(Kind::Repartition(_)) = alter_table_task.alter_table.kind.as_ref()
            && let Kind::Repartition(repartition) =
                alter_table_task.alter_table.kind.take().unwrap()
        {
            let table_name = TableName::new(
                alter_table_task.alter_table.catalog_name,
                alter_table_task.alter_table.schema_name,
                alter_table_task.alter_table.table_name,
            );
            return self
                .submit_repartition_task(
                    table_id,
                    table_name,
                    repartition,
                    ddl_options.wait,
                    ddl_options.timeout,
                )
                .await;
        }

        let context = self.create_context();
        let procedure = AlterTableProcedure::new(table_id, alter_table_task, context)?;

        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));

        self.execute_procedure_and_wait(procedure_with_id).await
    }

    /// Submits and executes a create table task.
    #[tracing::instrument(skip_all)]
    pub async fn submit_create_table_task(
        &self,
        create_table_task: CreateTableTask,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let context = self.create_context();

        let procedure = CreateTableProcedure::new(create_table_task, context)?;

        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));

        self.execute_procedure_and_wait(procedure_with_id).await
    }

    /// Submits and executes a `[CreateViewTask]`.
    #[tracing::instrument(skip_all)]
    pub async fn submit_create_view_task(
        &self,
        create_view_task: CreateViewTask,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let context = self.create_context();

        let procedure = CreateViewProcedure::new(create_view_task, context);

        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));

        self.execute_procedure_and_wait(procedure_with_id).await
    }

    /// Submits and executes a create multiple logical table tasks.
    #[tracing::instrument(skip_all)]
    pub async fn submit_create_logical_table_tasks(
        &self,
        create_table_tasks: Vec<CreateTableTask>,
        physical_table_id: TableId,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let context = self.create_context();

        let procedure =
            CreateLogicalTablesProcedure::new(create_table_tasks, physical_table_id, context);

        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));

        self.execute_procedure_and_wait(procedure_with_id).await
    }

    /// Submits and executes alter multiple table tasks.
    #[tracing::instrument(skip_all)]
    pub async fn submit_alter_logical_table_tasks(
        &self,
        alter_table_tasks: Vec<AlterTableTask>,
        physical_table_id: TableId,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let context = self.create_context();

        let procedure =
            AlterLogicalTablesProcedure::new(alter_table_tasks, physical_table_id, context);

        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));

        self.execute_procedure_and_wait(procedure_with_id).await
    }

    /// Submits and executes a drop table task.
    #[tracing::instrument(skip_all)]
    pub async fn submit_drop_table_task(
        &self,
        drop_table_task: DropTableTask,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let context = self.create_context();

        let procedure = DropTableProcedure::new(drop_table_task, context);

        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));

        self.execute_procedure_and_wait(procedure_with_id).await
    }

    /// Submits and executes a create database task.
    #[tracing::instrument(skip_all)]
    pub async fn submit_create_database(
        &self,
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

        self.execute_procedure_and_wait(procedure_with_id).await
    }

    /// Submits and executes a drop table task.
    #[tracing::instrument(skip_all)]
    pub async fn submit_drop_database(
        &self,
        DropDatabaseTask {
            catalog,
            schema,
            drop_if_exists,
        }: DropDatabaseTask,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let context = self.create_context();
        let procedure = DropDatabaseProcedure::new(catalog, schema, drop_if_exists, context);
        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));

        self.execute_procedure_and_wait(procedure_with_id).await
    }

    pub async fn submit_alter_database(
        &self,
        alter_database_task: AlterDatabaseTask,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let context = self.create_context();
        let procedure = AlterDatabaseProcedure::new(alter_database_task, context)?;
        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));

        self.execute_procedure_and_wait(procedure_with_id).await
    }

    /// Submits and executes a create flow task.
    #[tracing::instrument(skip_all)]
    pub async fn submit_create_flow_task(
        &self,
        create_flow: CreateFlowTask,
        query_context: QueryContext,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let context = self.create_context();
        let procedure = CreateFlowProcedure::new(create_flow, query_context, context);
        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));

        self.execute_procedure_and_wait(procedure_with_id).await
    }

    /// Submits and executes a drop flow task.
    #[tracing::instrument(skip_all)]
    pub async fn submit_drop_flow_task(
        &self,
        drop_flow: DropFlowTask,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let context = self.create_context();
        let procedure = DropFlowProcedure::new(drop_flow, context);
        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));

        self.execute_procedure_and_wait(procedure_with_id).await
    }

    /// Submits and executes a drop view task.
    #[tracing::instrument(skip_all)]
    pub async fn submit_drop_view_task(
        &self,
        drop_view: DropViewTask,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let context = self.create_context();
        let procedure = DropViewProcedure::new(drop_view, context);
        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));

        self.execute_procedure_and_wait(procedure_with_id).await
    }

    /// Submits and executes a truncate table task.
    #[tracing::instrument(skip_all)]
    pub async fn submit_truncate_table_task(
        &self,
        truncate_table_task: TruncateTableTask,
        table_info_value: DeserializedValueWithBytes<TableInfoValue>,
        region_routes: Vec<RegionRoute>,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let context = self.create_context();
        let procedure = TruncateTableProcedure::new(
            truncate_table_task,
            table_info_value,
            region_routes,
            context,
        );

        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));

        self.execute_procedure_and_wait(procedure_with_id).await
    }

    /// Submits and executes a comment on task.
    #[tracing::instrument(skip_all)]
    pub async fn submit_comment_on_task(
        &self,
        comment_on_task: CommentOnTask,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let context = self.create_context();
        let procedure = CommentOnProcedure::new(comment_on_task, context);
        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));

        self.execute_procedure_and_wait(procedure_with_id).await
    }

    /// Executes a procedure and waits for the result.
    async fn execute_procedure_and_wait(
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

    /// Submits a procedure and returns the procedure id.
    async fn submit_procedure(&self, procedure_with_id: ProcedureWithId) -> Result<ProcedureId> {
        let procedure_id = procedure_with_id.id;
        let _ = self
            .procedure_manager
            .submit(procedure_with_id)
            .await
            .context(SubmitProcedureSnafu)?;

        Ok(procedure_id)
    }

    pub async fn submit_ddl_task(
        &self,
        ctx: &ExecutorContext,
        request: SubmitDdlTaskRequest,
    ) -> Result<SubmitDdlTaskResponse> {
        let span = ctx
            .tracing_context
            .as_ref()
            .map(TracingContext::from_w3c)
            .unwrap_or_else(TracingContext::from_current_span)
            .attach(tracing::info_span!("DdlManager::submit_ddl_task"));
        let ddl_options = DdlOptions {
            wait: request.wait,
            timeout: request.timeout,
        };
        async move {
            debug!("Submitting Ddl task: {:?}", request.task);
            match request.task {
                CreateTable(create_table_task) => {
                    handle_create_table_task(self, create_table_task).await
                }
                DropTable(drop_table_task) => handle_drop_table_task(self, drop_table_task).await,
                AlterTable(alter_table_task) => {
                    handle_alter_table_task(self, alter_table_task, ddl_options).await
                }
                TruncateTable(truncate_table_task) => {
                    handle_truncate_table_task(self, truncate_table_task).await
                }
                CreateLogicalTables(create_table_tasks) => {
                    handle_create_logical_table_tasks(self, create_table_tasks).await
                }
                AlterLogicalTables(alter_table_tasks) => {
                    handle_alter_logical_table_tasks(self, alter_table_tasks).await
                }
                DropLogicalTables(_) => todo!(),
                CreateDatabase(create_database_task) => {
                    handle_create_database_task(self, create_database_task).await
                }
                DropDatabase(drop_database_task) => {
                    handle_drop_database_task(self, drop_database_task).await
                }
                AlterDatabase(alter_database_task) => {
                    handle_alter_database_task(self, alter_database_task).await
                }
                CreateFlow(create_flow_task) => {
                    handle_create_flow_task(self, create_flow_task, request.query_context.into())
                        .await
                }
                DropFlow(drop_flow_task) => handle_drop_flow_task(self, drop_flow_task).await,
                CreateView(create_view_task) => {
                    handle_create_view_task(self, create_view_task).await
                }
                DropView(drop_view_task) => handle_drop_view_task(self, drop_view_task).await,
                CommentOn(comment_on_task) => handle_comment_on_task(self, comment_on_task).await,
                #[cfg(feature = "enterprise")]
                CreateTrigger(create_trigger_task) => {
                    handle_create_trigger_task(
                        self,
                        create_trigger_task,
                        request.query_context.into(),
                    )
                    .await
                }
                #[cfg(feature = "enterprise")]
                DropTrigger(drop_trigger_task) => {
                    handle_drop_trigger_task(self, drop_trigger_task, request.query_context.into())
                        .await
                }
            }
        }
        .trace(span)
        .await
    }
}

async fn handle_truncate_table_task(
    ddl_manager: &DdlManager,
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
        .submit_truncate_table_task(truncate_table_task, table_info_value, table_route)
        .await?;

    info!("Table: {table_id} is truncated via procedure_id {id:?}");

    Ok(SubmitDdlTaskResponse {
        key: id.to_string().into(),
        ..Default::default()
    })
}

async fn handle_alter_table_task(
    ddl_manager: &DdlManager,
    alter_table_task: AlterTableTask,
    ddl_options: DdlOptions,
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
        .submit_alter_table_task(table_id, alter_table_task, ddl_options)
        .await?;

    info!("Table: {table_id} is altered via procedure_id {id:?}");

    Ok(SubmitDdlTaskResponse {
        key: id.to_string().into(),
        ..Default::default()
    })
}

async fn handle_drop_table_task(
    ddl_manager: &DdlManager,
    drop_table_task: DropTableTask,
) -> Result<SubmitDdlTaskResponse> {
    let table_id = drop_table_task.table_id;
    let (id, _) = ddl_manager.submit_drop_table_task(drop_table_task).await?;

    info!("Table: {table_id} is dropped via procedure_id {id:?}");

    Ok(SubmitDdlTaskResponse {
        key: id.to_string().into(),
        ..Default::default()
    })
}

async fn handle_create_table_task(
    ddl_manager: &DdlManager,
    create_table_task: CreateTableTask,
) -> Result<SubmitDdlTaskResponse> {
    let (id, output) = ddl_manager
        .submit_create_table_task(create_table_task)
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
        .submit_create_logical_table_tasks(create_table_tasks, physical_table_id)
        .await?;

    info!(
        "{num_logical_tables} logical tables on physical table: {physical_table_id:?} is created via procedure_id {id:?}"
    );

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
    create_database_task: CreateDatabaseTask,
) -> Result<SubmitDdlTaskResponse> {
    let (id, _) = ddl_manager
        .submit_create_database(create_database_task.clone())
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
    drop_database_task: DropDatabaseTask,
) -> Result<SubmitDdlTaskResponse> {
    let (id, _) = ddl_manager
        .submit_drop_database(drop_database_task.clone())
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

async fn handle_alter_database_task(
    ddl_manager: &DdlManager,
    alter_database_task: AlterDatabaseTask,
) -> Result<SubmitDdlTaskResponse> {
    let (id, _) = ddl_manager
        .submit_alter_database(alter_database_task.clone())
        .await?;

    let procedure_id = id.to_string();
    info!(
        "Database {}.{} is altered via procedure_id {id:?}",
        alter_database_task.catalog(),
        alter_database_task.schema()
    );

    Ok(SubmitDdlTaskResponse {
        key: procedure_id.into(),
        ..Default::default()
    })
}

async fn handle_drop_flow_task(
    ddl_manager: &DdlManager,
    drop_flow_task: DropFlowTask,
) -> Result<SubmitDdlTaskResponse> {
    let (id, _) = ddl_manager
        .submit_drop_flow_task(drop_flow_task.clone())
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

#[cfg(feature = "enterprise")]
async fn handle_drop_trigger_task(
    ddl_manager: &DdlManager,
    drop_trigger_task: DropTriggerTask,
    query_context: QueryContext,
) -> Result<SubmitDdlTaskResponse> {
    let Some(m) = ddl_manager.trigger_ddl_manager.as_ref() else {
        use crate::error::UnsupportedSnafu;

        return UnsupportedSnafu {
            operation: "drop trigger",
        }
        .fail();
    };

    m.drop_trigger(
        drop_trigger_task,
        ddl_manager.procedure_manager.clone(),
        ddl_manager.ddl_context.clone(),
        query_context,
    )
    .await
}

async fn handle_drop_view_task(
    ddl_manager: &DdlManager,
    drop_view_task: DropViewTask,
) -> Result<SubmitDdlTaskResponse> {
    let (id, _) = ddl_manager
        .submit_drop_view_task(drop_view_task.clone())
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
    create_flow_task: CreateFlowTask,
    query_context: QueryContext,
) -> Result<SubmitDdlTaskResponse> {
    let (id, output) = ddl_manager
        .submit_create_flow_task(create_flow_task.clone(), query_context)
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
    if !create_flow_task.or_replace {
        info!(
            "Flow {}.{}({flow_id}) is created via procedure_id {id:?}",
            create_flow_task.catalog_name, create_flow_task.flow_name,
        );
    } else {
        info!(
            "Flow {}.{}({flow_id}) is replaced via procedure_id {id:?}",
            create_flow_task.catalog_name, create_flow_task.flow_name,
        );
    }

    Ok(SubmitDdlTaskResponse {
        key: procedure_id.into(),
        ..Default::default()
    })
}

#[cfg(feature = "enterprise")]
async fn handle_create_trigger_task(
    ddl_manager: &DdlManager,
    create_trigger_task: CreateTriggerTask,
    query_context: QueryContext,
) -> Result<SubmitDdlTaskResponse> {
    let Some(m) = ddl_manager.trigger_ddl_manager.as_ref() else {
        use crate::error::UnsupportedSnafu;

        return UnsupportedSnafu {
            operation: "create trigger",
        }
        .fail();
    };

    m.create_trigger(
        create_trigger_task,
        ddl_manager.procedure_manager.clone(),
        ddl_manager.ddl_context.clone(),
        query_context,
    )
    .await
}

async fn handle_alter_logical_table_tasks(
    ddl_manager: &DdlManager,
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
        .submit_alter_logical_table_tasks(alter_table_tasks, physical_table_id)
        .await?;

    info!(
        "{num_logical_tables} logical tables on physical table: {physical_table_id:?} is altered via procedure_id {id:?}"
    );

    let procedure_id = id.to_string();

    Ok(SubmitDdlTaskResponse {
        key: procedure_id.into(),
        ..Default::default()
    })
}

/// Handle the `[CreateViewTask]` and returns the DDL response when success.
async fn handle_create_view_task(
    ddl_manager: &DdlManager,
    create_view_task: CreateViewTask,
) -> Result<SubmitDdlTaskResponse> {
    let (id, output) = ddl_manager
        .submit_create_view_task(create_view_task)
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

async fn handle_comment_on_task(
    ddl_manager: &DdlManager,
    comment_on_task: CommentOnTask,
) -> Result<SubmitDdlTaskResponse> {
    let (id, _) = ddl_manager
        .submit_comment_on_task(comment_on_task.clone())
        .await?;

    let procedure_id = id.to_string();
    info!(
        "Comment on {}.{}.{} is updated via procedure_id {id:?}",
        comment_on_task.catalog_name, comment_on_task.schema_name, comment_on_task.object_name
    );

    Ok(SubmitDdlTaskResponse {
        key: procedure_id.into(),
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use common_error::ext::BoxedError;
    use common_procedure::local::LocalManager;
    use common_procedure::test_util::InMemoryPoisonStore;
    use common_procedure::{BoxedProcedure, ProcedureManagerRef};
    use store_api::storage::TableId;
    use table::table_name::TableName;

    use super::DdlManager;
    use crate::cache_invalidator::DummyCacheInvalidator;
    use crate::ddl::alter_table::AlterTableProcedure;
    use crate::ddl::create_table::CreateTableProcedure;
    use crate::ddl::drop_table::DropTableProcedure;
    use crate::ddl::flow_meta::FlowMetadataAllocator;
    use crate::ddl::table_meta::TableMetadataAllocator;
    use crate::ddl::truncate_table::TruncateTableProcedure;
    use crate::ddl::{DdlContext, NoopRegionFailureDetectorControl};
    use crate::ddl_manager::RepartitionProcedureFactory;
    use crate::key::TableMetadataManager;
    use crate::key::flow::FlowMetadataManager;
    use crate::kv_backend::memory::MemoryKvBackend;
    use crate::node_manager::{DatanodeManager, DatanodeRef, FlownodeManager, FlownodeRef};
    use crate::peer::Peer;
    use crate::region_keeper::MemoryRegionKeeper;
    use crate::region_registry::LeaderRegionRegistry;
    use crate::sequence::SequenceBuilder;
    use crate::state_store::KvStateStore;
    use crate::wal_provider::WalProvider;

    /// A dummy implemented [NodeManager].
    pub struct DummyDatanodeManager;

    #[async_trait::async_trait]
    impl DatanodeManager for DummyDatanodeManager {
        async fn datanode(&self, _datanode: &Peer) -> DatanodeRef {
            unimplemented!()
        }
    }

    #[async_trait::async_trait]
    impl FlownodeManager for DummyDatanodeManager {
        async fn flownode(&self, _node: &Peer) -> FlownodeRef {
            unimplemented!()
        }
    }

    struct DummyRepartitionProcedureFactory;

    #[async_trait::async_trait]
    impl RepartitionProcedureFactory for DummyRepartitionProcedureFactory {
        fn create(
            &self,
            _ddl_ctx: &DdlContext,
            _table_name: TableName,
            _table_id: TableId,
            _from_exprs: Vec<String>,
            _to_exprs: Vec<String>,
            _timeout: Option<Duration>,
        ) -> std::result::Result<BoxedProcedure, BoxedError> {
            unimplemented!()
        }

        fn register_loaders(
            &self,
            _ddl_ctx: &DdlContext,
            _procedure_manager: &ProcedureManagerRef,
        ) -> std::result::Result<(), BoxedError> {
            Ok(())
        }
    }

    #[test]
    fn test_try_new() {
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let table_metadata_manager = Arc::new(TableMetadataManager::new(kv_backend.clone()));
        let table_metadata_allocator = Arc::new(TableMetadataAllocator::new(
            Arc::new(SequenceBuilder::new("test", kv_backend.clone()).build()),
            Arc::new(WalProvider::default()),
        ));
        let flow_metadata_manager = Arc::new(FlowMetadataManager::new(kv_backend.clone()));
        let flow_metadata_allocator = Arc::new(FlowMetadataAllocator::with_noop_peer_allocator(
            Arc::new(SequenceBuilder::new("flow-test", kv_backend.clone()).build()),
        ));

        let state_store = Arc::new(KvStateStore::new(kv_backend.clone()));
        let poison_manager = Arc::new(InMemoryPoisonStore::default());
        let procedure_manager = Arc::new(LocalManager::new(
            Default::default(),
            state_store,
            poison_manager,
            None,
            None,
        ));

        let _ = DdlManager::try_new(
            DdlContext {
                node_manager: Arc::new(DummyDatanodeManager),
                cache_invalidator: Arc::new(DummyCacheInvalidator),
                table_metadata_manager,
                table_metadata_allocator,
                flow_metadata_manager,
                flow_metadata_allocator,
                memory_region_keeper: Arc::new(MemoryRegionKeeper::default()),
                leader_region_registry: Arc::new(LeaderRegionRegistry::default()),
                region_failure_detector_controller: Arc::new(NoopRegionFailureDetectorControl),
            },
            procedure_manager.clone(),
            Arc::new(DummyRepartitionProcedureFactory),
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
