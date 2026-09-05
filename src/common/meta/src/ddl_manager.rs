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
use api::v1::repartition::Source as PbRepartitionSource;
use common_error::ext::BoxedError;
use common_event_recorder::PersistentEventContext;
#[cfg(feature = "enterprise")]
use common_event_recorder::TriggerReason;
use common_procedure::{
    BoxedProcedure, BoxedProcedureLoader, Output, ProcedureContext, ProcedureId,
    ProcedureManagerRef, ProcedureWithId, watcher,
};
use common_telemetry::tracing_context::{FutureExt, TracingContext};
use common_telemetry::{debug, info, tracing};
use derive_builder::Builder;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::storage::{RegionId, TableId};
use table::table_name::TableName;

use crate::ddl::alter_database::AlterDatabaseProcedure;
use crate::ddl::alter_logical_tables::AlterLogicalTablesProcedure;
use crate::ddl::alter_table::{AlterTableProcedure, RegionRouteChanged, only_enables_skip_wal};
use crate::ddl::comment_on::CommentOnProcedure;
use crate::ddl::create_database::{CreateDatabaseMetadataCommitterRef, CreateDatabaseProcedure};
use crate::ddl::create_flow::{
    CreateFlowProcedure, FLOW_EXPERIMENTAL_ENABLE_INCREMENTAL_READ_KEY,
    FLOW_EXPERIMENTAL_ENABLE_INCREMENTAL_READ_SEQUENCE_RANGE,
};
use crate::ddl::create_logical_tables::CreateLogicalTablesProcedure;
use crate::ddl::create_table::CreateTableProcedure;
use crate::ddl::create_view::CreateViewProcedure;
use crate::ddl::drop_database::DropDatabaseProcedure;
use crate::ddl::drop_flow::DropFlowProcedure;
use crate::ddl::drop_table::DropTableProcedure;
use crate::ddl::drop_view::DropViewProcedure;
#[cfg(feature = "enterprise")]
use crate::ddl::purge_dropped_table::PurgeDroppedTableProcedure;
use crate::ddl::truncate_table::TruncateTableProcedure;
#[cfg(feature = "enterprise")]
use crate::ddl::undrop_table::UndropTableProcedure;
use crate::ddl::{DdlContext, utils};
use crate::error::{
    self, ConvertAlterTableRequestSnafu, CreateRepartitionProcedureSnafu, EmptyDdlTasksSnafu,
    PersistRepartitionGcRequirementSnafu, ProcedureOutputSnafu, RegisterProcedureLoaderSnafu,
    RegisterRepartitionProcedureLoaderSnafu, Result, SubmitProcedureSnafu, TableInfoNotFoundSnafu,
    TableNotFoundSnafu, TableRouteNotFoundSnafu, UnexpectedLogicalRouteTableSnafu,
    UnsupportedSnafu, WaitProcedureSnafu,
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
    DropTable, DropView, PurgeDroppedTable, TruncateTable, UndropTable,
};
#[cfg(feature = "enterprise")]
use crate::rpc::ddl::trigger::CreateTriggerTask;
#[cfg(feature = "enterprise")]
use crate::rpc::ddl::trigger::DropTriggerTask;
use crate::rpc::ddl::{
    AlterDatabaseTask, AlterTableTask, CommentOnTask, CreateDatabaseTask, CreateFlowTask,
    CreateTableTask, CreateViewTask, DropDatabaseTask, DropFlowTask, DropTableTask, DropViewTask,
    PurgeDroppedTableTask, QueryContext, SubmitDdlTaskRequest, SubmitDdlTaskResponse,
    TruncateTableTask, UndropTableTask,
};

const MAX_REGION_ROUTE_CHANGE_RETRIES: usize = 3;

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
    #[cfg(feature = "enterprise")]
    create_flow_handler: Option<CreateFlowHandlerRef>,
    #[cfg(feature = "enterprise")]
    drop_flow_handler: Option<DropFlowHandlerRef>,
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
        procedure_context: ProcedureContext,
    ) -> Result<SubmitDdlTaskResponse>;

    async fn drop_trigger(
        &self,
        drop_trigger_task: DropTriggerTask,
        procedure_manager: ProcedureManagerRef,
        ddl_context: DdlContext,
        query_context: QueryContext,
        procedure_context: ProcedureContext,
    ) -> Result<SubmitDdlTaskResponse>;

    fn as_any(&self) -> &dyn std::any::Any;
}

#[cfg(feature = "enterprise")]
pub type TriggerDdlManagerRef = Arc<dyn TriggerDdlManager>;

/// This trait is responsible for handling enterprise CREATE FLOW tasks.
#[cfg(feature = "enterprise")]
#[async_trait::async_trait]
pub trait CreateFlowHandler: Send + Sync {
    async fn create_flow(
        &self,
        create_flow_task: CreateFlowTask,
        procedure_manager: ProcedureManagerRef,
        ddl_context: DdlContext,
        query_context: QueryContext,
        procedure_context: ProcedureContext,
    ) -> Result<SubmitDdlTaskResponse>;
}

#[cfg(feature = "enterprise")]
pub type CreateFlowHandlerRef = Arc<dyn CreateFlowHandler>;

/// Hook for classifying and handling DROP FLOW requests.
#[async_trait::async_trait]
pub trait DropFlowHandler: Send + Sync {
    async fn drop_flow(
        &self,
        drop_flow_task: DropFlowTask,
        procedure_manager: ProcedureManagerRef,
        ddl_context: DdlContext,
        procedure_context: ProcedureContext,
    ) -> Result<SubmitDdlTaskResponse>;
}

pub type DropFlowHandlerRef = Arc<dyn DropFlowHandler>;

macro_rules! procedure_loader_entry {
    ($procedure:ident) => {
        (
            $procedure::TYPE_NAME,
            &|context: DdlContext| -> common_procedure::BoxedProcedureLoader {
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

pub enum RepartitionSource {
    Partitioned {
        exprs: Vec<String>,
        /// Full target partition columns to overwrite table metadata.
        ///
        /// `None` means the repartition keeps using the current table
        /// partition columns, so the procedure won't update
        /// `partition_key_indices`.
        target_partition_columns: Option<Vec<String>>,
    },
    Unpartitioned {
        partition_columns: Vec<String>,
    },
}

#[async_trait::async_trait]
pub trait RepartitionProcedureFactory: Send + Sync {
    #[allow(clippy::too_many_arguments)]
    fn create(
        &self,
        ddl_ctx: &DdlContext,
        table_name: TableName,
        table_id: TableId,
        source: RepartitionSource,
        to_exprs: Vec<String>,
        timeout: Option<Duration>,
    ) -> std::result::Result<BoxedProcedure, BoxedError>;

    fn register_loaders(
        &self,
        ddl_ctx: &DdlContext,
        procedure_manager: &ProcedureManagerRef,
    ) -> std::result::Result<(), BoxedError>;

    /// Persists the cluster-level GC requirement before submitting a repartition.
    async fn ensure_gc_requirement(&self) -> std::result::Result<(), BoxedError>;
}

/// The options for DDL tasks.
///
/// Note: These options may not be utilized by all procedures.
/// At present, they are specifically applied in `RepartitionProcedure`.
#[derive(Debug, Clone, Copy)]
pub struct DdlOptions {
    /// The timeout will be passed to the procedure.
    ///
    /// Note: Each procedure may implement its own timeout handling mechanism.
    pub timeout: Duration,
    /// The flag that controls whether to wait for the procedure to complete.
    ///
    /// If wait is `true`, the procedure will wait for completion(success or failure) and the result will be returned.
    /// Otherwise, the procedure will be submitted and return the [ProcedureId](common_procedure::ProcedureId) immediately.
    ///
    /// Note: The value of `wait` is independent of the `timeout` option. If a procedure ignores the `timeout` and `wait` is set to true, the operation returns until the procedure completes.
    pub wait: bool,
}

impl DdlManager {
    /// Returns a new [DdlManager].
    pub fn new(
        ddl_context: DdlContext,
        procedure_manager: ProcedureManagerRef,
        repartition_procedure_factory: RepartitionProcedureFactoryRef,
    ) -> Self {
        Self {
            ddl_context,
            procedure_manager,
            repartition_procedure_factory,
            #[cfg(feature = "enterprise")]
            trigger_ddl_manager: None,
            #[cfg(feature = "enterprise")]
            create_flow_handler: None,
            #[cfg(feature = "enterprise")]
            drop_flow_handler: None,
        }
    }

    #[cfg(feature = "enterprise")]
    pub fn with_drop_flow_handler(mut self, drop_flow_handler: DropFlowHandlerRef) -> Self {
        self.drop_flow_handler = Some(drop_flow_handler);
        self
    }

    #[cfg(feature = "enterprise")]
    pub fn with_trigger_ddl_manager(mut self, trigger_ddl_manager: TriggerDdlManagerRef) -> Self {
        self.trigger_ddl_manager = Some(trigger_ddl_manager);
        self
    }

    #[cfg(feature = "enterprise")]
    pub fn with_create_flow_handler(mut self, create_flow_handler: CreateFlowHandlerRef) -> Self {
        self.create_flow_handler = Some(create_flow_handler);
        self
    }

    pub fn with_create_database_metadata_committer(
        mut self,
        committer: CreateDatabaseMetadataCommitterRef,
    ) -> Self {
        self.ddl_context.create_database_metadata_committer = Some(committer);
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
            CreateDatabaseProcedure,
            AlterTableProcedure,
            AlterLogicalTablesProcedure,
            AlterDatabaseProcedure,
            DropTableProcedure,
            DropFlowProcedure,
            TruncateTableProcedure,
            DropDatabaseProcedure,
            DropViewProcedure,
            CommentOnProcedure
        );
        #[cfg(feature = "enterprise")]
        let loaders = {
            let soft_drop_loaders: Vec<(&str, &BoxedProcedureLoaderFactory)> =
                procedure_loader!(UndropTableProcedure, PurgeDroppedTableProcedure);
            loaders
                .into_iter()
                .chain(soft_drop_loaders)
                .collect::<Vec<_>>()
        };

        for (type_name, loader_factory) in loaders {
            let context = self.create_context();
            self.procedure_manager
                .register_loader(type_name, loader_factory(context))
                .context(RegisterProcedureLoaderSnafu { type_name })?;
        }

        #[cfg(feature = "enterprise")]
        {
            let type_name = PurgeDroppedTableProcedure::EXPIRED_TYPE_NAME;
            let context = self.create_context();
            self.procedure_manager
                .register_loader(
                    type_name,
                    Box::new(move |json: &str| {
                        PurgeDroppedTableProcedure::from_json(json, context.clone())
                            .map(|procedure| Box::new(procedure) as _)
                    }),
                )
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
        repartition: Repartition,
        wait: bool,
        timeout: Duration,
        procedure_context: ProcedureContext,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let context = self.create_context();

        let into_partition_exprs = repartition.into_partition_exprs;
        let source = repartition.source;

        let source = match source {
            Some(PbRepartitionSource::PartitionExprs(source)) => RepartitionSource::Partitioned {
                exprs: source.exprs,
                target_partition_columns: source
                    .target_partition_columns
                    .map(|columns| columns.columns),
            },
            Some(PbRepartitionSource::Unpartitioned(source)) => RepartitionSource::Unpartitioned {
                partition_columns: source.partition_columns,
            },
            None => {
                // Reads the deprecated field for backward compatibility with old persisted DDL tasks.
                #[allow(deprecated)]
                RepartitionSource::Partitioned {
                    exprs: repartition.from_partition_exprs,
                    target_partition_columns: None,
                }
            }
        };

        let procedure = self
            .repartition_procedure_factory
            .create(
                &context,
                table_name,
                table_id,
                source,
                into_partition_exprs,
                Some(timeout),
            )
            .context(CreateRepartitionProcedureSnafu)?;
        self.repartition_procedure_factory
            .ensure_gc_requirement()
            .await
            .context(PersistRepartitionGcRequirementSnafu)?;
        let procedure_with_id =
            ProcedureWithId::with_random_id(Box::new(procedure)).with_context(procedure_context);
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
        procedure_context: ProcedureContext,
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
                    procedure_context,
                )
                .await;
        }

        let lock_regions = alter_table_task
            .alter_table
            .kind
            .as_ref()
            .is_some_and(only_enables_skip_wal);

        let mut route_change_retries = 0;
        loop {
            // Hold the same physical region locks as migration while validating that
            // this route snapshot is still the one the procedure will mutate.
            let region_ids_to_lock = if lock_regions {
                let (_, route) = self
                    .table_metadata_manager()
                    .table_route_manager()
                    .get_physical_table_route(table_id)
                    .await?;
                route
                    .region_routes
                    .iter()
                    .map(|route| route.region.id)
                    .collect::<Vec<RegionId>>()
            } else {
                vec![]
            };

            let context = self.create_context();
            let procedure = AlterTableProcedure::new_with_region_locks(
                table_id,
                alter_table_task.clone(),
                region_ids_to_lock,
                context,
            )?;

            let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure))
                .with_context(procedure_context.clone());
            let result = self.execute_procedure_and_wait(procedure_with_id).await?;
            if result
                .1
                .as_ref()
                .is_some_and(|output| output.is::<RegionRouteChanged>())
            {
                if route_change_retries == MAX_REGION_ROUTE_CHANGE_RETRIES {
                    let source = error::UnexpectedSnafu {
                        err_msg: format!(
                            "Region route kept changing while altering table {table_id}"
                        ),
                    }
                    .build();
                    return Err(error::Error::retry_later(source));
                }
                route_change_retries += 1;
                continue;
            }
            return Ok(result);
        }
    }

    /// Submits and executes a create table task.
    #[tracing::instrument(skip_all)]
    pub async fn submit_create_table_task(
        &self,
        create_table_task: CreateTableTask,
        query_context: QueryContext,
        procedure_context: ProcedureContext,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let context = self.create_context();

        let procedure = CreateTableProcedure::new_with_query_context(
            create_table_task,
            query_context,
            context,
        )?;

        let procedure_with_id =
            ProcedureWithId::with_random_id(Box::new(procedure)).with_context(procedure_context);

        self.execute_procedure_and_wait(procedure_with_id).await
    }

    /// Submits and executes a `[CreateViewTask]`.
    #[tracing::instrument(skip_all)]
    pub async fn submit_create_view_task(
        &self,
        create_view_task: CreateViewTask,
        procedure_context: ProcedureContext,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let context = self.create_context();

        let procedure = CreateViewProcedure::new(create_view_task, context);

        let procedure_with_id =
            ProcedureWithId::with_random_id(Box::new(procedure)).with_context(procedure_context);

        self.execute_procedure_and_wait(procedure_with_id).await
    }

    /// Submits and executes a create multiple logical table tasks.
    #[tracing::instrument(skip_all)]
    pub async fn submit_create_logical_table_tasks(
        &self,
        create_table_tasks: Vec<CreateTableTask>,
        physical_table_id: TableId,
        procedure_context: ProcedureContext,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let context = self.create_context();

        let procedure =
            CreateLogicalTablesProcedure::new(create_table_tasks, physical_table_id, context);

        let procedure_with_id =
            ProcedureWithId::with_random_id(Box::new(procedure)).with_context(procedure_context);

        self.execute_procedure_and_wait(procedure_with_id).await
    }

    /// Submits and executes alter multiple table tasks.
    #[tracing::instrument(skip_all)]
    pub async fn submit_alter_logical_table_tasks(
        &self,
        alter_table_tasks: Vec<AlterTableTask>,
        physical_table_id: TableId,
        procedure_context: ProcedureContext,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let context = self.create_context();

        // Resolve the logical table ids up front: procedure locks are fixed
        // at submission, so `lock_key` cannot derive them during `Prepare`.
        let logical_table_ids = {
            let table_refs = alter_table_tasks
                .iter()
                .map(|task| task.table_ref())
                .collect::<Vec<_>>();
            utils::table_id::get_all_table_ids_by_names(
                self.table_metadata_manager().table_name_manager(),
                &table_refs,
            )
            .await?
        };

        let procedure = AlterLogicalTablesProcedure::new(
            alter_table_tasks,
            physical_table_id,
            logical_table_ids,
            context,
        );

        let procedure_with_id =
            ProcedureWithId::with_random_id(Box::new(procedure)).with_context(procedure_context);

        self.execute_procedure_and_wait(procedure_with_id).await
    }

    /// Submits and executes a drop table task.
    #[tracing::instrument(skip_all)]
    pub async fn submit_drop_table_task(
        &self,
        drop_table_task: DropTableTask,
        procedure_context: ProcedureContext,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let context = self.create_context();

        let procedure = DropTableProcedure::new(drop_table_task, context);

        let procedure_with_id =
            ProcedureWithId::with_random_id(Box::new(procedure)).with_context(procedure_context);

        self.execute_procedure_and_wait(procedure_with_id).await
    }

    /// Submits and executes an undrop table task.
    #[cfg_attr(not(feature = "enterprise"), allow(unused_variables))]
    #[tracing::instrument(skip_all)]
    pub async fn submit_undrop_table_task(
        &self,
        undrop_table_task: UndropTableTask,
        procedure_context: ProcedureContext,
    ) -> Result<(ProcedureId, Option<Output>)> {
        #[cfg(not(feature = "enterprise"))]
        {
            use crate::error::UnsupportedSnafu;

            return UnsupportedSnafu {
                operation: "undrop table is only available in GreptimeDB Enterprise Edition",
            }
            .fail();
        }
        #[cfg(feature = "enterprise")]
        {
            let context = self.create_context();
            let original_table_name = context
                .table_metadata_manager
                .get_dropped_table_by_id(undrop_table_task.table_id)
                .await?
                .with_context(|| TableNotFoundSnafu {
                    table_name: undrop_table_task.table_id.to_string(),
                })?
                .table_name;
            let procedure = UndropTableProcedure::new_with_original_table_name(
                undrop_table_task,
                context,
                Some(original_table_name),
            );
            let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure))
                .with_context(procedure_context);

            self.execute_procedure_and_wait(procedure_with_id).await
        }
    }

    /// Submits and executes a purge dropped table task.
    #[cfg_attr(not(feature = "enterprise"), allow(unused_variables))]
    #[tracing::instrument(skip_all)]
    pub async fn submit_purge_dropped_table_task(
        &self,
        purge_dropped_table_task: PurgeDroppedTableTask,
        procedure_context: ProcedureContext,
    ) -> Result<(ProcedureId, Option<Output>)> {
        #[cfg(not(feature = "enterprise"))]
        {
            use crate::error::UnsupportedSnafu;

            return UnsupportedSnafu {
                operation: "purge dropped table is only available in GreptimeDB Enterprise Edition",
            }
            .fail();
        }
        #[cfg(feature = "enterprise")]
        {
            let context = self.create_context();
            let procedure = PurgeDroppedTableProcedure::new(purge_dropped_table_task, context);
            let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure))
                .with_context(procedure_context);

            self.execute_procedure_and_wait(procedure_with_id).await
        }
    }

    /// Submits and executes a purge task that first rechecks the tombstone deadline.
    #[cfg_attr(not(feature = "enterprise"), allow(unused_variables))]
    #[tracing::instrument(skip_all)]
    pub async fn submit_expired_purge_dropped_table_task(
        &self,
        purge_dropped_table_task: PurgeDroppedTableTask,
    ) -> Result<(ProcedureId, Option<Output>)> {
        #[cfg(not(feature = "enterprise"))]
        {
            use crate::error::UnsupportedSnafu;

            return UnsupportedSnafu {
                operation:
                    "purge expired dropped table is only available in GreptimeDB Enterprise Edition",
            }
            .fail();
        }
        #[cfg(feature = "enterprise")]
        {
            let context = self.create_context();
            let procedure_context = ProcedureContext::from_event_context(
                PersistentEventContext::new(TriggerReason::ScheduledGc),
            );
            let procedure =
                PurgeDroppedTableProcedure::new_if_expired(purge_dropped_table_task, context);
            let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure))
                .with_context(procedure_context);

            self.execute_procedure_and_wait(procedure_with_id).await
        }
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
            creator,
        }: CreateDatabaseTask,
        procedure_context: ProcedureContext,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let context = self.create_context();
        let procedure = CreateDatabaseProcedure::new(
            catalog,
            schema,
            create_if_not_exists,
            options,
            creator,
            context,
        );
        let procedure_with_id =
            ProcedureWithId::with_random_id(Box::new(procedure)).with_context(procedure_context);

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
        procedure_context: ProcedureContext,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let context = self.create_context();
        let procedure = DropDatabaseProcedure::new(catalog, schema, drop_if_exists, context);
        let procedure_with_id =
            ProcedureWithId::with_random_id(Box::new(procedure)).with_context(procedure_context);

        self.execute_procedure_and_wait(procedure_with_id).await
    }

    pub async fn submit_alter_database(
        &self,
        alter_database_task: AlterDatabaseTask,
        procedure_context: ProcedureContext,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let context = self.create_context();
        let procedure = AlterDatabaseProcedure::new(alter_database_task, context)?;
        let procedure_with_id =
            ProcedureWithId::with_random_id(Box::new(procedure)).with_context(procedure_context);

        self.execute_procedure_and_wait(procedure_with_id).await
    }

    /// Submits and executes a create flow task.
    #[tracing::instrument(skip_all)]
    pub async fn submit_create_flow_task(
        &self,
        create_flow: CreateFlowTask,
        query_context: QueryContext,
        procedure_context: ProcedureContext,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let context = self.create_context();
        let procedure = CreateFlowProcedure::new(create_flow, query_context, context);
        let procedure_with_id =
            ProcedureWithId::with_random_id(Box::new(procedure)).with_context(procedure_context);

        self.execute_procedure_and_wait(procedure_with_id).await
    }

    /// Submits and executes a drop flow task.
    #[tracing::instrument(skip_all)]
    pub async fn submit_drop_flow_task(
        &self,
        drop_flow: DropFlowTask,
        procedure_context: ProcedureContext,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let context = self.create_context();
        let procedure = DropFlowProcedure::new(drop_flow, context);
        let procedure_with_id =
            ProcedureWithId::with_random_id(Box::new(procedure)).with_context(procedure_context);

        self.execute_procedure_and_wait(procedure_with_id).await
    }

    /// Submits and executes a drop view task.
    #[tracing::instrument(skip_all)]
    pub async fn submit_drop_view_task(
        &self,
        drop_view: DropViewTask,
        procedure_context: ProcedureContext,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let context = self.create_context();
        let procedure = DropViewProcedure::new(drop_view, context);
        let procedure_with_id =
            ProcedureWithId::with_random_id(Box::new(procedure)).with_context(procedure_context);

        self.execute_procedure_and_wait(procedure_with_id).await
    }

    /// Submits and executes a truncate table task.
    #[tracing::instrument(skip_all)]
    pub async fn submit_truncate_table_task(
        &self,
        truncate_table_task: TruncateTableTask,
        table_info_value: DeserializedValueWithBytes<TableInfoValue>,
        procedure_context: ProcedureContext,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let context = self.create_context();
        let procedure = TruncateTableProcedure::new(truncate_table_task, table_info_value, context);

        let procedure_with_id =
            ProcedureWithId::with_random_id(Box::new(procedure)).with_context(procedure_context);

        self.execute_procedure_and_wait(procedure_with_id).await
    }

    /// Submits and executes a comment on task.
    #[tracing::instrument(skip_all)]
    pub async fn submit_comment_on_task(
        &self,
        mut comment_on_task: CommentOnTask,
        procedure_context: ProcedureContext,
    ) -> Result<(ProcedureId, Option<Output>)> {
        let context = self.create_context();
        comment_on_task
            .enrich_object_id(
                context.table_metadata_manager.table_name_manager(),
                context.flow_metadata_manager.flow_name_manager(),
            )
            .await?;
        let procedure = CommentOnProcedure::new(comment_on_task, context);
        let procedure_with_id =
            ProcedureWithId::with_random_id(Box::new(procedure)).with_context(procedure_context);

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
        context: ExecutorContext,
        request: SubmitDdlTaskRequest,
    ) -> Result<SubmitDdlTaskResponse> {
        let ExecutorContext {
            tracing_context,
            query_context,
            actor,
            event_input,
        } = context;
        let query_context = query_context.context(UnsupportedSnafu {
            operation: "submit_ddl_task without query context",
        })?;
        let procedure_context = ProcedureContext {
            actor,
            event_context: event_input
                .map(|input| PersistentEventContext::from((input, query_context.protocol()))),
        };
        let span = tracing_context
            .as_ref()
            .map(TracingContext::from_w3c)
            .unwrap_or_else(TracingContext::from_current_span)
            .attach(tracing::info_span!("DdlManager::submit_ddl_task"));
        let SubmitDdlTaskRequest {
            wait,
            timeout,
            task,
        } = request;
        let ddl_options = DdlOptions { wait, timeout };
        async move {
            debug!("Submitting Ddl task: {:?}", task);
            match task {
                CreateTable(create_table_task) => {
                    handle_create_table_task(
                        self,
                        create_table_task,
                        query_context,
                        procedure_context,
                    )
                    .await
                }
                DropTable(drop_table_task) => {
                    handle_drop_table_task(self, drop_table_task, procedure_context).await
                }
                UndropTable(undrop_table_task) => {
                    handle_undrop_table_task(self, undrop_table_task, procedure_context).await
                }
                PurgeDroppedTable(purge_dropped_table_task) => {
                    handle_purge_dropped_table_task(
                        self,
                        purge_dropped_table_task,
                        procedure_context,
                    )
                    .await
                }
                AlterTable(alter_table_task) => {
                    handle_alter_table_task(self, alter_table_task, ddl_options, procedure_context)
                        .await
                }
                TruncateTable(truncate_table_task) => {
                    handle_truncate_table_task(self, truncate_table_task, procedure_context).await
                }
                CreateLogicalTables(create_table_tasks) => {
                    handle_create_logical_table_tasks(self, create_table_tasks, procedure_context)
                        .await
                }
                AlterLogicalTables(alter_table_tasks) => {
                    handle_alter_logical_table_tasks(self, alter_table_tasks, procedure_context)
                        .await
                }
                DropLogicalTables(_) => todo!(),
                CreateDatabase(create_database_task) => {
                    handle_create_database_task(self, create_database_task, procedure_context).await
                }
                DropDatabase(drop_database_task) => {
                    handle_drop_database_task(self, drop_database_task, procedure_context).await
                }
                AlterDatabase(alter_database_task) => {
                    handle_alter_database_task(self, alter_database_task, procedure_context).await
                }
                CreateFlow(create_flow_task) => {
                    handle_create_flow_task(
                        self,
                        create_flow_task,
                        query_context,
                        procedure_context,
                    )
                    .await
                }
                DropFlow(drop_flow_task) => {
                    #[cfg(feature = "enterprise")]
                    if let Some(handler) = self.drop_flow_handler.as_ref() {
                        return handler
                            .drop_flow(
                                drop_flow_task,
                                self.procedure_manager.clone(),
                                self.ddl_context.clone(),
                                procedure_context,
                            )
                            .await;
                    }
                    handle_drop_flow_task(self, drop_flow_task, procedure_context).await
                }
                CreateView(create_view_task) => {
                    handle_create_view_task(self, create_view_task, procedure_context).await
                }
                DropView(drop_view_task) => {
                    handle_drop_view_task(self, drop_view_task, procedure_context).await
                }
                CommentOn(comment_on_task) => {
                    handle_comment_on_task(self, comment_on_task, procedure_context).await
                }
                #[cfg(feature = "enterprise")]
                CreateTrigger(create_trigger_task) => {
                    handle_create_trigger_task(
                        self,
                        create_trigger_task,
                        query_context,
                        procedure_context,
                    )
                    .await
                }
                #[cfg(feature = "enterprise")]
                DropTrigger(drop_trigger_task) => {
                    handle_drop_trigger_task(
                        self,
                        drop_trigger_task,
                        query_context,
                        procedure_context,
                    )
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
    procedure_context: ProcedureContext,
) -> Result<SubmitDdlTaskResponse> {
    let table_id = truncate_table_task.table_id;
    let table_metadata_manager = &ddl_manager.table_metadata_manager();
    let table_ref = truncate_table_task.table_ref();

    let table_info_value = table_metadata_manager
        .table_info_manager()
        .get(table_id)
        .await?
        .with_context(|| TableInfoNotFoundSnafu {
            table: table_ref.to_string(),
        })?;
    let physical_table_id = table_metadata_manager
        .table_route_manager()
        .get_physical_table_id(table_id)
        .await?;
    ensure!(
        physical_table_id == table_id,
        error::UnexpectedSnafu {
            err_msg: "Truncate table is only supported for physical tables."
        }
    );

    let (id, _) = ddl_manager
        .submit_truncate_table_task(truncate_table_task, table_info_value, procedure_context)
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
    procedure_context: ProcedureContext,
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
    // Classify before the route guard: a mixed annotation batch must surface
    // its own error here, not a misleading "non-physical route" one. Families
    // that only rewrite the logical table's metadata may target logical tables.
    let annotation_family = match alter_table_task.alter_table.kind.as_ref() {
        Some(kind) => common_grpc_expr::annotation_alter_family(kind)
            .context(ConvertAlterTableRequestSnafu)?,
        None => None,
    };
    ensure!(
        table_route_value.is_physical()
            || annotation_family.is_some_and(|family| family.allows_logical_tables()),
        UnexpectedLogicalRouteTableSnafu {
            err_msg: format!("{:?} is a non-physical TableRouteValue.", table_ref),
        }
    );

    let (id, _) = ddl_manager
        .submit_alter_table_task(table_id, alter_table_task, procedure_context, ddl_options)
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
    procedure_context: ProcedureContext,
) -> Result<SubmitDdlTaskResponse> {
    let table_id = drop_table_task.table_id;
    let (id, _) = ddl_manager
        .submit_drop_table_task(drop_table_task, procedure_context)
        .await?;

    info!("Table: {table_id} is dropped via procedure_id {id:?}");

    Ok(SubmitDdlTaskResponse {
        key: id.to_string().into(),
        ..Default::default()
    })
}

async fn handle_undrop_table_task(
    ddl_manager: &DdlManager,
    undrop_table_task: UndropTableTask,
    procedure_context: ProcedureContext,
) -> Result<SubmitDdlTaskResponse> {
    let table_id = undrop_table_task.table_id;
    let (id, _) = ddl_manager
        .submit_undrop_table_task(undrop_table_task, procedure_context)
        .await?;

    info!("Table: {table_id} is undropped via procedure_id {id:?}");

    Ok(SubmitDdlTaskResponse {
        key: id.to_string().into(),
        ..Default::default()
    })
}

async fn handle_purge_dropped_table_task(
    ddl_manager: &DdlManager,
    purge_dropped_table_task: PurgeDroppedTableTask,
    procedure_context: ProcedureContext,
) -> Result<SubmitDdlTaskResponse> {
    let (id, _) = ddl_manager
        .submit_purge_dropped_table_task(purge_dropped_table_task, procedure_context)
        .await?;

    info!("Dropped table is purged via procedure_id {id:?}");

    Ok(SubmitDdlTaskResponse {
        key: id.to_string().into(),
        ..Default::default()
    })
}

async fn handle_create_table_task(
    ddl_manager: &DdlManager,
    create_table_task: CreateTableTask,
    query_context: QueryContext,
    procedure_context: ProcedureContext,
) -> Result<SubmitDdlTaskResponse> {
    let (id, output) = ddl_manager
        .submit_create_table_task(create_table_task, query_context, procedure_context)
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
    procedure_context: ProcedureContext,
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
        .submit_create_logical_table_tasks(create_table_tasks, physical_table_id, procedure_context)
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
    procedure_context: ProcedureContext,
) -> Result<SubmitDdlTaskResponse> {
    let catalog = create_database_task.catalog.clone();
    let schema = create_database_task.schema.clone();
    let (id, _) = ddl_manager
        .submit_create_database(create_database_task, procedure_context)
        .await?;

    let procedure_id = id.to_string();
    info!(
        "Database {}.{} is created via procedure_id {id:?}",
        catalog, schema
    );

    Ok(SubmitDdlTaskResponse {
        key: procedure_id.into(),
        ..Default::default()
    })
}

async fn handle_drop_database_task(
    ddl_manager: &DdlManager,
    drop_database_task: DropDatabaseTask,
    procedure_context: ProcedureContext,
) -> Result<SubmitDdlTaskResponse> {
    let (id, _) = ddl_manager
        .submit_drop_database(drop_database_task.clone(), procedure_context)
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
    procedure_context: ProcedureContext,
) -> Result<SubmitDdlTaskResponse> {
    let (id, _) = ddl_manager
        .submit_alter_database(alter_database_task.clone(), procedure_context)
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
    procedure_context: ProcedureContext,
) -> Result<SubmitDdlTaskResponse> {
    let (id, _) = ddl_manager
        .submit_drop_flow_task(drop_flow_task.clone(), procedure_context)
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
    procedure_context: ProcedureContext,
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
        procedure_context,
    )
    .await
}

async fn handle_drop_view_task(
    ddl_manager: &DdlManager,
    drop_view_task: DropViewTask,
    procedure_context: ProcedureContext,
) -> Result<SubmitDdlTaskResponse> {
    let (id, _) = ddl_manager
        .submit_drop_view_task(drop_view_task.clone(), procedure_context)
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
    procedure_context: ProcedureContext,
) -> Result<SubmitDdlTaskResponse> {
    if create_flow_task
        .flow_options
        .get(FLOW_EXPERIMENTAL_ENABLE_INCREMENTAL_READ_KEY)
        .is_some_and(|value| value == FLOW_EXPERIMENTAL_ENABLE_INCREMENTAL_READ_SEQUENCE_RANGE)
    {
        return error::UnexpectedSnafu {
            err_msg: format!(
                "reserved flow option value for {FLOW_EXPERIMENTAL_ENABLE_INCREMENTAL_READ_KEY} is internal"
            ),
        }
        .fail();
    }

    #[cfg(feature = "enterprise")]
    if let Some(handler) = ddl_manager.create_flow_handler.as_ref() {
        return handler
            .create_flow(
                create_flow_task,
                ddl_manager.procedure_manager.clone(),
                ddl_manager.ddl_context.clone(),
                query_context,
                procedure_context,
            )
            .await;
    }

    let (id, output) = ddl_manager
        .submit_create_flow_task(create_flow_task.clone(), query_context, procedure_context)
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
    procedure_context: ProcedureContext,
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
        procedure_context,
    )
    .await
}

async fn handle_alter_logical_table_tasks(
    ddl_manager: &DdlManager,
    alter_table_tasks: Vec<AlterTableTask>,
    procedure_context: ProcedureContext,
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
        .submit_alter_logical_table_tasks(alter_table_tasks, physical_table_id, procedure_context)
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
    procedure_context: ProcedureContext,
) -> Result<SubmitDdlTaskResponse> {
    let (id, output) = ddl_manager
        .submit_create_view_task(create_view_task, procedure_context)
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
    procedure_context: ProcedureContext,
) -> Result<SubmitDdlTaskResponse> {
    let (id, _) = ddl_manager
        .submit_comment_on_task(comment_on_task.clone(), procedure_context)
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
    #[cfg(feature = "enterprise")]
    use std::sync::Mutex;
    use std::time::Duration;

    #[cfg(feature = "enterprise")]
    use common_base::protocol::Channel;
    use common_error::ext::BoxedError;
    #[cfg(feature = "enterprise")]
    use common_error::ext::ErrorExt;
    #[cfg(feature = "enterprise")]
    use common_error::status_code::StatusCode;
    #[cfg(feature = "enterprise")]
    use common_event_recorder::{PersistentEventContext, ProcedureEventInput, TriggerReason};
    use common_procedure::local::LocalManager;
    use common_procedure::test_util::InMemoryPoisonStore;
    use common_procedure::{
        BoxedProcedure, ProcedureContext, ProcedureManager, ProcedureManagerRef,
    };
    use store_api::storage::TableId;
    use table::table_name::TableName;

    use super::DdlManager;
    use crate::cache_invalidator::DummyCacheInvalidator;
    use crate::ddl::alter_table::AlterTableProcedure;
    use crate::ddl::create_database::{
        AtomicCreateOutcome, CreateDatabaseMetadataCommitter, CreateDatabaseProcedure,
    };
    #[cfg(feature = "enterprise")]
    use crate::ddl::create_flow::{
        CreateFlowProcedure, FLOW_EXPERIMENTAL_ENABLE_INCREMENTAL_READ_KEY,
        FLOW_EXPERIMENTAL_ENABLE_INCREMENTAL_READ_SEQUENCE_RANGE,
    };
    use crate::ddl::create_table::CreateTableProcedure;
    use crate::ddl::drop_table::DropTableProcedure;
    use crate::ddl::flow_meta::FlowMetadataAllocator;
    use crate::ddl::table_meta::TableMetadataAllocator;
    use crate::ddl::truncate_table::TruncateTableProcedure;
    use crate::ddl::{DdlContext, NoopRegionFailureDetectorControl};
    use crate::ddl_manager::{RepartitionProcedureFactory, RepartitionSource};
    use crate::key::TableMetadataManager;
    use crate::key::flow::FlowMetadataManager;
    use crate::kv_backend::memory::MemoryKvBackend;
    use crate::node_manager::{DatanodeManager, DatanodeRef, FlownodeManager, FlownodeRef};
    use crate::peer::Peer;
    use crate::procedure_executor::ExecutorContext;
    use crate::region_keeper::MemoryRegionKeeper;
    use crate::region_registry::LeaderRegionRegistry;
    #[cfg(feature = "enterprise")]
    use crate::rpc::ddl::trigger::{CreateTriggerTask, DropTriggerTask};
    #[cfg(feature = "enterprise")]
    use crate::rpc::ddl::{
        CreateFlowTask, DdlTask, QueryContext, SubmitDdlTaskRequest, SubmitDdlTaskResponse,
    };
    use crate::rpc::ddl::{CreatorGrantIntent, UndropTableTask};
    #[cfg(not(feature = "enterprise"))]
    use crate::rpc::ddl::{DdlTask, PurgeDroppedTableTask, QueryContext, SubmitDdlTaskRequest};
    use crate::sequence::SequenceBuilder;
    use crate::state_store::KvStateStore;
    use crate::test_util::{MockDatanodeManager, new_ddl_context};
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
            _source: RepartitionSource,
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

        async fn ensure_gc_requirement(&self) -> std::result::Result<(), BoxedError> {
            Ok(())
        }
    }

    struct LoaderCommitter;

    #[async_trait::async_trait]
    impl CreateDatabaseMetadataCommitter for LoaderCommitter {
        async fn commit(
            &self,
            _catalog: &str,
            _schema: &str,
            _value: &crate::key::schema_name::SchemaNameValue,
            _creator: &CreatorGrantIntent,
        ) -> std::result::Result<AtomicCreateOutcome, BoxedError> {
            unreachable!()
        }
    }

    #[cfg(feature = "enterprise")]
    #[derive(Default)]
    struct RecordingTriggerDdlManager {
        procedure_contexts: Mutex<Vec<ProcedureContext>>,
    }

    #[cfg(feature = "enterprise")]
    #[derive(Default)]
    struct RecordingCreateFlowHandler {
        tasks: Mutex<Vec<CreateFlowTask>>,
        contexts: Mutex<Vec<(QueryContext, ProcedureContext)>>,
        response: Mutex<Option<SubmitDdlTaskResponse>>,
        fail: bool,
    }

    #[cfg(feature = "enterprise")]
    #[async_trait::async_trait]
    impl super::CreateFlowHandler for RecordingCreateFlowHandler {
        async fn create_flow(
            &self,
            create_flow_task: CreateFlowTask,
            _procedure_manager: ProcedureManagerRef,
            _ddl_context: DdlContext,
            query_context: QueryContext,
            procedure_context: ProcedureContext,
        ) -> crate::error::Result<SubmitDdlTaskResponse> {
            self.tasks.lock().unwrap().push(create_flow_task);
            self.contexts
                .lock()
                .unwrap()
                .push((query_context, procedure_context));
            if self.fail {
                return crate::error::UnsupportedSnafu {
                    operation: "test create flow handler",
                }
                .fail();
            }
            Ok(self.response.lock().unwrap().take().unwrap_or_default())
        }
    }

    #[cfg(feature = "enterprise")]
    fn test_create_flow_task() -> CreateFlowTask {
        CreateFlowTask {
            catalog_name: "greptime".to_string(),
            flow_name: "test_flow".to_string(),
            source_table_names: vec![],
            sink_table_name: TableName::new("greptime", "public", "sink"),
            or_replace: false,
            create_if_not_exists: false,
            expire_after: None,
            eval_interval_secs: None,
            eval_offset_secs: None,
            comment: String::new(),
            sql: "select 1".to_string(),
            flow_options: Default::default(),
            eval_schedule: None,
        }
    }

    #[cfg(feature = "enterprise")]
    #[async_trait::async_trait]
    impl super::TriggerDdlManager for RecordingTriggerDdlManager {
        async fn create_trigger(
            &self,
            _create_trigger_task: CreateTriggerTask,
            _procedure_manager: ProcedureManagerRef,
            _ddl_context: DdlContext,
            _query_context: QueryContext,
            procedure_context: ProcedureContext,
        ) -> crate::error::Result<crate::rpc::ddl::SubmitDdlTaskResponse> {
            self.procedure_contexts
                .lock()
                .unwrap()
                .push(procedure_context);
            Ok(Default::default())
        }

        async fn drop_trigger(
            &self,
            _drop_trigger_task: DropTriggerTask,
            _procedure_manager: ProcedureManagerRef,
            _ddl_context: DdlContext,
            _query_context: QueryContext,
            procedure_context: ProcedureContext,
        ) -> crate::error::Result<crate::rpc::ddl::SubmitDdlTaskResponse> {
            self.procedure_contexts
                .lock()
                .unwrap()
                .push(procedure_context);
            Ok(Default::default())
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    #[test]
    fn test_generic_loader_captures_configured_committer() {
        let mut context = new_ddl_context(Arc::new(MockDatanodeManager::new(())));
        let committer = Arc::new(LoaderCommitter);
        context.create_database_metadata_committer = Some(committer.clone());
        let loader = procedure_loader!(CreateDatabaseProcedure)[0].1(context);
        assert_eq!(Arc::strong_count(&committer), 2);

        let loaded = loader(
            r#"{
                "state":"CreateMetadata",
                "catalog":"greptime",
                "schema":"metrics",
                "create_if_not_exists":false,
                "options":{},
                "creator":{"username":"alice","created_at_ns":42}
            }"#,
        )
        .unwrap();

        assert_eq!(loaded.type_name(), "metasrv-procedure::CreateDatabase");
        assert_eq!(Arc::strong_count(&committer), 3);
        drop(loaded);
        assert_eq!(Arc::strong_count(&committer), 2);
    }

    #[test]
    fn test_register_loaders() {
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

        let ddl_manager = DdlManager::new(
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
                soft_drop_enabled: false,
                soft_drop_retention: None,
                create_database_metadata_committer: None,
            },
            procedure_manager.clone(),
            Arc::new(DummyRepartitionProcedureFactory),
        );
        ddl_manager.register_loaders().unwrap();

        let expected_loaders = vec![
            CreateTableProcedure::TYPE_NAME,
            AlterTableProcedure::TYPE_NAME,
            DropTableProcedure::TYPE_NAME,
            TruncateTableProcedure::TYPE_NAME,
        ];

        for loader in expected_loaders {
            assert!(procedure_manager.contains_loader(loader));
        }

        let soft_drop_loaders = [
            "metasrv-procedure::UndropTable",
            "metasrv-procedure::PurgeDroppedTable",
            "metasrv-procedure::PurgeExpiredDroppedTable",
        ];
        for loader in soft_drop_loaders {
            assert_eq!(
                cfg!(feature = "enterprise"),
                procedure_manager.contains_loader(loader)
            );
        }
    }

    fn build_soft_drop_test_ddl_manager() -> DdlManager {
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

        DdlManager::new(
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
                soft_drop_enabled: true,
                soft_drop_retention: Some(std::time::Duration::from_secs(1)),
                create_database_metadata_committer: None,
            },
            procedure_manager,
            Arc::new(DummyRepartitionProcedureFactory),
        )
    }

    #[cfg(feature = "enterprise")]
    #[tokio::test]
    async fn test_reserved_sequence_range_is_rejected_before_enterprise_handler() {
        let handler = Arc::new(RecordingCreateFlowHandler::default());
        let ddl_manager =
            build_soft_drop_test_ddl_manager().with_create_flow_handler(handler.clone());
        let mut task = test_create_flow_task();
        task.flow_options.insert(
            FLOW_EXPERIMENTAL_ENABLE_INCREMENTAL_READ_KEY.to_string(),
            FLOW_EXPERIMENTAL_ENABLE_INCREMENTAL_READ_SEQUENCE_RANGE.to_string(),
        );

        let result = ddl_manager
            .submit_ddl_task(
                ExecutorContext {
                    query_context: Some(QueryContext::default()),
                    ..Default::default()
                },
                SubmitDdlTaskRequest::new(DdlTask::new_create_flow(task)),
            )
            .await;

        assert!(result.is_err());
        assert!(handler.tasks.lock().unwrap().is_empty());
    }

    #[cfg(feature = "enterprise")]
    #[tokio::test]
    async fn test_create_flow_handler_dispatches_without_procedure() {
        let response = SubmitDdlTaskResponse {
            key: b"enterprise".to_vec(),
            ..Default::default()
        };
        let handler = Arc::new(RecordingCreateFlowHandler {
            response: Mutex::new(Some(response)),
            ..Default::default()
        });
        let ddl_manager =
            build_soft_drop_test_ddl_manager().with_create_flow_handler(handler.clone());
        let procedure_context = ProcedureContext {
            actor: Some("test-user".to_string()),
            event_context: Some(
                PersistentEventContext::new(TriggerReason::Manual).with_protocol("mysql"),
            ),
        };
        let query_context = QueryContext {
            channel: Channel::Mysql as u8,
            ..Default::default()
        };
        let actual = ddl_manager
            .submit_ddl_task(
                ExecutorContext {
                    query_context: Some(query_context.clone()),
                    actor: procedure_context.actor.clone(),
                    event_input: Some(ProcedureEventInput::new(TriggerReason::Manual)),
                    ..Default::default()
                },
                SubmitDdlTaskRequest::new(DdlTask::new_create_flow(test_create_flow_task())),
            )
            .await
            .unwrap();
        assert_eq!(actual.key, b"enterprise");
        assert_eq!(handler.tasks.lock().unwrap().len(), 1);
        assert_eq!(
            handler.contexts.lock().unwrap().as_slice(),
            &[(query_context, procedure_context)]
        );
    }

    #[cfg(feature = "enterprise")]
    #[tokio::test]
    async fn test_create_flow_handler_error_does_not_fallback() {
        let handler = Arc::new(RecordingCreateFlowHandler {
            fail: true,
            ..Default::default()
        });
        let ddl_manager =
            build_soft_drop_test_ddl_manager().with_create_flow_handler(handler.clone());
        let err = ddl_manager
            .submit_ddl_task(
                ExecutorContext {
                    query_context: Some(QueryContext::default()),
                    ..Default::default()
                },
                SubmitDdlTaskRequest::new(DdlTask::new_create_flow(test_create_flow_task())),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("test create flow handler"));
        assert_eq!(handler.tasks.lock().unwrap().len(), 1);
    }

    #[cfg(feature = "enterprise")]
    #[tokio::test]
    async fn test_create_flow_without_handler_uses_ordinary_procedure() {
        let ddl_manager = build_soft_drop_test_ddl_manager();
        ddl_manager.procedure_manager.start().await.unwrap();
        let err = ddl_manager
            .submit_ddl_task(
                ExecutorContext {
                    query_context: Some(QueryContext::default()),
                    ..Default::default()
                },
                SubmitDdlTaskRequest::new(DdlTask::new_create_flow(test_create_flow_task())),
            )
            .await
            .unwrap_err();
        assert!(!err.to_string().contains("test create flow handler"));
        assert_eq!(
            ddl_manager
                .procedure_manager
                .list_procedures()
                .await
                .unwrap()
                .iter()
                .filter(|procedure| procedure.type_name == CreateFlowProcedure::TYPE_NAME)
                .count(),
            1
        );
    }

    #[cfg(feature = "enterprise")]
    #[tokio::test]
    async fn test_trigger_ddl_forwards_procedure_context() {
        let trigger_ddl_manager = Arc::new(RecordingTriggerDdlManager::default());
        let ddl_manager = build_soft_drop_test_ddl_manager()
            .with_trigger_ddl_manager(trigger_ddl_manager.clone());
        let procedure_context = ProcedureContext {
            actor: Some("test-user".to_string()),
            event_context: Some(
                PersistentEventContext::new(TriggerReason::Manual).with_protocol("mysql"),
            ),
        };
        let executor_context = || ExecutorContext {
            query_context: Some(QueryContext {
                channel: Channel::Mysql as u8,
                ..Default::default()
            }),
            actor: Some("test-user".to_string()),
            event_input: Some(ProcedureEventInput::new(TriggerReason::Manual)),
            ..Default::default()
        };

        ddl_manager
            .submit_ddl_task(
                executor_context(),
                SubmitDdlTaskRequest::new(DdlTask::CreateTrigger(CreateTriggerTask {
                    catalog_name: "greptime".to_string(),
                    trigger_name: "test_trigger".to_string(),
                    if_not_exists: false,
                    sql: "SELECT 1".to_string(),
                    channels: vec![],
                    labels: Default::default(),
                    annotations: Default::default(),
                    interval: Duration::from_secs(1),
                    raw_interval_expr: None,
                    r#for: None,
                    for_raw_expr: None,
                    keep_firing_for: None,
                    keep_firing_for_raw_expr: None,
                })),
            )
            .await
            .unwrap();
        ddl_manager
            .submit_ddl_task(
                executor_context(),
                SubmitDdlTaskRequest::new(DdlTask::DropTrigger(DropTriggerTask {
                    catalog_name: "greptime".to_string(),
                    trigger_name: "test_trigger".to_string(),
                    drop_if_exists: false,
                })),
            )
            .await
            .unwrap();

        assert_eq!(
            *trigger_ddl_manager.procedure_contexts.lock().unwrap(),
            vec![procedure_context.clone(), procedure_context]
        );
    }

    #[cfg(feature = "enterprise")]
    #[tokio::test]
    async fn test_submit_undrop_missing_tombstone_returns_table_not_found_directly() {
        let ddl_manager = build_soft_drop_test_ddl_manager();

        let err = ddl_manager
            .submit_undrop_table_task(
                UndropTableTask { table_id: 1024 },
                ProcedureContext::default(),
            )
            .await
            .unwrap_err();

        assert_eq!(err.status_code(), StatusCode::TableNotFound);
        assert!(matches!(err, crate::error::Error::TableNotFound { .. }));
    }

    #[cfg(not(feature = "enterprise"))]
    #[tokio::test]
    async fn test_submit_undrop_and_purge_rejected_in_non_enterprise_build() {
        let ddl_manager = build_soft_drop_test_ddl_manager();

        let err = ddl_manager
            .submit_undrop_table_task(
                UndropTableTask { table_id: 1024 },
                ProcedureContext::default(),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, crate::error::Error::Unsupported { .. }));

        let err = ddl_manager
            .submit_purge_dropped_table_task(
                PurgeDroppedTableTask { table_id: 1024 },
                ProcedureContext::default(),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, crate::error::Error::Unsupported { .. }));

        let err = ddl_manager
            .submit_expired_purge_dropped_table_task(PurgeDroppedTableTask { table_id: 1024 })
            .await
            .unwrap_err();
        assert!(matches!(err, crate::error::Error::Unsupported { .. }));

        for task in [
            DdlTask::UndropTable(UndropTableTask { table_id: 1024 }),
            DdlTask::PurgeDroppedTable(PurgeDroppedTableTask { table_id: 1024 }),
        ] {
            let err = ddl_manager
                .submit_ddl_task(
                    ExecutorContext {
                        query_context: Some(QueryContext::default()),
                        ..Default::default()
                    },
                    SubmitDdlTaskRequest::new(task),
                )
                .await
                .unwrap_err();
            assert!(matches!(err, crate::error::Error::Unsupported { .. }));
        }
    }

    async fn ddl_manager_with_context(ddl_context: DdlContext) -> DdlManager {
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let state_store = Arc::new(KvStateStore::new(kv_backend.clone()));
        let poison_manager = Arc::new(InMemoryPoisonStore::default());
        let procedure_manager = Arc::new(LocalManager::new(
            Default::default(),
            state_store,
            poison_manager,
            None,
            None,
        ));
        procedure_manager.start().await.unwrap();
        let ddl_manager = DdlManager::new(
            ddl_context,
            procedure_manager,
            Arc::new(DummyRepartitionProcedureFactory),
        );
        ddl_manager.register_loaders().unwrap();
        ddl_manager
    }

    fn set_options_expr(table_name: &str, options: &[(&str, &str)]) -> api::v1::AlterTableExpr {
        api::v1::AlterTableExpr {
            catalog_name: common_catalog::consts::DEFAULT_CATALOG_NAME.to_string(),
            schema_name: common_catalog::consts::DEFAULT_SCHEMA_NAME.to_string(),
            table_name: table_name.to_string(),
            kind: Some(api::v1::alter_table_expr::Kind::SetTableOptions(
                api::v1::SetTableOptions {
                    table_options: options
                        .iter()
                        .map(|(key, value)| api::v1::Option {
                            key: key.to_string(),
                            value: value.to_string(),
                        })
                        .collect(),
                },
            )),
        }
    }

    #[tokio::test]
    async fn test_logical_table_annotation_alter_routing() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        let node_manager = Arc::new(crate::test_util::MockDatanodeManager::new(
            crate::ddl::test_util::datanode_handler::DatanodeWatcher::new(tx),
        ));
        let ddl_context = crate::test_util::new_ddl_context(node_manager);
        let phy_id = crate::ddl::test_util::create_physical_table(&ddl_context, "phy").await;
        let logical_id =
            crate::ddl::test_util::create_logical_table(ddl_context.clone(), phy_id, "logical")
                .await;
        let ddl_manager = ddl_manager_with_context(ddl_context.clone()).await;

        // A semantic alter on a logical table passes the route guard, updates
        // only the logical table's metadata, and dispatches nothing.
        ddl_manager
            .submit_ddl_task(
                ExecutorContext {
                    query_context: Some(QueryContext::default()),
                    ..Default::default()
                },
                SubmitDdlTaskRequest::new(DdlTask::new_alter_table(set_options_expr(
                    "logical",
                    &[("greptime.semantic.signal_type", "metric")],
                ))),
            )
            .await
            .unwrap();
        rx.try_recv().unwrap_err();
        let table_info = ddl_manager
            .table_metadata_manager()
            .table_info_manager()
            .get(logical_id)
            .await
            .unwrap()
            .unwrap()
            .into_inner()
            .table_info;
        assert_eq!(
            table_info
                .meta
                .options
                .extra_options
                .get("greptime.semantic.signal_type"),
            Some(&"metric".to_string())
        );

        // A mixed batch fails with its own error, not the route guard's.
        let err = ddl_manager
            .submit_ddl_task(
                ExecutorContext {
                    query_context: Some(QueryContext::default()),
                    ..Default::default()
                },
                SubmitDdlTaskRequest::new(DdlTask::new_alter_table(set_options_expr(
                    "logical",
                    &[("greptime.semantic.source", "prometheus"), ("ttl", "7d")],
                ))),
            )
            .await
            .unwrap_err();
        let msg = common_error::ext::ErrorExt::output_msg(&err);
        assert!(msg.contains("must be altered separately"), "{msg}");

        // The repartition hint drives physical repartitioning; on a logical
        // route it stays rejected by the guard.
        let err = ddl_manager
            .submit_ddl_task(
                ExecutorContext {
                    query_context: Some(QueryContext::default()),
                    ..Default::default()
                },
                SubmitDdlTaskRequest::new(DdlTask::new_alter_table(set_options_expr(
                    "logical",
                    &[("repartition.column.hint", "host")],
                ))),
            )
            .await
            .unwrap_err();
        let msg = common_error::ext::ErrorExt::output_msg(&err);
        assert!(msg.contains("non-physical TableRouteValue"), "{msg}");
    }
}
