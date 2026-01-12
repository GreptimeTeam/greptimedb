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

pub mod executor;
pub mod template;

use std::collections::HashMap;

use api::v1::CreateTableExpr;
use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_procedure::error::{
    ExternalSnafu, FromJsonSnafu, Result as ProcedureResult, ToJsonSnafu,
};
use common_procedure::{Context as ProcedureContext, LockKey, Procedure, ProcedureId, Status};
use common_telemetry::info;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use store_api::metadata::ColumnMetadata;
use store_api::storage::RegionNumber;
use strum::AsRefStr;
use table::metadata::{RawTableInfo, TableId};
use table::table_name::TableName;
use table::table_reference::TableReference;
pub(crate) use template::{CreateRequestBuilder, build_template_from_raw_table_info};

use crate::ddl::create_table::executor::CreateTableExecutor;
use crate::ddl::create_table::template::build_template;
use crate::ddl::utils::map_to_procedure_error;
use crate::ddl::{DdlContext, TableMetadata};
use crate::error::{self, Result};
use crate::key::table_route::PhysicalTableRouteValue;
use crate::lock_key::{CatalogLock, SchemaLock, TableNameLock};
use crate::metrics;
use crate::region_keeper::OperatingRegionGuard;
use crate::rpc::ddl::CreateTableTask;
use crate::rpc::router::{RegionRoute, operating_leader_regions};

pub struct CreateTableProcedure {
    pub context: DdlContext,
    /// The serializable data.
    pub data: CreateTableData,
    /// The guards of opening.
    pub opening_regions: Vec<OperatingRegionGuard>,
    /// The executor of the procedure.
    pub executor: CreateTableExecutor,
}

fn build_executor_from_create_table_data(
    create_table_expr: &CreateTableExpr,
) -> Result<CreateTableExecutor> {
    let template = build_template(create_table_expr)?;
    let builder = CreateRequestBuilder::new(template, None);
    let table_name = TableName::new(
        create_table_expr.catalog_name.clone(),
        create_table_expr.schema_name.clone(),
        create_table_expr.table_name.clone(),
    );
    let executor =
        CreateTableExecutor::new(table_name, create_table_expr.create_if_not_exists, builder);
    Ok(executor)
}

impl CreateTableProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::CreateTable";

    pub fn new(task: CreateTableTask, context: DdlContext) -> Result<Self> {
        let executor = build_executor_from_create_table_data(&task.create_table)?;

        Ok(Self {
            context,
            data: CreateTableData::new(task),
            opening_regions: vec![],
            executor,
        })
    }

    pub fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data: CreateTableData = serde_json::from_str(json).context(FromJsonSnafu)?;
        let create_table_expr = &data.task.create_table;
        let executor = build_executor_from_create_table_data(create_table_expr)
            .map_err(BoxedError::new)
            .context(ExternalSnafu {
                clean_poisons: false,
            })?;

        Ok(CreateTableProcedure {
            context,
            data,
            opening_regions: vec![],
            executor,
        })
    }

    fn table_info(&self) -> &RawTableInfo {
        &self.data.task.table_info
    }

    pub(crate) fn table_id(&self) -> TableId {
        self.table_info().ident.table_id
    }

    fn region_wal_options(&self) -> Result<&HashMap<RegionNumber, String>> {
        self.data
            .region_wal_options
            .as_ref()
            .context(error::UnexpectedSnafu {
                err_msg: "region_wal_options is not allocated",
            })
    }

    fn table_route(&self) -> Result<&PhysicalTableRouteValue> {
        self.data
            .table_route
            .as_ref()
            .context(error::UnexpectedSnafu {
                err_msg: "table_route is not allocated",
            })
    }

    /// On the prepare step, it performs:
    /// - Checks whether the table exists.
    /// - Allocates the table id.
    ///
    /// Abort(non-retry):
    /// - TableName exists and `create_if_not_exists` is false.
    /// - Failed to allocate [TableMetadata].
    pub(crate) async fn on_prepare(&mut self) -> Result<Status> {
        let table_id = self
            .executor
            .on_prepare(&self.context.table_metadata_manager)
            .await?;
        // Return the table id if the table already exists.
        if let Some(table_id) = table_id {
            return Ok(Status::done_with_output(table_id));
        }

        self.data.state = CreateTableState::DatanodeCreateRegions;
        let TableMetadata {
            table_id,
            table_route,
            region_wal_options,
        } = self
            .context
            .table_metadata_allocator
            .create(&self.data.task)
            .await?;
        self.set_allocated_metadata(table_id, table_route, region_wal_options);

        Ok(Status::executing(true))
    }

    /// Creates regions on datanodes
    ///
    /// Abort(non-retry):
    /// - Failed to create [CreateRequestBuilder].
    /// - Failed to get the table route of physical table (for logical table).
    ///
    /// Retry:
    /// - If the underlying servers returns one of the following [Code](tonic::status::Code):
    ///   - [Code::Cancelled](tonic::status::Code::Cancelled)
    ///   - [Code::DeadlineExceeded](tonic::status::Code::DeadlineExceeded)
    ///   - [Code::Unavailable](tonic::status::Code::Unavailable)
    pub async fn on_datanode_create_regions(&mut self) -> Result<Status> {
        let table_route = self.table_route()?.clone();
        // Registers opening regions
        let guards = self.register_opening_regions(&self.context, &table_route.region_routes)?;
        if !guards.is_empty() {
            self.opening_regions = guards;
        }
        self.create_regions(&table_route.region_routes).await
    }

    async fn create_regions(&mut self, region_routes: &[RegionRoute]) -> Result<Status> {
        let table_id = self.table_id();
        let region_wal_options = self.region_wal_options()?;
        let column_metadatas = self
            .executor
            .on_create_regions(
                &self.context.node_manager,
                table_id,
                region_routes,
                region_wal_options,
            )
            .await?;

        self.data.column_metadatas = column_metadatas;
        self.data.state = CreateTableState::CreateMetadata;
        Ok(Status::executing(true))
    }

    /// Creates table metadata
    ///
    /// Abort(not-retry):
    /// - Failed to create table metadata.
    async fn on_create_metadata(&mut self, pid: ProcedureId) -> Result<Status> {
        let table_id = self.table_id();
        let table_ref = self.data.table_ref();
        let manager = &self.context.table_metadata_manager;

        let raw_table_info = self.table_info().clone();
        // Safety: the region_wal_options must be allocated.
        let region_wal_options = self.region_wal_options()?.clone();
        // Safety: the table_route must be allocated.
        let physical_table_route = self.table_route()?.clone();
        self.executor
            .on_create_metadata(
                manager,
                &self.context.region_failure_detector_controller,
                raw_table_info,
                &self.data.column_metadatas,
                physical_table_route,
                region_wal_options,
            )
            .await?;

        info!(
            "Successfully created table: {}, table_id: {}, procedure_id: {}",
            table_ref, table_id, pid
        );

        self.opening_regions.clear();
        Ok(Status::done_with_output(table_id))
    }

    /// Registers and returns the guards of the opening region if they don't exist.
    fn register_opening_regions(
        &self,
        context: &DdlContext,
        region_routes: &[RegionRoute],
    ) -> Result<Vec<OperatingRegionGuard>> {
        let opening_regions = operating_leader_regions(region_routes);
        if self.opening_regions.len() == opening_regions.len() {
            return Ok(vec![]);
        }

        let mut opening_region_guards = Vec::with_capacity(opening_regions.len());

        for (region_id, datanode_id) in opening_regions {
            let guard = context
                .memory_region_keeper
                .register(datanode_id, region_id)
                .context(error::RegionOperatingRaceSnafu {
                    region_id,
                    peer_id: datanode_id,
                })?;
            opening_region_guards.push(guard);
        }
        Ok(opening_region_guards)
    }

    pub fn set_allocated_metadata(
        &mut self,
        table_id: TableId,
        table_route: PhysicalTableRouteValue,
        region_wal_options: HashMap<RegionNumber, String>,
    ) {
        self.data.task.table_info.ident.table_id = table_id;
        self.data.table_route = Some(table_route);
        self.data.region_wal_options = Some(region_wal_options);
    }
}

#[async_trait]
impl Procedure for CreateTableProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    fn recover(&mut self) -> ProcedureResult<()> {
        // Only registers regions if the table route is allocated.
        if let Some(x) = &self.data.table_route {
            self.opening_regions = self
                .register_opening_regions(&self.context, &x.region_routes)
                .map_err(BoxedError::new)
                .context(ExternalSnafu {
                    clean_poisons: false,
                })?;
        }

        Ok(())
    }

    async fn execute(&mut self, ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let state = &self.data.state;

        let _timer = metrics::METRIC_META_PROCEDURE_CREATE_TABLE
            .with_label_values(&[state.as_ref()])
            .start_timer();

        match state {
            CreateTableState::Prepare => self.on_prepare().await,
            CreateTableState::DatanodeCreateRegions => self.on_datanode_create_regions().await,
            CreateTableState::CreateMetadata => self.on_create_metadata(ctx.procedure_id).await,
        }
        .map_err(map_to_procedure_error)
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        let table_ref = &self.data.table_ref();

        LockKey::new(vec![
            CatalogLock::Read(table_ref.catalog).into(),
            SchemaLock::read(table_ref.catalog, table_ref.schema).into(),
            TableNameLock::new(table_ref.catalog, table_ref.schema, table_ref.table).into(),
        ])
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, AsRefStr, PartialEq)]
pub enum CreateTableState {
    /// Prepares to create the table
    Prepare,
    /// Creates regions on the Datanode
    DatanodeCreateRegions,
    /// Creates metadata
    CreateMetadata,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateTableData {
    pub state: CreateTableState,
    pub task: CreateTableTask,
    #[serde(default)]
    pub column_metadatas: Vec<ColumnMetadata>,
    /// None stands for not allocated yet.
    table_route: Option<PhysicalTableRouteValue>,
    /// None stands for not allocated yet.
    pub region_wal_options: Option<HashMap<RegionNumber, String>>,
}

impl CreateTableData {
    pub fn new(task: CreateTableTask) -> Self {
        CreateTableData {
            state: CreateTableState::Prepare,
            column_metadatas: vec![],
            task,
            table_route: None,
            region_wal_options: None,
        }
    }

    fn table_ref(&self) -> TableReference<'_> {
        self.task.table_ref()
    }
}
