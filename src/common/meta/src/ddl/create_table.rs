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

use std::collections::HashMap;

use api::v1::region::region_request::Body as PbRegionRequest;
use api::v1::region::{RegionRequest, RegionRequestHeader};
use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_procedure::error::{
    ExternalSnafu, FromJsonSnafu, Result as ProcedureResult, ToJsonSnafu,
};
use common_procedure::{Context as ProcedureContext, LockKey, Procedure, Status};
use common_telemetry::info;
use common_telemetry::tracing_context::TracingContext;
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};
use store_api::storage::{RegionId, RegionNumber};
use strum::AsRefStr;
use table::metadata::{RawTableInfo, TableId};
use table::table_reference::TableReference;

use crate::ddl::create_table_template::{build_template, CreateRequestBuilder};
use crate::ddl::utils::{
    add_peer_context_if_needed, convert_region_routes_to_detecting_regions, map_to_procedure_error,
    region_storage_path,
};
use crate::ddl::{DdlContext, TableMetadata};
use crate::error::{self, Result};
use crate::key::table_name::TableNameKey;
use crate::key::table_route::{PhysicalTableRouteValue, TableRouteValue};
use crate::lock_key::{CatalogLock, SchemaLock, TableNameLock};
use crate::metrics;
use crate::region_keeper::OperatingRegionGuard;
use crate::rpc::ddl::CreateTableTask;
use crate::rpc::router::{
    find_leader_regions, find_leaders, operating_leader_regions, RegionRoute,
};
pub struct CreateTableProcedure {
    pub context: DdlContext,
    pub creator: TableCreator,
}

impl CreateTableProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::CreateTable";

    pub fn new(task: CreateTableTask, context: DdlContext) -> Self {
        Self {
            context,
            creator: TableCreator::new(task),
        }
    }

    pub fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data = serde_json::from_str(json).context(FromJsonSnafu)?;

        Ok(CreateTableProcedure {
            context,
            creator: TableCreator {
                data,
                opening_regions: vec![],
            },
        })
    }

    fn table_info(&self) -> &RawTableInfo {
        &self.creator.data.task.table_info
    }

    pub(crate) fn table_id(&self) -> TableId {
        self.table_info().ident.table_id
    }

    fn region_wal_options(&self) -> Result<&HashMap<RegionNumber, String>> {
        self.creator
            .data
            .region_wal_options
            .as_ref()
            .context(error::UnexpectedSnafu {
                err_msg: "region_wal_options is not allocated",
            })
    }

    fn table_route(&self) -> Result<&PhysicalTableRouteValue> {
        self.creator
            .data
            .table_route
            .as_ref()
            .context(error::UnexpectedSnafu {
                err_msg: "table_route is not allocated",
            })
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn set_allocated_metadata(
        &mut self,
        table_id: TableId,
        table_route: PhysicalTableRouteValue,
        region_wal_options: HashMap<RegionNumber, String>,
    ) {
        self.creator
            .set_allocated_metadata(table_id, table_route, region_wal_options)
    }

    /// On the prepare step, it performs:
    /// - Checks whether the table exists.
    /// - Allocates the table id.
    ///
    /// Abort(non-retry):
    /// - TableName exists and `create_if_not_exists` is false.
    /// - Failed to allocate [TableMetadata].
    pub(crate) async fn on_prepare(&mut self) -> Result<Status> {
        let expr = &self.creator.data.task.create_table;
        let table_name_value = self
            .context
            .table_metadata_manager
            .table_name_manager()
            .get(TableNameKey::new(
                &expr.catalog_name,
                &expr.schema_name,
                &expr.table_name,
            ))
            .await?;

        if let Some(value) = table_name_value {
            ensure!(
                expr.create_if_not_exists,
                error::TableAlreadyExistsSnafu {
                    table_name: self.creator.data.table_ref().to_string(),
                }
            );

            let table_id = value.table_id();
            return Ok(Status::done_with_output(table_id));
        }

        self.creator.data.state = CreateTableState::DatanodeCreateRegions;
        let TableMetadata {
            table_id,
            table_route,
            region_wal_options,
        } = self
            .context
            .table_metadata_allocator
            .create(&self.creator.data.task)
            .await?;
        self.creator
            .set_allocated_metadata(table_id, table_route, region_wal_options);

        Ok(Status::executing(true))
    }

    pub fn new_region_request_builder(
        &self,
        physical_table_id: Option<TableId>,
    ) -> Result<CreateRequestBuilder> {
        let create_table_expr = &self.creator.data.task.create_table;
        let template = build_template(create_table_expr)?;
        Ok(CreateRequestBuilder::new(template, physical_table_id))
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
        let request_builder = self.new_region_request_builder(None)?;
        // Registers opening regions
        let guards = self
            .creator
            .register_opening_regions(&self.context, &table_route.region_routes)?;
        if !guards.is_empty() {
            self.creator.opening_regions = guards;
        }
        self.create_regions(&table_route.region_routes, request_builder)
            .await
    }

    async fn create_regions(
        &mut self,
        region_routes: &[RegionRoute],
        request_builder: CreateRequestBuilder,
    ) -> Result<Status> {
        let create_table_data = &self.creator.data;
        // Safety: the region_wal_options must be allocated
        let region_wal_options = self.region_wal_options()?;
        let create_table_expr = &create_table_data.task.create_table;
        let catalog = &create_table_expr.catalog_name;
        let schema = &create_table_expr.schema_name;
        let storage_path = region_storage_path(catalog, schema);
        let leaders = find_leaders(region_routes);
        let mut create_region_tasks = Vec::with_capacity(leaders.len());

        for datanode in leaders {
            let requester = self.context.node_manager.datanode(&datanode).await;

            let regions = find_leader_regions(region_routes, &datanode);
            let mut requests = Vec::with_capacity(regions.len());
            for region_number in regions {
                let region_id = RegionId::new(self.table_id(), region_number);
                let create_region_request =
                    request_builder.build_one(region_id, storage_path.clone(), region_wal_options);
                requests.push(PbRegionRequest::Create(create_region_request));
            }

            for request in requests {
                let request = RegionRequest {
                    header: Some(RegionRequestHeader {
                        tracing_context: TracingContext::from_current_span().to_w3c(),
                        ..Default::default()
                    }),
                    body: Some(request),
                };

                let datanode = datanode.clone();
                let requester = requester.clone();
                create_region_tasks.push(async move {
                    requester
                        .handle(request)
                        .await
                        .map_err(add_peer_context_if_needed(datanode))
                });
            }
        }

        join_all(create_region_tasks)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        self.creator.data.state = CreateTableState::CreateMetadata;

        // TODO(weny): Add more tests.
        Ok(Status::executing(true))
    }

    /// Creates table metadata
    ///
    /// Abort(not-retry):
    /// - Failed to create table metadata.
    async fn on_create_metadata(&mut self) -> Result<Status> {
        let table_id = self.table_id();
        let manager = &self.context.table_metadata_manager;

        let raw_table_info = self.table_info().clone();
        // Safety: the region_wal_options must be allocated.
        let region_wal_options = self.region_wal_options()?.clone();
        // Safety: the table_route must be allocated.
        let physical_table_route = self.table_route()?.clone();
        let detecting_regions =
            convert_region_routes_to_detecting_regions(&physical_table_route.region_routes);
        let table_route = TableRouteValue::Physical(physical_table_route);
        manager
            .create_table_metadata(raw_table_info, table_route, region_wal_options)
            .await?;
        self.context
            .register_failure_detectors(detecting_regions)
            .await;
        info!("Created table metadata for table {table_id}");

        self.creator.opening_regions.clear();
        Ok(Status::done_with_output(table_id))
    }
}

#[async_trait]
impl Procedure for CreateTableProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    fn recover(&mut self) -> ProcedureResult<()> {
        // Only registers regions if the table route is allocated.
        if let Some(x) = &self.creator.data.table_route {
            self.creator.opening_regions = self
                .creator
                .register_opening_regions(&self.context, &x.region_routes)
                .map_err(BoxedError::new)
                .context(ExternalSnafu {
                    clean_poisons: false,
                })?;
        }

        Ok(())
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let state = &self.creator.data.state;

        let _timer = metrics::METRIC_META_PROCEDURE_CREATE_TABLE
            .with_label_values(&[state.as_ref()])
            .start_timer();

        match state {
            CreateTableState::Prepare => self.on_prepare().await,
            CreateTableState::DatanodeCreateRegions => self.on_datanode_create_regions().await,
            CreateTableState::CreateMetadata => self.on_create_metadata().await,
        }
        .map_err(map_to_procedure_error)
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.creator.data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        let table_ref = &self.creator.data.table_ref();

        LockKey::new(vec![
            CatalogLock::Read(table_ref.catalog).into(),
            SchemaLock::read(table_ref.catalog, table_ref.schema).into(),
            TableNameLock::new(table_ref.catalog, table_ref.schema, table_ref.table).into(),
        ])
    }
}

pub struct TableCreator {
    /// The serializable data.
    pub data: CreateTableData,
    /// The guards of opening.
    pub opening_regions: Vec<OperatingRegionGuard>,
}

impl TableCreator {
    pub fn new(task: CreateTableTask) -> Self {
        Self {
            data: CreateTableData {
                state: CreateTableState::Prepare,
                task,
                table_route: None,
                region_wal_options: None,
            },
            opening_regions: vec![],
        }
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

    fn set_allocated_metadata(
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
    /// None stands for not allocated yet.
    table_route: Option<PhysicalTableRouteValue>,
    /// None stands for not allocated yet.
    pub region_wal_options: Option<HashMap<RegionNumber, String>>,
}

impl CreateTableData {
    fn table_ref(&self) -> TableReference<'_> {
        self.task.table_ref()
    }
}
