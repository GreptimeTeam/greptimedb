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
use api::v1::region::{
    CreateRequest as PbCreateRegionRequest, RegionColumnDef, RegionRequest, RegionRequestHeader,
};
use api::v1::{ColumnDef, SemanticType};
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
use store_api::storage::RegionId;
use strum::AsRefStr;
use table::engine::TableReference;
use table::metadata::{RawTableInfo, TableId};

use crate::ddl::utils::{handle_operate_region_error, handle_retry_error, region_storage_path};
use crate::ddl::DdlContext;
use crate::error::{self, Result};
use crate::key::table_name::TableNameKey;
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

    pub fn new(
        cluster_id: u64,
        task: CreateTableTask,
        region_routes: Vec<RegionRoute>,
        context: DdlContext,
    ) -> Self {
        Self {
            context,
            creator: TableCreator::new(cluster_id, task, region_routes),
        }
    }

    pub fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data = serde_json::from_str(json).context(FromJsonSnafu)?;

        let mut creator = TableCreator {
            data,
            opening_regions: vec![],
        };

        creator
            .register_opening_regions(&context)
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;

        Ok(CreateTableProcedure { context, creator })
    }

    pub fn table_info(&self) -> &RawTableInfo {
        &self.creator.data.task.table_info
    }

    fn table_id(&self) -> TableId {
        self.table_info().ident.table_id
    }

    pub fn region_routes(&self) -> &Vec<RegionRoute> {
        &self.creator.data.region_routes
    }

    /// Checks whether the table exists.
    async fn on_prepare(&mut self) -> Result<Status> {
        let expr = &self.creator.data.task.create_table;
        let exist = self
            .context
            .table_metadata_manager
            .table_name_manager()
            .exists(TableNameKey::new(
                &expr.catalog_name,
                &expr.schema_name,
                &expr.table_name,
            ))
            .await?;

        if exist {
            ensure!(
                self.creator.data.task.create_table.create_if_not_exists,
                error::TableAlreadyExistsSnafu {
                    table_name: self.creator.data.table_ref().to_string(),
                }
            );

            return Ok(Status::Done);
        }

        self.creator.data.state = CreateTableState::DatanodeCreateRegions;

        Ok(Status::executing(true))
    }

    pub fn create_region_request_template(&self) -> Result<PbCreateRegionRequest> {
        let create_table_expr = &self.creator.data.task.create_table;

        let column_defs = create_table_expr
            .column_defs
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let semantic_type = if create_table_expr.time_index == c.name {
                    SemanticType::Timestamp
                } else if create_table_expr.primary_keys.contains(&c.name) {
                    SemanticType::Tag
                } else {
                    SemanticType::Field
                };

                RegionColumnDef {
                    column_def: Some(ColumnDef {
                        name: c.name.clone(),
                        data_type: c.data_type,
                        is_nullable: c.is_nullable,
                        default_constraint: c.default_constraint.clone(),
                        semantic_type: semantic_type as i32,
                        comment: String::new(),
                        datatype_extension: c.datatype_extension.clone(),
                    }),
                    column_id: i as u32,
                }
            })
            .collect::<Vec<_>>();

        let primary_key = create_table_expr
            .primary_keys
            .iter()
            .map(|key| {
                column_defs
                    .iter()
                    .find_map(|c| {
                        c.column_def.as_ref().and_then(|x| {
                            if &x.name == key {
                                Some(c.column_id)
                            } else {
                                None
                            }
                        })
                    })
                    .context(error::PrimaryKeyNotFoundSnafu { key })
            })
            .collect::<Result<_>>()?;

        Ok(PbCreateRegionRequest {
            region_id: 0,
            engine: create_table_expr.engine.to_string(),
            column_defs,
            primary_key,
            path: String::new(),
            options: create_table_expr.table_options.clone(),
            wal_options: HashMap::default(),
        })
    }

    pub async fn on_datanode_create_regions(&mut self) -> Result<Status> {
        // Registers opening regions
        self.creator.register_opening_regions(&self.context)?;

        let create_table_data = &self.creator.data;
        let region_routes = &create_table_data.region_routes;

        let create_table_expr = &create_table_data.task.create_table;
        let catalog = &create_table_expr.catalog_name;
        let schema = &create_table_expr.schema_name;
        let storage_path = region_storage_path(catalog, schema);

        let request_template = self.create_region_request_template()?;

        let leaders = find_leaders(region_routes);
        let mut create_region_tasks = Vec::with_capacity(leaders.len());

        for datanode in leaders {
            let requester = self.context.datanode_manager.datanode(&datanode).await;

            let regions = find_leader_regions(region_routes, &datanode);
            let requests = regions
                .iter()
                .map(|region_number| {
                    let region_id = RegionId::new(self.table_id(), *region_number);

                    let mut create_region_request = request_template.clone();
                    create_region_request.region_id = region_id.as_u64();
                    create_region_request.path = storage_path.clone();
                    PbRegionRequest::Create(create_region_request)
                })
                .collect::<Vec<_>>();

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
                    if let Err(err) = requester.handle(request).await {
                        return Err(handle_operate_region_error(datanode)(err));
                    }
                    Ok(())
                });
            }
        }

        join_all(create_region_tasks)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        self.creator.data.state = CreateTableState::CreateMetadata;

        // Ensures the procedures after the crash start from the `DatanodeCreateRegions` stage.
        // TODO(weny): Add more tests.
        Ok(Status::executing(false))
    }

    async fn on_create_metadata(&self) -> Result<Status> {
        let table_id = self.table_id();
        let manager = &self.context.table_metadata_manager;

        let raw_table_info = self.table_info().clone();
        let region_routes = self.region_routes().clone();
        manager
            .create_table_metadata(raw_table_info, region_routes)
            .await?;
        info!("Created table metadata for table {table_id}");

        Ok(Status::Done)
    }
}

#[async_trait]
impl Procedure for CreateTableProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
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
        .map_err(handle_retry_error)
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.creator.data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        let table_ref = &self.creator.data.table_ref();
        let key = common_catalog::format_full_table_name(
            table_ref.catalog,
            table_ref.schema,
            table_ref.table,
        );

        LockKey::single(key)
    }
}

pub struct TableCreator {
    /// The serializable data.
    pub data: CreateTableData,
    /// The guards of opening.
    pub opening_regions: Vec<OperatingRegionGuard>,
}

impl TableCreator {
    pub fn new(cluster_id: u64, task: CreateTableTask, region_routes: Vec<RegionRoute>) -> Self {
        Self {
            data: CreateTableData {
                state: CreateTableState::Prepare,
                cluster_id,
                task,
                region_routes,
            },
            opening_regions: vec![],
        }
    }

    /// Register opening regions if doesn't exist.
    pub fn register_opening_regions(&mut self, context: &DdlContext) -> Result<()> {
        let region_routes = &self.data.region_routes;

        let opening_regions = operating_leader_regions(region_routes);

        if self.opening_regions.len() == opening_regions.len() {
            return Ok(());
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

        self.opening_regions = opening_region_guards;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, AsRefStr)]
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
    pub region_routes: Vec<RegionRoute>,
    pub cluster_id: u64,
}

impl CreateTableData {
    fn table_ref(&self) -> TableReference<'_> {
        self.task.table_ref()
    }
}
