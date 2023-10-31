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

use api::v1::region::region_request::Body as PbRegionRequest;
use api::v1::region::{
    CreateRequest as PbCreateRegionRequest, RegionColumnDef, RegionRequest, RegionRequestHeader,
};
use api::v1::{ColumnDef, SemanticType};
use async_trait::async_trait;
use common_procedure::error::{FromJsonSnafu, Result as ProcedureResult, ToJsonSnafu};
use common_procedure::{Context as ProcedureContext, LockKey, Procedure, Status};
use common_telemetry::info;
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
use crate::rpc::ddl::CreateDatabaseTask;
use crate::rpc::router::{find_leader_regions, find_leaders, RegionRoute};

pub struct CreateDatabaseProcedure {
    pub context: DdlContext,
    pub creator: DatabaseCreator,
}

impl CreateDatabaseProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::CreateDatabase";

    pub fn new(
        cluster_id: u64,
        task: CreateDatabaseTask,
        region_routes: Vec<RegionRoute>,
        context: DdlContext,
    ) -> Self {
        Self {
            context,
            creator: DatabaseCreator::new(cluster_id, task, region_routes),
        }
    }

    pub fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data = serde_json::from_str(json).context(FromJsonSnafu)?;
        Ok(CreateDatabaseProcedure {
            context,
            creator: DatabaseCreator { data },
        })
    }

    pub fn database_info(&self) -> &RawDatabaseInfo {
        &self.creator.data.task.database_info
    }

    fn database_id(&self) -> DatabaseId {
        self.table_info().ident.database_id
    }

    pub fn region_routes(&self) -> &Vec<RegionRoute> {
        &self.creator.data.region_routes
    }

    /// Checks whether the database exists.
    async fn on_prepare(&mut self) -> Result<Status> {
        let expr = &self.creator.data.task.create_database;
        let exist = self
            .context
            .database_metadata_manager
            .database_name_manager()
            .exists(DatabaseNameKey::new(
                &expr.database_name,
            ))
            .await?;

        if exist {
            ensure!(
                self.creator.data.task.create_database.create_if_not_exists,
                error::DatabaseAlreadyExistsSnafu {
                    database_name: self.creator.data.database_ref().to_string(),
                }
            );

            return Ok(Status::Done);
        }

        self.creator.data.state = CreateTableState::DatanodeCreateRegions;

        Ok(Status::executing(true))
    }

    pub fn create_region_request_template(&self) -> Result<PbCreateRegionRequest> {
        let create_database_expr = &self.creator.data.task.create_database;

        Ok(PbCreateRegionRequest {
            region_id: 0,
            engine: create_database_expr.engine.to_string(),
            None,
            None,
            path: String::new(),
            options: create_database_expr.database_options.clone(),
        })
    }

    pub async fn on_datanode_create_regions(&mut self) -> Result<Status> {
        let create_database_data = &self.creator.data;
        let region_routes = &create_database_data.region_routes;

        let create_database_expr = &create_database_data.task.create_database;
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
                    let region_id = RegionId::new(self.database_id(), *region_number);

                    let mut create_region_request = request_template.clone();
                    create_region_request.region_id = region_id.as_u64();
                    create_region_request.path = storage_path.clone();
                    PbRegionRequest::Create(create_region_request)
                })
                .collect::<Vec<_>>();

            for request in requests {
                let request = RegionRequest {
                    header: Some(RegionRequestHeader {
                        trace_id: common_telemetry::trace_id().unwrap_or_default(),
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

        self.creator.data.state = CreateDatabaseState::CreateMetadata;

        Ok(Status::executing(true))
    }

    async fn on_create_metadata(&self) -> Result<Status> {
        let database_id = self.database_id();
        let manager = &self.context.database_metadata_manager;

        let raw_database_info = self.database_info().clone();
        let region_routes = self.region_routes().clone();
        manager
            .create_database_metadata(raw_database_info, region_routes)
            .await?;
        info!("Created database metadata for database {database_id}");

        Ok(Status::Done)
    }
}

#[async_trait]
impl Procedure for CreateDatabaseProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let state = &self.creator.data.state;

        let _timer = common_telemetry::timer!(
            metrics::METRIC_META_PROCEDURE_CREATE_TABLE,
            &[("step", state.as_ref().to_string())]
        );

        match state {
            CreateDatabaseState::Prepare => self.on_prepare().await,
            CreateDatabaseState::DatanodeCreateRegions => self.on_datanode_create_regions().await,
            CreateDatabaseState::CreateMetadata => self.on_create_metadata().await,
        }
        .map_err(handle_retry_error)
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.creator.data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        let database_ref = &self.creator.data.database_ref();
        let key = common_catalog::build_db_string( // todo john
            table_ref.database_name,
        );

        LockKey::single(key)
    }
}

pub struct DatabaseCreator {
    pub data: CreateDatabaseData,
}

impl DatabaseCreator {
    pub fn new(cluster_id: u64, task: CreateDatabaseTask, region_routes: Vec<RegionRoute>) -> Self {
        Self {
            data: CreateDatabaseData {
                state: CreateDatabaseState::Prepare,
                cluster_id,
                task,
                region_routes,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, AsRefStr)]
pub enum CreateDatabaseState {
    /// Prepares to create the database
    Prepare,
    /// Creates regions on the Datanode
    DatanodeCreateRegions,
    /// Creates metadata
    CreateMetadata,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateDatabaseData {
    pub state: CreateDatabaseState,
    pub task: CreateDatabaseTask,
    pub region_routes: Vec<RegionRoute>,
    pub cluster_id: u64,
}

impl CreateDatabaseData {
    fn database_ref(&self) -> TableReference<'_> {
        self.task.database_ref()
    }
}
