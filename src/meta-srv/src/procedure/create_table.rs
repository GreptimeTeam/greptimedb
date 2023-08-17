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

use async_trait::async_trait;
use client::Database;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_meta::key::table_name::TableNameKey;
use common_meta::rpc::ddl::CreateTableTask;
use common_meta::rpc::router::{find_leader_regions, find_leaders, RegionRoute};
use common_meta::table_name::TableName;
use common_procedure::error::{FromJsonSnafu, Result as ProcedureResult, ToJsonSnafu};
use common_procedure::{Context as ProcedureContext, LockKey, Procedure, Status};
use common_telemetry::info;
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use table::engine::TableReference;
use table::metadata::{RawTableInfo, TableId};

use super::utils::{handle_request_datanode_error, handle_retry_error};
use crate::ddl::DdlContext;
use crate::error::{self, Result, TableMetadataManagerSnafu};

pub struct CreateTableProcedure {
    context: DdlContext,
    creator: TableCreator,
}

impl CreateTableProcedure {
    pub(crate) const TYPE_NAME: &'static str = "metasrv-procedure::CreateTable";

    pub(crate) fn new(
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

    pub(crate) fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data = serde_json::from_str(json).context(FromJsonSnafu)?;
        Ok(CreateTableProcedure {
            context,
            creator: TableCreator { data },
        })
    }

    fn table_name(&self) -> TableName {
        self.creator.data.task.table_name()
    }

    pub fn table_info(&self) -> &RawTableInfo {
        &self.creator.data.task.table_info
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
            .await
            .context(TableMetadataManagerSnafu)?;

        if exist {
            ensure!(
                self.creator.data.task.create_table.create_if_not_exists,
                error::TableAlreadyExistsSnafu {
                    table_name: self.creator.data.table_ref().to_string(),
                }
            );

            return Ok(Status::Done);
        }

        self.creator.data.state = CreateTableState::DatanodeCreateTable;

        Ok(Status::executing(true))
    }

    async fn on_create_metadata(&self) -> Result<Status> {
        let _timer = common_telemetry::timer!(
            crate::metrics::METRIC_META_CREATE_TABLE_PROCEDURE_CREATE_META
        );

        let table_id = self.table_info().ident.table_id as TableId;

        let manager = &self.context.table_metadata_manager;

        let raw_table_info = self.table_info().clone();
        let region_routes = self.region_routes().clone();

        manager
            .create_table_metadata(raw_table_info, region_routes)
            .await
            .context(TableMetadataManagerSnafu)?;
        info!("Created table metadata for table {table_id}");

        Ok(Status::Done)
    }

    async fn on_datanode_create_table(&mut self) -> Result<Status> {
        let _timer = common_telemetry::timer!(
            crate::metrics::METRIC_META_CREATE_TABLE_PROCEDURE_CREATE_TABLE
        );
        let region_routes = &self.creator.data.region_routes;
        let table_name = self.table_name();
        let clients = self.context.datanode_clients.clone();
        let leaders = find_leaders(region_routes);
        let mut joins = Vec::with_capacity(leaders.len());
        let table_id = self.table_info().ident.table_id;

        for datanode in leaders {
            let client = clients.get_client(&datanode).await;
            let client = Database::new(&table_name.catalog_name, &table_name.schema_name, client);

            let regions = find_leader_regions(region_routes, &datanode);
            let mut create_expr_for_region = self.creator.data.task.create_table.clone();
            create_expr_for_region.region_numbers = regions;
            create_expr_for_region.table_id = Some(api::v1::TableId { id: table_id });

            joins.push(common_runtime::spawn_bg(async move {
                if let Err(err) = client.create(create_expr_for_region).await {
                    if err.status_code() != StatusCode::TableAlreadyExists {
                        return Err(handle_request_datanode_error(datanode)(err));
                    }
                }
                Ok(())
            }));
        }

        let _r = join_all(joins)
            .await
            .into_iter()
            .map(|e| e.context(error::JoinSnafu).flatten())
            .collect::<Result<Vec<_>>>()?;

        self.creator.data.state = CreateTableState::CreateMetadata;

        Ok(Status::executing(true))
    }
}

#[async_trait]
impl Procedure for CreateTableProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        match self.creator.data.state {
            CreateTableState::Prepare => self.on_prepare().await,
            CreateTableState::DatanodeCreateTable => self.on_datanode_create_table().await,
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
    data: CreateTableData,
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
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum CreateTableState {
    /// Prepares to create the table
    Prepare,
    /// Datanode creates the table
    DatanodeCreateTable,
    /// Creates metadata
    CreateMetadata,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateTableData {
    state: CreateTableState,
    task: CreateTableTask,
    region_routes: Vec<RegionRoute>,
    cluster_id: u64,
}

impl CreateTableData {
    fn table_ref(&self) -> TableReference<'_> {
        self.task.table_ref()
    }
}
