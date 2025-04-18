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

mod check;
mod metadata;
mod region_request;
mod update_metadata;

use api::region::RegionResponse;
use api::v1::CreateTableExpr;
use async_trait::async_trait;
use common_catalog::consts::METRIC_ENGINE;
use common_procedure::error::{FromJsonSnafu, Result as ProcedureResult, ToJsonSnafu};
use common_procedure::{Context as ProcedureContext, LockKey, Procedure, Status};
use common_telemetry::{debug, error, warn};
use futures::future;
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use store_api::metadata::ColumnMetadata;
use store_api::metric_engine_consts::ALTER_PHYSICAL_EXTENSION_KEY;
use store_api::storage::{RegionId, RegionNumber};
use strum::AsRefStr;
use table::metadata::{RawTableInfo, TableId};

use crate::ddl::utils::{add_peer_context_if_needed, handle_retry_error, sync_follower_regions};
use crate::ddl::DdlContext;
use crate::error::{DecodeJsonSnafu, MetadataCorruptionSnafu, Result};
use crate::key::table_route::TableRouteValue;
use crate::lock_key::{CatalogLock, SchemaLock, TableLock, TableNameLock};
use crate::metrics;
use crate::rpc::ddl::CreateTableTask;
use crate::rpc::router::{find_leaders, RegionRoute};

pub struct CreateLogicalTablesProcedure {
    pub context: DdlContext,
    pub data: CreateTablesData,
}

impl CreateLogicalTablesProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::CreateLogicalTables";

    pub fn new(
        tasks: Vec<CreateTableTask>,
        physical_table_id: TableId,
        context: DdlContext,
    ) -> Self {
        Self {
            context,
            data: CreateTablesData {
                state: CreateTablesState::Prepare,
                tasks,
                table_ids_already_exists: vec![],
                physical_table_id,
                physical_region_numbers: vec![],
                physical_columns: vec![],
            },
        }
    }

    pub fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data = serde_json::from_str(json).context(FromJsonSnafu)?;
        Ok(Self { context, data })
    }

    /// On the prepares step, it performs:
    /// - Checks whether physical table exists.
    /// - Checks whether logical tables exist.
    /// - Allocates the table ids.
    /// - Modify tasks to sort logical columns on their names.
    ///
    /// Abort(non-retry):
    /// - The physical table does not exist.
    /// - Failed to check whether tables exist.
    /// - One of logical tables has existing, and the table creation task without setting `create_if_not_exists`.
    pub(crate) async fn on_prepare(&mut self) -> Result<Status> {
        self.check_input_tasks()?;
        // Sets physical region numbers
        self.fill_physical_table_info().await?;
        // Checks if the tables exist
        self.check_tables_already_exist().await?;

        // If all tables already exist, returns the table_ids.
        if self
            .data
            .table_ids_already_exists
            .iter()
            .all(Option::is_some)
        {
            return Ok(Status::done_with_output(
                self.data
                    .table_ids_already_exists
                    .drain(..)
                    .flatten()
                    .collect::<Vec<_>>(),
            ));
        }

        // Allocates table ids and sort columns on their names.
        self.allocate_table_ids().await?;

        self.data.state = CreateTablesState::DatanodeCreateRegions;
        Ok(Status::executing(true))
    }

    pub async fn on_datanode_create_regions(&mut self) -> Result<Status> {
        let (_, physical_table_route) = self
            .context
            .table_metadata_manager
            .table_route_manager()
            .get_physical_table_route(self.data.physical_table_id)
            .await?;

        self.create_regions(&physical_table_route.region_routes)
            .await
    }

    /// Creates table metadata for logical tables and update corresponding physical
    /// table's metadata.
    ///
    /// Abort(not-retry):
    /// - Failed to create table metadata.
    pub async fn on_create_metadata(&mut self) -> Result<Status> {
        self.update_physical_table_metadata().await?;
        let table_ids = self.create_logical_tables_metadata().await?;

        Ok(Status::done_with_output(table_ids))
    }

    async fn create_regions(&mut self, region_routes: &[RegionRoute]) -> Result<Status> {
        let leaders = find_leaders(region_routes);
        let mut create_region_tasks = Vec::with_capacity(leaders.len());

        for peer in leaders {
            let requester = self.context.node_manager.datanode(&peer).await;
            let Some(request) = self.make_request(&peer, region_routes)? else {
                debug!("no region request to send to datanode {}", peer);
                // We can skip the rest of the datanodes,
                // the rest of the datanodes should have the same result.
                break;
            };

            create_region_tasks.push(async move {
                requester
                    .handle(request)
                    .await
                    .map_err(add_peer_context_if_needed(peer))
            });
        }

        let mut results = future::join_all(create_region_tasks)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        // Collects response from datanodes.
        let phy_raw_schemas = results
            .iter_mut()
            .map(|res| res.extensions.remove(ALTER_PHYSICAL_EXTENSION_KEY))
            .collect::<Vec<_>>();

        if phy_raw_schemas.is_empty() {
            self.submit_sync_region_requests(results, region_routes)
                .await;
            self.data.state = CreateTablesState::CreateMetadata;
            return Ok(Status::executing(false));
        }

        // Verify all the physical schemas are the same
        // Safety: previous check ensures this vec is not empty
        let first = phy_raw_schemas.first().unwrap();
        ensure!(
            phy_raw_schemas.iter().all(|x| x == first),
            MetadataCorruptionSnafu {
                err_msg: "The physical schemas from datanodes are not the same."
            }
        );

        // Decodes the physical raw schemas
        if let Some(phy_raw_schemas) = first {
            self.data.physical_columns =
                ColumnMetadata::decode_list(phy_raw_schemas).context(DecodeJsonSnafu)?;
        } else {
            warn!("creating logical table result doesn't contains extension key `{ALTER_PHYSICAL_EXTENSION_KEY}`,leaving the physical table's schema unchanged");
        }

        self.submit_sync_region_requests(results, region_routes)
            .await;
        self.data.state = CreateTablesState::CreateMetadata;

        Ok(Status::executing(true))
    }

    async fn submit_sync_region_requests(
        &self,
        results: Vec<RegionResponse>,
        region_routes: &[RegionRoute],
    ) {
        if let Err(err) = sync_follower_regions(
            &self.context,
            self.data.physical_table_id,
            results,
            region_routes,
            METRIC_ENGINE,
        )
        .await
        {
            error!(err; "Failed to sync regions for physical table_id: {}",self.data.physical_table_id);
        }
    }
}

#[async_trait]
impl Procedure for CreateLogicalTablesProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let state = &self.data.state;

        let _timer = metrics::METRIC_META_PROCEDURE_CREATE_TABLES
            .with_label_values(&[state.as_ref()])
            .start_timer();

        match state {
            CreateTablesState::Prepare => self.on_prepare().await,
            CreateTablesState::DatanodeCreateRegions => self.on_datanode_create_regions().await,
            CreateTablesState::CreateMetadata => self.on_create_metadata().await,
        }
        .map_err(handle_retry_error)
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        // CatalogLock, SchemaLock,
        // TableLock
        // TableNameLock(s)
        let mut lock_key = Vec::with_capacity(2 + 1 + self.data.tasks.len());
        let table_ref = self.data.tasks[0].table_ref();
        lock_key.push(CatalogLock::Read(table_ref.catalog).into());
        lock_key.push(SchemaLock::read(table_ref.catalog, table_ref.schema).into());
        lock_key.push(TableLock::Write(self.data.physical_table_id).into());

        for task in &self.data.tasks {
            lock_key.push(
                TableNameLock::new(
                    &task.create_table.catalog_name,
                    &task.create_table.schema_name,
                    &task.create_table.table_name,
                )
                .into(),
            );
        }
        LockKey::new(lock_key)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateTablesData {
    state: CreateTablesState,
    tasks: Vec<CreateTableTask>,
    table_ids_already_exists: Vec<Option<TableId>>,
    physical_table_id: TableId,
    physical_region_numbers: Vec<RegionNumber>,
    physical_columns: Vec<ColumnMetadata>,
}

impl CreateTablesData {
    pub fn state(&self) -> &CreateTablesState {
        &self.state
    }

    fn all_create_table_exprs(&self) -> Vec<&CreateTableExpr> {
        self.tasks
            .iter()
            .map(|task| &task.create_table)
            .collect::<Vec<_>>()
    }

    /// Returns the remaining tasks.
    /// The length of tasks must be greater than 0.
    fn remaining_tasks(&self) -> Vec<(RawTableInfo, TableRouteValue)> {
        self.tasks
            .iter()
            .zip(self.table_ids_already_exists.iter())
            .flat_map(|(task, table_id)| {
                if table_id.is_none() {
                    let table_info = task.table_info.clone();
                    let region_ids = self
                        .physical_region_numbers
                        .iter()
                        .map(|region_number| {
                            RegionId::new(table_info.ident.table_id, *region_number)
                        })
                        .collect();
                    let table_route = TableRouteValue::logical(self.physical_table_id, region_ids);
                    Some((table_info, table_route))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, AsRefStr)]
pub enum CreateTablesState {
    /// Prepares to create the tables
    Prepare,
    /// Creates regions on the Datanode
    DatanodeCreateRegions,
    /// Creates metadata
    CreateMetadata,
}
