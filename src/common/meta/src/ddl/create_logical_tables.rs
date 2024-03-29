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

use std::collections::HashMap;
use std::ops::Deref;

use api::v1::region::region_request::Body as PbRegionRequest;
use api::v1::region::{CreateRequests, RegionRequest, RegionRequestHeader};
use api::v1::CreateTableExpr;
use async_trait::async_trait;
use common_procedure::error::{FromJsonSnafu, Result as ProcedureResult, ToJsonSnafu};
use common_procedure::{Context as ProcedureContext, LockKey, Procedure, Status};
use common_telemetry::tracing_context::TracingContext;
use common_telemetry::{info, warn};
use futures_util::future::join_all;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};
use store_api::metadata::ColumnMetadata;
use store_api::metric_engine_consts::ALTER_PHYSICAL_EXTENSION_KEY;
use store_api::storage::{RegionId, RegionNumber};
use strum::AsRefStr;
use table::metadata::{RawTableInfo, TableId};

use crate::cache_invalidator::Context;
use crate::ddl::create_table_template::{build_template, CreateRequestBuilder};
use crate::ddl::utils::{add_peer_context_if_needed, handle_retry_error, region_storage_path};
use crate::ddl::{physical_table_metadata, DdlContext};
use crate::error::{
    DecodeJsonSnafu, MetadataCorruptionSnafu, Result, TableAlreadyExistsSnafu,
    TableInfoNotFoundSnafu,
};
use crate::instruction::CacheIdent;
use crate::key::table_info::TableInfoValue;
use crate::key::table_name::TableNameKey;
use crate::key::table_route::TableRouteValue;
use crate::key::DeserializedValueWithBytes;
use crate::lock_key::{CatalogLock, SchemaLock, TableLock, TableNameLock};
use crate::peer::Peer;
use crate::rpc::ddl::CreateTableTask;
use crate::rpc::router::{find_leader_regions, find_leaders, RegionRoute};
use crate::table_name::TableName;
use crate::{metrics, ClusterId};

pub struct CreateLogicalTablesProcedure {
    pub context: DdlContext,
    pub data: CreateTablesData,
}

impl CreateLogicalTablesProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::CreateLogicalTables";

    pub fn new(
        cluster_id: ClusterId,
        tasks: Vec<CreateTableTask>,
        physical_table_id: TableId,
        context: DdlContext,
    ) -> Self {
        Self {
            context,
            data: CreateTablesData {
                cluster_id,
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
        self.check_input_tasks().await?;
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
                    .into_iter()
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
        let physical_table_id = self.data.physical_table_id();
        let (_, physical_table_route) = self
            .context
            .table_metadata_manager
            .table_route_manager()
            .get_physical_table_route(physical_table_id)
            .await?;
        let region_routes = &physical_table_route.region_routes;

        self.create_regions(region_routes).await
    }

    /// Creates table metadata for logical tables and update corresponding physical
    /// table's metadata.
    ///
    /// Abort(not-retry):
    /// - Failed to create table metadata.
    pub async fn on_create_metadata(&mut self) -> Result<Status> {
        let manager = &self.context.table_metadata_manager;
        let physical_table_id = self.data.physical_table_id();
        let remaining_tasks = self.data.remaining_tasks();
        let num_tables = remaining_tasks.len();

        if num_tables > 0 {
            let chunk_size = manager.create_logical_tables_metadata_chunk_size();
            if num_tables > chunk_size {
                let chunks = remaining_tasks
                    .into_iter()
                    .chunks(chunk_size)
                    .into_iter()
                    .map(|chunk| chunk.collect::<Vec<_>>())
                    .collect::<Vec<_>>();
                for chunk in chunks {
                    manager.create_logical_tables_metadata(chunk).await?;
                }
            } else {
                manager
                    .create_logical_tables_metadata(remaining_tasks)
                    .await?;
            }
        }

        // The `table_id` MUST be collected after the [Prepare::Prepare],
        // ensures the all `table_id`s have been allocated.
        let table_ids = self
            .data
            .tasks
            .iter()
            .map(|task| task.table_info.ident.table_id)
            .collect::<Vec<_>>();

        if !self.data.physical_columns.is_empty() {
            // fetch old physical table's info
            let physical_table_info = self
                .context
                .table_metadata_manager
                .table_info_manager()
                .get(self.data.physical_table_id)
                .await?
                .with_context(|| TableInfoNotFoundSnafu {
                    table: format!("table id - {}", self.data.physical_table_id),
                })?;

            // generate new table info
            let new_table_info = self
                .data
                .build_new_physical_table_info(&physical_table_info);

            let physical_table_name = TableName::new(
                &new_table_info.catalog_name,
                &new_table_info.schema_name,
                &new_table_info.name,
            );

            // update physical table's metadata
            self.context
                .table_metadata_manager
                .update_table_info(physical_table_info, new_table_info)
                .await?;

            // invalid table cache
            self.context
                .cache_invalidator
                .invalidate(
                    &Context::default(),
                    vec![
                        CacheIdent::TableId(self.data.physical_table_id),
                        CacheIdent::TableName(physical_table_name),
                    ],
                )
                .await?;
        } else {
            warn!("No physical columns found, leaving the physical table's schema unchanged");
        }

        info!("Created {num_tables} tables {table_ids:?} metadata for physical table {physical_table_id}");

        Ok(Status::done_with_output(table_ids))
    }

    fn create_region_request_builder(
        &self,
        physical_table_id: TableId,
        task: &CreateTableTask,
    ) -> Result<CreateRequestBuilder> {
        let create_expr = &task.create_table;
        let template = build_template(create_expr)?;
        Ok(CreateRequestBuilder::new(template, Some(physical_table_id)))
    }

    fn one_datanode_region_requests(
        &self,
        datanode: &Peer,
        region_routes: &[RegionRoute],
    ) -> Result<CreateRequests> {
        let create_tables_data = &self.data;
        let tasks = &create_tables_data.tasks;
        let physical_table_id = create_tables_data.physical_table_id();
        let regions = find_leader_regions(region_routes, datanode);
        let mut requests = Vec::with_capacity(tasks.len() * regions.len());

        for task in tasks {
            let create_table_expr = &task.create_table;
            let catalog = &create_table_expr.catalog_name;
            let schema = &create_table_expr.schema_name;
            let logical_table_id = task.table_info.ident.table_id;
            let storage_path = region_storage_path(catalog, schema);
            let request_builder = self.create_region_request_builder(physical_table_id, task)?;

            for region_number in &regions {
                let region_id = RegionId::new(logical_table_id, *region_number);
                let create_region_request =
                    request_builder.build_one(region_id, storage_path.clone(), &HashMap::new())?;
                requests.push(create_region_request);
            }
        }

        Ok(CreateRequests { requests })
    }

    async fn create_regions(&mut self, region_routes: &[RegionRoute]) -> Result<Status> {
        let leaders = find_leaders(region_routes);
        let mut create_region_tasks = Vec::with_capacity(leaders.len());

        for datanode in leaders {
            let requester = self.context.datanode_manager.datanode(&datanode).await;
            let creates = self.one_datanode_region_requests(&datanode, region_routes)?;
            let request = RegionRequest {
                header: Some(RegionRequestHeader {
                    tracing_context: TracingContext::from_current_span().to_w3c(),
                    ..Default::default()
                }),
                body: Some(PbRegionRequest::Creates(creates)),
            };
            create_region_tasks.push(async move {
                requester
                    .handle(request)
                    .await
                    .map_err(add_peer_context_if_needed(datanode))
            });
        }

        // collect response from datanodes
        let raw_schemas = join_all(create_region_tasks)
            .await
            .into_iter()
            .map(|response| {
                response.map(|mut response| response.extension.remove(ALTER_PHYSICAL_EXTENSION_KEY))
            })
            .collect::<Result<Vec<_>>>()?;

        if raw_schemas.is_empty() {
            self.data.state = CreateTablesState::CreateMetadata;
            return Ok(Status::executing(false));
        }

        // verify all datanodes return the same raw schemas
        // Safety: previous check ensures this vector is not empty.
        let first = raw_schemas.first().unwrap();
        ensure!(
            raw_schemas.iter().all(|x| x == first),
            MetadataCorruptionSnafu {
                err_msg: "Raw schemas from datanodes are not the same"
            }
        );

        // decode raw schemas and store it
        if let Some(raw_schema) = first {
            let physical_columns =
                ColumnMetadata::decode_list(raw_schema).context(DecodeJsonSnafu)?;
            self.data.physical_columns = physical_columns;
        } else {
            warn!("creating logical table result doesn't contains extension key `{ALTER_PHYSICAL_EXTENSION_KEY}`,leaving the physical table's schema unchanged");
        }

        self.data.state = CreateTablesState::CreateMetadata;

        Ok(Status::executing(true))
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
        lock_key.push(TableLock::Write(self.data.physical_table_id()).into());

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
    cluster_id: ClusterId,
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

    fn physical_table_id(&self) -> TableId {
        self.physical_table_id
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

    /// Generate the new physical table info.
    ///
    /// This method will consumes the physical columns.
    fn build_new_physical_table_info(
        &mut self,
        old_table_info: &DeserializedValueWithBytes<TableInfoValue>,
    ) -> RawTableInfo {
        let raw_table_info = old_table_info.deref().table_info.clone();

        physical_table_metadata::build_new_physical_table_info(
            raw_table_info,
            &self.physical_columns,
        )
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
