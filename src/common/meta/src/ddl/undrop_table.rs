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

use api::v1::region::{
    OpenRequest as PbOpenRegionRequest, RegionRequest, RegionRequestHeader, region_request,
};
use async_trait::async_trait;
use common_procedure::error::{FromJsonSnafu, ToJsonSnafu};
use common_procedure::{
    Context as ProcedureContext, LockKey, Procedure, Result as ProcedureResult, Status,
};
use common_telemetry::tracing_context::TracingContext;
use common_wal::options::WalOptions;
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, ensure};
use store_api::storage::{RegionId, RegionNumber};
use strum::AsRefStr;
use table::metadata::TableId;
use table::table_name::TableName;
use table::table_reference::TableReference;

use crate::ddl::utils::{add_peer_context_if_needed, map_to_procedure_error, region_storage_path};
use crate::ddl::{CreateRequestBuilder, DdlContext, build_template_from_raw_table_info};
use crate::error::{self, Result};
use crate::instruction::CacheIdent;
use crate::key::table_name::TableNameKey;
use crate::key::table_route::TableRouteValue;
use crate::lock_key::{CatalogLock, SchemaLock, TableLock, TableNameLock};
use crate::rpc::ddl::UndropTableTask;
use crate::rpc::router::{RegionRoute, find_leader_regions, find_leaders};

pub struct UndropTableProcedure {
    context: DdlContext,
    data: UndropTableData,
}

impl UndropTableProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::UndropTable";

    pub fn new(task: UndropTableTask, context: DdlContext) -> Self {
        Self {
            context,
            data: UndropTableData::new(task),
        }
    }

    pub fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data: UndropTableData = serde_json::from_str(json).context(FromJsonSnafu)?;
        Ok(Self { context, data })
    }

    pub(crate) async fn on_prepare(&mut self) -> Result<Status> {
        let table_ref = self.data.table_ref();
        ensure!(
            !self
                .context
                .table_metadata_manager
                .table_name_manager()
                .exists(TableNameKey::new(
                    table_ref.catalog,
                    table_ref.schema,
                    table_ref.table
                ))
                .await?,
            error::TableAlreadyExistsSnafu {
                table_name: table_ref.to_string()
            }
        );

        let dropped_table = self
            .context
            .table_metadata_manager
            .get_dropped_table_by_id(self.data.task.table_id)
            .await?
            .with_context(|| error::TableNotFoundSnafu {
                table_name: table_ref.to_string(),
            })?;
        self.data.table_name = Some(dropped_table.table_name.clone());
        self.data.table_route_value = Some(dropped_table.table_route_value.clone());
        self.data.region_wal_options = dropped_table.region_wal_options;
        self.data.table_info = Some(dropped_table.table_info_value.table_info);
        self.data.state = UndropTableState::RestoreMetadata;
        Ok(Status::executing(true))
    }

    async fn on_restore_metadata(&mut self) -> Result<Status> {
        let table_route_value = self.data.table_route_value();
        self.context
            .table_metadata_manager
            .restore_table_metadata(
                self.data.task.table_id,
                self.data.table_name(),
                table_route_value,
                &self.data.region_wal_options,
            )
            .await?;
        self.data.state = UndropTableState::OpenRegions;
        Ok(Status::executing(true))
    }

    async fn on_open_regions(&mut self) -> Result<Status> {
        let TableRouteValue::Physical(route) = self.data.table_route_value() else {
            self.data.state = UndropTableState::InvalidateTableCache;
            return Ok(Status::executing(true));
        };

        open_regions(
            &self.context,
            self.data.task.table_id,
            self.data.table_name(),
            self.data.table_info(),
            &route.region_routes,
            &self.data.region_wal_options,
        )
        .await?;
        self.data.state = UndropTableState::InvalidateTableCache;
        Ok(Status::executing(true))
    }

    async fn on_broadcast(&mut self) -> Result<Status> {
        let ctx = crate::cache_invalidator::Context {
            subject: Some(format!(
                "Invalidate table cache by undropping table {}, table_id: {}",
                self.data.table_name().table_ref(),
                self.data.task.table_id,
            )),
        };
        self.context
            .cache_invalidator
            .invalidate(
                &ctx,
                &[
                    CacheIdent::TableName(self.data.table_name().table_ref().into()),
                    CacheIdent::TableId(self.data.task.table_id),
                ],
            )
            .await?;
        Ok(Status::done())
    }
}

#[async_trait]
impl Procedure for UndropTableProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    fn recover(&mut self) -> ProcedureResult<()> {
        Ok(())
    }

    async fn execute(&mut self, _: &ProcedureContext) -> ProcedureResult<Status> {
        match self.data.state {
            UndropTableState::Prepare => self.on_prepare().await,
            UndropTableState::RestoreMetadata => self.on_restore_metadata().await,
            UndropTableState::OpenRegions => self.on_open_regions().await,
            UndropTableState::InvalidateTableCache => self.on_broadcast().await,
        }
        .map_err(map_to_procedure_error)
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        let table_ref = self.data.table_ref();
        LockKey::new(vec![
            CatalogLock::Read(table_ref.catalog).into(),
            SchemaLock::read(table_ref.catalog, table_ref.schema).into(),
            TableNameLock::new(table_ref.catalog, table_ref.schema, table_ref.table).into(),
            TableLock::Write(self.data.task.table_id).into(),
        ])
    }
}

pub(crate) async fn open_regions(
    context: &DdlContext,
    table_id: TableId,
    table_name: &TableName,
    table_info: &table::metadata::TableInfo,
    region_routes: &[RegionRoute],
    region_wal_options: &HashMap<RegionNumber, WalOptions>,
) -> Result<()> {
    let template = build_template_from_raw_table_info(table_info)?;
    let builder = CreateRequestBuilder::new(template, None);
    let storage_path = region_storage_path(&table_name.catalog_name, &table_name.schema_name);
    let wal_options = region_wal_options
        .iter()
        .map(|(region_number, wal_options)| {
            serde_json::to_string(wal_options)
                .map(|wal_options| (*region_number, wal_options))
                .context(error::SerdeJsonSnafu)
        })
        .collect::<Result<HashMap<_, _>>>()?;
    let leaders = find_leaders(region_routes);
    let mut tasks = Vec::with_capacity(leaders.len());
    for datanode in leaders {
        let requester = context.node_manager.datanode(&datanode).await;
        for region_number in find_leader_regions(region_routes, &datanode) {
            let region_id = RegionId::new(table_id, region_number);
            let create_request = builder.build_one(
                region_id,
                storage_path.clone(),
                &wal_options,
                &HashMap::new(),
            );
            let request = RegionRequest {
                header: Some(RegionRequestHeader {
                    tracing_context: TracingContext::from_current_span().to_w3c(),
                    ..Default::default()
                }),
                body: Some(region_request::Body::Open(PbOpenRegionRequest {
                    region_id: create_request.region_id,
                    engine: create_request.engine,
                    path: create_request.path,
                    options: create_request.options,
                })),
            };
            let datanode = datanode.clone();
            let requester = requester.clone();
            tasks.push(async move {
                requester
                    .handle(request)
                    .await
                    .map_err(add_peer_context_if_needed(datanode))
            });
        }
    }

    join_all(tasks)
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UndropTableData {
    state: UndropTableState,
    task: UndropTableTask,
    table_name: Option<TableName>,
    table_info: Option<table::metadata::TableInfo>,
    table_route_value: Option<TableRouteValue>,
    #[serde(default)]
    region_wal_options: HashMap<RegionNumber, WalOptions>,
}

impl UndropTableData {
    fn new(task: UndropTableTask) -> Self {
        Self {
            state: UndropTableState::Prepare,
            task,
            table_name: None,
            table_info: None,
            table_route_value: None,
            region_wal_options: HashMap::new(),
        }
    }

    fn table_ref(&self) -> TableReference<'_> {
        self.task.table_ref()
    }

    fn table_name(&self) -> &TableName {
        self.table_name.as_ref().unwrap()
    }

    fn table_info(&self) -> &table::metadata::TableInfo {
        self.table_info.as_ref().unwrap()
    }

    fn table_route_value(&self) -> &TableRouteValue {
        self.table_route_value.as_ref().unwrap()
    }
}

#[derive(Debug, Serialize, Deserialize, AsRefStr, PartialEq)]
enum UndropTableState {
    Prepare,
    RestoreMetadata,
    OpenRegions,
    InvalidateTableCache,
}
