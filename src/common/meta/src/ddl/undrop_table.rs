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

use std::collections::{HashMap, HashSet};

use api::v1::region::{
    OpenRequest as PbOpenRegionRequest, RegionRequest, RegionRequestHeader, region_request,
};
use async_trait::async_trait;
use common_procedure::error::{FromJsonSnafu, ToJsonSnafu};
use common_procedure::{
    Context as ProcedureContext, EventContext, EventTrigger, LockKey, Procedure,
    Result as ProcedureResult, Status,
};
use common_telemetry::tracing_context::TracingContext;
use common_telemetry::warn;
use common_wal::options::WalOptions;
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, ensure};
use store_api::storage::{RegionId, RegionNumber};
use strum::AsRefStr;
use table::metadata::TableId;
use table::table_name::TableName;

use crate::ddl::drop_table::executor::DropTableExecutor;
use crate::ddl::table_ddl_event::{
    TableDdlEvent, TableDdlEventType, TableDdlLocator, versioned_table_ddl_payload_or_error,
};
use crate::ddl::utils::{
    add_peer_context_if_needed, convert_region_routes_to_detecting_regions,
    is_metric_engine_logical_table, map_to_procedure_error, region_storage_path,
};
use crate::ddl::{CreateRequestBuilder, DdlContext, build_template_from_raw_table_info};
use crate::error::{self, Result};
use crate::instruction::CacheIdent;
use crate::key::table_name::TableNameKey;
use crate::key::table_route::TableRouteValue;
use crate::lock_key::{CatalogLock, SchemaLock, TableLock, TableNameLock};
use crate::rpc::ddl::UndropTableTask;
use crate::rpc::router::{
    RegionRoute, find_follower_regions, find_followers, find_leader_regions, find_leaders,
};

pub struct UndropTableProcedure {
    context: DdlContext,
    data: UndropTableData,
}

impl UndropTableProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::UndropTable";

    pub fn new(task: UndropTableTask, context: DdlContext) -> Self {
        Self::new_with_original_table_name(task, context, None)
    }

    pub(crate) fn new_with_original_table_name(
        task: UndropTableTask,
        context: DdlContext,
        table_name: Option<TableName>,
    ) -> Self {
        let mut data = UndropTableData::new(task);
        data.table_name = table_name;
        Self { context, data }
    }

    pub fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data: UndropTableData = serde_json::from_str(json).context(FromJsonSnafu)?;
        Ok(Self { context, data })
    }

    pub(crate) async fn on_prepare(&mut self) -> Result<Status> {
        let dropped_table = self
            .context
            .table_metadata_manager
            .get_dropped_table_by_id(self.data.task.table_id)
            .await?
            .with_context(|| error::TableNotFoundSnafu {
                table_name: self.data.task.table_id.to_string(),
            })?;
        let table_name = &dropped_table.table_name;
        ensure!(
            !self
                .context
                .table_metadata_manager
                .table_name_manager()
                .exists(TableNameKey::from(table_name))
                .await?,
            error::TableAlreadyExistsSnafu {
                table_name: table_name.to_string()
            }
        );
        self.data.table_name = Some(dropped_table.table_name.clone());
        self.data.table_route_value = Some(dropped_table.table_route_value.clone());
        self.data.region_wal_options = dropped_table.region_wal_options;
        ensure!(
            !is_metric_engine_logical_table(
                &dropped_table.table_info_value.table_info,
                self.data.table_route_value()
            ),
            error::UnsupportedSnafu {
                operation: "undropping metric logical tables".to_string()
            }
        );
        self.data.table_info = Some(dropped_table.table_info_value.table_info);
        self.data.state = UndropTableState::OpenRegions;
        Ok(Status::executing(true))
    }

    async fn on_restore_metadata(&mut self) -> Result<Status> {
        self.ensure_table_name_loaded().await?;
        let table_name = self.data.table_name().clone();
        let table_route_value = self.data.table_route_value();
        if let Err(err) = self
            .context
            .table_metadata_manager
            .restore_table_metadata(
                self.data.task.table_id,
                &table_name,
                table_route_value,
                &self.data.region_wal_options,
            )
            .await
        {
            let should_cleanup_opened_regions = !err.is_retry_later();
            let err = self.map_restore_metadata_error(err);
            if should_cleanup_opened_regions
                && let Err(cleanup_err) = self.cleanup_opened_regions_after_restore_failure().await
            {
                warn!(
                    cleanup_err;
                    "Failed to close opened regions after undrop metadata restore failure, table_id: {}",
                    self.data.task.table_id
                );
            }
            return Err(err);
        }
        self.data.state = UndropTableState::InvalidateTableCache;
        Ok(Status::executing(true))
    }

    async fn ensure_table_name_loaded(&mut self) -> Result<()> {
        if self.data.table_name.is_some() {
            return Ok(());
        }

        let dropped_table = self
            .context
            .table_metadata_manager
            .get_dropped_table_by_id(self.data.task.table_id)
            .await?
            .with_context(|| error::TableNotFoundSnafu {
                table_name: self.data.task.table_id.to_string(),
            })?;
        self.data.table_name = Some(dropped_table.table_name);
        Ok(())
    }

    fn map_restore_metadata_error(&self, err: error::Error) -> error::Error {
        match err {
            error::Error::TombstoneTargetAlreadyExists { .. } => {
                let table_name = self
                    .data
                    .table_name
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| self.data.task.table_id.to_string());
                error::TableAlreadyExistsSnafu { table_name }.build()
            }
            err => err,
        }
    }

    async fn cleanup_opened_regions_after_restore_failure(&self) -> Result<()> {
        let Some(table_route_value) = self.data.table_route_value.as_ref() else {
            return Ok(());
        };
        let TableRouteValue::Physical(route) = table_route_value else {
            return Ok(());
        };
        let region_routes = route.region_routes.clone();
        let close_result = if let Some(table_name) = self.data.table_name.as_ref() {
            let executor =
                DropTableExecutor::new(table_name.clone(), self.data.task.table_id, false);

            executor
                .on_close_regions(
                    &self.context.node_manager,
                    &self.context.leader_region_registry,
                    &region_routes,
                    false,
                )
                .await
        } else {
            Ok(())
        };
        self.context
            .deregister_failure_detectors(convert_region_routes_to_detecting_regions(
                &region_routes,
            ))
            .await;
        close_result?;
        Ok(())
    }

    async fn on_open_regions(&mut self) -> Result<Status> {
        self.ensure_live_table_not_exists().await?;
        let TableRouteValue::Physical(route) = self.data.table_route_value() else {
            self.data.state = UndropTableState::RestoreMetadata;
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
        self.data.state = UndropTableState::RestoreMetadata;
        Ok(Status::executing(true))
    }

    async fn ensure_live_table_not_exists(&self) -> Result<()> {
        ensure!(
            !self
                .context
                .table_metadata_manager
                .table_name_manager()
                .exists(TableNameKey::from(self.data.table_name()))
                .await?,
            error::TableAlreadyExistsSnafu {
                table_name: self.data.table_name().to_string()
            }
        );
        Ok(())
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
        let mut lock_key = Vec::new();
        if let Some(table_name) = &self.data.table_name {
            lock_key.push(CatalogLock::Read(&table_name.catalog_name).into());
            lock_key
                .push(SchemaLock::read(&table_name.catalog_name, &table_name.schema_name).into());
            lock_key.push(
                TableNameLock::new(
                    &table_name.catalog_name,
                    &table_name.schema_name,
                    &table_name.table_name,
                )
                .into(),
            );
        }
        lock_key.push(TableLock::Write(self.data.task.table_id).into());
        LockKey::new(lock_key)
    }

    fn event(&self, ctx: &EventContext<'_>) -> Option<Box<dyn common_event_recorder::Event>> {
        let event = match &ctx.trigger {
            EventTrigger::Submitted => {
                let locator = self
                    .data
                    .table_name
                    .as_ref()
                    .map(|table_name| {
                        TableDdlLocator::new(
                            &table_name.catalog_name,
                            &table_name.schema_name,
                            &table_name.table_name,
                        )
                    })
                    .unwrap_or_default()
                    .with_table_id(self.data.task.table_id);
                let payload = versioned_table_ddl_payload_or_error(&self.data.task);
                TableDdlEvent::submitted(TableDdlEventType::UndropTable, locator, payload)
            }
            _ => TableDdlEvent::lifecycle(TableDdlEventType::UndropTable),
        };

        Some(Box::new(event))
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
    open_regions_inner(
        context,
        table_id,
        table_name,
        table_info,
        region_routes,
        region_wal_options,
    )
    .await
}

async fn open_regions_inner(
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
    let mut seen_peer_ids = HashSet::new();
    let peers = find_leaders(region_routes)
        .into_iter()
        .chain(find_followers(region_routes))
        .filter(|peer| seen_peer_ids.insert(peer.id));
    let mut tasks = Vec::new();
    for datanode in peers {
        let requester = context.node_manager.datanode(&datanode).await;
        let region_numbers = find_leader_regions(region_routes, &datanode)
            .into_iter()
            .chain(find_follower_regions(region_routes, &datanode));
        for region_number in region_numbers {
            let region_id = RegionId::new(table_id, region_number);
            let create_request = builder.build_one(
                region_id,
                storage_path.clone(),
                region_wal_options,
                &HashMap::new(),
            )?;
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
                if let Err(err) = requester.handle(request).await {
                    return Err(add_peer_context_if_needed(datanode)(err));
                }
                Ok(())
            });
        }
    }

    join_all(tasks)
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    context
        .register_failure_detectors(convert_region_routes_to_detecting_regions(region_routes))
        .await;
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::region::RegionResponse;
    use api::v1::region::region_request;
    use async_trait::async_trait;
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_error::ext::ErrorExt;
    use common_error::status_code::StatusCode;
    use store_api::storage::RegionId;
    use table::table_name::TableName;
    use tokio::sync::{Mutex, mpsc};

    use super::*;
    use crate::ddl::test_util::datanode_handler::DatanodeWatcher;
    use crate::ddl::{DetectingRegion, RegionFailureDetectorController};
    use crate::peer::Peer;
    use crate::rpc::router::{Region, RegionRoute};
    use crate::test_util::{MockDatanodeManager, new_ddl_context};

    #[derive(Default)]
    struct RecordingRegionFailureDetectorController {
        deregistered: Mutex<Vec<DetectingRegion>>,
    }

    #[async_trait]
    impl RegionFailureDetectorController for RecordingRegionFailureDetectorController {
        async fn register_failure_detectors(&self, _detecting_regions: Vec<DetectingRegion>) {}

        async fn reset_failure_detectors(&self, _detecting_regions: Vec<DetectingRegion>) {}

        async fn deregister_failure_detectors(&self, detecting_regions: Vec<DetectingRegion>) {
            self.deregistered.lock().await.extend(detecting_regions);
        }
    }

    #[test]
    fn test_map_restore_metadata_error_without_table_name() {
        let context = new_ddl_context(Arc::new(MockDatanodeManager::new(())));
        let procedure = UndropTableProcedure::new(UndropTableTask { table_id: 42 }, context);

        let err = procedure.map_restore_metadata_error(
            error::TombstoneTargetAlreadyExistsSnafu {
                key: "table-name-key".to_string(),
            }
            .build(),
        );

        assert_eq!(StatusCode::TableAlreadyExists, err.status_code());
    }

    #[tokio::test]
    async fn test_cleanup_opened_regions_without_table_name_deregisters_detectors() {
        let detector_controller = Arc::new(RecordingRegionFailureDetectorController::default());
        let mut context = new_ddl_context(Arc::new(MockDatanodeManager::new(())));
        context.region_failure_detector_controller = detector_controller.clone();

        let table_id = 1024;
        let region_id = RegionId::new(table_id, 1);
        let mut procedure = UndropTableProcedure::new(UndropTableTask { table_id }, context);
        procedure.data.table_route_value = Some(TableRouteValue::physical(vec![RegionRoute {
            region: Region::new_test(region_id),
            leader_peer: Some(Peer::empty(1)),
            follower_peers: vec![],
            leader_state: None,
            leader_down_since: None,
            write_route_policy: None,
        }]));

        procedure
            .cleanup_opened_regions_after_restore_failure()
            .await
            .unwrap();

        assert_eq!(
            detector_controller.deregistered.lock().await.as_slice(),
            &[(1, region_id)]
        );
    }

    #[tokio::test]
    async fn test_cleanup_opened_regions_deregisters_detectors_when_close_fails() {
        let (tx, _rx) = mpsc::channel(8);
        let datanode_handler = DatanodeWatcher::new(tx).with_handler(|_, request| {
            if matches!(request.body, Some(region_request::Body::Close(_))) {
                return error::UnexpectedSnafu {
                    err_msg: "mock close error".to_string(),
                }
                .fail();
            }
            Ok(RegionResponse::new(0))
        });
        let detector_controller = Arc::new(RecordingRegionFailureDetectorController::default());
        let mut context = new_ddl_context(Arc::new(MockDatanodeManager::new(datanode_handler)));
        context.region_failure_detector_controller = detector_controller.clone();

        let table_id = 1024;
        let region_id = RegionId::new(table_id, 1);
        let mut procedure = UndropTableProcedure::new(UndropTableTask { table_id }, context);
        procedure.data.table_name = Some(TableName::new(
            DEFAULT_CATALOG_NAME,
            DEFAULT_SCHEMA_NAME,
            "foo",
        ));
        procedure.data.table_route_value = Some(TableRouteValue::physical(vec![RegionRoute {
            region: Region::new_test(region_id),
            leader_peer: Some(Peer::empty(1)),
            follower_peers: vec![],
            leader_state: None,
            leader_down_since: None,
            write_route_policy: None,
        }]));

        let err = procedure
            .cleanup_opened_regions_after_restore_failure()
            .await
            .unwrap_err();

        assert_eq!(StatusCode::Unexpected, err.status_code());
        assert_eq!(
            detector_controller.deregistered.lock().await.as_slice(),
            &[(1, region_id)]
        );
    }
}
