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

use api::v1::region::{region_request, DropRequest as PbDropRegionRequest};
use api::v1::DropTableExpr;
use async_trait::async_trait;
use client::region::RegionRequester;
use client::Database;
use common_catalog::consts::MITO2_ENGINE;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_meta::cache_invalidator::Context;
use common_meta::ident::TableIdent;
use common_meta::key::table_info::TableInfoValue;
use common_meta::key::table_name::TableNameKey;
use common_meta::key::table_route::TableRouteValue;
use common_meta::rpc::ddl::DropTableTask;
use common_meta::rpc::router::{find_leader_regions, find_leaders, RegionRoute};
use common_meta::table_name::TableName;
use common_procedure::error::{FromJsonSnafu, ToJsonSnafu};
use common_procedure::{
    Context as ProcedureContext, LockKey, Procedure, Result as ProcedureResult, Status,
};
use common_telemetry::{debug, info};
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use store_api::storage::RegionId;
use strum::AsRefStr;
use table::engine::TableReference;
use table::metadata::{RawTableInfo, TableId};

use super::utils::handle_retry_error;
use crate::ddl::DdlContext;
use crate::error::{self, Result, TableMetadataManagerSnafu};
use crate::metrics;
use crate::procedure::utils::handle_request_datanode_error;

pub struct DropTableProcedure {
    context: DdlContext,
    data: DropTableData,
}

impl DropTableProcedure {
    pub(crate) const TYPE_NAME: &'static str = "metasrv-procedure::DropTable";

    pub(crate) fn new(
        cluster_id: u64,
        task: DropTableTask,
        table_route_value: TableRouteValue,
        table_info_value: TableInfoValue,
        context: DdlContext,
    ) -> Self {
        Self {
            context,
            data: DropTableData::new(cluster_id, task, table_route_value, table_info_value),
        }
    }

    pub(crate) fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data = serde_json::from_str(json).context(FromJsonSnafu)?;
        Ok(Self { context, data })
    }

    async fn on_prepare(&mut self) -> Result<Status> {
        let table_ref = &self.data.table_ref();

        let exist = self
            .context
            .table_metadata_manager
            .table_name_manager()
            .exists(TableNameKey::new(
                table_ref.catalog,
                table_ref.schema,
                table_ref.table,
            ))
            .await
            .context(TableMetadataManagerSnafu)?;

        ensure!(
            exist,
            error::TableNotFoundSnafu {
                name: table_ref.to_string()
            }
        );

        self.data.state = DropTableState::RemoveMetadata;

        Ok(Status::executing(true))
    }

    /// Removes the table metadata.
    async fn on_remove_metadata(&mut self) -> Result<Status> {
        let table_metadata_manager = &self.context.table_metadata_manager;
        let table_info_value = &self.data.table_info_value;
        let table_route_value = &self.data.table_route_value;
        let table_id = self.data.table_id();

        table_metadata_manager
            .delete_table_metadata(table_info_value, table_route_value)
            .await
            .context(error::TableMetadataManagerSnafu)?;

        info!("Deleted table metadata for table {table_id}");

        self.data.state = DropTableState::InvalidateTableCache;

        Ok(Status::executing(true))
    }

    /// Broadcasts invalidate table cache instruction.
    async fn on_broadcast(&mut self) -> Result<Status> {
        let table_name = self.data.table_name();
        let engine = &self.data.table_info().meta.engine;

        let table_ident = TableIdent {
            catalog: table_name.catalog_name,
            schema: table_name.schema_name,
            table: table_name.table_name,
            table_id: self.data.task.table_id,
            engine: engine.to_string(),
        };

        self.context
            .cache_invalidator
            .invalidate_table(
                &Context {
                    subject: Some("Invalidate Table Cache by dropping table procedure".to_string()),
                },
                table_ident,
            )
            .await
            .context(error::InvalidateTableCacheSnafu)?;

        self.data.state = if engine == MITO2_ENGINE {
            DropTableState::DatanodeDropRegions
        } else {
            DropTableState::DatanodeDropTable
        };

        Ok(Status::executing(true))
    }

    async fn on_datanode_drop_regions(&self) -> Result<Status> {
        let table_id = self.data.table_id();

        let region_routes = &self.data.region_routes();
        let leaders = find_leaders(region_routes);
        let mut drop_region_tasks = Vec::with_capacity(leaders.len());

        for datanode in leaders {
            let clients = self.context.datanode_clients.clone();

            let regions = find_leader_regions(region_routes, &datanode);
            let region_ids = regions
                .iter()
                .map(|region_number| RegionId::new(table_id, *region_number))
                .collect::<Vec<_>>();

            drop_region_tasks.push(async move {
                for region_id in region_ids {
                    debug!("Dropping region {region_id} on Datanode {datanode:?}");

                    let request = region_request::Body::Drop(PbDropRegionRequest {
                        region_id: region_id.as_u64(),
                    });

                    let client = clients.get_client(&datanode).await;
                    let requester = RegionRequester::new(client);

                    if let Err(err) = requester.handle(request).await {
                        if err.status_code() != StatusCode::RegionNotFound {
                            return Err(handle_request_datanode_error(datanode)(err));
                        }
                    }
                }
                Ok(())
            });
        }

        join_all(drop_region_tasks)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        Ok(Status::Done)
    }

    /// Executes drop table instruction on datanode.
    async fn on_datanode_drop_table(&mut self) -> Result<Status> {
        let region_routes = &self.data.region_routes();

        let table_ref = self.data.table_ref();
        let table_id = self.data.task.table_id;

        let clients = self.context.datanode_clients.clone();
        let leaders = find_leaders(region_routes);
        let mut joins = Vec::with_capacity(leaders.len());

        let expr = DropTableExpr {
            catalog_name: table_ref.catalog.to_string(),
            schema_name: table_ref.schema.to_string(),
            table_name: table_ref.table.to_string(),
            table_id: Some(api::v1::TableId { id: table_id }),
        };

        for datanode in leaders {
            debug!("Dropping table {table_ref} on Datanode {datanode:?}");

            let client = clients.get_client(&datanode).await;
            let client = Database::new(table_ref.catalog, table_ref.schema, client);
            let expr = expr.clone();
            joins.push(common_runtime::spawn_bg(async move {
                if let Err(err) = client.drop_table(expr).await {
                    // TODO(weny): add tests for `TableNotFound`
                    if err.status_code() != StatusCode::TableNotFound {
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

        Ok(Status::Done)
    }
}

#[async_trait]
impl Procedure for DropTableProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let state = &self.data.state;

        let _timer = common_telemetry::timer!(
            metrics::METRIC_META_PROCEDURE_DROP_TABLE,
            &[("step", state.as_ref().to_string())]
        );

        match self.data.state {
            DropTableState::Prepare => self.on_prepare().await,
            DropTableState::RemoveMetadata => self.on_remove_metadata().await,
            DropTableState::InvalidateTableCache => self.on_broadcast().await,
            DropTableState::DatanodeDropTable => self.on_datanode_drop_table().await,
            DropTableState::DatanodeDropRegions => self.on_datanode_drop_regions().await,
        }
        .map_err(handle_retry_error)
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        let table_ref = &self.data.table_ref();
        let key = common_catalog::format_full_table_name(
            table_ref.catalog,
            table_ref.schema,
            table_ref.table,
        );

        LockKey::single(key)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DropTableData {
    state: DropTableState,
    cluster_id: u64,
    task: DropTableTask,
    table_route_value: TableRouteValue,
    table_info_value: TableInfoValue,
}

impl DropTableData {
    pub fn new(
        cluster_id: u64,
        task: DropTableTask,
        table_route_value: TableRouteValue,
        table_info_value: TableInfoValue,
    ) -> Self {
        Self {
            state: DropTableState::Prepare,
            cluster_id,
            task,
            table_info_value,
            table_route_value,
        }
    }

    fn table_ref(&self) -> TableReference {
        self.task.table_ref()
    }

    fn table_name(&self) -> TableName {
        self.task.table_name()
    }

    fn region_routes(&self) -> &Vec<RegionRoute> {
        &self.table_route_value.region_routes
    }

    fn table_info(&self) -> &RawTableInfo {
        &self.table_info_value.table_info
    }

    fn table_id(&self) -> TableId {
        self.table_info().ident.table_id
    }
}

#[derive(Debug, Serialize, Deserialize, AsRefStr)]
enum DropTableState {
    /// Prepares to drop the table
    Prepare,
    /// Removes metadata
    RemoveMetadata,
    /// Invalidates Table Cache
    InvalidateTableCache,
    /// Datanode drops the table
    DatanodeDropTable,
    /// Drop regions on Datanode
    DatanodeDropRegions,
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::procedure::utils::mock::EchoRegionServer;
    use crate::procedure::utils::test_data;

    #[tokio::test]
    async fn test_on_datanode_drop_regions() {
        let drop_table_task = DropTableTask {
            catalog: "my_catalog".to_string(),
            schema: "my_schema".to_string(),
            table: "my_table".to_string(),
            table_id: 42,
        };
        let procedure = DropTableProcedure::new(
            1,
            drop_table_task,
            TableRouteValue::new(test_data::new_region_routes()),
            TableInfoValue::new(test_data::new_table_info()),
            test_data::new_ddl_context(),
        );

        let (region_server, mut rx) = EchoRegionServer::new();

        let datanodes = find_leaders(&procedure.data.table_route_value.region_routes);
        for peer in datanodes {
            let client = region_server.new_client(&peer);
            procedure
                .context
                .datanode_clients
                .insert_client(peer, client)
                .await;
        }

        let expected_dropped_regions = Arc::new(Mutex::new(HashSet::from([
            RegionId::new(42, 1),
            RegionId::new(42, 2),
            RegionId::new(42, 3),
        ])));
        let handle = tokio::spawn({
            let expected_dropped_regions = expected_dropped_regions.clone();
            let mut max_recv = expected_dropped_regions.lock().unwrap().len();
            async move {
                while let Some(region_request::Body::Drop(request)) = rx.recv().await {
                    let region_id = RegionId::from_u64(request.region_id);

                    expected_dropped_regions.lock().unwrap().remove(&region_id);

                    max_recv -= 1;
                    if max_recv == 0 {
                        break;
                    }
                }
            }
        });

        let status = procedure.on_datanode_drop_regions().await.unwrap();
        assert!(matches!(status, Status::Done));

        handle.await.unwrap();

        assert!(expected_dropped_regions.lock().unwrap().is_empty());
    }
}
