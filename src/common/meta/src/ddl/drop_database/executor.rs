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

use std::any::Any;

use common_procedure::Status;
use common_telemetry::info;
use serde::{Deserialize, Serialize};
use snafu::OptionExt;
use table::metadata::TableId;
use table::table_name::TableName;

use crate::ddl::DdlContext;
use crate::ddl::drop_database::cursor::DropDatabaseCursor;
use crate::ddl::drop_database::{DropDatabaseContext, DropTableTarget, State};
use crate::ddl::drop_table::executor::DropTableExecutor;
use crate::ddl::utils::get_region_wal_options;
use crate::error::{self, Result};
use crate::key::table_route::TableRouteValue;
use crate::region_keeper::OperatingRegionGuard;
use crate::rpc::router::{RegionRoute, operating_leader_regions};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct DropDatabaseExecutor {
    table_id: TableId,
    physical_table_id: TableId,
    table_name: TableName,
    /// The physical table region routes.
    pub(crate) physical_region_routes: Vec<RegionRoute>,
    pub(crate) target: DropTableTarget,
    #[serde(skip)]
    dropping_regions: Vec<OperatingRegionGuard>,
}

impl DropDatabaseExecutor {
    /// Returns a new [DropDatabaseExecutor].
    pub fn new(
        table_id: TableId,
        physical_table_id: TableId,
        table_name: TableName,
        physical_region_routes: Vec<RegionRoute>,
        target: DropTableTarget,
    ) -> Self {
        Self {
            table_id,
            physical_table_id,
            table_name,
            physical_region_routes,
            target,
            dropping_regions: vec![],
        }
    }
}

impl DropDatabaseExecutor {
    /// Registers the operating regions.
    pub(crate) fn register_dropping_regions(&mut self, ddl_ctx: &DdlContext) -> Result<()> {
        if !self.dropping_regions.is_empty() {
            return Ok(());
        }
        let dropping_regions = operating_leader_regions(&self.physical_region_routes);
        let mut dropping_region_guards = Vec::with_capacity(dropping_regions.len());
        for (region_id, datanode_id) in dropping_regions {
            let guard = ddl_ctx
                .memory_region_keeper
                .register(datanode_id, region_id)
                .context(error::RegionOperatingRaceSnafu {
                    region_id,
                    peer_id: datanode_id,
                })?;
            dropping_region_guards.push(guard);
        }
        self.dropping_regions = dropping_region_guards;
        Ok(())
    }
}

#[async_trait::async_trait]
#[typetag::serde]
impl State for DropDatabaseExecutor {
    fn recover(&mut self, ddl_ctx: &DdlContext) -> Result<()> {
        self.register_dropping_regions(ddl_ctx)
    }

    async fn next(
        &mut self,
        ddl_ctx: &DdlContext,
        _ctx: &mut DropDatabaseContext,
    ) -> Result<(Box<dyn State>, Status)> {
        self.register_dropping_regions(ddl_ctx)?;
        let executor = DropTableExecutor::new(self.table_name.clone(), self.table_id, true);
        // Deletes metadata for table permanently.
        let table_route_value = TableRouteValue::new(
            self.table_id,
            self.physical_table_id,
            self.physical_region_routes.clone(),
        );

        // Deletes topic-region mapping if dropping physical table
        let region_wal_options = get_region_wal_options(
            &ddl_ctx.table_metadata_manager,
            &table_route_value,
            self.physical_table_id,
        )
        .await?;

        executor
            .on_destroy_metadata(ddl_ctx, &table_route_value, &region_wal_options)
            .await?;
        executor.invalidate_table_cache(ddl_ctx).await?;
        executor
            .on_drop_regions(
                &ddl_ctx.node_manager,
                &ddl_ctx.leader_region_registry,
                &self.physical_region_routes,
                true,
                false,
            )
            .await?;
        info!("Table: {}({}) is dropped", self.table_name, self.table_id);

        Ok((
            Box::new(DropDatabaseCursor::new(self.target)),
            Status::executing(false),
        ))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::region::RegionResponse;
    use api::v1::region::RegionRequest;
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_error::ext::BoxedError;
    use common_query::request::QueryRequest;
    use common_recordbatch::SendableRecordBatchStream;
    use table::table_name::TableName;

    use crate::ddl::drop_database::cursor::DropDatabaseCursor;
    use crate::ddl::drop_database::executor::DropDatabaseExecutor;
    use crate::ddl::drop_database::{DropDatabaseContext, DropTableTarget, State};
    use crate::ddl::test_util::{create_logical_table, create_physical_table};
    use crate::error::{self, Error, Result};
    use crate::key::datanode_table::DatanodeTableKey;
    use crate::peer::Peer;
    use crate::rpc::router::region_distribution;
    use crate::test_util::{MockDatanodeHandler, MockDatanodeManager, new_ddl_context};

    #[derive(Clone)]
    pub struct NaiveDatanodeHandler;

    #[async_trait::async_trait]
    impl MockDatanodeHandler for NaiveDatanodeHandler {
        async fn handle(&self, _peer: &Peer, _request: RegionRequest) -> Result<RegionResponse> {
            Ok(RegionResponse::new(0))
        }

        async fn handle_query(
            &self,
            _peer: &Peer,
            _request: QueryRequest,
        ) -> Result<SendableRecordBatchStream> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn test_next_with_physical_table() {
        let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
        let ddl_context = new_ddl_context(node_manager);
        let physical_table_id = create_physical_table(&ddl_context, "phy").await;
        let (_, table_route) = ddl_context
            .table_metadata_manager
            .table_route_manager()
            .get_physical_table_route(physical_table_id)
            .await
            .unwrap();
        {
            let mut state = DropDatabaseExecutor::new(
                physical_table_id,
                physical_table_id,
                TableName::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, "phy"),
                table_route.region_routes.clone(),
                DropTableTarget::Physical,
            );
            let mut ctx = DropDatabaseContext {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                drop_if_exists: false,
                tables: None,
            };
            let (state, status) = state.next(&ddl_context, &mut ctx).await.unwrap();
            assert!(!status.need_persist());
            let cursor = state.as_any().downcast_ref::<DropDatabaseCursor>().unwrap();
            assert_eq!(cursor.target, DropTableTarget::Physical);
        }
        // Execute again
        let mut ctx = DropDatabaseContext {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            drop_if_exists: false,
            tables: None,
        };
        let mut state = DropDatabaseExecutor::new(
            physical_table_id,
            physical_table_id,
            TableName::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, "phy"),
            table_route.region_routes.clone(),
            DropTableTarget::Physical,
        );
        let (state, status) = state.next(&ddl_context, &mut ctx).await.unwrap();
        assert!(!status.need_persist());
        let cursor = state.as_any().downcast_ref::<DropDatabaseCursor>().unwrap();
        assert_eq!(cursor.target, DropTableTarget::Physical);
    }

    #[tokio::test]
    async fn test_next_logical_table() {
        let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
        let ddl_context = new_ddl_context(node_manager);
        let physical_table_id = create_physical_table(&ddl_context, "phy").await;
        create_logical_table(ddl_context.clone(), physical_table_id, "metric").await;
        let logical_table_id = physical_table_id + 1;
        let (_, table_route) = ddl_context
            .table_metadata_manager
            .table_route_manager()
            .get_physical_table_route(logical_table_id)
            .await
            .unwrap();
        {
            let mut state = DropDatabaseExecutor::new(
                logical_table_id,
                physical_table_id,
                TableName::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, "metric"),
                table_route.region_routes.clone(),
                DropTableTarget::Logical,
            );
            let mut ctx = DropDatabaseContext {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                drop_if_exists: false,
                tables: None,
            };
            let (state, status) = state.next(&ddl_context, &mut ctx).await.unwrap();
            assert!(!status.need_persist());
            let cursor = state.as_any().downcast_ref::<DropDatabaseCursor>().unwrap();
            assert_eq!(cursor.target, DropTableTarget::Logical);
        }
        // Execute again
        let mut ctx = DropDatabaseContext {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            drop_if_exists: false,
            tables: None,
        };
        let mut state = DropDatabaseExecutor::new(
            logical_table_id,
            physical_table_id,
            TableName::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, "metric"),
            table_route.region_routes,
            DropTableTarget::Logical,
        );
        let (state, status) = state.next(&ddl_context, &mut ctx).await.unwrap();
        assert!(!status.need_persist());
        let cursor = state.as_any().downcast_ref::<DropDatabaseCursor>().unwrap();
        assert_eq!(cursor.target, DropTableTarget::Logical);
        // Checks table info
        ddl_context
            .table_metadata_manager
            .table_info_manager()
            .get(physical_table_id)
            .await
            .unwrap()
            .unwrap();
        // Checks table route
        let table_route = ddl_context
            .table_metadata_manager
            .table_route_manager()
            .table_route_storage()
            .get(physical_table_id)
            .await
            .unwrap()
            .unwrap();
        let region_routes = table_route.region_routes().unwrap();
        for datanode_id in region_distribution(region_routes).into_keys() {
            ddl_context
                .table_metadata_manager
                .datanode_table_manager()
                .get(&DatanodeTableKey::new(datanode_id, physical_table_id))
                .await
                .unwrap()
                .unwrap();
        }
    }

    #[derive(Clone)]
    pub struct RetryErrorDatanodeHandler;

    #[async_trait::async_trait]
    impl MockDatanodeHandler for RetryErrorDatanodeHandler {
        async fn handle(&self, _peer: &Peer, _request: RegionRequest) -> Result<RegionResponse> {
            Err(Error::RetryLater {
                source: BoxedError::new(
                    error::UnexpectedSnafu {
                        err_msg: "retry later",
                    }
                    .build(),
                ),
                clean_poisons: false,
            })
        }

        async fn handle_query(
            &self,
            _peer: &Peer,
            _request: QueryRequest,
        ) -> Result<SendableRecordBatchStream> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn test_next_retryable_err() {
        let node_manager = Arc::new(MockDatanodeManager::new(RetryErrorDatanodeHandler));
        let ddl_context = new_ddl_context(node_manager);
        let physical_table_id = create_physical_table(&ddl_context, "phy").await;
        let (_, table_route) = ddl_context
            .table_metadata_manager
            .table_route_manager()
            .get_physical_table_route(physical_table_id)
            .await
            .unwrap();
        let mut state = DropDatabaseExecutor::new(
            physical_table_id,
            physical_table_id,
            TableName::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, "phy"),
            table_route.region_routes,
            DropTableTarget::Physical,
        );
        let mut ctx = DropDatabaseContext {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            drop_if_exists: false,
            tables: None,
        };
        let err = state.next(&ddl_context, &mut ctx).await.unwrap_err();
        assert!(err.is_retry_later());
    }

    #[tokio::test]
    async fn test_on_recovery() {
        let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
        let ddl_context = new_ddl_context(node_manager);
        let physical_table_id = create_physical_table(&ddl_context, "phy").await;
        let (_, table_route) = ddl_context
            .table_metadata_manager
            .table_route_manager()
            .get_physical_table_route(physical_table_id)
            .await
            .unwrap();
        {
            let mut state = DropDatabaseExecutor::new(
                physical_table_id,
                physical_table_id,
                TableName::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, "phy"),
                table_route.region_routes.clone(),
                DropTableTarget::Physical,
            );
            let mut ctx = DropDatabaseContext {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                drop_if_exists: false,
                tables: None,
            };
            state.recover(&ddl_context).unwrap();
            assert_eq!(state.dropping_regions.len(), 1);
            let (state, status) = state.next(&ddl_context, &mut ctx).await.unwrap();
            assert!(!status.need_persist());
            let cursor = state.as_any().downcast_ref::<DropDatabaseCursor>().unwrap();
            assert_eq!(cursor.target, DropTableTarget::Physical);
        }
    }
}
