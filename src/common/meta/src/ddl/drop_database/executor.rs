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

use super::cursor::DropDatabaseCursor;
use super::{DropDatabaseContext, DropTableTarget};
use crate::ddl::drop_database::State;
use crate::ddl::drop_table::executor::DropTableExecutor;
use crate::ddl::DdlContext;
use crate::error::{self, Result};
use crate::region_keeper::OperatingRegionGuard;
use crate::rpc::router::{operating_leader_regions, RegionRoute};
use crate::table_name::TableName;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct DropDatabaseExecutor {
    table_id: TableId,
    table_name: TableName,
    pub(crate) region_routes: Vec<RegionRoute>,
    pub(crate) target: DropTableTarget,
    #[serde(skip)]
    dropping_regions: Vec<OperatingRegionGuard>,
}

impl DropDatabaseExecutor {
    /// Returns a new [DropDatabaseExecutor].
    pub fn new(
        table_id: TableId,
        table_name: TableName,
        region_routes: Vec<RegionRoute>,
        target: DropTableTarget,
    ) -> Self {
        Self {
            table_name,
            table_id,
            region_routes,
            target,
            dropping_regions: vec![],
        }
    }
}

impl DropDatabaseExecutor {
    fn register_dropping_regions(&mut self, ddl_ctx: &DdlContext) -> Result<()> {
        let dropping_regions = operating_leader_regions(&self.region_routes);
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
    async fn next(
        &mut self,
        ddl_ctx: &DdlContext,
        _ctx: &mut DropDatabaseContext,
    ) -> Result<(Box<dyn State>, Status)> {
        self.register_dropping_regions(ddl_ctx)?;
        let executor = DropTableExecutor::new(self.table_name.clone(), self.table_id, true);
        executor
            .on_remove_metadata(ddl_ctx, &self.region_routes)
            .await?;
        executor.invalidate_table_cache(ddl_ctx).await?;
        executor
            .on_drop_regions(ddl_ctx, &self.region_routes)
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

    use api::v1::region::{QueryRequest, RegionRequest};
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_error::ext::BoxedError;
    use common_recordbatch::SendableRecordBatchStream;

    use crate::datanode_manager::HandleResponse;
    use crate::ddl::drop_database::cursor::DropDatabaseCursor;
    use crate::ddl::drop_database::executor::DropDatabaseExecutor;
    use crate::ddl::drop_database::{DropDatabaseContext, DropTableTarget, State};
    use crate::ddl::test_util::{create_logical_table, create_physical_table};
    use crate::error::{self, Error, Result};
    use crate::peer::Peer;
    use crate::table_name::TableName;
    use crate::test_util::{new_ddl_context, MockDatanodeHandler, MockDatanodeManager};

    #[derive(Clone)]
    pub struct NaiveDatanodeHandler;

    #[async_trait::async_trait]
    impl MockDatanodeHandler for NaiveDatanodeHandler {
        async fn handle(&self, _peer: &Peer, _request: RegionRequest) -> Result<HandleResponse> {
            Ok(HandleResponse::new(0))
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
        let datanode_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
        let ddl_context = new_ddl_context(datanode_manager);
        let physical_table_id = create_physical_table(ddl_context.clone(), 0, "phy").await;
        let (_, table_route) = ddl_context
            .table_metadata_manager
            .table_route_manager()
            .get_physical_table_route(physical_table_id)
            .await
            .unwrap();
        {
            let mut state = DropDatabaseExecutor::new(
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
            TableName::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, "phy"),
            table_route.region_routes,
            DropTableTarget::Physical,
        );
        let (state, status) = state.next(&ddl_context, &mut ctx).await.unwrap();
        assert!(!status.need_persist());
        let cursor = state.as_any().downcast_ref::<DropDatabaseCursor>().unwrap();
        assert_eq!(cursor.target, DropTableTarget::Physical);
    }

    #[tokio::test]
    async fn test_next_logical_table() {
        let datanode_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
        let ddl_context = new_ddl_context(datanode_manager);
        let physical_table_id = create_physical_table(ddl_context.clone(), 0, "phy").await;
        create_logical_table(ddl_context.clone(), 0, physical_table_id, "metric").await;
        let logical_table_id = physical_table_id + 1;
        let (_, table_route) = ddl_context
            .table_metadata_manager
            .table_route_manager()
            .get_physical_table_route(logical_table_id)
            .await
            .unwrap();
        {
            let mut state = DropDatabaseExecutor::new(
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
            physical_table_id,
            TableName::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, "phy"),
            table_route.region_routes,
            DropTableTarget::Logical,
        );
        let (state, status) = state.next(&ddl_context, &mut ctx).await.unwrap();
        assert!(!status.need_persist());
        let cursor = state.as_any().downcast_ref::<DropDatabaseCursor>().unwrap();
        assert_eq!(cursor.target, DropTableTarget::Logical);
    }

    #[derive(Clone)]
    pub struct RetryErrorDatanodeHandler;

    #[async_trait::async_trait]
    impl MockDatanodeHandler for RetryErrorDatanodeHandler {
        async fn handle(&self, _peer: &Peer, _request: RegionRequest) -> Result<HandleResponse> {
            Err(Error::RetryLater {
                source: BoxedError::new(
                    error::UnexpectedSnafu {
                        err_msg: "retry later",
                    }
                    .build(),
                ),
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
        let datanode_manager = Arc::new(MockDatanodeManager::new(RetryErrorDatanodeHandler));
        let ddl_context = new_ddl_context(datanode_manager);
        let physical_table_id = create_physical_table(ddl_context.clone(), 0, "phy").await;
        let (_, table_route) = ddl_context
            .table_metadata_manager
            .table_route_manager()
            .get_physical_table_route(physical_table_id)
            .await
            .unwrap();
        let mut state = DropDatabaseExecutor::new(
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
}
