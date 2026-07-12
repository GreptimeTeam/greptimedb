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

use api::v1::meta::ReconcileRequest;
use async_trait::async_trait;
use catalog::CatalogManagerRef;
use common_error::ext::BoxedError;
use common_function::handlers::ProcedureServiceHandler;
use common_meta::cache_invalidator::CacheInvalidatorRef;
use common_meta::key::TableMetadataManagerRef;
use common_meta::procedure_executor::{ExecutorContext, ProcedureExecutorRef};
use common_meta::rpc::ddl::{DdlTask, SubmitDdlTaskRequest};
use common_meta::rpc::procedure::{
    GcRegionsRequest as MetaGcRegionsRequest, GcResponse as MetaGcResponse,
    GcTableRequest as MetaGcTableRequest, ManageRegionFollowerRequest, MigrateRegionRequest,
    ProcedureStateResponse,
};
use common_query::error as query_error;
use common_query::error::Result as QueryResult;
use snafu::ResultExt;
use table::table_name::TableName;

use crate::error;
use crate::utils::to_meta_query_context;

/// The operator for procedures which implements [`ProcedureServiceHandler`].
#[derive(Clone)]
pub struct ProcedureServiceOperator {
    procedure_executor: ProcedureExecutorRef,
    catalog_manager: CatalogManagerRef,
    table_metadata_manager: TableMetadataManagerRef,
    cache_invalidator: CacheInvalidatorRef,
}

impl ProcedureServiceOperator {
    pub fn new(
        procedure_executor: ProcedureExecutorRef,
        catalog_manager: CatalogManagerRef,
        table_metadata_manager: TableMetadataManagerRef,
        cache_invalidator: CacheInvalidatorRef,
    ) -> Self {
        Self {
            procedure_executor,
            catalog_manager,
            table_metadata_manager,
            cache_invalidator,
        }
    }
}

#[async_trait]
impl ProcedureServiceHandler for ProcedureServiceOperator {
    async fn purge_table(
        &self,
        table_name: TableName,
        query_ctx: session::context::QueryContextRef,
    ) -> QueryResult<()> {
        let dropped = self
            .table_metadata_manager
            .get_dropped_table(&table_name)
            .await
            .map_err(BoxedError::new)
            .context(query_error::ProcedureServiceSnafu)?
            .ok_or_else(|| {
                error::TableNotFoundSnafu {
                    table_name: table_name.to_string(),
                }
                .build()
            })
            .map_err(BoxedError::new)
            .context(query_error::ProcedureServiceSnafu)?;
        let request = SubmitDdlTaskRequest::new(
            to_meta_query_context(query_ctx),
            DdlTask::new_purge_dropped_table(dropped.table_id),
        );
        self.procedure_executor
            .submit_ddl_task(&ExecutorContext::default(), request)
            .await
            .map_err(BoxedError::new)
            .context(query_error::ProcedureServiceSnafu)?;
        if let Err(err) = self.cache_invalidator.invalidate_all() {
            common_telemetry::warn!(err; "Failed to invalidate caches after purging table '{}'; the purge has already committed", table_name);
        }
        Ok(())
    }

    async fn migrate_region(&self, request: MigrateRegionRequest) -> QueryResult<Option<String>> {
        Ok(self
            .procedure_executor
            .migrate_region(&ExecutorContext::default(), request)
            .await
            .map_err(BoxedError::new)
            .context(query_error::ProcedureServiceSnafu)?
            .pid
            .map(|pid| String::from_utf8_lossy(&pid.key).to_string()))
    }

    async fn reconcile(&self, request: ReconcileRequest) -> QueryResult<Option<String>> {
        Ok(self
            .procedure_executor
            .reconcile(&ExecutorContext::default(), request)
            .await
            .map_err(BoxedError::new)
            .context(query_error::ProcedureServiceSnafu)?
            .pid
            .map(|pid| String::from_utf8_lossy(&pid.key).to_string()))
    }

    async fn query_procedure_state(&self, pid: &str) -> QueryResult<ProcedureStateResponse> {
        self.procedure_executor
            .query_procedure_state(&ExecutorContext::default(), pid)
            .await
            .map_err(BoxedError::new)
            .context(query_error::ProcedureServiceSnafu)
    }

    async fn manage_region_follower(
        &self,
        request: ManageRegionFollowerRequest,
    ) -> QueryResult<()> {
        self.procedure_executor
            .manage_region_follower(&ExecutorContext::default(), request)
            .await
            .map_err(BoxedError::new)
            .context(query_error::ProcedureServiceSnafu)
    }

    fn catalog_manager(&self) -> &CatalogManagerRef {
        &self.catalog_manager
    }

    async fn gc_regions(&self, request: MetaGcRegionsRequest) -> QueryResult<MetaGcResponse> {
        self.procedure_executor
            .gc_regions(&ExecutorContext::default(), request)
            .await
            .map_err(BoxedError::new)
            .context(query_error::ProcedureServiceSnafu)
    }

    async fn gc_table(&self, request: MetaGcTableRequest) -> QueryResult<MetaGcResponse> {
        self.procedure_executor
            .gc_table(&ExecutorContext::default(), request)
            .await
            .map_err(BoxedError::new)
            .context(query_error::ProcedureServiceSnafu)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use api::v1::meta::{ProcedureDetailResponse, ReconcileResponse};
    use common_meta::cache_invalidator::{CacheInvalidator, Context};
    use common_meta::key::TableMetadataManager;
    use common_meta::key::table_route::TableRouteValue;
    use common_meta::key::test_utils::new_test_table_info_with_name;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::procedure_executor::{ProcedureExecutor, ProcedureExecutorRef};
    use common_meta::rpc::ddl::{DdlTask, SubmitDdlTaskRequest, SubmitDdlTaskResponse};
    use common_meta::rpc::procedure::{MigrateRegionResponse, ProcedureStateResponse};
    use session::context::QueryContextBuilder;
    use table::table_name::TableName;

    use super::*;

    #[derive(Default)]
    struct RecordingProcedureExecutor {
        requests: Mutex<Vec<SubmitDdlTaskRequest>>,
        fail: bool,
    }

    #[derive(Default)]
    struct RecordingCacheInvalidator {
        invalidate_all_calls: Mutex<usize>,
        fail: bool,
    }

    #[async_trait]
    impl CacheInvalidator for RecordingCacheInvalidator {
        async fn invalidate(
            &self,
            _: &Context,
            _: &[common_meta::instruction::CacheIdent],
        ) -> common_meta::error::Result<()> {
            Ok(())
        }

        fn invalidate_all(&self) -> common_meta::error::Result<()> {
            *self.invalidate_all_calls.lock().unwrap() += 1;
            if self.fail {
                return common_meta::error::UnsupportedSnafu {
                    operation: "test cache invalidation failure",
                }
                .fail();
            }
            Ok(())
        }
    }

    #[async_trait]
    impl ProcedureExecutor for RecordingProcedureExecutor {
        async fn submit_ddl_task(
            &self,
            _: &ExecutorContext,
            request: SubmitDdlTaskRequest,
        ) -> common_meta::error::Result<SubmitDdlTaskResponse> {
            self.requests.lock().unwrap().push(request);
            if self.fail {
                return common_meta::error::UnsupportedSnafu {
                    operation: "test purge failure",
                }
                .fail();
            }
            Ok(SubmitDdlTaskResponse::default())
        }
        async fn migrate_region(
            &self,
            _: &ExecutorContext,
            _: MigrateRegionRequest,
        ) -> common_meta::error::Result<MigrateRegionResponse> {
            unimplemented!()
        }
        async fn reconcile(
            &self,
            _: &ExecutorContext,
            _: ReconcileRequest,
        ) -> common_meta::error::Result<ReconcileResponse> {
            unimplemented!()
        }
        async fn query_procedure_state(
            &self,
            _: &ExecutorContext,
            _: &str,
        ) -> common_meta::error::Result<ProcedureStateResponse> {
            unimplemented!()
        }
        async fn list_procedures(
            &self,
            _: &ExecutorContext,
        ) -> common_meta::error::Result<ProcedureDetailResponse> {
            unimplemented!()
        }
    }

    async fn operator_with_tombstone(
        table_id: u32,
        name: &TableName,
        fail: bool,
    ) -> (
        ProcedureServiceOperator,
        Arc<RecordingProcedureExecutor>,
        Arc<RecordingCacheInvalidator>,
    ) {
        let manager = Arc::new(TableMetadataManager::new(Arc::new(
            MemoryKvBackend::default(),
        )));
        let mut info = new_test_table_info_with_name(table_id, &name.table_name);
        info.catalog_name = name.catalog_name.clone();
        info.schema_name = name.schema_name.clone();
        let route = TableRouteValue::physical(vec![]);
        manager
            .create_table_metadata(info, route.clone(), HashMap::new())
            .await
            .unwrap();
        manager
            .delete_table_metadata(table_id, name, &route, &HashMap::new(), None)
            .await
            .unwrap();
        let executor = Arc::new(RecordingProcedureExecutor {
            fail,
            ..Default::default()
        });
        let cache = Arc::new(RecordingCacheInvalidator::default());
        let operator = ProcedureServiceOperator::new(
            executor.clone() as ProcedureExecutorRef,
            catalog::memory::MemoryCatalogManager::new(),
            manager,
            cache.clone(),
        );
        (operator, executor, cache)
    }

    #[tokio::test]
    async fn test_purge_table_submits_tombstone_id_and_query_context() {
        let name = TableName::new("catalog", "schema", "metrics");
        let (operator, executor, cache) = operator_with_tombstone(42, &name, false).await;
        let manager = operator.table_metadata_manager.clone();
        let mut live = new_test_table_info_with_name(99, "metrics");
        live.catalog_name = "catalog".to_string();
        live.schema_name = "schema".to_string();
        manager
            .create_table_metadata(live, TableRouteValue::physical(vec![]), HashMap::new())
            .await
            .unwrap();
        let query_ctx = QueryContextBuilder::default()
            .current_catalog("catalog".to_string())
            .current_schema("schema".to_string())
            .build()
            .into();

        operator.purge_table(name.clone(), query_ctx).await.unwrap();

        let requests = executor.requests.lock().unwrap();
        assert!(
            matches!(&requests[0].task, DdlTask::PurgeDroppedTable(task) if task.table_id == 42)
        );
        assert_eq!(requests[0].query_context.current_catalog, "catalog");
        assert_eq!(requests[0].query_context.current_schema, "schema");
        assert_eq!(1, *cache.invalidate_all_calls.lock().unwrap());
    }

    #[tokio::test]
    async fn test_purge_table_missing_tombstone_is_table_not_found() {
        let manager = Arc::new(TableMetadataManager::new(Arc::new(
            MemoryKvBackend::default(),
        )));
        let executor = Arc::new(RecordingProcedureExecutor::default());
        let operator = ProcedureServiceOperator::new(
            executor.clone(),
            catalog::memory::MemoryCatalogManager::new(),
            manager,
            Arc::new(RecordingCacheInvalidator::default()),
        );
        let error = operator
            .purge_table(
                TableName::new("catalog", "schema", "missing"),
                QueryContextBuilder::default()
                    .current_catalog("catalog".to_string())
                    .current_schema("schema".to_string())
                    .build()
                    .into(),
            )
            .await
            .unwrap_err();
        assert_eq!(
            common_error::status_code::StatusCode::TableNotFound,
            common_error::ext::ErrorExt::status_code(&error)
        );
        assert!(executor.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_purge_table_propagates_submission_failure() {
        let name = TableName::new("catalog", "schema", "metrics");
        let (operator, executor, cache) = operator_with_tombstone(42, &name, true).await;
        assert!(
            operator
                .purge_table(
                    name,
                    QueryContextBuilder::default()
                        .current_catalog("catalog".to_string())
                        .current_schema("schema".to_string())
                        .build()
                        .into()
                )
                .await
                .is_err()
        );
        assert_eq!(1, executor.requests.lock().unwrap().len());
        assert_eq!(0, *cache.invalidate_all_calls.lock().unwrap());
    }

    #[tokio::test]
    async fn test_purge_table_ignores_post_submission_cache_invalidation_failure() {
        let name = TableName::new("catalog", "schema", "metrics");
        let (mut operator, executor, _) = operator_with_tombstone(42, &name, false).await;
        let cache = Arc::new(RecordingCacheInvalidator {
            fail: true,
            ..Default::default()
        });
        operator.cache_invalidator = cache.clone();

        operator
            .purge_table(name, QueryContextBuilder::default().build().into())
            .await
            .unwrap();

        assert_eq!(1, executor.requests.lock().unwrap().len());
        assert_eq!(1, *cache.invalidate_all_calls.lock().unwrap());
    }
}
