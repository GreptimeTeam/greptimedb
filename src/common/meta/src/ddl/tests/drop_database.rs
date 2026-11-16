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

use std::sync::Arc;

use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_procedure::{Context as ProcedureContext, Procedure, ProcedureId};
use common_procedure_test::{
    MockContextProvider, execute_procedure_until, execute_procedure_until_done,
};
use futures::TryStreamExt;

use crate::ddl::drop_database::DropDatabaseProcedure;
use crate::ddl::drop_database::executor::{DropDatabaseBatchExecutor, DropDatabaseExecutor};
use crate::ddl::test_util::datanode_handler::{NaiveDatanodeHandler, RetryErrorDatanodeHandler};
use crate::ddl::test_util::{create_logical_table, create_physical_table};
use crate::key::schema_name::SchemaNameKey;
use crate::test_util::{MockDatanodeManager, new_ddl_context};

#[tokio::test]
async fn test_drop_database_with_logical_tables() {
    common_telemetry::init_default_ut_logging();
    let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
    let ddl_context = new_ddl_context(node_manager);
    ddl_context
        .table_metadata_manager
        .schema_manager()
        .create(
            SchemaNameKey::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME),
            None,
            false,
        )
        .await
        .unwrap();
    // Creates physical table
    let phy_id = create_physical_table(&ddl_context, "phy").await;
    // Creates 3 logical tables
    create_logical_table(ddl_context.clone(), phy_id, "table1").await;
    create_logical_table(ddl_context.clone(), phy_id, "table2").await;
    create_logical_table(ddl_context.clone(), phy_id, "table3").await;

    let mut procedure = DropDatabaseProcedure::new(
        DEFAULT_CATALOG_NAME.to_string(),
        DEFAULT_SCHEMA_NAME.to_string(),
        false,
        ddl_context.clone(),
    );

    let ctx = ProcedureContext {
        procedure_id: ProcedureId::random(),
        provider: Arc::new(MockContextProvider::default()),
        event_context: None,
    };

    while !procedure.execute(&ctx).await.unwrap().is_done() {
        procedure.execute(&ctx).await.unwrap();
    }

    let tables = ddl_context
        .table_metadata_manager
        .table_name_manager()
        .tables(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert!(tables.is_empty());
}

#[tokio::test]
async fn test_drop_database_retryable_error() {
    common_telemetry::init_default_ut_logging();
    let node_manager = Arc::new(MockDatanodeManager::new(RetryErrorDatanodeHandler));
    let ddl_context = new_ddl_context(node_manager);
    ddl_context
        .table_metadata_manager
        .schema_manager()
        .create(
            SchemaNameKey::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME),
            None,
            false,
        )
        .await
        .unwrap();
    // Creates physical table
    let phy_id = create_physical_table(&ddl_context, "phy").await;
    // Creates 3 logical tables
    create_logical_table(ddl_context.clone(), phy_id, "table1").await;
    create_logical_table(ddl_context.clone(), phy_id, "table2").await;
    create_logical_table(ddl_context.clone(), phy_id, "table3").await;

    let mut procedure = DropDatabaseProcedure::new(
        DEFAULT_CATALOG_NAME.to_string(),
        DEFAULT_SCHEMA_NAME.to_string(),
        false,
        ddl_context.clone(),
    );

    let ctx = ProcedureContext {
        procedure_id: ProcedureId::random(),
        provider: Arc::new(MockContextProvider::default()),
        event_context: None,
    };

    loop {
        match procedure.execute(&ctx).await {
            Ok(_) => {
                // go next
            }
            Err(err) => {
                assert!(err.is_retry_later());
                break;
            }
        }
    }
}

#[tokio::test]
async fn test_drop_database_recover() {
    common_telemetry::init_default_ut_logging();
    let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
    let ddl_context = new_ddl_context(node_manager);
    ddl_context
        .table_metadata_manager
        .schema_manager()
        .create(
            SchemaNameKey::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME),
            None,
            false,
        )
        .await
        .unwrap();
    // Creates a physical table
    let phy_id = create_physical_table(&ddl_context, "phy").await;
    // Creates a logical tables
    create_logical_table(ddl_context.clone(), phy_id, "table1").await;
    let mut procedure = DropDatabaseProcedure::new(
        DEFAULT_CATALOG_NAME.to_string(),
        DEFAULT_SCHEMA_NAME.to_string(),
        false,
        ddl_context.clone(),
    );
    let num_operating_regions = 1;
    // Recover the persisted logical batch before its metadata is removed.
    execute_procedure_until(&mut procedure, |p| {
        p.state().as_any().is::<DropDatabaseBatchExecutor>()
    })
    .await;
    let data = procedure.dump().unwrap();
    let mut procedure = DropDatabaseProcedure::from_json(&data, ddl_context.clone()).unwrap();
    procedure.recover().unwrap();
    assert_eq!(ddl_context.memory_region_keeper.len(), 0);
    // Before dropping the physical table
    execute_procedure_until(&mut procedure, |p| {
        p.state()
            .as_any()
            .downcast_ref::<DropDatabaseExecutor>()
            .is_some()
    })
    .await;
    // Dump data
    let data = procedure.dump().unwrap();
    assert_eq!(ddl_context.memory_region_keeper.len(), 0);
    let mut procedure = DropDatabaseProcedure::from_json(&data, ddl_context.clone()).unwrap();
    procedure.recover().unwrap();
    assert_eq!(
        ddl_context.memory_region_keeper.len(),
        num_operating_regions
    );
    ddl_context.memory_region_keeper.clear();
    execute_procedure_until_done(&mut procedure).await;
    let tables = ddl_context
        .table_metadata_manager
        .table_name_manager()
        .tables(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert!(tables.is_empty());
}

#[tokio::test]
async fn test_drop_database_batches_recover_after_metadata_deletion() {
    use crate::ddl::test_util::datanode_handler::DatanodeWatcher;
    use api::v1::region::region_request::Body;
    use store_api::storage::RegionId;

    // A full batch, multiple batches, and a trailing partial batch.
    for (count, name_padding, expected_batches) in
        [(0, 0, 0), (1, 0, 1), (64, 0, 1), (129, 0, 3), (20, 2048, 2)]
    {
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        let ddl = new_ddl_context(Arc::new(MockDatanodeManager::new(DatanodeWatcher::new(tx))));
        ddl.table_metadata_manager
            .schema_manager()
            .create(
                SchemaNameKey::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME),
                None,
                false,
            )
            .await
            .unwrap();
        let physical = create_physical_table(&ddl, "z_physical").await;
        let mut ids = Vec::new();
        for i in 0..count {
            create_logical_table(
                ddl.clone(),
                physical,
                &format!("logical_{i:04}{}", "x".repeat(name_padding)),
            )
            .await;
        }
        let mut procedure = DropDatabaseProcedure::new(
            DEFAULT_CATALOG_NAME.into(),
            DEFAULT_SCHEMA_NAME.into(),
            false,
            ddl.clone(),
        );
        let ctx = ProcedureContext {
            procedure_id: ProcedureId::random(),
            provider: Arc::new(MockContextProvider::default()),
            event_context: None,
        };
        let mut batches = 0;
        loop {
            if let Some(batch) = procedure
                .state()
                .as_any()
                .downcast_ref::<DropDatabaseBatchExecutor>()
            {
                assert!(batch.tables.len() <= 64);
                ids.extend(batch.tables.iter().map(|(id, _)| *id));
                // Crash after keys are gone but before cache invalidation and cursor advancement.
                let dump = procedure.dump().unwrap();
                ddl.table_metadata_manager
                    .destroy_logical_tables_metadata(&batch.tables)
                    .await
                    .unwrap();
                procedure = DropDatabaseProcedure::from_json(&dump, ddl.clone()).unwrap();
                procedure.recover().unwrap();
                batches += 1;
            }
            if procedure.execute(&ctx).await.unwrap().is_done() {
                break;
            }
        }
        assert_eq!(ids.len(), count);
        assert_eq!(batches, expected_batches);
        let (_, request) = rx.try_recv().unwrap();
        let Some(Body::Drop(drop)) = request.body else {
            panic!("expected physical drop")
        };
        assert_eq!(RegionId::from_u64(drop.region_id).table_id(), physical);
        assert!(drop.force);
        assert!(
            rx.try_recv().is_err(),
            "logical tables must not send region RPCs"
        );
        assert!(
            ddl.table_metadata_manager
                .table_info_manager()
                .batch_get(&ids)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            ddl.table_metadata_manager
                .table_name_manager()
                .tables(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME)
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .is_empty()
        );
    }
}

#[tokio::test]
async fn test_drop_database_legacy_state_fixture() {
    let ddl = new_ddl_context(Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler)));
    let fixture = include_str!(
        "../../../../../../tests/compatibility/procedure/drop_database/legacy_executor.json"
    );
    let mut procedure = DropDatabaseProcedure::from_json(fixture, ddl).unwrap();
    procedure.recover().unwrap();
    assert!(procedure.state().as_any().is::<DropDatabaseExecutor>());
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(fixture).unwrap(),
        serde_json::from_str::<serde_json::Value>(&procedure.dump().unwrap()).unwrap()
    );
}

#[tokio::test]
async fn test_drop_database_batch_cache_failure_recovery() {
    use crate::cache_invalidator::{CacheInvalidator, Context};
    use crate::instruction::CacheIdent;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct FailingInvalidator {
        fail: AtomicBool,
        calls: Mutex<Vec<Vec<CacheIdent>>>,
    }
    #[async_trait::async_trait]
    impl CacheInvalidator for FailingInvalidator {
        async fn invalidate(&self, _: &Context, caches: &[CacheIdent]) -> crate::error::Result<()> {
            self.calls.lock().unwrap().push(caches.to_vec());
            if self.fail.swap(false, Ordering::Relaxed) {
                return crate::error::UnexpectedSnafu {
                    err_msg: "injected cache failure",
                }
                .fail();
            }
            Ok(())
        }
        fn invalidate_all(&self) -> crate::error::Result<()> {
            // This test invalidator owns only call records, not cached metadata.
            Ok(())
        }
    }
    let invalidator = Arc::new(FailingInvalidator {
        fail: AtomicBool::new(false),
        calls: Mutex::new(Vec::new()),
    });
    let mut ddl = new_ddl_context(Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler)));
    ddl.cache_invalidator = invalidator.clone();
    ddl.table_metadata_manager
        .schema_manager()
        .create(
            SchemaNameKey::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME),
            None,
            false,
        )
        .await
        .unwrap();
    let physical = create_physical_table(&ddl, "physical").await;
    create_logical_table(ddl.clone(), physical, "logical").await;
    let mut procedure = DropDatabaseProcedure::new(
        DEFAULT_CATALOG_NAME.into(),
        DEFAULT_SCHEMA_NAME.into(),
        false,
        ddl.clone(),
    );
    execute_procedure_until(&mut procedure, |p| {
        p.state().as_any().is::<DropDatabaseBatchExecutor>()
    })
    .await;
    let dump = procedure.dump().unwrap();
    invalidator.calls.lock().unwrap().clear();
    invalidator.fail.store(true, Ordering::Relaxed);
    let ctx = ProcedureContext {
        procedure_id: ProcedureId::random(),
        provider: Arc::new(MockContextProvider::default()),
        event_context: None,
    };
    assert!(procedure.execute(&ctx).await.is_err());
    assert_eq!(procedure.dump().unwrap(), dump);
    let mut restored = DropDatabaseProcedure::from_json(&dump, ddl).unwrap();
    restored.recover().unwrap();
    restored.execute(&ctx).await.unwrap();
    let calls = invalidator.calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0], calls[1]);
    assert_eq!(calls[0].len(), 2);
}
