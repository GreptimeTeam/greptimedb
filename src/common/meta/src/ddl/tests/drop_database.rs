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
    execute_procedure_until, execute_procedure_until_done, MockContextProvider,
};
use futures::TryStreamExt;

use crate::ddl::drop_database::executor::DropDatabaseExecutor;
use crate::ddl::drop_database::DropDatabaseProcedure;
use crate::ddl::test_util::datanode_handler::{NaiveDatanodeHandler, RetryErrorDatanodeHandler};
use crate::ddl::test_util::{create_logical_table, create_physical_table};
use crate::key::schema_name::SchemaNameKey;
use crate::test_util::{new_ddl_context, MockDatanodeManager};

#[tokio::test]
async fn test_drop_database_with_logical_tables() {
    common_telemetry::init_default_ut_logging();
    let cluster_id = 1;
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
    let phy_id = create_physical_table(&ddl_context, cluster_id, "phy").await;
    // Creates 3 logical tables
    create_logical_table(ddl_context.clone(), cluster_id, phy_id, "table1").await;
    create_logical_table(ddl_context.clone(), cluster_id, phy_id, "table2").await;
    create_logical_table(ddl_context.clone(), cluster_id, phy_id, "table3").await;

    let mut procedure = DropDatabaseProcedure::new(
        DEFAULT_CATALOG_NAME.to_string(),
        DEFAULT_SCHEMA_NAME.to_string(),
        false,
        ddl_context.clone(),
    );

    let ctx = ProcedureContext {
        procedure_id: ProcedureId::random(),
        provider: Arc::new(MockContextProvider::default()),
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
    let cluster_id = 1;
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
    let phy_id = create_physical_table(&ddl_context, cluster_id, "phy").await;
    // Creates 3 logical tables
    create_logical_table(ddl_context.clone(), cluster_id, phy_id, "table1").await;
    create_logical_table(ddl_context.clone(), cluster_id, phy_id, "table2").await;
    create_logical_table(ddl_context.clone(), cluster_id, phy_id, "table3").await;

    let mut procedure = DropDatabaseProcedure::new(
        DEFAULT_CATALOG_NAME.to_string(),
        DEFAULT_SCHEMA_NAME.to_string(),
        false,
        ddl_context.clone(),
    );

    let ctx = ProcedureContext {
        procedure_id: ProcedureId::random(),
        provider: Arc::new(MockContextProvider::default()),
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
    let cluster_id = 1;
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
    let phy_id = create_physical_table(&ddl_context, cluster_id, "phy").await;
    // Creates a logical tables
    create_logical_table(ddl_context.clone(), cluster_id, phy_id, "table1").await;
    let mut procedure = DropDatabaseProcedure::new(
        DEFAULT_CATALOG_NAME.to_string(),
        DEFAULT_SCHEMA_NAME.to_string(),
        false,
        ddl_context.clone(),
    );
    let num_operating_regions = 1;
    // Before dropping the logical table
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
