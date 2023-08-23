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

#![feature(assert_matches)]

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::time::Duration;

    use catalog::remote::mock::MockTableEngine;
    use catalog::remote::region_alive_keeper::RegionAliveKeepers;
    use catalog::remote::{CachedMetaKvBackend, RemoteCatalogManager};
    use catalog::{CatalogManager, RegisterSchemaRequest, RegisterTableRequest};
    use common_catalog::consts::{
        DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, INFORMATION_SCHEMA_NAME, MITO_ENGINE,
    };
    use common_meta::helper::CatalogValue;
    use common_meta::ident::TableIdent;
    use common_meta::key::catalog_name::CatalogNameKey;
    use common_meta::key::TableMetadataManager;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::kv_backend::KvBackend;
    use common_meta::rpc::store::{CompareAndPutRequest, PutRequest};
    use datatypes::schema::RawSchema;
    use table::engine::manager::{MemoryTableEngineManager, TableEngineManagerRef};
    use table::engine::{EngineContext, TableEngineRef};
    use table::requests::CreateTableRequest;
    use table::test_util::EmptyTable;
    use tokio::time::Instant;

    struct TestingComponents {
        catalog_manager: Arc<RemoteCatalogManager>,
        table_engine_manager: TableEngineManagerRef,
        region_alive_keepers: Arc<RegionAliveKeepers>,
    }

    impl TestingComponents {
        fn table_engine(&self) -> TableEngineRef {
            self.table_engine_manager.engine(MITO_ENGINE).unwrap()
        }
    }

    #[tokio::test]
    async fn test_cached_backend() {
        let backend = CachedMetaKvBackend::wrap(Arc::new(MemoryKvBackend::default()));

        let default_catalog_key = CatalogNameKey::new(DEFAULT_CATALOG_NAME).to_string();

        let req = PutRequest::new()
            .with_key(default_catalog_key.as_bytes())
            .with_value(CatalogValue.as_bytes().unwrap());
        backend.put(req).await.unwrap();

        let ret = backend.get(b"__catalog_name/greptime").await.unwrap();
        let _ = ret.unwrap();

        let req = CompareAndPutRequest::new()
            .with_key(b"__catalog_name/greptime".to_vec())
            .with_expect(CatalogValue.as_bytes().unwrap())
            .with_value(b"123".to_vec());
        let _ = backend.compare_and_put(req).await.unwrap();

        let ret = backend.get(b"__catalog_name/greptime").await.unwrap();
        assert_eq!(b"123", ret.as_ref().unwrap().value.as_slice());

        let req = PutRequest::new()
            .with_key(b"__catalog_name/greptime".to_vec())
            .with_value(b"1234".to_vec());
        let _ = backend.put(req).await;

        let ret = backend.get(b"__catalog_name/greptime").await.unwrap();
        assert_eq!(b"1234", ret.unwrap().value.as_slice());

        backend
            .delete(b"__catalog_name/greptime", false)
            .await
            .unwrap();

        let ret = backend.get(b"__catalog_name/greptime").await.unwrap();
        assert!(ret.is_none());
    }

    async fn prepare_components(node_id: u64) -> TestingComponents {
        let backend = Arc::new(MemoryKvBackend::default());

        let req = PutRequest::new()
            .with_key(b"__catalog_name/greptime".to_vec())
            .with_value(b"".to_vec());
        backend.put(req).await.unwrap();

        let req = PutRequest::new()
            .with_key(b"__schema_name/greptime-public".to_vec())
            .with_value(b"".to_vec());
        backend.put(req).await.unwrap();

        let cached_backend = Arc::new(CachedMetaKvBackend::wrap(backend));

        let table_engine = Arc::new(MockTableEngine::default());
        let engine_manager = Arc::new(MemoryTableEngineManager::alias(
            MITO_ENGINE.to_string(),
            table_engine,
        ));

        let region_alive_keepers = Arc::new(RegionAliveKeepers::new(engine_manager.clone(), 5000));

        let catalog_manager = RemoteCatalogManager::new(
            engine_manager.clone(),
            node_id,
            region_alive_keepers.clone(),
            Arc::new(TableMetadataManager::new(cached_backend)),
        );
        catalog_manager.start().await.unwrap();

        TestingComponents {
            catalog_manager: Arc::new(catalog_manager),
            table_engine_manager: engine_manager,
            region_alive_keepers,
        }
    }

    #[tokio::test]
    async fn test_remote_catalog_default() {
        common_telemetry::init_default_ut_logging();
        let node_id = 42;
        let TestingComponents {
            catalog_manager, ..
        } = prepare_components(node_id).await;
        assert_eq!(
            vec![DEFAULT_CATALOG_NAME.to_string()],
            catalog_manager.catalog_names().await.unwrap()
        );

        let mut schema_names = catalog_manager
            .schema_names(DEFAULT_CATALOG_NAME)
            .await
            .unwrap();
        schema_names.sort_unstable();
        assert_eq!(
            vec![
                INFORMATION_SCHEMA_NAME.to_string(),
                DEFAULT_SCHEMA_NAME.to_string()
            ],
            schema_names
        );
    }

    #[tokio::test]
    async fn test_remote_catalog_register_nonexistent() {
        common_telemetry::init_default_ut_logging();
        let node_id = 42;
        let components = prepare_components(node_id).await;

        // register a new table with an nonexistent catalog
        let catalog_name = "nonexistent_catalog".to_string();
        let schema_name = "nonexistent_schema".to_string();
        let table_name = "fail_table".to_string();
        // this schema has no effect
        let table_schema = RawSchema::new(vec![]);
        let table = components
            .table_engine()
            .create_table(
                &EngineContext {},
                CreateTableRequest {
                    id: 1,
                    catalog_name: catalog_name.clone(),
                    schema_name: schema_name.clone(),
                    table_name: table_name.clone(),
                    desc: None,
                    schema: table_schema,
                    region_numbers: vec![0],
                    primary_key_indices: vec![],
                    create_if_not_exists: false,
                    table_options: Default::default(),
                    engine: MITO_ENGINE.to_string(),
                },
            )
            .await
            .unwrap();
        let reg_req = RegisterTableRequest {
            catalog: catalog_name,
            schema: schema_name,
            table_name,
            table_id: 1,
            table,
        };
        let res = components.catalog_manager.register_table(reg_req).await;

        // because nonexistent_catalog does not exist yet.
        assert_matches!(
            res.err().unwrap(),
            catalog::error::Error::CatalogNotFound { .. }
        );
    }

    #[tokio::test]
    async fn test_register_table() {
        let node_id = 42;
        let components = prepare_components(node_id).await;
        let mut schema_names = components
            .catalog_manager
            .schema_names(DEFAULT_CATALOG_NAME)
            .await
            .unwrap();
        schema_names.sort_unstable();
        assert_eq!(
            vec![
                INFORMATION_SCHEMA_NAME.to_string(),
                DEFAULT_SCHEMA_NAME.to_string(),
            ],
            schema_names
        );

        // register a new table with an nonexistent catalog
        let catalog_name = DEFAULT_CATALOG_NAME.to_string();
        let schema_name = DEFAULT_SCHEMA_NAME.to_string();
        let table_name = "test_table".to_string();
        let table_id = 1;
        // this schema has no effect
        let table_schema = RawSchema::new(vec![]);
        let table = components
            .table_engine()
            .create_table(
                &EngineContext {},
                CreateTableRequest {
                    id: table_id,
                    catalog_name: catalog_name.clone(),
                    schema_name: schema_name.clone(),
                    table_name: table_name.clone(),
                    desc: None,
                    schema: table_schema,
                    region_numbers: vec![0],
                    primary_key_indices: vec![],
                    create_if_not_exists: false,
                    table_options: Default::default(),
                    engine: MITO_ENGINE.to_string(),
                },
            )
            .await
            .unwrap();
        let reg_req = RegisterTableRequest {
            catalog: catalog_name,
            schema: schema_name,
            table_name: table_name.clone(),
            table_id,
            table,
        };
        assert!(components
            .catalog_manager
            .register_table(reg_req)
            .await
            .unwrap());
        assert_eq!(
            vec![table_name],
            components
                .catalog_manager
                .table_names(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_register_catalog_schema_table() {
        let node_id = 42;
        let components = prepare_components(node_id).await;

        let catalog_name = "test_catalog".to_string();
        let schema_name = "nonexistent_schema".to_string();

        // register catalog to catalog manager
        assert!(components
            .catalog_manager
            .clone()
            .register_catalog(catalog_name.clone())
            .await
            .is_ok());
        assert_eq!(
            HashSet::<String>::from_iter(vec![
                DEFAULT_CATALOG_NAME.to_string(),
                catalog_name.clone()
            ]),
            HashSet::from_iter(components.catalog_manager.catalog_names().await.unwrap())
        );

        let table_to_register = components
            .table_engine()
            .create_table(
                &EngineContext {},
                CreateTableRequest {
                    id: 2,
                    catalog_name: catalog_name.clone(),
                    schema_name: schema_name.clone(),
                    table_name: "".to_string(),
                    desc: None,
                    schema: RawSchema::new(vec![]),
                    region_numbers: vec![0],
                    primary_key_indices: vec![],
                    create_if_not_exists: false,
                    table_options: Default::default(),
                    engine: MITO_ENGINE.to_string(),
                },
            )
            .await
            .unwrap();

        let reg_req = RegisterTableRequest {
            catalog: catalog_name.clone(),
            schema: schema_name.clone(),
            table_name: " fail_table".to_string(),
            table_id: 2,
            table: table_to_register,
        };
        // this register will fail since schema does not exist yet
        assert_matches!(
            components
                .catalog_manager
                .register_table(reg_req.clone())
                .await
                .unwrap_err(),
            catalog::error::Error::SchemaNotFound { .. }
        );

        let register_schema_request = RegisterSchemaRequest {
            catalog: catalog_name.to_string(),
            schema: schema_name.to_string(),
        };
        assert!(components
            .catalog_manager
            .register_schema(register_schema_request)
            .await
            .expect("Register schema should not fail"));
        assert!(components
            .catalog_manager
            .register_table(reg_req)
            .await
            .unwrap());

        assert_eq!(
            HashSet::from([schema_name.clone(), INFORMATION_SCHEMA_NAME.to_string()]),
            components
                .catalog_manager
                .schema_names(&catalog_name)
                .await
                .unwrap()
                .into_iter()
                .collect()
        )
    }

    #[tokio::test]
    async fn test_register_table_before_and_after_region_alive_keeper_started() {
        let components = prepare_components(42).await;
        let catalog_manager = &components.catalog_manager;
        let region_alive_keepers = &components.region_alive_keepers;

        let table_before = TableIdent {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table: "table_before".to_string(),
            table_id: 1,
            engine: MITO_ENGINE.to_string(),
        };
        let request = RegisterTableRequest {
            catalog: table_before.catalog.clone(),
            schema: table_before.schema.clone(),
            table_name: table_before.table.clone(),
            table_id: table_before.table_id,
            table: Arc::new(EmptyTable::new(CreateTableRequest {
                id: table_before.table_id,
                catalog_name: table_before.catalog.clone(),
                schema_name: table_before.schema.clone(),
                table_name: table_before.table.clone(),
                desc: None,
                schema: RawSchema::new(vec![]),
                region_numbers: vec![0],
                primary_key_indices: vec![],
                create_if_not_exists: false,
                table_options: Default::default(),
                engine: MITO_ENGINE.to_string(),
            })),
        };
        assert!(catalog_manager.register_table(request).await.unwrap());

        let keeper = region_alive_keepers
            .find_keeper(table_before.table_id)
            .await
            .unwrap();
        let deadline = keeper.deadline(0).await.unwrap();
        let far_future = Instant::now() + Duration::from_secs(86400 * 365 * 29);
        // assert region alive countdown is not started
        assert!(deadline > far_future);

        region_alive_keepers.start().await;

        let table_after = TableIdent {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table: "table_after".to_string(),
            table_id: 2,
            engine: MITO_ENGINE.to_string(),
        };
        let request = RegisterTableRequest {
            catalog: table_after.catalog.clone(),
            schema: table_after.schema.clone(),
            table_name: table_after.table.clone(),
            table_id: table_after.table_id,
            table: Arc::new(EmptyTable::new(CreateTableRequest {
                id: table_after.table_id,
                catalog_name: table_after.catalog.clone(),
                schema_name: table_after.schema.clone(),
                table_name: table_after.table.clone(),
                desc: None,
                schema: RawSchema::new(vec![]),
                region_numbers: vec![0],
                primary_key_indices: vec![],
                create_if_not_exists: false,
                table_options: Default::default(),
                engine: MITO_ENGINE.to_string(),
            })),
        };
        assert!(catalog_manager.register_table(request).await.unwrap());

        let keeper = region_alive_keepers
            .find_keeper(table_after.table_id)
            .await
            .unwrap();
        let deadline = keeper.deadline(0).await.unwrap();
        // assert countdown is started for the table registered after [RegionAliveKeepers] started
        assert!(deadline <= Instant::now() + Duration::from_secs(20));

        let keeper = region_alive_keepers
            .find_keeper(table_before.table_id)
            .await
            .unwrap();
        let deadline = keeper.deadline(0).await.unwrap();
        // assert countdown is started for the table registered before [RegionAliveKeepers] started, too
        assert!(deadline <= Instant::now() + Duration::from_secs(20));
    }
}
