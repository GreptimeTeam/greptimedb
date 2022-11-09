#![feature(assert_matches)]

mod mock;

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::collections::HashSet;
    use std::sync::Arc;

    use catalog::remote::{
        KvBackend, KvBackendRef, RemoteCatalogManager, RemoteCatalogProvider, RemoteSchemaProvider,
    };
    use catalog::{CatalogManager, CatalogManagerRef, RegisterTableRequest};
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, MIN_USER_TABLE_ID};
    use common_catalog::{CatalogKey, CatalogValue, SchemaKey, SchemaValue};
    use datatypes::schema::Schema;
    use futures_util::StreamExt;
    use table::engine::{EngineContext, TableEngineRef};
    use table::requests::CreateTableRequest;

    use crate::mock::{MockKvBackend, MockTableEngine};

    #[tokio::test]
    async fn test_backend() {
        common_telemetry::init_default_ut_logging();
        let backend = MockKvBackend::default();

        let default_catalog_key = CatalogKey {
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
        }
        .to_string();

        backend
            .set(
                default_catalog_key.as_bytes(),
                &CatalogValue {}.as_bytes().unwrap(),
            )
            .await
            .unwrap();

        let schema_key = SchemaKey {
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
        }
        .to_string();
        backend
            .set(schema_key.as_bytes(), &SchemaValue {}.as_bytes().unwrap())
            .await
            .unwrap();

        let mut iter = backend.range("__c-".as_bytes());
        let mut res = HashSet::new();
        while let Some(r) = iter.next().await {
            let kv = r.unwrap();
            res.insert(String::from_utf8_lossy(&kv.0).to_string());
        }
        assert_eq!(
            vec!["__c-greptime".to_string()],
            res.into_iter().collect::<Vec<_>>()
        );
    }

    async fn prepare_components(node_id: u64) -> (KvBackendRef, TableEngineRef, CatalogManagerRef) {
        let backend = Arc::new(MockKvBackend::default()) as KvBackendRef;
        let table_engine = Arc::new(MockTableEngine::default());
        let catalog_manager =
            RemoteCatalogManager::new(table_engine.clone(), node_id, backend.clone());
        catalog_manager.start().await.unwrap();
        (backend, table_engine, Arc::new(catalog_manager))
    }

    #[tokio::test]
    async fn test_remote_catalog_default() {
        common_telemetry::init_default_ut_logging();
        let node_id = 42;
        let (_, _, catalog_manager) = prepare_components(node_id).await;
        assert_eq!(
            vec![DEFAULT_CATALOG_NAME.to_string()],
            catalog_manager.catalog_names().unwrap()
        );

        let default_catalog = catalog_manager
            .catalog(DEFAULT_CATALOG_NAME)
            .unwrap()
            .unwrap();
        assert_eq!(
            vec![DEFAULT_SCHEMA_NAME.to_string()],
            default_catalog.schema_names().unwrap()
        );
    }

    #[tokio::test]
    async fn test_remote_catalog_register_nonexistent() {
        common_telemetry::init_default_ut_logging();
        let node_id = 42;
        let (_, table_engine, catalog_manager) = prepare_components(node_id).await;
        // register a new table with an nonexistent catalog
        let catalog_name = "nonexistent_catalog".to_string();
        let schema_name = "nonexistent_schema".to_string();
        let table_name = "fail_table".to_string();
        // this schema has no effect
        let table_schema = Arc::new(Schema::new(vec![]));
        let table = table_engine
            .create_table(
                &EngineContext {},
                CreateTableRequest {
                    id: 1,
                    catalog_name: catalog_name.clone(),
                    schema_name: schema_name.clone(),
                    table_name: table_name.clone(),
                    desc: None,
                    schema: table_schema.clone(),
                    region_numbers: vec![0],
                    primary_key_indices: vec![],
                    create_if_not_exists: false,
                    table_options: Default::default(),
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
        let res = catalog_manager.register_table(reg_req).await;

        // because nonexistent_catalog does not exist yet.
        assert_matches!(
            res.err().unwrap(),
            catalog::error::Error::CatalogNotFound { .. }
        );
    }

    #[tokio::test]
    async fn test_register_table() {
        let node_id = 42;
        let (_, table_engine, catalog_manager) = prepare_components(node_id).await;
        let default_catalog = catalog_manager
            .catalog(DEFAULT_CATALOG_NAME)
            .unwrap()
            .unwrap();
        assert_eq!(
            vec![DEFAULT_SCHEMA_NAME.to_string()],
            default_catalog.schema_names().unwrap()
        );

        let default_schema = default_catalog
            .schema(DEFAULT_SCHEMA_NAME)
            .unwrap()
            .unwrap();
        assert_eq!(vec!["numbers"], default_schema.table_names().unwrap());

        // register a new table with an nonexistent catalog
        let catalog_name = DEFAULT_CATALOG_NAME.to_string();
        let schema_name = DEFAULT_SCHEMA_NAME.to_string();
        let table_name = "test_table".to_string();
        let table_id = 1;
        // this schema has no effect
        let table_schema = Arc::new(Schema::new(vec![]));
        let table = table_engine
            .create_table(
                &EngineContext {},
                CreateTableRequest {
                    id: table_id,
                    catalog_name: catalog_name.clone(),
                    schema_name: schema_name.clone(),
                    table_name: table_name.clone(),
                    desc: None,
                    schema: table_schema.clone(),
                    region_numbers: vec![0],
                    primary_key_indices: vec![],
                    create_if_not_exists: false,
                    table_options: Default::default(),
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
        assert_eq!(1, catalog_manager.register_table(reg_req).await.unwrap());
        assert_eq!(
            HashSet::from([table_name, "numbers".to_string()]),
            default_schema
                .table_names()
                .unwrap()
                .into_iter()
                .collect::<HashSet<_>>()
        );
    }

    #[tokio::test]
    async fn test_register_catalog_schema_table() {
        let node_id = 42;
        let (backend, table_engine, catalog_manager) = prepare_components(node_id).await;

        let catalog_name = "test_catalog".to_string();
        let schema_name = "nonexistent_schema".to_string();
        let catalog = Arc::new(RemoteCatalogProvider::new(
            catalog_name.clone(),
            backend.clone(),
        ));

        // register catalog to catalog manager
        catalog_manager
            .register_catalog(catalog_name.clone(), catalog)
            .unwrap();
        assert_eq!(
            HashSet::<String>::from_iter(
                vec![DEFAULT_CATALOG_NAME.to_string(), catalog_name.clone()].into_iter()
            ),
            HashSet::from_iter(catalog_manager.catalog_names().unwrap().into_iter())
        );

        let table_to_register = table_engine
            .create_table(
                &EngineContext {},
                CreateTableRequest {
                    id: 2,
                    catalog_name: catalog_name.clone(),
                    schema_name: schema_name.clone(),
                    table_name: "".to_string(),
                    desc: None,
                    schema: Arc::new(Schema::new(vec![])),
                    region_numbers: vec![0],
                    primary_key_indices: vec![],
                    create_if_not_exists: false,
                    table_options: Default::default(),
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
            catalog_manager
                .register_table(reg_req.clone())
                .await
                .unwrap_err(),
            catalog::error::Error::SchemaNotFound { .. }
        );

        let new_catalog = catalog_manager
            .catalog(&catalog_name)
            .unwrap()
            .expect("catalog should exist since it's already registered");
        let schema = Arc::new(RemoteSchemaProvider::new(
            catalog_name.clone(),
            schema_name.clone(),
            node_id,
            backend.clone(),
        ));

        let prev = new_catalog
            .register_schema(schema_name.clone(), schema.clone())
            .expect("Register schema should not fail");
        assert!(prev.is_none());
        assert_eq!(1, catalog_manager.register_table(reg_req).await.unwrap());

        assert_eq!(
            HashSet::from([schema_name.clone()]),
            new_catalog.schema_names().unwrap().into_iter().collect()
        )
    }

    #[tokio::test]
    async fn test_next_table_id() {
        let node_id = 42;
        let (_, _, catalog_manager) = prepare_components(node_id).await;
        assert_eq!(
            MIN_USER_TABLE_ID,
            catalog_manager.next_table_id().await.unwrap()
        );

        assert_eq!(
            MIN_USER_TABLE_ID + 1,
            catalog_manager.next_table_id().await.unwrap()
        );
    }
}
