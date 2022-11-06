mod mock;

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use catalog::remote::{
        KvBackendRef, OpendalBackend, RemoteCatalogManager, RemoteCatalogProvider,
        RemoteSchemaProvider,
    };
    use catalog::{CatalogList, CatalogManager, CatalogProvider, RegisterTableRequest};
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use datatypes::schema::Schema;
    use table::engine::{EngineContext, TableEngineRef};
    use table::requests::CreateTableRequest;
    use tempdir::TempDir;

    use super::*;

    async fn create_opendal_backend(path: &str) -> Arc<OpendalBackend> {
        let accessor = Arc::new(
            opendal::services::fs::Builder::default()
                .root(path)
                .build()
                .unwrap(),
        );
        Arc::new(OpendalBackend::new(accessor))
    }

    async fn prepare_components(
        node_id: u64,
    ) -> (KvBackendRef, TableEngineRef, RemoteCatalogManager, TempDir) {
        let dir = tempdir::TempDir::new("opendal_test").unwrap();
        let backend = create_opendal_backend(dir.path().to_str().unwrap()).await;
        // let backend = create_opendal_backend("/tmp/remote_catalog").await;
        let engine = Arc::new(mock::MockTableEngine::default());
        let catalog_manager =
            RemoteCatalogManager::new(engine.clone(), node_id, backend.clone() as _);
        catalog_manager.start().await.unwrap();
        (backend, engine, catalog_manager, dir)
    }

    #[tokio::test]
    async fn test_create_backend() {
        let node_id = 42;
        let (_, _, catalog_manager, _dir) = prepare_components(node_id).await;
        assert_eq!(
            vec![DEFAULT_CATALOG_NAME.to_owned()],
            catalog_manager.catalog_names().unwrap()
        );
        let default_catalog = catalog_manager
            .catalog(DEFAULT_CATALOG_NAME)
            .unwrap()
            .expect("default catalog must exist");
        assert_eq!(
            vec![DEFAULT_SCHEMA_NAME.to_owned()],
            default_catalog.schema_names().unwrap()
        );
        let default_schema = default_catalog
            .schema(DEFAULT_SCHEMA_NAME)
            .unwrap()
            .expect("default schema must exist");
        assert_eq!(
            vec!["numbers".to_string()],
            default_schema.table_names().unwrap()
        );
    }

    #[tokio::test]
    async fn test_register_to_default_schema() {
        common_telemetry::init_default_ut_logging();
        let node_id = 42;
        let (_, table_engine, catalog_manager, _dir) = prepare_components(node_id).await;
        let default_schema = catalog_manager
            .catalog(DEFAULT_CATALOG_NAME)
            .unwrap()
            .expect("default catalog must exist")
            .schema(DEFAULT_SCHEMA_NAME)
            .unwrap()
            .expect("default schema must exist");

        let table_name = "some_table";
        let table_id = 42;
        let table = table_engine
            .create_table(
                &EngineContext {},
                CreateTableRequest {
                    id: table_id,
                    catalog_name: DEFAULT_CATALOG_NAME.to_string(),
                    schema_name: DEFAULT_SCHEMA_NAME.to_string(),
                    table_name: table_name.to_owned(),
                    desc: None,
                    schema: Arc::new(Schema::new(vec![])),
                    region_numbers: vec![0],
                    primary_key_indices: vec![],
                    create_if_not_exists: false,
                    table_options: HashMap::from([("table_id".to_string(), table_id.to_string())]),
                },
            )
            .await
            .unwrap();

        assert!(default_schema
            .register_table(table_name.to_owned(), table)
            .unwrap()
            .is_none());
        assert_eq!(
            HashSet::from([table_name.to_owned(), "numbers".to_owned()]),
            default_schema
                .table_names()
                .unwrap()
                .into_iter()
                .collect::<HashSet<_>>()
        );
        let table_read = default_schema
            .table(table_name)
            .unwrap()
            .expect("Should return table just returned");
        assert_eq!(table_name.to_owned(), table_read.table_info().name);
        assert_eq!(table_id, table_read.table_info().ident.table_id);
    }

    #[tokio::test]
    async fn test_register_catalog_and_schema() {
        let node_id = 42;
        let (backend, _, catalog_manager, _dir) = prepare_components(node_id).await;
        let catalog_name = "some_catalog";
        let new_catalog = Arc::new(RemoteCatalogProvider::new(
            catalog_name.to_string(),
            node_id,
            backend.clone(),
        ));
        assert!(catalog_manager
            .register_catalog(catalog_name.to_owned(), new_catalog.clone())
            .unwrap()
            .is_none());

        assert_eq!(
            HashSet::from_iter(
                vec![DEFAULT_CATALOG_NAME.to_string(), catalog_name.to_string()].into_iter()
            ),
            catalog_manager
                .catalog_names()
                .unwrap()
                .into_iter()
                .collect::<HashSet<_>>()
        );

        let schema_name = "some_schema";
        let new_schema = Arc::new(RemoteSchemaProvider::new(
            catalog_name.to_string(),
            schema_name.to_string(),
            node_id,
            backend.clone(),
        ));
        assert!(new_catalog
            .register_schema(schema_name.to_string(), new_schema)
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn test_register_table_to_new_schema() {
        let node_id = 42;
        let (backend, table_engine, catalog_manager, _dir) = prepare_components(node_id).await;
        let catalog_name = "some_catalog";
        let new_catalog = Arc::new(RemoteCatalogProvider::new(
            catalog_name.to_string(),
            node_id,
            backend.clone(),
        ));
        catalog_manager
            .register_catalog(catalog_name.to_owned(), new_catalog.clone())
            .unwrap();
        let schema_name = "some_schema";
        let new_schema = Arc::new(RemoteSchemaProvider::new(
            catalog_name.to_string(),
            schema_name.to_string(),
            node_id,
            backend.clone(),
        ));
        new_catalog
            .register_schema(schema_name.to_string(), new_schema)
            .unwrap();

        let table_name = "some_table";
        let table_id = 42;
        let table = table_engine
            .create_table(
                &EngineContext {},
                CreateTableRequest {
                    id: table_id,
                    catalog_name: catalog_name.to_string(),
                    schema_name: schema_name.to_string(),
                    table_name: table_name.to_owned(),
                    desc: None,
                    schema: Arc::new(Schema::new(vec![])),
                    region_numbers: vec![0],
                    primary_key_indices: vec![],
                    create_if_not_exists: false,
                    table_options: HashMap::from([("table_id".to_string(), table_id.to_string())]),
                },
            )
            .await
            .unwrap();

        assert_eq!(
            1,
            catalog_manager
                .register_table(RegisterTableRequest {
                    catalog: catalog_name.to_string(),
                    schema: schema_name.to_string(),
                    table_name: table_name.to_string(),
                    table_id,
                    table: table.clone(),
                })
                .await
                .unwrap()
        );

        assert_eq!(catalog_name, table.table_info().catalog_name);
        assert_eq!(schema_name, table.table_info().schema_name);
    }
}
