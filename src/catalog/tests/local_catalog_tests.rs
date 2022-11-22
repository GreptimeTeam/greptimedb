#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use catalog::local::LocalCatalogManager;
    use catalog::{CatalogManager, RegisterTableRequest};
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_telemetry::{error, info};
    use mito::config::EngineConfig;
    use table::table::numbers::NumbersTable;
    use table::TableRef;
    use tokio::sync::Mutex;

    async fn create_local_catalog_manager() -> Result<LocalCatalogManager, catalog::error::Error> {
        let (_dir, object_store) =
            mito::table::test_util::new_test_object_store("setup_mock_engine_and_table").await;
        let mock_engine = Arc::new(mito::table::test_util::MockMitoEngine::new(
            EngineConfig::default(),
            mito::table::test_util::MockEngine::default(),
            object_store,
        ));
        let catalog_manager = LocalCatalogManager::try_new(mock_engine).await.unwrap();
        catalog_manager.start().await?;
        Ok(catalog_manager)
    }

    #[tokio::test]
    async fn test_duplicate_register() {
        let catalog_manager = create_local_catalog_manager().await.unwrap();
        let request = RegisterTableRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: "test_table".to_string(),
            table_id: 42,
            table: Arc::new(NumbersTable::new(42)),
        };
        assert_eq!(
            1,
            catalog_manager
                .register_table(request.clone())
                .await
                .unwrap()
        );

        // register table with same table id will succeed with 0 as return val.
        assert_eq!(0, catalog_manager.register_table(request).await.unwrap());

        let err = catalog_manager
            .register_table(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: "test_table".to_string(),
                table_id: 43,
                table: Arc::new(NumbersTable::new(43)),
            })
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("Table `greptime.public.test_table` already exists"),
            "Actual error message: {}",
            err
        );
    }

    #[test]
    fn test_concurrent_register() {
        common_telemetry::init_default_ut_logging();
        let rt = Arc::new(tokio::runtime::Builder::new_multi_thread().build().unwrap());
        let catalog_manager =
            Arc::new(rt.block_on(async { create_local_catalog_manager().await.unwrap() }));

        let succeed: Arc<Mutex<Option<TableRef>>> = Arc::new(Mutex::new(None));

        let mut handles = Vec::with_capacity(8);
        for i in 0..8 {
            let catalog = catalog_manager.clone();
            let succeed = succeed.clone();
            let handle = rt.spawn(async move {
                let table_id = 42 + i;
                let table = Arc::new(NumbersTable::new(table_id));
                let req = RegisterTableRequest {
                    catalog: DEFAULT_CATALOG_NAME.to_string(),
                    schema: DEFAULT_SCHEMA_NAME.to_string(),
                    table_name: "test_table".to_string(),
                    table_id,
                    table: table.clone(),
                };
                match catalog.register_table(req).await {
                    Ok(res) => {
                        if res == 1 {
                            let mut succeed = succeed.lock().await;
                            info!("Successfully registered table: {}", table_id);
                            *succeed = Some(table);
                        }
                    }
                    Err(_) => {
                        error!("Failed to register table {}", table_id);
                    }
                }
            });
            handles.push(handle);
        }

        rt.block_on(async move {
            for handle in handles {
                handle.await.unwrap();
            }
            let guard = succeed.lock().await;
            let table = guard.as_ref().unwrap();
            let table_registered = catalog_manager
                .table(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, "test_table")
                .unwrap()
                .unwrap();
            assert_eq!(
                table_registered.table_info().ident.table_id,
                table.table_info().ident.table_id
            );
        });
    }
}
