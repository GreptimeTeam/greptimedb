#![feature(btree_drain_filter)]
#![feature(assert_matches)]

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::fmt::{Display, Formatter};
    use std::ops::RangeInclusive;
    use std::sync::Arc;

    use async_stream::stream;
    use catalog::error::Error;
    use catalog::remote::helper::{CatalogKey, CatalogValue, SchemaKey, SchemaValue};
    use catalog::remote::{
        Kv, KvBackend, KvBackendRef, RemoteCatalogManager, RemoteCatalogProvider,
        RemoteSchemaProvider, ValueIter,
    };
    use catalog::{
        CatalogManager, CatalogManagerRef, RegisterTableRequest, DEFAULT_CATALOG_NAME,
        DEFAULT_SCHEMA_NAME,
    };
    use common_recordbatch::RecordBatch;
    use common_telemetry::info;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::StringVector;
    use futures_util::StreamExt;
    use serde::Serializer;
    use table::engine::{EngineContext, TableEngine, TableEngineRef};
    use table::requests::{
        AlterTableRequest, CreateTableRequest, DropTableRequest, OpenTableRequest,
    };
    use table::TableRef;
    use tokio::sync::RwLock;

    struct MockKvBackend {
        map: RwLock<BTreeMap<Vec<u8>, Vec<u8>>>,
    }

    impl Display for MockKvBackend {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            futures::executor::block_on(async {
                let map = self.map.read().await;
                for (k, v) in map.iter() {
                    f.serialize_str(&String::from_utf8_lossy(k))?;
                    f.serialize_str(" -> ")?;
                    f.serialize_str(&String::from_utf8_lossy(v))?;
                    f.serialize_str("\n")?;
                }
                Ok(())
            })
        }
    }

    #[async_trait::async_trait]
    impl KvBackend for MockKvBackend {
        type Error = Error;

        fn range<'a, 'b>(&'a self, key: &[u8]) -> ValueIter<'b, Self::Error>
        where
            'a: 'b,
        {
            let prefix = key.to_vec();
            let prefix_string = String::from_utf8_lossy(&prefix).to_string();
            Box::pin(stream!({
                let maps = self.map.read().await.clone();
                for (k, v) in maps.range(prefix.clone()..) {
                    let key_string = String::from_utf8_lossy(k).to_string();
                    let matches = key_string.starts_with(&prefix_string);
                    if matches {
                        yield Ok(Kv(k.clone(), v.clone()))
                    } else {
                        info!("Stream finished");
                        return;
                    }
                }
            }))
        }

        async fn set(&self, key: &[u8], val: &[u8]) -> Result<(), Self::Error> {
            let mut map = self.map.write().await;
            map.insert(key.to_vec(), val.to_vec());
            Ok(())
        }

        async fn delete_range(&self, key: &[u8], end: &[u8]) -> Result<(), Self::Error> {
            let start = key.to_vec();
            let end = end.to_vec();
            let range = RangeInclusive::new(start, end);

            let mut map = self.map.write().await;
            let _: BTreeMap<_, _> = map.drain_filter(|k, _| range.contains(k)).collect();
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_backend() {
        common_telemetry::init_default_ut_logging();
        let node_id = "localhost".to_string();
        let backend = MockKvBackend {
            map: Default::default(),
        };

        let default_catalog_key = CatalogKey {
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            node_id: node_id.clone(),
        }
        .to_string();

        backend
            .set(
                default_catalog_key.as_bytes(),
                &CatalogValue {}.to_bytes().unwrap(),
            )
            .await
            .unwrap();

        let schema_key = SchemaKey {
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
            node_id: node_id.clone(),
        }
        .to_string();
        backend
            .set(schema_key.as_bytes(), &SchemaValue {}.to_bytes().unwrap())
            .await
            .unwrap();

        let mut iter = backend.range("__c-".as_bytes());
        let mut res = HashSet::new();
        while let Some(r) = iter.next().await {
            let kv = r.unwrap();
            res.insert(String::from_utf8_lossy(&kv.0).to_string());
        }
        assert_eq!(
            vec!["__c-greptime-localhost".to_string()],
            res.into_iter().collect::<Vec<_>>()
        );
    }

    struct MockTableEngine {
        tables: RwLock<HashMap<String, TableRef>>,
    }

    #[async_trait::async_trait]
    impl TableEngine for MockTableEngine {
        fn name(&self) -> &str {
            "MockTableEngine"
        }

        /// Create a table with only one column
        async fn create_table(
            &self,
            _ctx: &EngineContext,
            request: CreateTableRequest,
        ) -> table::Result<TableRef> {
            let table_name = request.table_name.clone();

            let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
                "name",
                ConcreteDataType::string_datatype(),
                true,
            )]));

            let data = vec![Arc::new(StringVector::from(vec!["a", "b", "c"])) as _];
            let record_batch = RecordBatch::new(schema, data).unwrap();
            let table: TableRef =
                Arc::new(test_util::MemTable::new(&table_name, record_batch)) as Arc<_>;

            let mut tables = self.tables.write().await;
            tables.insert(table_name, table.clone() as TableRef);
            Ok(table)
        }

        async fn open_table(
            &self,
            _ctx: &EngineContext,
            request: OpenTableRequest,
        ) -> table::Result<Option<TableRef>> {
            Ok(self.tables.read().await.get(&request.table_name).cloned())
        }

        async fn alter_table(
            &self,
            _ctx: &EngineContext,
            _request: AlterTableRequest,
        ) -> table::Result<TableRef> {
            unimplemented!()
        }

        fn get_table(&self, _ctx: &EngineContext, name: &str) -> table::Result<Option<TableRef>> {
            futures::executor::block_on(async { Ok(self.tables.read().await.get(name).cloned()) })
        }

        fn table_exists(&self, _ctx: &EngineContext, name: &str) -> bool {
            futures::executor::block_on(async { self.tables.read().await.contains_key(name) })
        }

        async fn drop_table(
            &self,
            _ctx: &EngineContext,
            _request: DropTableRequest,
        ) -> table::Result<()> {
            unimplemented!()
        }
    }

    async fn prepare_components(
        node_id: String,
    ) -> (KvBackendRef, TableEngineRef, CatalogManagerRef) {
        let backend = Arc::new(MockKvBackend {
            map: Default::default(),
        }) as KvBackendRef;
        let table_engine = Arc::new(MockTableEngine {
            tables: Default::default(),
        });
        let catalog_manager =
            RemoteCatalogManager::new(table_engine.clone(), node_id, backend.clone());
        catalog_manager.start().await.unwrap();
        (backend, table_engine, Arc::new(catalog_manager))
    }

    #[tokio::test]
    async fn test_remote_catalog_default() {
        common_telemetry::init_default_ut_logging();
        let node_id = "test_node_id".to_string();
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
        let node_id = "test_node_id".to_string();
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
                    catalog_name: Some(catalog_name.clone()),
                    schema_name: Some(schema_name.clone()),
                    table_name: table_name.clone(),
                    desc: None,
                    schema: table_schema.clone(),
                    primary_key_indices: vec![],
                    create_if_not_exists: false,
                    table_options: Default::default(),
                },
            )
            .await
            .unwrap();
        let reg_req = RegisterTableRequest {
            catalog: Some(catalog_name),
            schema: Some(schema_name),
            table_name,
            table_id: 1,
            table,
        };
        let res = catalog_manager.register_table(reg_req).await;

        // because nonexistent_catalog does not exist yet.
        assert_matches!(res.err().unwrap(), Error::CatalogNotFound { .. });
    }

    #[tokio::test]
    async fn test_register_table() {
        let node_id = "test_node_id".to_string();
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
        assert_eq!(Vec::<String>::new(), default_schema.table_names().unwrap());

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
                    catalog_name: Some(catalog_name.clone()),
                    schema_name: Some(schema_name.clone()),
                    table_name: table_name.clone(),
                    desc: None,
                    schema: table_schema.clone(),
                    primary_key_indices: vec![],
                    create_if_not_exists: false,
                    table_options: Default::default(),
                },
            )
            .await
            .unwrap();
        let reg_req = RegisterTableRequest {
            catalog: Some(catalog_name),
            schema: Some(schema_name),
            table_name: table_name.clone(),
            table_id,
            table,
        };
        assert_eq!(1, catalog_manager.register_table(reg_req).await.unwrap());
        assert_eq!(vec![table_name], default_schema.table_names().unwrap());
    }

    #[tokio::test]
    async fn test_register_catalog_schema_table() {
        let node_id = "test_node_id".to_string();
        let (backend, table_engine, catalog_manager) = prepare_components(node_id.clone()).await;

        let catalog_name = "test_catalog".to_string();
        let schema_name = "nonexistent_schema".to_string();
        let catalog = Arc::new(RemoteCatalogProvider::new(
            catalog_name.clone(),
            node_id.clone(),
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
                    catalog_name: Some(catalog_name.clone()),
                    schema_name: Some(schema_name.clone()),
                    table_name: "".to_string(),
                    desc: None,
                    schema: Arc::new(Schema::new(vec![])),
                    primary_key_indices: vec![],
                    create_if_not_exists: false,
                    table_options: Default::default(),
                },
            )
            .await
            .unwrap();

        let reg_req = RegisterTableRequest {
            catalog: Some(catalog_name.clone()),
            schema: Some(schema_name.clone()),
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
            Error::SchemaNotFound { .. }
        );

        let new_catalog = catalog_manager
            .catalog(&catalog_name)
            .unwrap()
            .expect("catalog should exist since it's already registered");
        let schema = Arc::new(RemoteSchemaProvider::new(
            catalog_name.clone(),
            schema_name.clone(),
            node_id.clone(),
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
}
