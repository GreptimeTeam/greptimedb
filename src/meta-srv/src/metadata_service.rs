use std::sync::Arc;

use api::v1::meta::CompareAndPutRequest;
use async_trait::async_trait;
use catalog::helper::{CatalogKey, CatalogValue, SchemaKey, SchemaValue};
use common_telemetry::info;
use snafu::ResultExt;

use crate::error;
use crate::error::Result;
use crate::service::store::kv::KvStoreRef;

/// This trait defines some methods of metadata
#[async_trait]
pub trait MetadataService: Send + Sync {
    async fn create_schema(&self, catalog_name: &str, schema_name: &str) -> Result<()>;

    async fn delete_schema(&self, catalog_name: &str, schema_name: &str) -> Result<()>;
}

pub type MetadataServiceRef = Arc<dyn MetadataService>;

#[derive(Clone)]
pub struct DefaultMetadataService {
    kv_store: KvStoreRef,
}

impl DefaultMetadataService {
    pub fn new(kv_store: KvStoreRef) -> Self {
        Self { kv_store }
    }
}

#[async_trait]
impl MetadataService for DefaultMetadataService {
    async fn create_schema(&self, catalog_name: &str, schema_name: &str) -> Result<()> {
        let kv_store = self.kv_store.clone();

        let default_catalog_key = CatalogKey {
            catalog_name: catalog_name.to_string(),
        }
        .to_string();

        let default_schema_key = SchemaKey {
            catalog_name: catalog_name.to_string(),
            schema_name: schema_name.to_string(),
        }
        .to_string();

        let req = CompareAndPutRequest {
            key: default_catalog_key.into(),
            expect: vec![],
            value: CatalogValue {}
                .as_bytes()
                .context(error::InvalidCatalogValueSnafu)?,
            ..Default::default()
        };

        let resp = kv_store.compare_and_put(req).await?;

        if resp.success {
            info!("Successfully created the default catalog: {}", catalog_name);
        }

        let req = CompareAndPutRequest {
            key: default_schema_key.into(),
            expect: vec![],
            value: SchemaValue {}
                .as_bytes()
                .context(error::InvalidCatalogValueSnafu)?,
            ..Default::default()
        };
        let resp = kv_store.compare_and_put(req).await?;

        if resp.success {
            info!("Successfully created the default schema: {}", schema_name);
        }

        Ok(())
    }

    async fn delete_schema(&self, _catalog_name: &str, _schema_name: &str) -> Result<()> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::meta::PutRequest;
    use catalog::helper::{CatalogKey, SchemaKey};

    use super::{DefaultMetadataService, MetadataService};
    use crate::service::store::ext::KvStoreExt;
    use crate::service::store::kv::{KvStore, KvStoreRef};
    use crate::service::store::memory::MemStore;

    #[tokio::test]
    async fn test_create_schema() {
        let empty_kv_store = Arc::new(MemStore::default());
        do_test_create_schema(empty_kv_store).await;

        let kv_store = MemStore::default();
        let key: Vec<u8> = CatalogKey {
            catalog_name: "catalog".to_string(),
        }
        .to_string()
        .into();
        kv_store
            .put(PutRequest {
                key,
                value: vec![],
                ..Default::default()
            })
            .await
            .unwrap();
        do_test_create_schema(Arc::new(kv_store)).await;
    }

    async fn do_test_create_schema(kv_store: KvStoreRef) {
        let service = DefaultMetadataService::new(kv_store.clone());
        let result = service.create_schema("catalog", "public").await;
        assert!(result.is_ok());

        let key: Vec<u8> = CatalogKey {
            catalog_name: "catalog".to_string(),
        }
        .to_string()
        .into();

        let result = kv_store.get(key.clone()).await.unwrap();

        assert!(result.is_some());
        let kv = result.unwrap();

        assert_eq!(key, kv.key);

        let key: Vec<u8> = SchemaKey {
            catalog_name: "catalog".to_string(),
            schema_name: "public".to_string(),
        }
        .to_string()
        .into();

        let result = kv_store.get(key.clone()).await.unwrap();

        assert!(result.is_some());
        let kv = result.unwrap();

        assert_eq!(key, kv.key);
    }
}
