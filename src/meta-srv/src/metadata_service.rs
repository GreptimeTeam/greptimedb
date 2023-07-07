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

use async_trait::async_trait;
use catalog::helper::{CatalogKey, CatalogValue, SchemaKey, SchemaValue};
use common_meta::rpc::store::CompareAndPutRequest;
use common_telemetry::{info, timer};
use metrics::increment_counter;
use snafu::{ensure, ResultExt};

use crate::error;
use crate::error::Result;
use crate::service::store::kv::KvStoreRef;

/// This trait defines some methods of metadata
#[async_trait]
pub trait MetadataService: Send + Sync {
    // An error occurs if the schema exists and "if_not_exist" == false.
    async fn create_schema(
        &self,
        catalog_name: &str,
        schema_name: &str,
        if_not_exist: bool,
    ) -> Result<()>;

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
    async fn create_schema(
        &self,
        catalog_name: &str,
        schema_name: &str,
        if_not_exist: bool,
    ) -> Result<()> {
        let _timer = timer!(crate::metrics::METRIC_META_CREATE_SCHEMA);
        let kv_store = self.kv_store.clone();

        let catalog_key = CatalogKey {
            catalog_name: catalog_name.to_string(),
        }
        .to_string();

        let schema_key = SchemaKey {
            catalog_name: catalog_name.to_string(),
            schema_name: schema_name.to_string(),
        }
        .to_string();

        let req = CompareAndPutRequest {
            key: catalog_key.into(),
            expect: vec![],
            value: CatalogValue {}
                .as_bytes()
                .context(error::InvalidCatalogValueSnafu)?,
        };

        let resp = kv_store.compare_and_put(req).await?;

        if resp.success {
            increment_counter!(crate::metrics::METRIC_META_CREATE_CATALOG);
            info!("Successfully created a catalog: {}", catalog_name);
        }

        let req = CompareAndPutRequest {
            key: schema_key.into(),
            expect: vec![],
            value: SchemaValue {}
                .as_bytes()
                .context(error::InvalidCatalogValueSnafu)?,
        };
        let resp = kv_store.compare_and_put(req).await?;

        if resp.success {
            info!("Successfully created a schema: {}", schema_name);
        }

        ensure!(
            resp.success || if_not_exist,
            error::SchemaAlreadyExistsSnafu { schema_name }
        );

        Ok(())
    }

    async fn delete_schema(&self, _catalog_name: &str, _schema_name: &str) -> Result<()> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use catalog::helper::{CatalogKey, SchemaKey};

    use super::{DefaultMetadataService, MetadataService};
    use crate::service::store::kv::KvStoreRef;
    use crate::service::store::memory::MemStore;

    #[tokio::test]
    async fn test_create_schema() {
        let kv_store = Arc::new(MemStore::default());
        let service = DefaultMetadataService::new(kv_store.clone());

        service
            .create_schema("catalog", "public", false)
            .await
            .unwrap();
        verify_result(kv_store.clone()).await;

        let result = service.create_schema("catalog", "public", false).await;
        assert!(result.is_err());

        service
            .create_schema("catalog", "public", true)
            .await
            .unwrap();
        verify_result(kv_store.clone()).await;
    }

    async fn verify_result(kv_store: KvStoreRef) {
        let key: Vec<u8> = CatalogKey {
            catalog_name: "catalog".to_string(),
        }
        .to_string()
        .into();

        let result = kv_store.get(&key).await.unwrap();
        let kv = result.unwrap();
        assert_eq!(key, kv.key());

        let key: Vec<u8> = SchemaKey {
            catalog_name: "catalog".to_string(),
            schema_name: "public".to_string(),
        }
        .to_string()
        .into();

        let result = kv_store.get(&key).await.unwrap();
        let kv = result.unwrap();
        assert_eq!(key, kv.key());
    }
}
