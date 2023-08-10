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
use common_meta::key::catalog_name::CatalogNameKey;
use common_meta::key::schema_name::SchemaNameKey;
use common_meta::key::TableMetadataManagerRef;
use common_telemetry::{info, timer};
use metrics::increment_counter;
use snafu::{ensure, ResultExt};

use crate::error;
use crate::error::Result;

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
    table_metadata_manager: TableMetadataManagerRef,
}

impl DefaultMetadataService {
    pub fn new(table_metadata_manager: TableMetadataManagerRef) -> Self {
        Self {
            table_metadata_manager,
        }
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

        self.table_metadata_manager
            .catalog_manager()
            .create(CatalogNameKey::new(catalog_name))
            .await
            .context(error::TableMetadataManagerSnafu)?;

        increment_counter!(crate::metrics::METRIC_META_CREATE_CATALOG);
        info!("Successfully created a catalog: {}", catalog_name);

        let schema = SchemaNameKey::new(catalog_name, schema_name);

        let exist = self
            .table_metadata_manager
            .schema_manager()
            .exist(schema)
            .await
            .context(error::TableMetadataManagerSnafu)?;

        ensure!(
            !exist || if_not_exist,
            error::SchemaAlreadyExistsSnafu { schema_name }
        );

        if !exist {
            self.table_metadata_manager
                .schema_manager()
                .create(schema)
                .await
                .context(error::TableMetadataManagerSnafu)?;

            info!("Successfully created a schema: {}", schema_name);
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

    use common_meta::key::catalog_name::CatalogNameKey;
    use common_meta::key::schema_name::SchemaNameKey;
    use common_meta::key::{TableMetaKey, TableMetadataManager};

    use super::{DefaultMetadataService, MetadataService};
    use crate::service::store::kv::{KvBackendAdapter, KvStoreRef};
    use crate::service::store::memory::MemStore;

    #[tokio::test]
    async fn test_create_schema() {
        let kv_store = Arc::new(MemStore::default());
        let table_metadata_manager = Arc::new(TableMetadataManager::new(KvBackendAdapter::wrap(
            kv_store.clone(),
        )));
        let service = DefaultMetadataService::new(table_metadata_manager);

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
        let key = CatalogNameKey::new("catalog").as_raw_key();

        let result = kv_store.get(&key).await.unwrap();
        let kv = result.unwrap();
        assert_eq!(key, kv.key());

        let key = SchemaNameKey::new("catalog", "public").as_raw_key();

        let result = kv_store.get(&key).await.unwrap();
        let kv = result.unwrap();
        assert_eq!(key, kv.key());
    }
}
