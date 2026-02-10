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

//! Schema-level metadata manager.

use std::sync::Arc;

use snafu::OptionExt;
use store_api::storage::TableId;

use crate::cache::{SchemaCacheRef, TableSchemaCacheRef};
use crate::error::TableInfoNotFoundSnafu;
use crate::{SchemaOptions, error};

pub type SchemaMetadataManagerRef = Arc<SchemaMetadataManager>;

pub struct SchemaMetadataManager {
    table_id_schema_cache: TableSchemaCacheRef,
    schema_cache: SchemaCacheRef,
}

impl SchemaMetadataManager {
    /// Creates a new database meta
    pub fn new(table_id_schema_cache: TableSchemaCacheRef, schema_cache: SchemaCacheRef) -> Self {
        Self {
            table_id_schema_cache,
            schema_cache,
        }
    }

    /// Gets schema options by table id.
    pub async fn get_schema_options_by_table_id(
        &self,
        table_id: TableId,
    ) -> error::Result<Option<Arc<SchemaOptions>>> {
        let schema_name = self
            .table_id_schema_cache
            .get(table_id)
            .await?
            .with_context(|| TableInfoNotFoundSnafu {
                table: format!("table id: {}", table_id),
            })?;

        self.schema_cache.get_by_ref(&schema_name).await
    }

    #[cfg(any(test, feature = "testing"))]
    pub async fn register_region_table_info(
        &self,
        table_id: TableId,
        table_name: &str,
        schema_name: &str,
        catalog_name: &str,
        schema_value: Option<crate::key::schema_name::SchemaNameValue>,
        kv_backend: crate::kv_backend::KvBackendRef,
    ) {
        use table::metadata::{TableInfo, TableType};
        let value = crate::key::table_info::TableInfoValue::new(TableInfo {
            ident: Default::default(),
            name: table_name.to_string(),
            desc: None,
            catalog_name: catalog_name.to_string(),
            schema_name: schema_name.to_string(),
            meta: table::metadata::TableMeta::empty(),
            table_type: TableType::Base,
        });
        let table_info_manager = crate::key::table_info::TableInfoManager::new(kv_backend.clone());
        let (txn, _) = table_info_manager
            .build_create_txn(table_id, &value)
            .unwrap();
        let resp = kv_backend.txn(txn).await.unwrap();
        assert!(resp.succeeded, "Failed to create table metadata");
        let key = crate::key::schema_name::SchemaNameKey {
            catalog: catalog_name,
            schema: schema_name,
        };

        crate::key::schema_name::SchemaManager::new(kv_backend.clone())
            .create(key, schema_value, false)
            .await
            .expect("Failed to create schema metadata");
        common_telemetry::info!(
            "Register table: {}, id: {}, schema: {}, catalog: {}",
            table_name,
            table_id,
            schema_name,
            catalog_name
        );
    }
}
