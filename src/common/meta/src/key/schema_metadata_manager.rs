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

use crate::error::TableInfoNotFoundSnafu;
use crate::key::schema_name::{SchemaManager, SchemaNameKey};
use crate::key::table_info::{TableInfoManager, TableInfoManagerRef};
use crate::kv_backend::KvBackendRef;
use crate::{error, SchemaOptions};

pub type SchemaMetadataManagerRef = Arc<SchemaMetadataManager>;

pub struct SchemaMetadataManager {
    table_info_manager: TableInfoManagerRef,
    schema_manager: SchemaManager,
    #[cfg(any(test, feature = "testing"))]
    kv_backend: KvBackendRef,
}

impl SchemaMetadataManager {
    /// Creates a new database meta
    #[cfg(not(any(test, feature = "testing")))]
    pub fn new(kv_backend: KvBackendRef) -> Self {
        let table_info_manager = Arc::new(TableInfoManager::new(kv_backend.clone()));
        let schema_manager = SchemaManager::new(kv_backend);
        Self {
            table_info_manager,
            schema_manager,
        }
    }

    /// Creates a new database meta
    #[cfg(any(test, feature = "testing"))]
    pub fn new(kv_backend: KvBackendRef) -> Self {
        let table_info_manager = Arc::new(TableInfoManager::new(kv_backend.clone()));
        let schema_manager = SchemaManager::new(kv_backend.clone());
        Self {
            table_info_manager,
            schema_manager,
            kv_backend,
        }
    }

    /// Gets schema options by table id.
    pub async fn get_schema_options_by_table_id(
        &self,
        table_id: TableId,
    ) -> error::Result<Option<SchemaOptions>> {
        let table_info = self
            .table_info_manager
            .get(table_id)
            .await?
            .with_context(|| TableInfoNotFoundSnafu {
                table: format!("table id: {}", table_id),
            })?;

        let key = SchemaNameKey::new(
            &table_info.table_info.catalog_name,
            &table_info.table_info.schema_name,
        );
        self.schema_manager.get(key).await
    }

    #[cfg(any(test, feature = "testing"))]
    pub async fn register_region_table_info(
        &self,
        table_id: TableId,
        table_name: &str,
        schema_name: &str,
        catalog_name: &str,
        schema_value: Option<crate::key::schema_name::SchemaNameValue>,
    ) {
        use table::metadata::{RawTableInfo, TableType};
        let value = crate::key::table_info::TableInfoValue::new(RawTableInfo {
            ident: Default::default(),
            name: table_name.to_string(),
            desc: None,
            catalog_name: catalog_name.to_string(),
            schema_name: schema_name.to_string(),
            meta: Default::default(),
            table_type: TableType::Base,
        });
        let (txn, _) = self
            .table_info_manager
            .build_create_txn(table_id, &value)
            .unwrap();
        let resp = self.kv_backend.txn(txn).await.unwrap();
        assert!(resp.succeeded, "Failed to create table metadata");
        let key = SchemaNameKey {
            catalog: catalog_name,
            schema: schema_name,
        };
        self.schema_manager
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
