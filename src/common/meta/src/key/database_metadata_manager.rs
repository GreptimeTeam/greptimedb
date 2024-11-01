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

use store_api::storage::TableId;

use crate::error;
use crate::key::schema_name::{SchemaManager, SchemaNameKey, SchemaNameValue};
use crate::key::table_info::{TableInfoManager, TableInfoManagerRef};
use crate::kv_backend::KvBackendRef;

pub type SchemaMetadataManagerRef = Arc<SchemaMetadataManager>;

pub struct SchemaMetadataManager {
    table_info_manager: TableInfoManagerRef,
    schema_manager: SchemaManager,
}

impl SchemaMetadataManager {
    /// Creates a new database meta
    pub fn new(kv_backend: KvBackendRef) -> Self {
        let table_info_manager = Arc::new(TableInfoManager::new(kv_backend.clone()));
        let schema_manager = SchemaManager::new(kv_backend);
        Self {
            table_info_manager,
            schema_manager,
        }
    }

    /// Gets schema metadata by table id.
    pub async fn get_schema_metadata_by_table_id(
        &self,
        table_id: TableId,
    ) -> error::Result<SchemaNameValue> {
        let table_info = self
            .table_info_manager
            .get(table_id)
            .await
            .unwrap()
            .expect("table not found");

        let key = SchemaNameKey::new(
            &table_info.table_info.catalog_name,
            &table_info.table_info.schema_name,
        );
        Ok(self
            .schema_manager
            .get(key)
            .await
            .unwrap()
            .expect("database not found"))
    }
}
