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

//! Utilities to get table metadata.

use std::sync::Arc;

use catalog::kvbackend::MetaKvBackend;
use common_meta::key::table_info::TableInfoValue;
use common_meta::key::table_name::TableNameKey;
use common_meta::key::{TableMetadataManager, TableMetadataManagerRef};
use meta_client::{MetaClientOptions, MetaClientType};

// TODO(yingwen): Error handling.
#[derive(Clone)]
pub struct TableMetadataHelper {
    table_metadata_manager: TableMetadataManagerRef,
}

impl TableMetadataHelper {
    pub async fn new(meta_options: &MetaClientOptions) -> Self {
        let backend = build_kv_backend(meta_options).await;
        let table_metadata_manager = Arc::new(TableMetadataManager::new(Arc::new(backend)));
        Self {
            table_metadata_manager,
        }
    }

    /// Get table info.
    pub async fn get_table(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Option<TableInfoValue> {
        let table_name = TableNameKey::new(catalog, schema, table);
        let table_id = self
            .table_metadata_manager
            .table_name_manager()
            .get(table_name)
            .await
            .unwrap()
            .map(|v| v.table_id())
            .unwrap();

        self.table_metadata_manager
            .table_info_manager()
            .get(table_id)
            .await
            .unwrap()
            .map(|v| v.into_inner())
    }
}

async fn build_kv_backend(meta_options: &MetaClientOptions) -> MetaKvBackend {
    let meta_client = meta_client::create_meta_client(MetaClientType::Frontend, meta_options)
        .await
        .unwrap();

    MetaKvBackend::new(meta_client)
}
