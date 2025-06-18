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

use async_trait::async_trait;
use clap::Parser;
use client::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_catalog::format_full_table_name;
use common_error::ext::BoxedError;
use common_meta::ddl::utils::get_region_wal_options;
use common_meta::key::table_name::TableNameManager;
use common_meta::key::TableMetadataManager;
use common_meta::kv_backend::KvBackendRef;
use store_api::storage::TableId;

use crate::error::{InvalidArgumentsSnafu, TableNotFoundSnafu};
use crate::metadata::common::StoreConfig;
use crate::metadata::control::del::CLI_TOMBSTONE_PREFIX;
use crate::metadata::control::utils::get_table_id_by_name;
use crate::Tool;

/// Delete table metadata logically from the metadata store.
#[derive(Debug, Default, Parser)]
pub struct DelTableCommand {
    /// The table id to delete from the metadata store.
    #[clap(long)]
    table_id: Option<u32>,

    /// The table name to delete from the metadata store.
    #[clap(long)]
    table_name: Option<String>,

    /// The schema name of the table.
    #[clap(long, default_value = DEFAULT_SCHEMA_NAME)]
    schema_name: String,

    /// The catalog name of the table.
    #[clap(long, default_value = DEFAULT_CATALOG_NAME)]
    catalog_name: String,

    #[clap(flatten)]
    store: StoreConfig,
}

impl DelTableCommand {
    fn validate(&self) -> Result<(), BoxedError> {
        if self.table_id.is_none() && self.table_name.is_none() {
            return Err(BoxedError::new(
                InvalidArgumentsSnafu {
                    msg: "You must specify either --table-id or --table-name.",
                }
                .build(),
            ));
        }
        Ok(())
    }
}

impl DelTableCommand {
    pub async fn build(&self) -> Result<Box<dyn Tool>, BoxedError> {
        self.validate()?;
        let kv_backend = self.store.build().await?;
        Ok(Box::new(DelTableTool {
            table_id: self.table_id,
            table_name: self.table_name.clone(),
            schema_name: self.schema_name.clone(),
            catalog_name: self.catalog_name.clone(),
            table_name_manager: TableNameManager::new(kv_backend.clone()),
            table_metadata_deleter: TableMetadataDeleter::new(kv_backend),
        }))
    }
}

struct DelTableTool {
    table_id: Option<u32>,
    table_name: Option<String>,
    schema_name: String,
    catalog_name: String,
    table_name_manager: TableNameManager,
    table_metadata_deleter: TableMetadataDeleter,
}

#[async_trait]
impl Tool for DelTableTool {
    async fn do_work(&self) -> Result<(), BoxedError> {
        let table_id = if let Some(table_name) = &self.table_name {
            let catalog_name = &self.catalog_name;
            let schema_name = &self.schema_name;

            let Some(table_id) = get_table_id_by_name(
                &self.table_name_manager,
                catalog_name,
                schema_name,
                table_name,
            )
            .await?
            else {
                println!(
                    "Table({}) not found",
                    format_full_table_name(catalog_name, schema_name, table_name)
                );
                return Ok(());
            };
            table_id
        } else {
            // Safety: we have validated that table_id or table_name is not None
            self.table_id.unwrap()
        };
        self.table_metadata_deleter.delete(table_id).await?;
        println!("Table({}) deleted", table_id);

        Ok(())
    }
}

struct TableMetadataDeleter {
    table_metadata_manager: TableMetadataManager,
}

impl TableMetadataDeleter {
    fn new(kv_backend: KvBackendRef) -> Self {
        Self {
            table_metadata_manager: TableMetadataManager::new_with_custom_tombstone_prefix(
                kv_backend.clone(),
                CLI_TOMBSTONE_PREFIX,
            ),
        }
    }

    async fn delete(&self, table_id: TableId) -> Result<(), BoxedError> {
        let (table_info, table_route) = self
            .table_metadata_manager
            .get_full_table_info(table_id)
            .await
            .map_err(BoxedError::new)?;
        let Some(table_info) = table_info else {
            return Err(BoxedError::new(TableNotFoundSnafu { table_id }.build()));
        };
        let Some(table_route) = table_route else {
            return Err(BoxedError::new(TableNotFoundSnafu { table_id }.build()));
        };
        let physical_table_id = self
            .table_metadata_manager
            .table_route_manager()
            .get_physical_table_id(table_id)
            .await
            .map_err(BoxedError::new)?;

        let table_name = table_info.table_name();
        let region_wal_options = get_region_wal_options(
            &self.table_metadata_manager,
            &table_route,
            physical_table_id,
        )
        .await
        .map_err(BoxedError::new)?;

        self.table_metadata_manager
            .delete_table_metadata(table_id, &table_name, &table_route, &region_wal_options)
            .await
            .map_err(BoxedError::new)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use common_error::ext::ErrorExt;
    use common_error::status_code::StatusCode;
    use common_meta::key::table_route::TableRouteValue;
    use common_meta::key::TableMetadataManager;
    use common_meta::kv_backend::chroot::ChrootKvBackend;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::kv_backend::{KvBackend, KvBackendRef};
    use common_meta::rpc::store::RangeRequest;

    use crate::metadata::control::del::table::TableMetadataDeleter;
    use crate::metadata::control::del::CLI_TOMBSTONE_PREFIX;
    use crate::metadata::control::test_utils::prepare_physical_table_metadata;

    #[tokio::test]
    async fn test_delete_table_not_found() {
        let kv_backend = Arc::new(MemoryKvBackend::new()) as KvBackendRef;

        let table_metadata_deleter = TableMetadataDeleter::new(kv_backend);
        let table_id = 1;
        let err = table_metadata_deleter.delete(table_id).await.unwrap_err();
        assert_eq!(err.status_code(), StatusCode::TableNotFound);
    }

    #[tokio::test]
    async fn test_delete_table_metadata() {
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let table_metadata_manager = TableMetadataManager::new(kv_backend.clone());
        let table_id = 1024;
        let (table_info, table_route) = prepare_physical_table_metadata("my_table", table_id).await;
        table_metadata_manager
            .create_table_metadata(
                table_info,
                TableRouteValue::Physical(table_route),
                HashMap::new(),
            )
            .await
            .unwrap();

        let total_keys = kv_backend.len();
        assert!(total_keys > 0);

        let table_metadata_deleter = TableMetadataDeleter::new(kv_backend.clone());
        table_metadata_deleter.delete(table_id).await.unwrap();

        // Check the tombstone keys are deleted
        let chroot =
            ChrootKvBackend::new(CLI_TOMBSTONE_PREFIX.as_bytes().to_vec(), kv_backend.clone());
        let req = RangeRequest::default().with_range(vec![0], vec![0]);
        let resp = chroot.range(req).await.unwrap();
        assert_eq!(resp.kvs.len(), total_keys);
    }
}
