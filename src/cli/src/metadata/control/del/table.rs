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
use common_error::ext::BoxedError;
use common_meta::ddl::utils::get_region_wal_options;
use common_meta::key::TableMetadataManager;
use common_meta::kv_backend::KvBackendRef;
use store_api::storage::TableId;

use crate::Tool;
use crate::common::StoreConfig;
use crate::error::TableNotFoundSnafu;
use crate::metadata::control::del::CLI_TOMBSTONE_PREFIX;
use crate::metadata::control::selector::TableSelector;

/// Delete table metadata logically from the metadata store.
#[derive(Debug, Default, Parser)]
pub struct DelTableCommand {
    #[clap(flatten)]
    selector: TableSelector,

    /// The store config.
    #[clap(flatten)]
    store: StoreConfig,
}

impl DelTableCommand {
    pub async fn build(&self) -> Result<Box<dyn Tool>, BoxedError> {
        self.selector.validate()?;
        let kv_backend = self.store.build().await?;
        Ok(Box::new(DelTableTool {
            selector: self.selector.clone(),
            table_metadata_deleter: TableMetadataDeleter::new(kv_backend),
        }))
    }
}

struct DelTableTool {
    selector: TableSelector,
    table_metadata_deleter: TableMetadataDeleter,
}

#[async_trait]
impl Tool for DelTableTool {
    async fn do_work(&self) -> Result<(), BoxedError> {
        let Some(table_id) = self
            .selector
            .resolve_table_id(
                self.table_metadata_deleter
                    .table_metadata_manager
                    .table_name_manager(),
            )
            .await?
        else {
            println!("Table({}) not found", self.selector.formatted_table_name());
            return Ok(());
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
                kv_backend,
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

    use clap::Parser;
    use common_error::ext::ErrorExt;
    use common_error::status_code::StatusCode;
    use common_meta::key::TableMetadataManager;
    use common_meta::key::table_route::TableRouteValue;
    use common_meta::kv_backend::chroot::ChrootKvBackend;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::kv_backend::{KvBackend, KvBackendRef};
    use common_meta::rpc::store::RangeRequest;

    use crate::metadata::control::del::CLI_TOMBSTONE_PREFIX;
    use crate::metadata::control::del::table::{DelTableCommand, TableMetadataDeleter};
    use crate::metadata::control::test_utils::prepare_physical_table_metadata;

    #[tokio::test]
    async fn test_del_table_selector_requires_single_target() {
        let command = DelTableCommand::parse_from([
            "table",
            "--backend",
            "memory-store",
            "--store-addrs",
            "memory://",
        ]);

        let err = match command.build().await {
            Ok(_) => panic!("expected validation failure"),
            Err(err) => err,
        };
        assert!(
            err.output_msg()
                .contains("You must specify either --table-id or --table-name.")
        );
    }

    #[tokio::test]
    async fn test_del_table_selector_rejects_both_targets() {
        let command = DelTableCommand::parse_from([
            "table",
            "--table-id",
            "1024",
            "--table-name",
            "my_table",
            "--backend",
            "memory-store",
            "--store-addrs",
            "memory://",
        ]);

        let err = match command.build().await {
            Ok(_) => panic!("expected validation failure"),
            Err(err) => err,
        };
        assert!(
            err.output_msg()
                .contains("You must specify either --table-id or --table-name.")
        );
    }

    #[tokio::test]
    async fn test_del_table_command_builds_tool_with_table_id() {
        let command = DelTableCommand::parse_from([
            "table",
            "--table-id",
            "1024",
            "--backend",
            "memory-store",
            "--store-addrs",
            "memory://",
        ]);

        let _tool = command.build().await.unwrap();
    }

    #[tokio::test]
    async fn test_del_table_command_builds_tool_with_table_name() {
        let command = DelTableCommand::parse_from([
            "table",
            "--table-name",
            "my_table",
            "--backend",
            "memory-store",
            "--store-addrs",
            "memory://",
        ]);

        let _tool = command.build().await.unwrap();
    }

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
