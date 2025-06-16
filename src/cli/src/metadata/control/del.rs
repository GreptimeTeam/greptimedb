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
use clap::{Parser, Subcommand};
use client::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_catalog::format_full_table_name;
use common_error::ext::BoxedError;
use common_meta::ddl::utils::get_region_wal_options;
use common_meta::key::table_name::TableNameKey;
use common_meta::key::TableMetadataManager;
use common_meta::kv_backend::KvBackendRef;
use common_meta::rpc::store::DeleteRangeRequest;

use crate::error::InvalidArgumentsSnafu;
use crate::metadata::common::StoreConfig;
use crate::Tool;

/// Subcommand for deleting metadata from the metadata store.
#[derive(Subcommand)]
pub enum DelCommand {
    Key(DelKeyCommand),
    Table(DelTableCommand),
}

impl DelCommand {
    pub async fn build(&self) -> Result<Box<dyn Tool>, BoxedError> {
        match self {
            DelCommand::Key(cmd) => cmd.build().await,
            DelCommand::Table(cmd) => cmd.build().await,
        }
    }
}

/// Delete key-value pairs from the metadata store.
#[derive(Debug, Default, Parser)]
pub struct DelKeyCommand {
    /// The key to delete from the metadata store.
    #[clap(long)]
    key: String,

    /// Delete key-value pairs with the given prefix.
    #[clap(long)]
    prefix: bool,

    #[clap(flatten)]
    store: StoreConfig,
}

impl DelKeyCommand {
    async fn build(&self) -> Result<Box<dyn Tool>, BoxedError> {
        let kv_backend = self.store.build().await?;
        Ok(Box::new(DelKeyTool {
            key: self.key.to_string(),
            prefix: self.prefix,
            kv_backend,
        }))
    }
}

struct DelKeyTool {
    key: String,
    prefix: bool,
    kv_backend: KvBackendRef,
}

#[async_trait]
impl Tool for DelKeyTool {
    async fn do_work(&self) -> Result<(), BoxedError> {
        let mut req = DeleteRangeRequest::default();
        if self.prefix {
            req = req.with_prefix(self.key.as_bytes());
        } else {
            req = req.with_key(self.key.as_bytes());
        }
        let resp = self
            .kv_backend
            .delete_range(req)
            .await
            .map_err(BoxedError::new)?;
        println!("{}", resp.deleted);

        Ok(())
    }
}

/// Delete table metadata from the metadata store.
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
    async fn build(&self) -> Result<Box<dyn Tool>, BoxedError> {
        self.validate()?;
        let kv_backend = self.store.build().await?;
        Ok(Box::new(DelTableTool {
            table_id: self.table_id,
            table_name: self.table_name.clone(),
            schema_name: self.schema_name.clone(),
            catalog_name: self.catalog_name.clone(),
            kv_backend,
        }))
    }
}

struct DelTableTool {
    table_id: Option<u32>,
    table_name: Option<String>,
    schema_name: String,
    catalog_name: String,
    kv_backend: KvBackendRef,
}

#[async_trait]
impl Tool for DelTableTool {
    async fn do_work(&self) -> Result<(), BoxedError> {
        let table_metadata_manager = TableMetadataManager::new(self.kv_backend.clone());
        let table_name_manager = table_metadata_manager.table_name_manager();
        let table_id = if let Some(table_name) = &self.table_name {
            let catalog_name = &self.catalog_name;
            let schema_name = &self.schema_name;
            let key = TableNameKey::new(catalog_name, schema_name, table_name);

            let Some(table_name) = table_name_manager.get(key).await.map_err(BoxedError::new)?
            else {
                println!(
                    "Table({}) not found",
                    format_full_table_name(catalog_name, schema_name, table_name)
                );
                return Ok(());
            };

            table_name.table_id()
        } else {
            // Safety: we have validated that table_id or table_name is not None
            self.table_id.unwrap()
        };

        let (table_info, table_route) = table_metadata_manager
            .get_full_table_info(table_id)
            .await
            .map_err(BoxedError::new)?;
        let Some(table_info) = table_info else {
            println!("Table info not found");
            return Ok(());
        };
        let Some(table_route) = table_route else {
            println!("Table route not found");
            return Ok(());
        };
        let physical_table_id = table_metadata_manager
            .table_route_manager()
            .get_physical_table_id(table_id)
            .await
            .map_err(BoxedError::new)?;

        let table_name = table_info.table_name();
        let region_wal_options =
            get_region_wal_options(&table_metadata_manager, &table_route, physical_table_id)
                .await
                .map_err(BoxedError::new)?;
        table_metadata_manager
            .destroy_table_metadata(table_id, &table_name, &table_route, &region_wal_options)
            .await
            .map_err(BoxedError::new)?;
        println!("Table({}) deleted", table_name.table_ref());

        Ok(())
    }
}
