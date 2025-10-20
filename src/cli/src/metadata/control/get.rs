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

use std::cmp::min;

use async_trait::async_trait;
use clap::{Parser, Subcommand};
use client::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_catalog::format_full_table_name;
use common_error::ext::BoxedError;
use common_meta::key::table_info::TableInfoKey;
use common_meta::key::table_route::TableRouteKey;
use common_meta::key::TableMetadataManager;
use common_meta::kv_backend::KvBackendRef;
use common_meta::range_stream::{PaginationStream, DEFAULT_PAGE_SIZE};
use common_meta::rpc::store::RangeRequest;
use futures::TryStreamExt;

use crate::error::InvalidArgumentsSnafu;
use crate::metadata::common::StoreConfig;
use crate::metadata::control::utils::{decode_key_value, get_table_id_by_name, json_formatter};
use crate::Tool;

/// Getting metadata from metadata store.
#[derive(Subcommand)]
pub enum GetCommand {
    Key(GetKeyCommand),
    Table(GetTableCommand),
}

impl GetCommand {
    pub async fn build(&self) -> Result<Box<dyn Tool>, BoxedError> {
        match self {
            GetCommand::Key(cmd) => cmd.build().await,
            GetCommand::Table(cmd) => cmd.build().await,
        }
    }
}

/// Get key-value pairs from the metadata store.
#[derive(Debug, Default, Parser)]
pub struct GetKeyCommand {
    /// The key to get from the metadata store.
    #[clap(default_value = "")]
    key: String,

    /// Whether to perform a prefix query. If true, returns all key-value pairs where the key starts with the given prefix.
    #[clap(long, default_value = "false")]
    prefix: bool,

    /// The maximum number of key-value pairs to return. If 0, returns all key-value pairs.
    #[clap(long, default_value = "0")]
    limit: u64,

    #[clap(flatten)]
    store: StoreConfig,
}

impl GetKeyCommand {
    pub async fn build(&self) -> Result<Box<dyn Tool>, BoxedError> {
        let kvbackend = self.store.build().await?;
        Ok(Box::new(GetKeyTool {
            kvbackend,
            key: self.key.clone(),
            prefix: self.prefix,
            limit: self.limit,
        }))
    }
}

struct GetKeyTool {
    kvbackend: KvBackendRef,
    key: String,
    prefix: bool,
    limit: u64,
}

#[async_trait]
impl Tool for GetKeyTool {
    async fn do_work(&self) -> Result<(), BoxedError> {
        let mut req = RangeRequest::default();
        if self.prefix {
            req = req.with_prefix(self.key.as_bytes());
        } else {
            req = req.with_key(self.key.as_bytes());
        }
        let page_size = if self.limit > 0 {
            min(self.limit as usize, DEFAULT_PAGE_SIZE)
        } else {
            DEFAULT_PAGE_SIZE
        };
        let pagination_stream =
            PaginationStream::new(self.kvbackend.clone(), req, page_size, decode_key_value);
        let mut stream = Box::pin(pagination_stream.into_stream());
        let mut counter = 0;

        while let Some((key, value)) = stream.try_next().await.map_err(BoxedError::new)? {
            print!("{}\n{}\n", key, value);
            counter += 1;
            if self.limit > 0 && counter >= self.limit {
                break;
            }
        }

        Ok(())
    }
}

/// Get table metadata from the metadata store via table id.
#[derive(Debug, Default, Parser)]
pub struct GetTableCommand {
    /// Get table metadata by table id.
    #[clap(long)]
    table_id: Option<u32>,

    /// Get table metadata by table name.
    #[clap(long)]
    table_name: Option<String>,

    /// The schema name of the table.
    #[clap(long, default_value = DEFAULT_SCHEMA_NAME)]
    schema_name: String,

    /// The catalog name of the table.
    #[clap(long, default_value = DEFAULT_CATALOG_NAME)]
    catalog_name: String,

    /// Pretty print the output.
    #[clap(long, default_value = "false")]
    pretty: bool,

    #[clap(flatten)]
    store: StoreConfig,
}

impl GetTableCommand {
    pub fn validate(&self) -> Result<(), BoxedError> {
        if matches!(
            (&self.table_id, &self.table_name),
            (Some(_), Some(_)) | (None, None)
        ) {
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

struct GetTableTool {
    kvbackend: KvBackendRef,
    table_id: Option<u32>,
    table_name: Option<String>,
    schema_name: String,
    catalog_name: String,
    pretty: bool,
}

#[async_trait]
impl Tool for GetTableTool {
    async fn do_work(&self) -> Result<(), BoxedError> {
        let table_metadata_manager = TableMetadataManager::new(self.kvbackend.clone());
        let table_name_manager = table_metadata_manager.table_name_manager();
        let table_info_manager = table_metadata_manager.table_info_manager();
        let table_route_manager = table_metadata_manager.table_route_manager();

        let table_id = if let Some(table_name) = &self.table_name {
            let catalog_name = &self.catalog_name;
            let schema_name = &self.schema_name;

            let Some(table_id) =
                get_table_id_by_name(table_name_manager, catalog_name, schema_name, table_name)
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

        let table_info = table_info_manager
            .get(table_id)
            .await
            .map_err(BoxedError::new)?;
        if let Some(table_info) = table_info {
            println!(
                "{}\n{}",
                TableInfoKey::new(table_id),
                json_formatter(self.pretty, &*table_info)
            );
        } else {
            println!("Table info not found");
        }

        let table_route = table_route_manager
            .table_route_storage()
            .get(table_id)
            .await
            .map_err(BoxedError::new)?;
        if let Some(table_route) = table_route {
            println!(
                "{}\n{}",
                TableRouteKey::new(table_id),
                json_formatter(self.pretty, &table_route)
            );
        } else {
            println!("Table route not found");
        }

        Ok(())
    }
}

impl GetTableCommand {
    pub async fn build(&self) -> Result<Box<dyn Tool>, BoxedError> {
        self.validate()?;
        let kvbackend = self.store.build().await?;
        Ok(Box::new(GetTableTool {
            kvbackend,
            table_id: self.table_id,
            table_name: self.table_name.clone(),
            schema_name: self.schema_name.clone(),
            catalog_name: self.catalog_name.clone(),
            pretty: self.pretty,
        }))
    }
}
