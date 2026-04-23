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
use common_error::ext::BoxedError;
use common_meta::key::TableMetadataManager;
use common_meta::key::table_info::TableInfoKey;
use common_meta::key::table_route::TableRouteKey;
use common_meta::kv_backend::KvBackendRef;
use common_meta::range_stream::{DEFAULT_PAGE_SIZE, PaginationStream};
use common_meta::rpc::store::RangeRequest;
use futures::TryStreamExt;

use crate::Tool;
use crate::common::StoreConfig;
use crate::metadata::control::selector::TableSelector;
use crate::metadata::control::utils::{decode_key_value, json_formatter};

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
    #[clap(flatten)]
    selector: TableSelector,

    /// Pretty print the output.
    #[clap(long, default_value = "false")]
    pretty: bool,

    #[clap(flatten)]
    store: StoreConfig,
}

struct GetTableTool {
    kvbackend: KvBackendRef,
    selector: TableSelector,
    pretty: bool,
}

#[async_trait]
impl Tool for GetTableTool {
    async fn do_work(&self) -> Result<(), BoxedError> {
        let table_metadata_manager = TableMetadataManager::new(self.kvbackend.clone());
        let table_name_manager = table_metadata_manager.table_name_manager();
        let table_info_manager = table_metadata_manager.table_info_manager();
        let table_route_manager = table_metadata_manager.table_route_manager();

        let Some(table_id) = self.selector.resolve_table_id(table_name_manager).await? else {
            println!("Table({}) not found", self.selector.formatted_table_name());
            return Ok(());
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
        self.selector.validate()?;
        let kvbackend = self.store.build().await?;
        Ok(Box::new(GetTableTool {
            kvbackend,
            selector: self.selector.clone(),
            pretty: self.pretty,
        }))
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use common_error::ext::ErrorExt;

    use super::GetTableCommand;

    #[tokio::test]
    async fn test_get_table_selector_requires_single_target() {
        let command = GetTableCommand::parse_from([
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
    async fn test_get_table_selector_rejects_both_targets() {
        let command = GetTableCommand::parse_from([
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
    async fn test_get_table_command_builds_tool_with_table_id() {
        let command = GetTableCommand::parse_from([
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
    async fn test_get_table_command_builds_tool_with_table_name() {
        let command = GetTableCommand::parse_from([
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
}
