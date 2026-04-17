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
use common_error::ext::BoxedError;
use common_meta::key::datanode_table::{DatanodeTableKey, RegionInfo};
use common_meta::key::table_info::TableInfoValue;
use common_meta::key::table_route::{PhysicalTableRouteValue, TableRouteValue};
use common_meta::key::{
    DeserializedValueWithBytes, MetadataValue, RegionDistribution, TableMetadataManager,
};
use common_meta::kv_backend::KvBackendRef;
use common_meta::rpc::router::{RegionRoute, region_distribution};
use store_api::storage::TableId;
use table::metadata::TableInfo;

use crate::Tool;
use crate::common::StoreConfig;
use crate::error::{InvalidArgumentsSnafu, TableNotFoundSnafu, UnexpectedSnafu};
use crate::metadata::control::selector::TableSelector;

/// Put table metadata into the metadata store.
#[derive(Subcommand)]
pub enum PutTableCommand {
    Info(PutTableInfoCommand),
    Route(PutTableRouteCommand),
}

impl PutTableCommand {
    pub async fn build(&self) -> Result<Box<dyn Tool>, BoxedError> {
        match self {
            PutTableCommand::Info(cmd) => cmd.build().await,
            PutTableCommand::Route(cmd) => cmd.build().await,
        }
    }
}

/// Put table info into the metadata store.
#[derive(Debug, Parser)]
pub struct PutTableInfoCommand {
    #[clap(flatten)]
    selector: TableSelector,

    /// The JSON-encoded [`TableInfoValue`] to put into the metadata store.
    #[clap(long)]
    value: String,

    #[clap(flatten)]
    store: StoreConfig,
}

impl PutTableInfoCommand {
    pub async fn build(&self) -> Result<Box<dyn Tool>, BoxedError> {
        self.selector.validate()?;
        let kv_backend = self.store.build().await?;
        Ok(Box::new(PutTableInfoTool {
            kv_backend,
            selector: self.selector.clone(),
            value: self.value.clone(),
        }))
    }
}

struct PutTableInfoTool {
    kv_backend: KvBackendRef,
    selector: TableSelector,
    value: String,
}

#[async_trait]
impl Tool for PutTableInfoTool {
    async fn do_work(&self) -> Result<(), BoxedError> {
        let table_metadata_manager = TableMetadataManager::new(self.kv_backend.clone());
        let Some(table_id) = self
            .selector
            .resolve_table_id(table_metadata_manager.table_name_manager())
            .await?
        else {
            println!("Table({}) not found", self.selector.formatted_table_name());
            return Ok(());
        };

        let (current_table_info, current_table_route) =
            load_table_metadata(&table_metadata_manager, table_id).await?;
        let new_table_info = TableInfoValue::try_from_raw_value(self.value.as_bytes())
            .map_err(|e| {
                BoxedError::new(
                    InvalidArgumentsSnafu {
                        msg: format!("Invalid table info JSON: {e}"),
                    }
                    .build(),
                )
            })?
            .table_info;
        validate_table_info(table_id, &current_table_info.table_info, &new_table_info)?;

        let region_distribution =
            physical_region_distribution(current_table_route.get_inner_ref())?;
        let needs_rename = current_table_info.table_info.name != new_table_info.name;

        let mut effective_table_info = current_table_info;

        if needs_rename {
            table_metadata_manager
                .rename_table(&effective_table_info, new_table_info.name.clone())
                .await
                .map_err(BoxedError::new)?;
            println!("Table({table_id}) renamed");

            let (refreshed_table_info, _) =
                load_table_metadata(&table_metadata_manager, table_id).await?;
            effective_table_info = refreshed_table_info;
        }

        if effective_table_info.table_info != new_table_info {
            table_metadata_manager
                .update_table_info(&effective_table_info, region_distribution, new_table_info)
                .await
                .map_err(BoxedError::new)?;
            println!("Table({table_id}) info updated");
        }

        Ok(())
    }
}

/// Put table route into the metadata store.
#[derive(Debug, Parser)]
pub struct PutTableRouteCommand {
    #[clap(flatten)]
    selector: TableSelector,

    /// The JSON-encoded [`TableRouteValue`] to put into the metadata store.
    #[clap(long)]
    value: String,

    #[clap(flatten)]
    store: StoreConfig,
}

impl PutTableRouteCommand {
    pub async fn build(&self) -> Result<Box<dyn Tool>, BoxedError> {
        self.selector.validate()?;
        let kv_backend = self.store.build().await?;
        Ok(Box::new(PutTableRouteTool {
            kv_backend,
            selector: self.selector.clone(),
            value: self.value.clone(),
        }))
    }
}

struct PutTableRouteTool {
    kv_backend: KvBackendRef,
    selector: TableSelector,
    value: String,
}

#[async_trait]
impl Tool for PutTableRouteTool {
    async fn do_work(&self) -> Result<(), BoxedError> {
        let table_metadata_manager = TableMetadataManager::new(self.kv_backend.clone());
        let Some(table_id) = self
            .selector
            .resolve_table_id(table_metadata_manager.table_name_manager())
            .await?
        else {
            println!("Table({}) not found", self.selector.formatted_table_name());
            return Ok(());
        };

        let (current_table_info, current_table_route) =
            load_table_metadata(&table_metadata_manager, table_id).await?;
        let current_physical_route =
            current_physical_route(table_id, current_table_route.get_inner_ref())?;
        let new_table_route =
            TableRouteValue::try_from_raw_value(self.value.as_bytes()).map_err(|e| {
                BoxedError::new(
                    InvalidArgumentsSnafu {
                        msg: format!("Invalid table route JSON: {e}"),
                    }
                    .build(),
                )
            })?;
        let new_region_routes = validate_physical_route(table_id, &new_table_route)?;

        let region_info =
            load_region_info(&table_metadata_manager, table_id, &current_physical_route).await?;
        let new_region_options = current_table_info.table_info.to_region_options();
        let new_region_wal_options = region_info.region_wal_options.clone();

        if current_table_route.get_inner_ref() != &new_table_route {
            table_metadata_manager
                .update_table_route(
                    table_id,
                    region_info,
                    &current_table_route,
                    new_region_routes,
                    &new_region_options,
                    &new_region_wal_options,
                )
                .await
                .map_err(BoxedError::new)?;
            println!("Table({table_id}) route updated");
        }

        Ok(())
    }
}

fn validate_table_info(
    table_id: TableId,
    current_table_info: &TableInfo,
    new_table_info: &TableInfo,
) -> Result<(), BoxedError> {
    if new_table_info.ident.table_id != table_id {
        return Err(BoxedError::new(
            InvalidArgumentsSnafu {
                msg: format!(
                    "Invalid table info: expected table id {table_id}, got {}",
                    new_table_info.ident.table_id
                ),
            }
            .build(),
        ));
    }

    if current_table_info.catalog_name != new_table_info.catalog_name {
        return Err(BoxedError::new(
            InvalidArgumentsSnafu {
                msg: format!(
                    "Invalid table info: catalog name is immutable, expected {}, got {}",
                    current_table_info.catalog_name, new_table_info.catalog_name
                ),
            }
            .build(),
        ));
    }

    if current_table_info.schema_name != new_table_info.schema_name {
        return Err(BoxedError::new(
            InvalidArgumentsSnafu {
                msg: format!(
                    "Invalid table info: schema name is immutable, expected {}, got {}",
                    current_table_info.schema_name, new_table_info.schema_name
                ),
            }
            .build(),
        ));
    }

    Ok(())
}

fn validate_physical_route(
    table_id: TableId,
    table_route: &TableRouteValue,
) -> Result<Vec<RegionRoute>, BoxedError> {
    if !table_route.is_physical() {
        return Err(BoxedError::new(
            InvalidArgumentsSnafu {
                msg: format!(
                    "Invalid table route for table {table_id}: only physical table routes are supported"
                ),
            }
            .build(),
        ));
    }

    table_route
        .region_routes()
        .cloned()
        .map_err(BoxedError::new)
}

fn current_physical_route(
    table_id: TableId,
    table_route: &TableRouteValue,
) -> Result<PhysicalTableRouteValue, BoxedError> {
    if !table_route.is_physical() {
        return Err(BoxedError::new(
            InvalidArgumentsSnafu {
                msg: format!(
                    "Invalid table route for table {table_id}: only physical table routes are supported"
                ),
            }
            .build(),
        ));
    }

    Ok(table_route.clone().into_physical_table_route())
}

async fn load_region_info(
    table_metadata_manager: &TableMetadataManager,
    table_id: TableId,
    table_route: &PhysicalTableRouteValue,
) -> Result<RegionInfo, BoxedError> {
    let datanode_id = region_distribution(&table_route.region_routes)
        .into_keys()
        .next()
        .ok_or_else(|| {
            BoxedError::new(
                UnexpectedSnafu {
                    msg: format!(
                        "Missing datanode assignment for physical table route: {table_id}"
                    ),
                }
                .build(),
            )
        })?;

    table_metadata_manager
        .datanode_table_manager()
        .get(&DatanodeTableKey::new(datanode_id, table_id))
        .await
        .map_err(BoxedError::new)?
        .map(|value| value.region_info)
        .ok_or_else(|| {
            BoxedError::new(
                UnexpectedSnafu {
                    msg: format!(
                        "Missing datanode table metadata for physical table route: {table_id}"
                    ),
                }
                .build(),
            )
        })
}

async fn load_table_metadata(
    table_metadata_manager: &TableMetadataManager,
    table_id: TableId,
) -> Result<
    (
        DeserializedValueWithBytes<TableInfoValue>,
        DeserializedValueWithBytes<TableRouteValue>,
    ),
    BoxedError,
> {
    let (table_info, table_route) = table_metadata_manager
        .get_full_table_info(table_id)
        .await
        .map_err(BoxedError::new)?;
    let table_info =
        table_info.ok_or_else(|| BoxedError::new(TableNotFoundSnafu { table_id }.build()))?;
    let table_route =
        table_route.ok_or_else(|| BoxedError::new(TableNotFoundSnafu { table_id }.build()))?;
    Ok((table_info, table_route))
}

fn physical_region_distribution(
    table_route: &TableRouteValue,
) -> Result<Option<RegionDistribution>, BoxedError> {
    if !table_route.is_physical() {
        return Ok(None);
    }

    table_route
        .region_routes()
        .map(|routes| Some(region_distribution(routes)))
        .map_err(BoxedError::new)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use clap::Parser;
    use client::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_error::ext::ErrorExt;
    use common_meta::key::TableMetadataManager;
    use common_meta::key::datanode_table::{DatanodeTableKey, DatanodeTableManager};
    use common_meta::key::table_info::TableInfoValue;
    use common_meta::key::table_name::TableNameKey;
    use common_meta::key::table_route::TableRouteValue;
    use common_meta::kv_backend::KvBackendRef;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::peer::Peer;

    use super::{
        PutTableCommand, PutTableInfoCommand, PutTableInfoTool, PutTableRouteCommand,
        PutTableRouteTool,
    };
    use crate::Tool;
    use crate::metadata::control::put::PutCommand;
    use crate::metadata::control::selector::TableSelector;
    use crate::metadata::control::test_utils::prepare_physical_table_metadata;

    #[tokio::test]
    async fn test_put_table_selector_validation() {
        let command = PutTableInfoCommand::parse_from([
            "info",
            "--value",
            "{}",
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
    async fn test_put_table_command_builds_tool_with_table_name() {
        let command = PutTableInfoCommand::parse_from([
            "info",
            "--table-name",
            "my_table",
            "--value",
            "{}",
            "--backend",
            "memory-store",
            "--store-addrs",
            "memory://",
        ]);

        let _tool = command.build().await.unwrap();
    }

    #[tokio::test]
    async fn test_put_table_info_renames_table() {
        let kv_backend = Arc::new(MemoryKvBackend::new()) as KvBackendRef;
        let table_metadata_manager = TableMetadataManager::new(kv_backend.clone());
        let table_id = 1024;
        let (table_info, table_route) =
            prepare_physical_table_metadata("old_table", table_id).await;
        table_metadata_manager
            .create_table_metadata(
                table_info.clone(),
                TableRouteValue::Physical(table_route),
                HashMap::new(),
            )
            .await
            .unwrap();

        let mut new_table_info = table_info;
        new_table_info.name = "new_table".to_string();
        let tool = PutTableInfoTool {
            kv_backend: kv_backend.clone(),
            selector: TableSelector::with_table_id(table_id),
            value: serde_json::to_string(&TableInfoValue::new(new_table_info)).unwrap(),
        };

        tool.do_work().await.unwrap();

        let renamed = table_metadata_manager
            .table_name_manager()
            .get(TableNameKey::new(
                DEFAULT_CATALOG_NAME,
                DEFAULT_SCHEMA_NAME,
                "new_table",
            ))
            .await
            .unwrap();
        assert_eq!(renamed.unwrap().table_id(), table_id);
    }

    #[tokio::test]
    async fn test_put_table_info_rejects_schema_change() {
        let kv_backend = Arc::new(MemoryKvBackend::new()) as KvBackendRef;
        let table_metadata_manager = TableMetadataManager::new(kv_backend.clone());
        let table_id = 1024;
        let (table_info, table_route) =
            prepare_physical_table_metadata("old_table", table_id).await;
        table_metadata_manager
            .create_table_metadata(
                table_info.clone(),
                TableRouteValue::Physical(table_route),
                HashMap::new(),
            )
            .await
            .unwrap();

        let mut new_table_info = table_info;
        new_table_info.schema_name = "another_schema".to_string();
        let tool = PutTableInfoTool {
            kv_backend,
            selector: TableSelector::with_table_id(table_id),
            value: serde_json::to_string(&TableInfoValue::new(new_table_info)).unwrap(),
        };

        let err = tool.do_work().await.unwrap_err();
        assert!(
            err.output_msg()
                .contains("Invalid table info: schema name is immutable")
        );
    }

    #[tokio::test]
    async fn test_put_table_route_updates_route_and_datanode_table() {
        let kv_backend = Arc::new(MemoryKvBackend::new()) as KvBackendRef;
        let table_metadata_manager = TableMetadataManager::new(kv_backend.clone());
        let table_id = 1024;
        let (table_info, table_route) = prepare_physical_table_metadata("my_table", table_id).await;
        table_metadata_manager
            .create_table_metadata(
                table_info,
                TableRouteValue::Physical(table_route.clone()),
                HashMap::new(),
            )
            .await
            .unwrap();

        let mut region_routes = table_route.region_routes.clone();
        region_routes[0].leader_peer = Some(Peer::empty(2));
        let new_table_route = TableRouteValue::physical(region_routes);
        let tool = PutTableRouteTool {
            kv_backend: kv_backend.clone(),
            selector: TableSelector::with_table_id(table_id),
            value: serde_json::to_string(&new_table_route).unwrap(),
        };

        tool.do_work().await.unwrap();

        let (_, current_route) = table_metadata_manager
            .get_full_table_info(table_id)
            .await
            .unwrap();
        let current_route = current_route.unwrap().into_inner();
        assert_eq!(
            current_route.region_routes().unwrap(),
            new_table_route.region_routes().unwrap()
        );

        let datanode_table_manager = DatanodeTableManager::new(kv_backend);
        let updated = datanode_table_manager
            .get(&DatanodeTableKey::new(2, table_id))
            .await
            .unwrap();
        assert!(updated.is_some());
    }

    #[tokio::test]
    async fn test_put_table_route_rejects_logical_route() {
        let kv_backend = Arc::new(MemoryKvBackend::new()) as KvBackendRef;
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

        let tool = PutTableRouteTool {
            kv_backend,
            selector: TableSelector::with_table_id(table_id),
            value: serde_json::to_string(&TableRouteValue::logical(table_id + 1)).unwrap(),
        };

        let err = tool.do_work().await.unwrap_err();
        assert!(
            err.output_msg()
                .contains("only physical table routes are supported")
        );
    }

    #[tokio::test]
    async fn test_put_table_command_builds_tool() {
        let value = serde_json::to_string(&TableRouteValue::logical(1025)).unwrap();
        let command =
            PutCommand::Table(PutTableCommand::Route(PutTableRouteCommand::parse_from([
                "route",
                "--table-id",
                "1024",
                "--value",
                value.as_str(),
                "--backend",
                "memory-store",
                "--store-addrs",
                "memory://",
            ])));

        let _tool = command.build().await.unwrap();
    }
}
