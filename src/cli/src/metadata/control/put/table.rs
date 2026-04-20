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
use common_meta::key::table_route::TableRouteValue;
use common_meta::key::{
    DeserializedValueWithBytes, MetadataValue, RegionDistribution, TableMetadataManager,
};
use common_meta::kv_backend::KvBackendRef;
use common_meta::rpc::router::{RegionRoute, region_distribution};
use snafu::ensure;
use store_api::storage::TableId;
use table::metadata::TableInfo;

use crate::Tool;
use crate::common::StoreConfig;
use crate::error::{Error, InvalidArgumentsSnafu, TableNotFoundSnafu, UnexpectedSnafu};
use crate::metadata::control::put::read_value;
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

    /// Read the JSON-encoded [`TableInfoValue`] from standard input.
    #[clap(long, required = true)]
    value_stdin: bool,

    #[clap(flatten)]
    store: StoreConfig,
}

impl PutTableInfoCommand {
    pub async fn build(&self) -> Result<Box<dyn Tool>, BoxedError> {
        let kv_backend = self.store.build().await?;
        self.build_tool(tokio::io::stdin(), kv_backend).await
    }

    async fn build_tool<R>(
        &self,
        reader: R,
        kv_backend: KvBackendRef,
    ) -> Result<Box<dyn Tool>, BoxedError>
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        self.selector.validate()?;
        Ok(Box::new(PutTableInfoTool {
            kv_backend,
            selector: self.selector.clone(),
            value: read_value(reader).await?,
        }))
    }
}

struct PutTableInfoTool {
    kv_backend: KvBackendRef,
    selector: TableSelector,
    value: Vec<u8>,
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
            return Err(BoxedError::new(
                UnexpectedSnafu {
                    msg: format!("Table({}) not found", self.selector.formatted_table_name()),
                }
                .build(),
            ));
        };

        let (current_table_info, current_table_route) =
            load_table_metadata(&table_metadata_manager, table_id).await?;
        let new_table_info = TableInfoValue::try_from_raw_value(&self.value)
            .map_err(|e| {
                BoxedError::new(
                    InvalidArgumentsSnafu {
                        msg: format!("Invalid table info JSON: {e}"),
                    }
                    .build(),
                )
            })?
            .table_info;
        validate_table_info(table_id, &current_table_info.table_info, &new_table_info)
            .map_err(BoxedError::new)?;

        let region_distribution =
            physical_region_distribution(current_table_route.get_inner_ref())?;

        if current_table_info.table_info != new_table_info {
            table_metadata_manager
                .update_table_info(&current_table_info, region_distribution, new_table_info)
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

    /// Read the JSON-encoded [`TableRouteValue`] from standard input.
    #[clap(long, required = true)]
    value_stdin: bool,

    #[clap(flatten)]
    store: StoreConfig,
}

impl PutTableRouteCommand {
    pub async fn build(&self) -> Result<Box<dyn Tool>, BoxedError> {
        let kv_backend = self.store.build().await?;
        self.build_tool(tokio::io::stdin(), kv_backend).await
    }

    async fn build_tool<R>(
        &self,
        reader: R,
        kv_backend: KvBackendRef,
    ) -> Result<Box<dyn Tool>, BoxedError>
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        self.selector.validate()?;
        Ok(Box::new(PutTableRouteTool {
            kv_backend,
            selector: self.selector.clone(),
            value: read_value(reader).await?,
        }))
    }
}

struct PutTableRouteTool {
    kv_backend: KvBackendRef,
    selector: TableSelector,
    value: Vec<u8>,
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
            return Err(BoxedError::new(
                UnexpectedSnafu {
                    msg: format!("Table({}) not found", self.selector.formatted_table_name()),
                }
                .build(),
            ));
        };

        let (current_table_info, current_table_route) =
            load_table_metadata(&table_metadata_manager, table_id).await?;
        let current_region_routes = current_table_route
            .region_routes()
            .map_err(BoxedError::new)?;
        let new_table_route = TableRouteValue::try_from_raw_value(&self.value).map_err(|e| {
            BoxedError::new(
                InvalidArgumentsSnafu {
                    msg: format!("Invalid table route JSON: {e}"),
                }
                .build(),
            )
        })?;
        let new_region_routes = new_table_route.region_routes().map_err(BoxedError::new)?;
        validate_table_route(table_id, new_region_routes).map_err(BoxedError::new)?;
        let region_info =
            load_region_info(&table_metadata_manager, table_id, current_region_routes).await?;
        let new_region_options = current_table_info.table_info.to_region_options();
        let new_region_wal_options = region_info.region_wal_options.clone();

        if current_table_route.get_inner_ref() != &new_table_route {
            table_metadata_manager
                .update_table_route(
                    table_id,
                    region_info,
                    &current_table_route,
                    new_region_routes.clone(),
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

fn validate_table_route(table_id: TableId, new_region_routes: &[RegionRoute]) -> Result<(), Error> {
    for route in new_region_routes {
        ensure!(
            route.region.id.table_id() == table_id,
            InvalidArgumentsSnafu {
                msg: format!(
                    "Invalid table route: all region routes must have table id {table_id}, but got {}",
                    route.region.id.table_id()
                ),
            }
        );
    }
    Ok(())
}

fn validate_table_info(
    table_id: TableId,
    current_table_info: &TableInfo,
    new_table_info: &TableInfo,
) -> Result<(), Error> {
    ensure!(
        new_table_info.ident.table_id == table_id,
        InvalidArgumentsSnafu {
            msg: format!(
                "Invalid table info: expected table id {table_id}, got {}",
                new_table_info.ident.table_id
            ),
        }
    );

    ensure!(
        current_table_info.catalog_name == new_table_info.catalog_name,
        InvalidArgumentsSnafu {
            msg: format!(
                "Invalid table info: catalog name is immutable, expected {}, got {}",
                current_table_info.catalog_name, new_table_info.catalog_name
            ),
        }
    );

    ensure!(
        current_table_info.schema_name == new_table_info.schema_name,
        InvalidArgumentsSnafu {
            msg: format!(
                "Invalid table info: schema name is immutable, expected {}, got {}",
                current_table_info.schema_name, new_table_info.schema_name
            ),
        }
    );

    ensure!(
        current_table_info.name == new_table_info.name,
        InvalidArgumentsSnafu {
            msg: format!(
                "Invalid table info: table name is immutable, expected {}, got {}",
                current_table_info.name, new_table_info.name
            ),
        }
    );

    Ok(())
}

async fn load_region_info(
    table_metadata_manager: &TableMetadataManager,
    table_id: TableId,
    region_routes: &[RegionRoute],
) -> Result<RegionInfo, BoxedError> {
    let datanode_id = region_distribution(region_routes)
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
    use common_error::ext::{BoxedError, ErrorExt};
    use common_meta::key::TableMetadataManager;
    use common_meta::key::datanode_table::{DatanodeTableKey, DatanodeTableManager};
    use common_meta::key::table_info::TableInfoValue;
    use common_meta::key::table_route::TableRouteValue;
    use common_meta::kv_backend::KvBackendRef;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::peer::Peer;
    use tokio::io::BufReader;

    use super::{PutTableInfoCommand, PutTableInfoTool, PutTableRouteCommand, PutTableRouteTool};
    use crate::Tool;
    use crate::metadata::control::selector::TableSelector;
    use crate::metadata::control::test_utils::prepare_physical_table_metadata;

    impl PutTableInfoCommand {
        async fn build_for_test<R>(
            &self,
            reader: R,
            kv_backend: KvBackendRef,
        ) -> Result<Box<dyn Tool>, BoxedError>
        where
            R: tokio::io::AsyncRead + Unpin,
        {
            self.build_tool(reader, kv_backend).await
        }
    }

    impl PutTableRouteCommand {
        async fn build_for_test<R>(
            &self,
            reader: R,
            kv_backend: KvBackendRef,
        ) -> Result<Box<dyn Tool>, BoxedError>
        where
            R: tokio::io::AsyncRead + Unpin,
        {
            self.build_tool(reader, kv_backend).await
        }
    }

    #[tokio::test]
    async fn test_put_table_selector_validation() {
        let command = PutTableInfoCommand::parse_from([
            "info",
            "--value-stdin",
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
            "--value-stdin",
            "--backend",
            "memory-store",
            "--store-addrs",
            "memory://",
        ]);

        let _tool = command
            .build_for_test(
                BufReader::new(&b"{}"[..]),
                Arc::new(MemoryKvBackend::new()) as KvBackendRef,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_put_table_info_rejects_table_name_change() {
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
            value: serde_json::to_vec(&TableInfoValue::new(new_table_info)).unwrap(),
        };

        let err = tool.do_work().await.unwrap_err();
        assert!(
            err.output_msg()
                .contains("Invalid table info: table name is immutable")
        );
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
            value: serde_json::to_vec(&TableInfoValue::new(new_table_info)).unwrap(),
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
            value: serde_json::to_vec(&new_table_route).unwrap(),
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
            value: serde_json::to_vec(&TableRouteValue::logical(table_id + 1)).unwrap(),
        };

        let err = tool.do_work().await.unwrap_err();
        assert!(err.output_msg().contains("non-physical TableRouteValue."));
    }

    #[tokio::test]
    async fn test_put_table_command_builds_tool() {
        let value = serde_json::to_vec(&TableRouteValue::logical(1025)).unwrap();
        let command = PutTableRouteCommand::parse_from([
            "route",
            "--table-id",
            "1024",
            "--value-stdin",
            "--backend",
            "memory-store",
            "--store-addrs",
            "memory://",
        ]);

        let _tool = command
            .build_for_test(
                BufReader::new(value.as_slice()),
                Arc::new(MemoryKvBackend::new()) as KvBackendRef,
            )
            .await
            .unwrap();
    }
}
