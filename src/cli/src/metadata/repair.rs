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

mod alter_table;
mod create_table;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use clap::Parser;
use client::api::v1::CreateTableExpr;
use client::client_manager::NodeClients;
use client::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_grpc::channel_manager::ChannelConfig;
use common_meta::error::Error as CommonMetaError;
use common_meta::key::TableMetadataManager;
use common_meta::kv_backend::KvBackendRef;
use common_meta::peer::Peer;
use common_meta::region_rpc::RegionRpcRef;
use common_meta::rpc::router::{RegionRoute, find_leaders};
use common_telemetry::{error, info, warn};
use futures::TryStreamExt;
use snafu::{ResultExt, ensure};
use store_api::storage::TableId;

use crate::Tool;
use crate::common::StoreConfig;
use crate::error::{
    InvalidArgumentsSnafu, Result, SendRequestToDatanodeSnafu, TableMetadataSnafu, UnexpectedSnafu,
};
use crate::metadata::utils::{FullTableMetadata, IteratorInput, TableMetadataIterator};

/// Repair metadata of logical tables.
#[derive(Debug, Default, Parser)]
pub struct RepairLogicalTablesCommand {
    /// The names of the tables to repair.
    #[clap(long, value_delimiter = ',', alias = "table-name")]
    table_names: Vec<String>,

    /// The id of the table to repair.
    #[clap(long, value_delimiter = ',', alias = "table-id")]
    table_ids: Vec<TableId>,

    /// The schema of the tables to repair.
    #[clap(long, default_value = DEFAULT_SCHEMA_NAME)]
    schema_name: String,

    /// The catalog of the tables to repair.
    #[clap(long, default_value = DEFAULT_CATALOG_NAME)]
    catalog_name: String,

    /// Whether to fail fast if any repair operation fails.
    #[clap(long)]
    fail_fast: bool,

    #[clap(flatten)]
    store: StoreConfig,

    /// The timeout for the client to operate the datanode.
    #[clap(long, default_value_t = 30)]
    client_timeout_secs: u64,

    /// The timeout for the client to connect to the datanode.
    #[clap(long, default_value_t = 3)]
    client_connect_timeout_secs: u64,
}

impl RepairLogicalTablesCommand {
    fn validate(&self) -> Result<()> {
        ensure!(
            !self.table_names.is_empty() || !self.table_ids.is_empty(),
            InvalidArgumentsSnafu {
                msg: "You must specify --table-names or --table-ids.",
            }
        );
        Ok(())
    }
}

impl RepairLogicalTablesCommand {
    pub async fn build(&self) -> std::result::Result<Box<dyn Tool>, BoxedError> {
        self.validate().map_err(BoxedError::new)?;
        let kv_backend = self.store.build().await?;
        let node_client_channel_config = ChannelConfig::new()
            .timeout(Some(Duration::from_secs(self.client_timeout_secs)))
            .connect_timeout(Duration::from_secs(self.client_connect_timeout_secs));
        let region_rpc: RegionRpcRef = Arc::new(NodeClients::new(node_client_channel_config));

        Ok(Box::new(RepairTool {
            table_names: self.table_names.clone(),
            table_ids: self.table_ids.clone(),
            schema_name: self.schema_name.clone(),
            catalog_name: self.catalog_name.clone(),
            fail_fast: self.fail_fast,
            kv_backend,
            region_rpc,
        }))
    }
}

struct RepairTool {
    table_names: Vec<String>,
    table_ids: Vec<TableId>,
    schema_name: String,
    catalog_name: String,
    fail_fast: bool,
    kv_backend: KvBackendRef,
    region_rpc: RegionRpcRef,
}

#[async_trait]
impl Tool for RepairTool {
    async fn do_work(&self) -> std::result::Result<(), BoxedError> {
        self.repair_tables().await.map_err(BoxedError::new)
    }
}

impl RepairTool {
    fn generate_iterator_input(&self) -> Result<IteratorInput> {
        if !self.table_names.is_empty() {
            let table_names = &self.table_names;
            let catalog = &self.catalog_name;
            let schema_name = &self.schema_name;

            let table_names = table_names
                .iter()
                .map(|table_name| (catalog.clone(), schema_name.clone(), table_name.clone()))
                .collect::<Vec<_>>();
            return Ok(IteratorInput::new_table_names(table_names));
        } else if !self.table_ids.is_empty() {
            return Ok(IteratorInput::new_table_ids(self.table_ids.clone()));
        };

        InvalidArgumentsSnafu {
            msg: "You must specify --table-names or --table-id.",
        }
        .fail()
    }

    async fn repair_tables(&self) -> Result<()> {
        let input = self.generate_iterator_input()?;
        let mut table_metadata_iterator =
            Box::pin(TableMetadataIterator::new(self.kv_backend.clone(), input).into_stream());
        let table_metadata_manager = TableMetadataManager::new(self.kv_backend.clone());

        let mut skipped_table = 0;
        let mut success_table = 0;
        while let Some(full_table_metadata) = table_metadata_iterator.try_next().await? {
            let full_table_name = full_table_metadata.full_table_name();
            if !full_table_metadata.is_metric_engine() {
                warn!(
                    "Skipping repair for non-metric engine table: {}",
                    full_table_name
                );
                skipped_table += 1;
                continue;
            }

            if full_table_metadata.is_physical_table() {
                warn!("Skipping repair for physical table: {}", full_table_name);
                skipped_table += 1;
                continue;
            }

            let (physical_table_id, physical_table_route) = table_metadata_manager
                .table_route_manager()
                .get_physical_table_route(full_table_metadata.table_id)
                .await
                .context(TableMetadataSnafu)?;

            if let Err(err) = self
                .repair_table(
                    &full_table_metadata,
                    physical_table_id,
                    &physical_table_route.region_routes,
                )
                .await
            {
                error!(
                    err;
                    "Failed to repair table: {}, skipped table: {}",
                    full_table_name,
                    skipped_table,
                );

                if self.fail_fast {
                    return Err(err);
                }
            } else {
                success_table += 1;
            }
        }

        info!(
            "Repair logical tables result: {} tables repaired, {} tables skipped",
            success_table, skipped_table
        );

        Ok(())
    }

    async fn alter_table_on_datanodes(
        &self,
        full_table_metadata: &FullTableMetadata,
        physical_region_routes: &[RegionRoute],
    ) -> Result<Vec<(Peer, CommonMetaError)>> {
        let logical_table_id = full_table_metadata.table_id;
        let alter_table_expr = alter_table::generate_alter_table_expr_for_all_columns(
            &full_table_metadata.table_info,
        )?;
        let region_rpc = self.region_rpc.clone();

        let mut failed_peers = Vec::new();
        info!(
            "Sending alter table requests to all datanodes for table: {}, number of regions:{}.",
            full_table_metadata.full_table_name(),
            physical_region_routes.len()
        );
        let leaders = find_leaders(physical_region_routes);
        for peer in &leaders {
            let alter_table_request = alter_table::make_alter_region_request_for_peer(
                logical_table_id,
                &alter_table_expr,
                peer,
                physical_region_routes,
            )?;
            if let Err(err) = region_rpc.handle_region(peer, alter_table_request).await {
                failed_peers.push((peer.clone(), err));
            }
        }

        Ok(failed_peers)
    }

    async fn create_table_on_datanode(
        &self,
        create_table_expr: &CreateTableExpr,
        logical_table_id: TableId,
        physical_table_id: TableId,
        peer: &Peer,
        physical_region_routes: &[RegionRoute],
    ) -> Result<()> {
        let region_rpc = self.region_rpc.clone();
        let create_table_request = create_table::make_create_region_request_for_peer(
            logical_table_id,
            physical_table_id,
            create_table_expr,
            peer,
            physical_region_routes,
        )?;

        region_rpc
            .handle_region(peer, create_table_request)
            .await
            .with_context(|_| SendRequestToDatanodeSnafu { peer: peer.clone() })?;

        Ok(())
    }

    async fn repair_table(
        &self,
        full_table_metadata: &FullTableMetadata,
        physical_table_id: TableId,
        physical_region_routes: &[RegionRoute],
    ) -> Result<()> {
        let full_table_name = full_table_metadata.full_table_name();
        // First we sends alter table requests to all datanodes with all columns.
        let failed_peers = self
            .alter_table_on_datanodes(full_table_metadata, physical_region_routes)
            .await?;

        if failed_peers.is_empty() {
            info!(
                "All alter table requests sent successfully for table: {}",
                full_table_name
            );
            return Ok(());
        }
        warn!(
            "Sending alter table requests to datanodes for table: {} failed for the datanodes: {:?}",
            full_table_name,
            failed_peers
                .iter()
                .map(|(peer, _)| peer.id)
                .collect::<Vec<_>>()
        );

        let create_table_expr =
            create_table::generate_create_table_expr(&full_table_metadata.table_info)?;

        let mut errors = Vec::new();
        for (peer, err) in failed_peers {
            if err.status_code() != StatusCode::RegionNotFound {
                error!(
                    err;
                    "Sending alter table requests to datanode: {} for table: {} failed",
                    peer.id,
                    full_table_name,
                );
                continue;
            }
            info!(
                "Region not found for table: {}, datanode: {}, trying to create the logical table on that datanode",
                full_table_name, peer.id
            );

            // If the alter table request fails for any datanode, we attempt to create the table on that datanode
            // as a fallback mechanism to ensure table consistency across the cluster.
            if let Err(err) = self
                .create_table_on_datanode(
                    &create_table_expr,
                    full_table_metadata.table_id,
                    physical_table_id,
                    &peer,
                    physical_region_routes,
                )
                .await
            {
                error!(
                    err;
                    "Failed to create table on datanode: {} for table: {}",
                    peer.id, full_table_name
                );
                errors.push(err);
                if self.fail_fast {
                    break;
                }
            } else {
                info!(
                    "Created table on datanode: {} for table: {}",
                    peer.id, full_table_name
                );
            }
        }

        if !errors.is_empty() {
            return UnexpectedSnafu {
                msg: format!(
                    "Failed to create table on datanodes for table: {}",
                    full_table_name,
                ),
            }
            .fail();
        }

        Ok(())
    }
}
