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

use std::collections::HashMap;

use api::v1::region::region_request::Body as PbRegionRequest;
use api::v1::region::{RegionRequest, RegionRequestHeader};
use common_telemetry::tracing_context::TracingContext;
use common_telemetry::warn;
use futures::future::join_all;
use snafu::ensure;
use store_api::metadata::ColumnMetadata;
use store_api::metric_engine_consts::TABLE_COLUMN_METADATA_EXTENSION_KEY;
use store_api::storage::{RegionId, RegionNumber};
use table::metadata::{TableId, TableInfo};
use table::table_name::TableName;

use crate::ddl::utils::raw_table_info::update_table_info_column_ids;
use crate::ddl::utils::{
    add_peer_context_if_needed, convert_region_routes_to_detecting_regions,
    extract_column_metadatas, region_storage_path,
};
use crate::ddl::{CreateRequestBuilder, RegionFailureDetectorControllerRef};
use crate::error::{self, Result};
use crate::key::TableMetadataManagerRef;
use crate::key::table_name::TableNameKey;
use crate::key::table_route::{PhysicalTableRouteValue, TableRouteValue};
use crate::node_manager::NodeManagerRef;
use crate::rpc::router::{RegionRoute, find_leader_regions, find_leaders};

/// [CreateTableExecutor] performs:
/// - Creates the metadata of the table.
/// - Creates the regions on the Datanode nodes.
pub struct CreateTableExecutor {
    create_if_not_exists: bool,
    table_name: TableName,
    builder: CreateRequestBuilder,
}

impl CreateTableExecutor {
    /// Creates a new [`CreateTableExecutor`].
    pub fn new(
        table_name: TableName,
        create_if_not_exists: bool,
        builder: CreateRequestBuilder,
    ) -> Self {
        Self {
            create_if_not_exists,
            table_name,
            builder,
        }
    }

    /// On the prepare step, it performs:
    /// - Checks whether the table exists.
    /// - Returns the table id if the table exists.
    ///
    /// Abort(non-retry):
    /// - Table exists and `create_if_not_exists` is `false`.
    /// - Failed to get the table name value.
    pub async fn on_prepare(
        &self,
        table_metadata_manager: &TableMetadataManagerRef,
    ) -> Result<Option<TableId>> {
        let table_name_value = table_metadata_manager
            .table_name_manager()
            .get(TableNameKey::new(
                &self.table_name.catalog_name,
                &self.table_name.schema_name,
                &self.table_name.table_name,
            ))
            .await?;

        if let Some(value) = table_name_value {
            ensure!(
                self.create_if_not_exists,
                error::TableAlreadyExistsSnafu {
                    table_name: self.table_name.to_string(),
                }
            );

            return Ok(Some(value.table_id()));
        }

        Ok(None)
    }

    pub async fn on_create_regions(
        &self,
        node_manager: &NodeManagerRef,
        table_id: TableId,
        region_routes: &[RegionRoute],
        region_wal_options: &HashMap<RegionNumber, String>,
    ) -> Result<Vec<ColumnMetadata>> {
        let storage_path =
            region_storage_path(&self.table_name.catalog_name, &self.table_name.schema_name);
        let leaders = find_leaders(region_routes);
        let mut create_region_tasks = Vec::with_capacity(leaders.len());
        let partition_exprs = region_routes
            .iter()
            .map(|r| (r.region.id.region_number(), r.region.partition_expr()))
            .collect::<HashMap<_, _>>();

        for datanode in leaders {
            let requester = node_manager.datanode(&datanode).await;

            let regions = find_leader_regions(region_routes, &datanode);
            let mut requests = Vec::with_capacity(regions.len());
            for region_number in regions {
                let region_id = RegionId::new(table_id, region_number);
                let create_region_request = self.builder.build_one(
                    region_id,
                    storage_path.clone(),
                    region_wal_options,
                    &partition_exprs,
                );
                requests.push(PbRegionRequest::Create(create_region_request));
            }

            for request in requests {
                let request = RegionRequest {
                    header: Some(RegionRequestHeader {
                        tracing_context: TracingContext::from_current_span().to_w3c(),
                        ..Default::default()
                    }),
                    body: Some(request),
                };

                let datanode = datanode.clone();
                let requester = requester.clone();
                create_region_tasks.push(async move {
                    requester
                        .handle(request)
                        .await
                        .map_err(add_peer_context_if_needed(datanode))
                });
            }
        }

        let mut results = join_all(create_region_tasks)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        let column_metadatas = if let Some(column_metadatas) =
            extract_column_metadatas(&mut results, TABLE_COLUMN_METADATA_EXTENSION_KEY)?
        {
            column_metadatas
        } else {
            warn!(
                "creating table result doesn't contains extension key `{TABLE_COLUMN_METADATA_EXTENSION_KEY}`,leaving the table's column metadata unchanged"
            );
            vec![]
        };

        Ok(column_metadatas)
    }

    /// Creates table metadata
    ///
    /// Abort(non-retry):
    /// - Failed to create table metadata.
    pub async fn on_create_metadata(
        &self,
        table_metadata_manager: &TableMetadataManagerRef,
        region_failure_detector_controller: &RegionFailureDetectorControllerRef,
        mut table_info: TableInfo,
        column_metadatas: &[ColumnMetadata],
        table_route: PhysicalTableRouteValue,
        region_wal_options: HashMap<RegionNumber, String>,
    ) -> Result<()> {
        if !column_metadatas.is_empty() {
            update_table_info_column_ids(&mut table_info, column_metadatas);
        }
        let detecting_regions =
            convert_region_routes_to_detecting_regions(&table_route.region_routes);
        let table_route = TableRouteValue::Physical(table_route);
        table_metadata_manager
            .create_table_metadata(table_info, table_route, region_wal_options)
            .await?;
        region_failure_detector_controller
            .register_failure_detectors(detecting_regions)
            .await;

        Ok(())
    }

    /// Returns the builder of the executor.
    pub fn builder(&self) -> &CreateRequestBuilder {
        &self.builder
    }
}
