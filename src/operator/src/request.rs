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
use std::sync::Arc;

use api::v1::region::region_request::Body as RegionRequestBody;
use api::v1::region::{CompactRequest, FlushRequest, RegionRequestHeader};
use catalog::CatalogManagerRef;
use common_catalog::build_db_string;
use common_meta::datanode_manager::{AffectedRows, DatanodeManagerRef};
use common_meta::peer::Peer;
use common_telemetry::tracing_context::TracingContext;
use futures_util::future;
use partition::manager::{PartitionInfo, PartitionRuleManagerRef};
use session::context::QueryContextRef;
use snafu::prelude::*;
use store_api::storage::RegionId;
use table::requests::{CompactTableRequest, FlushTableRequest};

use crate::error::{
    CatalogSnafu, FindRegionLeaderSnafu, FindTablePartitionRuleSnafu, JoinTaskSnafu,
    RequestInsertsSnafu, Result, TableNotFoundSnafu,
};
use crate::region_req_factory::RegionRequestFactory;

/// Region requester which processes flush, compact requests etc.
pub struct Requester {
    catalog_manager: CatalogManagerRef,
    partition_manager: PartitionRuleManagerRef,
    datanode_manager: DatanodeManagerRef,
}

pub type RequesterRef = Arc<Requester>;

impl Requester {
    pub fn new(
        catalog_manager: CatalogManagerRef,
        partition_manager: PartitionRuleManagerRef,
        datanode_manager: DatanodeManagerRef,
    ) -> Self {
        Self {
            catalog_manager,
            partition_manager,
            datanode_manager,
        }
    }

    /// Handle the request to flush table.
    pub async fn handle_table_flush(
        &self,
        request: FlushTableRequest,
        ctx: QueryContextRef,
    ) -> Result<AffectedRows> {
        let partitions = self
            .get_table_partitions(
                &request.catalog_name,
                &request.schema_name,
                &request.table_name,
            )
            .await?;

        let requests = partitions
            .into_iter()
            .map(|partition| {
                RegionRequestBody::Flush(FlushRequest {
                    region_id: partition.id.into(),
                })
            })
            .collect();

        self.do_request(
            requests,
            Some(build_db_string(&request.catalog_name, &request.schema_name)),
            &ctx,
        )
        .await
    }

    /// Handle the request to compact table.
    pub async fn handle_table_compaction(
        &self,
        request: CompactTableRequest,
        ctx: QueryContextRef,
    ) -> Result<AffectedRows> {
        let partitions = self
            .get_table_partitions(
                &request.catalog_name,
                &request.schema_name,
                &request.table_name,
            )
            .await?;

        let requests = partitions
            .into_iter()
            .map(|partition| {
                RegionRequestBody::Compact(CompactRequest {
                    region_id: partition.id.into(),
                })
            })
            .collect();

        self.do_request(
            requests,
            Some(build_db_string(&request.catalog_name, &request.schema_name)),
            &ctx,
        )
        .await
    }

    /// Handle the request to flush the region.
    pub async fn handle_region_flush(
        &self,
        region_id: RegionId,
        ctx: QueryContextRef,
    ) -> Result<AffectedRows> {
        let request = RegionRequestBody::Flush(FlushRequest {
            region_id: region_id.into(),
        });

        self.do_request(vec![request], None, &ctx).await
    }

    /// Handle the request to compact the region.
    pub async fn handle_region_compaction(
        &self,
        region_id: RegionId,
        ctx: QueryContextRef,
    ) -> Result<AffectedRows> {
        let request = RegionRequestBody::Compact(CompactRequest {
            region_id: region_id.into(),
        });

        self.do_request(vec![request], None, &ctx).await
    }
}

impl Requester {
    async fn do_request(
        &self,
        requests: Vec<RegionRequestBody>,
        db_string: Option<String>,
        ctx: &QueryContextRef,
    ) -> Result<AffectedRows> {
        let request_factory = RegionRequestFactory::new(RegionRequestHeader {
            tracing_context: TracingContext::from_current_span().to_w3c(),
            dbname: db_string.unwrap_or_else(|| ctx.get_db_string()),
        });

        let tasks = self
            .group_requests_by_peer(requests)
            .await?
            .into_iter()
            .map(|(peer, body)| {
                let request = request_factory.build_request(body);
                let datanode_manager = self.datanode_manager.clone();
                common_runtime::spawn_write(async move {
                    datanode_manager
                        .datanode(&peer)
                        .await
                        .handle(request)
                        .await
                        .context(RequestInsertsSnafu)
                })
            });
        let results = future::try_join_all(tasks).await.context(JoinTaskSnafu)?;

        let affected_rows = results.into_iter().sum::<Result<AffectedRows>>()?;

        Ok(affected_rows)
    }

    async fn group_requests_by_peer(
        &self,
        requests: Vec<RegionRequestBody>,
    ) -> Result<HashMap<Peer, RegionRequestBody>> {
        let mut inserts: HashMap<Peer, RegionRequestBody> = HashMap::new();

        for req in requests {
            let region_id = match &req {
                RegionRequestBody::Flush(req) => req.region_id,
                RegionRequestBody::Compact(req) => req.region_id,
                _ => todo!(),
            };

            let peer = self
                .partition_manager
                .find_region_leader(region_id.into())
                .await
                .context(FindRegionLeaderSnafu)?;

            inserts.insert(peer, req);
        }

        Ok(inserts)
    }

    async fn get_table_partitions(
        &self,
        catalog: &str,
        schema: &str,
        table_name: &str,
    ) -> Result<Vec<PartitionInfo>> {
        let table = self
            .catalog_manager
            .table(catalog, schema, table_name)
            .await
            .context(CatalogSnafu)?;

        let table = table.with_context(|| TableNotFoundSnafu {
            table_name: common_catalog::format_full_table_name(catalog, schema, table_name),
        })?;
        let table_info = table.table_info();

        self.partition_manager
            .find_table_partitions(table_info.ident.table_id)
            .await
            .with_context(|_| FindTablePartitionRuleSnafu {
                table_name: common_catalog::format_full_table_name(catalog, schema, table_name),
            })
    }
}
