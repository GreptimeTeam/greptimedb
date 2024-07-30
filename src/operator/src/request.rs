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

use std::sync::Arc;

use api::v1::region::region_request::Body as RegionRequestBody;
use api::v1::region::{CompactRequest, FlushRequest, RegionRequestHeader};
use catalog::CatalogManagerRef;
use common_catalog::build_db_string;
use common_meta::node_manager::{AffectedRows, NodeManagerRef};
use common_meta::peer::Peer;
use common_telemetry::tracing_context::TracingContext;
use common_telemetry::{error, info};
use futures_util::future;
use partition::manager::{PartitionInfo, PartitionRuleManagerRef};
use session::context::QueryContextRef;
use snafu::prelude::*;
use store_api::storage::RegionId;
use table::requests::{CompactTableRequest, FlushTableRequest};

use crate::error::{
    CatalogSnafu, FindRegionLeaderSnafu, FindTablePartitionRuleSnafu, JoinTaskSnafu,
    RequestRegionSnafu, Result, TableNotFoundSnafu, UnsupportedRegionRequestSnafu,
};
use crate::region_req_factory::RegionRequestFactory;

/// Region requester which processes flush, compact requests etc.
pub struct Requester {
    catalog_manager: CatalogManagerRef,
    partition_manager: PartitionRuleManagerRef,
    node_manager: NodeManagerRef,
}

pub type RequesterRef = Arc<Requester>;

impl Requester {
    pub fn new(
        catalog_manager: CatalogManagerRef,
        partition_manager: PartitionRuleManagerRef,
        node_manager: NodeManagerRef,
    ) -> Self {
        Self {
            catalog_manager,
            partition_manager,
            node_manager,
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

        info!("Handle table manual flush request: {:?}", request);

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
                    options: Some(request.compact_options.clone()),
                })
            })
            .collect();

        info!("Handle table manual compaction request: {:?}", request);

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

        info!("Handle region manual flush request: {region_id}");
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
            options: None, // todo(hl): maybe also support parameters in region compaction.
        });

        info!("Handle region manual compaction request: {region_id}");
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
            ..Default::default()
        });

        let tasks = requests.into_iter().map(|req_body| {
            let request = request_factory.build_request(req_body.clone());
            let partition_manager = self.partition_manager.clone();
            let node_manager = self.node_manager.clone();
            common_runtime::spawn_global(async move {
                let peer =
                    Self::find_region_leader_by_request(partition_manager, &req_body).await?;
                node_manager
                    .datanode(&peer)
                    .await
                    .handle(request)
                    .await
                    .context(RequestRegionSnafu)
            })
        });
        let results = future::try_join_all(tasks).await.context(JoinTaskSnafu)?;

        let affected_rows = results
            .into_iter()
            .map(|resp| resp.map(|r| r.affected_rows))
            .sum::<Result<AffectedRows>>()?;

        Ok(affected_rows)
    }

    async fn find_region_leader_by_request(
        partition_manager: PartitionRuleManagerRef,
        req: &RegionRequestBody,
    ) -> Result<Peer> {
        let region_id = match req {
            RegionRequestBody::Flush(req) => req.region_id,
            RegionRequestBody::Compact(req) => req.region_id,
            _ => {
                error!("Unsupported region request: {:?}", req);
                return UnsupportedRegionRequestSnafu {}.fail();
            }
        };

        partition_manager
            .find_region_leader(region_id.into())
            .await
            .context(FindRegionLeaderSnafu)
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
