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

use api::v1::region::{region_request, InsertRequests, RegionRequest, RegionRequestHeader};
use common_meta::datanode_manager::AffectedRows;
use common_meta::peer::Peer;
use futures_util::future;
use metrics::counter;
use snafu::{OptionExt, ResultExt};
use store_api::storage::RegionId;

use crate::catalog::FrontendCatalogManager;
use crate::error::{
    FindDatanodeSnafu, FindTableRouteSnafu, JoinTaskSnafu, RequestInsertsSnafu, Result,
};

/// A distributed inserter. It ingests gRPC [InsertRequests].
///
/// Table data partitioning and Datanode requests batching are handled inside.
pub struct DistInserter<'a> {
    catalog_manager: &'a FrontendCatalogManager,
    trace_id: u64,
    span_id: u64,
}

impl<'a> DistInserter<'a> {
    pub fn new(catalog_manager: &'a FrontendCatalogManager, trace_id: u64, span_id: u64) -> Self {
        Self {
            catalog_manager,
            trace_id,
            span_id,
        }
    }

    pub(crate) async fn insert(&self, requests: InsertRequests) -> Result<AffectedRows> {
        let requests = self.group_by_peer(requests).await?;
        let trace_id = self.trace_id;
        let span_id = self.span_id;
        let results = future::try_join_all(requests.into_iter().map(|(peer, inserts)| {
            let datanode_clients = self.catalog_manager.datanode_manager();
            common_runtime::spawn_write(async move {
                let request = RegionRequest {
                    header: Some(RegionRequestHeader { trace_id, span_id }),
                    body: Some(region_request::Body::Inserts(inserts)),
                };
                datanode_clients
                    .datanode(&peer)
                    .await
                    .handle(request)
                    .await
                    .context(RequestInsertsSnafu)
            })
        }))
        .await
        .context(JoinTaskSnafu)?;

        let affected_rows = results.into_iter().sum::<Result<u64>>()?;
        counter!(crate::metrics::DIST_INGEST_ROW_COUNT, affected_rows);
        Ok(affected_rows)
    }

    async fn group_by_peer(
        &self,
        requests: InsertRequests,
    ) -> Result<HashMap<Peer, InsertRequests>> {
        let partition_manager = self.catalog_manager.partition_manager();
        let mut inserts: HashMap<Peer, InsertRequests> = HashMap::new();

        for req in requests.requests {
            let region_id = RegionId::from_u64(req.region_id);
            let table_id = region_id.table_id();
            let region_number = region_id.region_number();
            let table_route = partition_manager
                .find_table_route(table_id)
                .await
                .context(FindTableRouteSnafu { table_id })?;
            let peer =
                table_route
                    .find_region_leader(region_number)
                    .context(FindDatanodeSnafu {
                        region: region_number,
                    })?;

            inserts.entry(peer.clone()).or_default().requests.push(req);
        }

        Ok(inserts)
    }
}
