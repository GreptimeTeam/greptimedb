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

use api::v1::meta::{
    router_server, Peer, PeerDict, ResponseHeader, RouteRequest, RouteResponse, TableRoute,
    TableRouteValue,
};
use common_meta::key::table_info::TableInfoValue;
use common_telemetry::timer;
use snafu::ResultExt;
use tonic::{Request, Response};

use crate::error::{Result, TableMetadataManagerSnafu};
use crate::metasrv::{Context, MetaSrv};
use crate::metrics::METRIC_META_ROUTE_REQUEST;
use crate::service::GrpcResult;
use crate::table_routes::fetch_tables;

#[async_trait::async_trait]
impl router_server::Router for MetaSrv {
    async fn route(&self, req: Request<RouteRequest>) -> GrpcResult<RouteResponse> {
        let req = req.into_inner();
        let cluster_id = req.header.as_ref().map_or(0, |h| h.cluster_id);

        let _timer = timer!(
            METRIC_META_ROUTE_REQUEST,
            &[
                ("op", "route".to_string()),
                ("cluster_id", cluster_id.to_string())
            ]
        );

        let ctx = self.new_ctx();
        let res = handle_route(req, ctx).await?;

        Ok(Response::new(res))
    }
}

async fn handle_route(req: RouteRequest, ctx: Context) -> Result<RouteResponse> {
    let RouteRequest { header, table_ids } = req;
    let cluster_id = header.as_ref().map_or(0, |h| h.cluster_id);

    let table_ids = table_ids.iter().map(|x| x.id).collect::<Vec<_>>();
    let tables = fetch_tables(&ctx, table_ids).await?;

    let (peers, table_routes) = fill_table_routes(tables)?;

    let header = Some(ResponseHeader::success(cluster_id));
    Ok(RouteResponse {
        header,
        peers,
        table_routes,
    })
}

pub(crate) fn fill_table_routes(
    tables: Vec<(TableInfoValue, TableRouteValue)>,
) -> Result<(Vec<Peer>, Vec<TableRoute>)> {
    let mut peer_dict = PeerDict::default();
    let mut table_routes = vec![];
    for (tgv, trv) in tables {
        let TableRouteValue {
            peers,
            mut table_route,
        } = trv;
        if let Some(table_route) = &mut table_route {
            for rr in &mut table_route.region_routes {
                if let Some(peer) = peers.get(rr.leader_peer_index as usize) {
                    rr.leader_peer_index = peer_dict.get_or_insert(peer.clone()) as u64;
                }
                for index in &mut rr.follower_peer_indexes {
                    if let Some(peer) = peers.get(*index as usize) {
                        *index = peer_dict.get_or_insert(peer.clone()) as u64;
                    }
                }
            }

            if let Some(table) = &mut table_route.table {
                table.table_schema = tgv.try_as_raw_value().context(TableMetadataManagerSnafu)?;
            }
        }
        if let Some(table_route) = table_route {
            table_routes.push(table_route)
        }
    }

    Ok((peer_dict.into_peers(), table_routes))
}
