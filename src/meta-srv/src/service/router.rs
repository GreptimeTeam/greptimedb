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

use api::v1::meta::{
    router_server, BatchPutRequest, CreateRequest, DeleteRequest, Error, KeyValue,
    MoveValueRequest, Peer, PeerDict, Region, RegionRoute, ResponseHeader, RouteRequest,
    RouteResponse, Table, TableName, TableRoute, TableRouteValue,
};
use catalog::helper::{TableGlobalKey, TableGlobalValue};
use common_telemetry::{timer, warn};
use snafu::{OptionExt, ResultExt};
use table::metadata::RawTableInfo;
use tonic::{Request, Response};

use crate::error;
use crate::error::Result;
use crate::keys::TableRouteKey;
use crate::metasrv::{Context, MetaSrv, SelectorRef};
use crate::metrics::METRIC_META_ROUTE_REQUEST;
use crate::sequence::SequenceRef;
use crate::service::store::ext::KvStoreExt;
use crate::service::store::kv::KvStoreRef;
use crate::service::GrpcResult;

#[async_trait::async_trait]
impl router_server::Router for MetaSrv {
    async fn create(&self, req: Request<CreateRequest>) -> GrpcResult<RouteResponse> {
        let req = req.into_inner();

        let CreateRequest {
            header, table_name, ..
        } = &req;
        let cluster_id = header.as_ref().map_or(0, |h| h.cluster_id);

        let _timer = timer!(
            METRIC_META_ROUTE_REQUEST,
            &[("op", "create"), ("cluster_id", &cluster_id.to_string())]
        );

        let table_name = table_name.clone().context(error::EmptyTableNameSnafu)?;
        let ctx = self.create_ctx(table_name);

        let selector = self.selector();
        let table_id_sequence = self.table_id_sequence();

        let res = handle_create(req, ctx, selector, table_id_sequence).await?;

        Ok(Response::new(res))
    }

    async fn route(&self, req: Request<RouteRequest>) -> GrpcResult<RouteResponse> {
        let req = req.into_inner();
        let cluster_id = req.header.as_ref().map_or(0, |h| h.cluster_id);

        let _timer = timer!(
            METRIC_META_ROUTE_REQUEST,
            &[("op", "route"), ("cluster_id", &cluster_id.to_string())]
        );

        let ctx = self.new_ctx();
        let res = handle_route(req, ctx).await?;

        Ok(Response::new(res))
    }

    async fn delete(&self, req: Request<DeleteRequest>) -> GrpcResult<RouteResponse> {
        let req = req.into_inner();
        let cluster_id = req.header.as_ref().map_or(0, |h| h.cluster_id);

        let _timer = timer!(
            METRIC_META_ROUTE_REQUEST,
            &[("op", "delete"), ("cluster_id", &cluster_id.to_string())]
        );

        let ctx = self.new_ctx();
        let res = handle_delete(req, ctx).await?;

        Ok(Response::new(res))
    }
}

impl MetaSrv {
    fn create_ctx(&self, table_name: TableName) -> Context {
        let mut ctx = self.new_ctx();
        let TableName {
            catalog_name,
            schema_name,
            table_name,
        } = table_name;
        ctx.catalog = Some(catalog_name);
        ctx.schema = Some(schema_name);
        ctx.table = Some(table_name);
        ctx
    }
}

async fn handle_create(
    req: CreateRequest,
    ctx: Context,
    selector: SelectorRef,
    table_id_sequence: SequenceRef,
) -> Result<RouteResponse> {
    let CreateRequest {
        header,
        table_name,
        partitions,
        table_info,
    } = req;
    let table_name = table_name.context(error::EmptyTableNameSnafu)?;

    let mut table_info: RawTableInfo =
        serde_json::from_slice(&table_info).with_context(|_| error::DeserializeFromJsonSnafu {
            input: format!(
                "Corrupted table info: {}",
                String::from_utf8_lossy(&table_info)
            ),
        })?;

    let cluster_id = header.as_ref().map_or(0, |h| h.cluster_id);
    let peers = selector.select(cluster_id, &ctx).await?;
    if peers.is_empty() {
        let header = Some(ResponseHeader::failed(
            cluster_id,
            Error::no_active_datanodes(),
        ));
        return Ok(RouteResponse {
            header,
            ..Default::default()
        });
    }
    let id = table_id_sequence.next().await?;
    table_info.ident.table_id = id as u32;

    let table_route_key = TableRouteKey::with_table_name(id, &table_name)
        .key()
        .into_bytes();

    let table = Table {
        id,
        table_name: Some(table_name.clone()),
        ..Default::default()
    };
    let mut region_routes = Vec::with_capacity(partitions.len());
    for (i, partition) in partitions.into_iter().enumerate() {
        let region = Region {
            id: i as u64,
            partition: Some(partition),
            ..Default::default()
        };
        let region_route = RegionRoute {
            region: Some(region),
            leader_peer_index: (i % peers.len()) as u64,
            follower_peer_indexes: vec![], // follower_peers is not supported at the moment
        };
        region_routes.push(region_route);
    }
    let table_route = TableRoute {
        table: Some(table),
        region_routes,
    };

    // save table route data into meta store
    let table_route_value = TableRouteValue {
        peers: peers.clone(),
        table_route: Some(table_route.clone()),
    };

    let table_global_key = TableGlobalKey {
        catalog_name: table_name.catalog_name.clone(),
        schema_name: table_name.schema_name.clone(),
        table_name: table_name.table_name.clone(),
    }
    .to_string()
    .into_bytes();

    let table_global_value = create_table_global_value(&table_route_value, table_info)?
        .as_bytes()
        .context(error::InvalidCatalogValueSnafu)?;

    let req = BatchPutRequest {
        kvs: vec![
            KeyValue {
                key: table_global_key,
                value: table_global_value,
            },
            KeyValue {
                key: table_route_key,
                value: table_route_value.into(),
            },
        ],
        prev_kv: true,
        ..Default::default()
    };

    let resp = ctx.kv_store.batch_put(req).await?;
    if !resp.prev_kvs.is_empty() {
        warn!(
            "Caution: table meta values are replaced! \
            Maybe last creation procedure for table {table_name:?} was aborted?"
        );
    }

    let header = Some(ResponseHeader::success(cluster_id));
    Ok(RouteResponse {
        header,
        peers,
        table_routes: vec![table_route],
    })
}

fn create_table_global_value(
    table_route_value: &TableRouteValue,
    table_info: RawTableInfo,
) -> Result<TableGlobalValue> {
    let peers = &table_route_value.peers;
    let region_routes = &table_route_value
        .table_route
        .as_ref()
        .context(error::UnexpectedSnafu {
            violated: "table route should have been set",
        })?
        .region_routes;

    let node_id = peers[region_routes[0].leader_peer_index as usize].id;

    let mut regions_id_map = HashMap::with_capacity(region_routes.len());
    for route in region_routes.iter() {
        let node_id = peers[route.leader_peer_index as usize].id;
        let region_id = route
            .region
            .as_ref()
            .context(error::UnexpectedSnafu {
                violated: "region should have been set",
            })?
            .id as u32;
        regions_id_map
            .entry(node_id)
            .or_insert_with(Vec::new)
            .push(region_id);
    }

    Ok(TableGlobalValue {
        node_id,
        regions_id_map,
        table_info,
    })
}

async fn handle_route(req: RouteRequest, ctx: Context) -> Result<RouteResponse> {
    let RouteRequest {
        header,
        table_names,
    } = req;
    let cluster_id = header.as_ref().map_or(0, |h| h.cluster_id);
    let table_global_keys = table_names.into_iter().map(|t| TableGlobalKey {
        catalog_name: t.catalog_name,
        schema_name: t.schema_name,
        table_name: t.table_name,
    });
    let tables = fetch_tables(&ctx.kv_store, table_global_keys).await?;
    let (peers, table_routes) = fill_table_routes(tables)?;

    let header = Some(ResponseHeader::success(cluster_id));
    Ok(RouteResponse {
        header,
        peers,
        table_routes,
    })
}

async fn handle_delete(req: DeleteRequest, ctx: Context) -> Result<RouteResponse> {
    let DeleteRequest { header, table_name } = req;
    let cluster_id = header.as_ref().map_or(0, |h| h.cluster_id);
    let tgk = table_name
        .map(|t| TableGlobalKey {
            catalog_name: t.catalog_name,
            schema_name: t.schema_name,
            table_name: t.table_name,
        })
        .context(error::EmptyTableNameSnafu)?;

    let tgv = get_table_global_value(&ctx.kv_store, &tgk)
        .await?
        .with_context(|| error::TableNotFoundSnafu {
            name: format!("{tgk}"),
        })?;

    let _ = remove_table_global_value(&ctx.kv_store, &tgk).await?;

    let trk = TableRouteKey::with_table_global_key(tgv.table_id() as u64, &tgk);
    let (_, trv) = remove_table_route_value(&ctx.kv_store, &trk).await?;
    let (peers, table_routes) = fill_table_routes(vec![(tgv, trv)])?;

    let header = Some(ResponseHeader::success(cluster_id));
    Ok(RouteResponse {
        header,
        peers,
        table_routes,
    })
}

fn fill_table_routes(
    tables: Vec<(TableGlobalValue, TableRouteValue)>,
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
                table.table_schema = tgv.as_bytes().context(error::InvalidCatalogValueSnafu)?;
            }
        }
        if let Some(table_route) = table_route {
            table_routes.push(table_route)
        }
    }

    Ok((peer_dict.into_peers(), table_routes))
}

async fn fetch_tables(
    kv_store: &KvStoreRef,
    keys: impl Iterator<Item = TableGlobalKey>,
) -> Result<Vec<(TableGlobalValue, TableRouteValue)>> {
    let mut tables = vec![];
    // Maybe we can optimize the for loop in the future, but in general,
    // there won't be many keys, in fact, there is usually just one.
    for tgk in keys {
        let tgv = get_table_global_value(kv_store, &tgk).await?;
        if tgv.is_none() {
            warn!("Table global value is absent: {}", tgk);
            continue;
        }
        let tgv = tgv.unwrap();

        let trk = TableRouteKey::with_table_global_key(tgv.table_id() as u64, &tgk);
        let trv = get_table_route_value(kv_store, &trk).await?;

        tables.push((tgv, trv));
    }

    Ok(tables)
}

async fn get_table_route_value(
    kv_store: &KvStoreRef,
    key: &TableRouteKey<'_>,
) -> Result<TableRouteValue> {
    let trkv = kv_store
        .get(key.key().into_bytes())
        .await?
        .context(error::TableRouteNotFoundSnafu { key: key.key() })?;
    let trv: TableRouteValue = trkv
        .value
        .as_slice()
        .try_into()
        .context(error::DecodeTableRouteSnafu)?;

    Ok(trv)
}

async fn remove_table_route_value(
    kv_store: &KvStoreRef,
    key: &TableRouteKey<'_>,
) -> Result<(Vec<u8>, TableRouteValue)> {
    let from_key = key.key().into_bytes();
    let to_key = key.removed_key().into_bytes();
    let v = move_value(kv_store, from_key, to_key)
        .await?
        .context(error::TableRouteNotFoundSnafu { key: key.key() })?;
    let trv: TableRouteValue =
        v.1.as_slice()
            .try_into()
            .context(error::DecodeTableRouteSnafu)?;

    Ok((v.0, trv))
}

async fn remove_table_global_value(
    kv_store: &KvStoreRef,
    key: &TableGlobalKey,
) -> Result<(Vec<u8>, TableGlobalValue)> {
    let key = key.to_string();
    let removed_key = crate::keys::to_removed_key(&key);
    let kv = move_value(kv_store, key.as_bytes(), removed_key)
        .await?
        .context(error::TableNotFoundSnafu { name: key })?;
    let value: TableGlobalValue =
        TableGlobalValue::from_bytes(&kv.1).context(error::InvalidCatalogValueSnafu)?;
    Ok((kv.0, value))
}

async fn get_table_global_value(
    kv_store: &KvStoreRef,
    key: &TableGlobalKey,
) -> Result<Option<TableGlobalValue>> {
    let tg_key = format!("{key}").into_bytes();
    let tkv = kv_store.get(tg_key).await?;
    match tkv {
        Some(tkv) => {
            let tv =
                TableGlobalValue::from_bytes(tkv.value).context(error::InvalidCatalogValueSnafu)?;
            Ok(Some(tv))
        }
        None => Ok(None),
    }
}

async fn move_value(
    kv_store: &KvStoreRef,
    from_key: impl Into<Vec<u8>>,
    to_key: impl Into<Vec<u8>>,
) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
    let from_key = from_key.into();
    let to_key = to_key.into();
    let move_req = MoveValueRequest {
        from_key,
        to_key,
        ..Default::default()
    };
    let res = kv_store.move_value(move_req).await?;

    Ok(res.kv.map(|kv| (kv.key, kv.value)))
}
