use api::v1::meta::router_server;
use api::v1::meta::CreateRequest;
use api::v1::meta::Error;
use api::v1::meta::PeerDict;
use api::v1::meta::PutRequest;
use api::v1::meta::RangeRequest;
use api::v1::meta::Region;
use api::v1::meta::RegionRoute;
use api::v1::meta::ResponseHeader;
use api::v1::meta::RouteRequest;
use api::v1::meta::RouteResponse;
use api::v1::meta::Table;
use api::v1::meta::TableRoute;
use api::v1::meta::TableRouteValue;
use common_catalog::TableGlobalKey;
use common_catalog::TableGlobalValue;
use common_telemetry::warn;
use snafu::OptionExt;
use snafu::ResultExt;
use tonic::Request;
use tonic::Response;

use super::store::kv::KvStoreRef;
use super::GrpcResult;
use crate::error;
use crate::error::Result;
use crate::keys::TableRouteKey;
use crate::metasrv::Context;
use crate::metasrv::MetaSrv;
use crate::metasrv::SelectorRef;
use crate::sequence::SequenceRef;

#[async_trait::async_trait]
impl router_server::Router for MetaSrv {
    async fn route(&self, req: Request<RouteRequest>) -> GrpcResult<RouteResponse> {
        let req = req.into_inner();
        let ctx = self.new_ctx();
        let res = handle_route(req, ctx).await?;

        Ok(Response::new(res))
    }

    async fn create(&self, req: Request<CreateRequest>) -> GrpcResult<RouteResponse> {
        let req = req.into_inner();
        let ctx = self.new_ctx();
        let selector = self.selector();
        let table_id_sequence = self.table_id_sequence();
        let res = handle_create(req, ctx, selector, table_id_sequence).await?;

        Ok(Response::new(res))
    }
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

    let mut peer_dict = PeerDict::default();
    let mut table_routes = vec![];
    for (tg, tr) in tables {
        let TableRouteValue {
            peers,
            mut table_route,
        } = tr;
        if let Some(ref mut table_route) = table_route {
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

            if let Some(ref mut table) = table_route.table {
                table.table_schema = tg.as_bytes().context(error::InvalidCatalogValueSnafu)?;
            }
        }
        if let Some(table_route) = table_route {
            table_routes.push(table_route)
        }
    }
    let peers = peer_dict.into_peers();

    let header = Some(ResponseHeader::success(cluster_id));
    Ok(RouteResponse {
        header,
        peers,
        table_routes,
    })
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
    } = req;
    let table_name = table_name.context(error::EmptyTableNameSnafu)?;
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
    let table_route_key = TableRouteKey::with_table_name(id, &table_name)
        .key()
        .into_bytes();

    let table = Table {
        id,
        table_name: Some(table_name),
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
    save_table_route_value(&ctx.kv_store, table_route_key, table_route_value).await?;

    let header = Some(ResponseHeader::success(cluster_id));
    Ok(RouteResponse {
        header,
        peers,
        table_routes: vec![table_route],
    })
}

async fn save_table_route_value(
    kv_store: &KvStoreRef,
    key: impl Into<Vec<u8>>,
    value: impl Into<Vec<u8>>,
) -> Result<()> {
    let key = key.into();
    let value = value.into();
    let put_req = PutRequest {
        key,
        value,
        ..Default::default()
    };
    let _ = kv_store.put(put_req).await?;

    Ok(())
}

async fn fetch_tables(
    kv_store: &KvStoreRef,
    keys: impl Iterator<Item = TableGlobalKey>,
) -> Result<Vec<(TableGlobalValue, TableRouteValue)>> {
    let mut tables = vec![];
    // Maybe we can optimize the for loop in the future, but in general,
    // there won't be many keys, in fact, there is usually just one.
    for tk in keys {
        let tv = get_table_global_value(kv_store, &tk).await?;
        if tv.is_none() {
            warn!("Table global value is absent: {}", tk);
            continue;
        }
        let tv = tv.unwrap();

        let table_id = tv.id as u64;
        let tr_key = TableRouteKey::with_table_global_key(table_id, &tk);
        let tr = get_table_route_value(kv_store, &tr_key).await?;

        tables.push((tv, tr));
    }

    Ok(tables)
}

async fn get_table_route_value(
    kv_store: &KvStoreRef,
    key: &TableRouteKey<'_>,
) -> Result<TableRouteValue> {
    let tr = get_from_store(kv_store, key.key().into_bytes())
        .await?
        .context(error::TableRouteNotFoundSnafu { key: key.key() })?;
    let tr: TableRouteValue = tr
        .as_slice()
        .try_into()
        .context(error::DecodeTableRouteSnafu)?;

    Ok(tr)
}

async fn get_table_global_value(
    kv_store: &KvStoreRef,
    key: &TableGlobalKey,
) -> Result<Option<TableGlobalValue>> {
    let tg_key = format!("{}", key).into_bytes();
    let tv = get_from_store(kv_store, tg_key).await?;
    match tv {
        Some(tv) => {
            let tv = TableGlobalValue::parse(&String::from_utf8_lossy(&tv))
                .context(error::InvalidCatalogValueSnafu)?;
            Ok(Some(tv))
        }
        None => Ok(None),
    }
}

async fn get_from_store(kv_store: &KvStoreRef, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
    let req = RangeRequest {
        key,
        ..Default::default()
    };
    let res = kv_store.range(req).await?;
    let mut kvs = res.kvs;
    if kvs.is_empty() {
        Ok(None)
    } else {
        Ok(Some(kvs.pop().unwrap().value))
    }
}
