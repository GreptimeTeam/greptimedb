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
    let tables = fetch_tables(ctx.kv_store, table_global_keys).await?;

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
                    rr.leader_peer_index = peer_dict.insert(peer.clone()) as u64;
                }
                for index in &mut rr.follower_peer_indexes {
                    if let Some(peer) = peers.get(*index as usize) {
                        *index = peer_dict.insert(peer.clone()) as u64;
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
    let table_route_key = TableRouteKey::with_table_name(id, &table_name);

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
    key: TableRouteKey,
    value: TableRouteValue,
) -> Result<()> {
    let key = key.key().into_bytes();
    let value: Vec<u8> = value.into();
    let put_req = PutRequest {
        key,
        value,
        ..Default::default()
    };
    let _ = kv_store.put(put_req).await?;

    Ok(())
}

async fn fetch_tables(
    kv_store: KvStoreRef,
    keys: impl Iterator<Item = TableGlobalKey>,
) -> Result<Vec<(TableGlobalValue, TableRouteValue)>> {
    let mut tables = vec![];
    // Maybe we can optimize the for loop in the future, but in general,
    // there won't be many keys, in fact, there is usually just one.
    for tk in keys {
        let tv = get_table_global_value(&kv_store, &tk).await?;
        if tv.is_none() {
            warn!("Table global value is absent: {}", tk);
            continue;
        }
        let tv = tv.unwrap();

        let table_id = tv.id as u64;
        let tr_key = TableRouteKey::with_table_global_key(table_id, &tk);
        let tr = get_table_route_value(&kv_store, tr_key).await?;

        tables.push((tv, tr));
    }

    Ok(tables)
}

async fn get_table_route_value(
    kv_store: &KvStoreRef,
    key: TableRouteKey,
) -> Result<TableRouteValue> {
    let tr_key = key.key();
    let tr = get_form_store(kv_store, tr_key.as_bytes().to_vec())
        .await?
        .context(error::TableRouteNotFoundSnafu { key: tr_key })?;
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
    let tv = get_form_store(kv_store, tg_key).await?;
    match tv {
        Some(tv) => {
            let tv = TableGlobalValue::parse(&String::from_utf8_lossy(&tv))
                .context(error::InvalidCatalogValueSnafu)?;
            Ok(Some(tv))
        }
        None => Ok(None),
    }
}

async fn get_form_store(kv_store: &KvStoreRef, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::meta::router_server::Router;
    use api::v1::meta::*;
    use tonic::IntoRequest;

    use super::*;
    use crate::metasrv::MetaSrvOptions;
    use crate::selector::Namespace;
    use crate::selector::Selector;
    use crate::sequence::Sequence;
    use crate::service::store::noop::NoopKvStore;

    #[should_panic]
    #[tokio::test]
    async fn test_handle_route() {
        let kv_store = Arc::new(NoopKvStore {});
        let meta_srv = MetaSrv::new(MetaSrvOptions::default(), kv_store, None).await;

        let req = RouteRequest {
            header: Some(RequestHeader::new((1, 1))),
            table_names: vec![
                TableName {
                    catalog_name: "catalog1".to_string(),
                    schema_name: "schema1".to_string(),
                    table_name: "table1".to_string(),
                },
                TableName {
                    catalog_name: "catalog1".to_string(),
                    schema_name: "schema1".to_string(),
                    table_name: "table2".to_string(),
                },
                TableName {
                    catalog_name: "catalog1".to_string(),
                    schema_name: "schema1".to_string(),
                    table_name: "table3".to_string(),
                },
            ],
        };

        let _res = meta_srv.route(req.into_request()).await.unwrap();
    }

    struct MockSelector;

    #[async_trait::async_trait]
    impl Selector for MockSelector {
        type Context = Context;
        type Output = Vec<Peer>;

        async fn select(&self, _ns: Namespace, _ctx: &Self::Context) -> Result<Self::Output> {
            Ok(vec![
                Peer {
                    id: 0,
                    addr: "127.0.0.1:3000".to_string(),
                },
                Peer {
                    id: 1,
                    addr: "127.0.0.1:3001".to_string(),
                },
            ])
        }
    }

    #[tokio::test]
    async fn test_handle_create() {
        let kv_store = Arc::new(NoopKvStore {});
        let table_name = TableName {
            catalog_name: "test_catalog".to_string(),
            schema_name: "test_db".to_string(),
            table_name: "table1".to_string(),
        };
        let p0 = Partition {
            column_list: vec![b"col1".to_vec(), b"col2".to_vec()],
            value_list: vec![b"v1".to_vec(), b"v2".to_vec()],
        };
        let p1 = Partition {
            column_list: vec![b"col1".to_vec(), b"col2".to_vec()],
            value_list: vec![b"v11".to_vec(), b"v22".to_vec()],
        };
        let req = CreateRequest {
            header: Some(RequestHeader::new((1, 1))),
            table_name: Some(table_name),
            partitions: vec![p0, p1],
        };
        let ctx = Context {
            datanode_lease_secs: 10,
            kv_store,
        };
        let selector = Arc::new(MockSelector {});
        let sequence = Arc::new(Sequence::new("test", 10, ctx.kv_store.clone()));
        let res = handle_create(req, ctx, selector, sequence).await.unwrap();

        assert_eq!(
            vec![
                Peer {
                    id: 0,
                    addr: "127.0.0.1:3000".to_string(),
                },
                Peer {
                    id: 1,
                    addr: "127.0.0.1:3001".to_string(),
                },
            ],
            res.peers
        );
        assert_eq!(1, res.table_routes.len());
        assert_eq!(2, res.table_routes.get(0).unwrap().region_routes.len());
    }
}
