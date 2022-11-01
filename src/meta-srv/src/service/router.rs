use api::v1::meta::router_server;
use api::v1::meta::CreateRequest;
use api::v1::meta::Peer;
use api::v1::meta::Region;
use api::v1::meta::RegionRoute;
use api::v1::meta::ResponseHeader;
use api::v1::meta::RouteRequest;
use api::v1::meta::RouteResponse;
use api::v1::meta::Table;
use api::v1::meta::TableRoute;
use common_time::util as time_util;
use snafu::OptionExt;
use tonic::Request;
use tonic::Response;

use super::store::kv::KvStoreRef;
use super::GrpcResult;
use crate::error;
use crate::error::Result;
use crate::keys::LeaseKey;
use crate::keys::LeaseValue;
use crate::lease;
use crate::metasrv::MetaSrv;

#[derive(Clone)]
struct Context {
    pub datanode_lease_secs: i64,
    pub kv_store: KvStoreRef,
}

#[async_trait::async_trait]
impl router_server::Router for MetaSrv {
    async fn route(&self, req: Request<RouteRequest>) -> GrpcResult<RouteResponse> {
        let req = req.into_inner();
        let ctx = Context {
            datanode_lease_secs: self.options().datanode_lease_secs,
            kv_store: self.kv_store(),
        };
        let res = handle_route(req, ctx).await?;

        Ok(Response::new(res))
    }

    async fn create(&self, req: Request<CreateRequest>) -> GrpcResult<RouteResponse> {
        let req = req.into_inner();
        let ctx = Context {
            datanode_lease_secs: self.options().datanode_lease_secs,
            kv_store: self.kv_store(),
        };
        let res = handle_create(req, ctx, LeaseBasedSelector::default()).await?;

        Ok(Response::new(res))
    }
}

#[async_trait::async_trait]
trait DatanodeSelector {
    async fn select(&self, id: u64, ctx: &Context) -> Result<Vec<Peer>>;
}

#[derive(Default)]
struct LeaseBasedSelector;

#[async_trait::async_trait]
impl DatanodeSelector for LeaseBasedSelector {
    async fn select(&self, id: u64, ctx: &Context) -> Result<Vec<Peer>> {
        // filter out the nodes out lease
        let lease_filter = |_: &LeaseKey, v: &LeaseValue| {
            time_util::current_time_millis() - v.timestamp_millis < ctx.datanode_lease_secs
        };
        let mut lease_kvs = lease::alive_datanodes(id, ctx.kv_store.clone(), lease_filter).await?;
        // TODO(jiachun): At the moment we are just pushing the latest to the forefront,
        // and it is better to use load-based strategies in the future.
        lease_kvs.sort_by(|a, b| b.1.timestamp_millis.cmp(&a.1.timestamp_millis));

        let peers = lease_kvs
            .into_iter()
            .map(|(k, v)| Peer {
                id: k.node_id,
                addr: v.node_addr,
            })
            .collect::<Vec<_>>();

        Ok(peers)
    }
}

async fn handle_route(_req: RouteRequest, _ctx: Context) -> Result<RouteResponse> {
    todo!()
}

async fn handle_create(
    req: CreateRequest,
    ctx: Context,
    selector: impl DatanodeSelector,
) -> Result<RouteResponse> {
    let CreateRequest {
        header,
        table_name,
        partitions,
    } = req;
    let table_name = table_name.context(error::EmptyTableNameSnafu)?;
    let cluster_id = header.as_ref().map_or(0, |h| h.cluster_id);

    let peers = selector.select(cluster_id, &ctx).await?;

    let table = Table {
        table_name: Some(table_name),
        ..Default::default()
    };
    let region_num = partitions.len();
    let mut region_routes = Vec::with_capacity(region_num);
    for i in 0..region_num {
        let region = Region {
            id: i as u64,
            ..Default::default()
        };
        let region_route = RegionRoute {
            region: Some(region),
            leader_peer_index: (i % peers.len()) as u64,
            follower_peer_indexes: vec![(i % peers.len()) as u64],
        };
        region_routes.push(region_route);
    }
    let table_route = TableRoute {
        table: Some(table),
        region_routes,
    };

    let header = Some(ResponseHeader::success(cluster_id));
    Ok(RouteResponse {
        header,
        peers,
        table_routes: vec![table_route],
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::meta::router_server::Router;
    use api::v1::meta::*;
    use tonic::IntoRequest;

    use super::*;
    use crate::metasrv::MetaSrvOptions;
    use crate::service::store::noop::NoopKvStore;

    #[should_panic]
    #[tokio::test]
    async fn test_handle_route() {
        let kv_store = Arc::new(NoopKvStore {});
        let meta_srv = MetaSrv::new(MetaSrvOptions::default(), kv_store).await;

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
    impl DatanodeSelector for MockSelector {
        async fn select(&self, _: u64, _: &Context) -> Result<Vec<Peer>> {
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
        let res = handle_create(req, ctx, MockSelector {}).await.unwrap();

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
