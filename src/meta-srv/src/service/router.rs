use api::v1::meta::router_server;
use api::v1::meta::CreateRequest;
use api::v1::meta::Region;
use api::v1::meta::RegionRoute;
use api::v1::meta::ResponseHeader;
use api::v1::meta::RouteRequest;
use api::v1::meta::RouteResponse;
use api::v1::meta::Table;
use api::v1::meta::TableRoute;
use snafu::OptionExt;
use tonic::Request;
use tonic::Response;

use super::GrpcResult;
use crate::error;
use crate::error::Result;
use crate::metasrv::Context;
use crate::metasrv::MetaSrv;
use crate::metasrv::SelectorRef;

#[async_trait::async_trait]
impl router_server::Router for MetaSrv {
    async fn route(&self, req: Request<RouteRequest>) -> GrpcResult<RouteResponse> {
        let req = req.into_inner();
        let res = handle_route(req).await?;

        Ok(Response::new(res))
    }

    async fn create(&self, req: Request<CreateRequest>) -> GrpcResult<RouteResponse> {
        let req = req.into_inner();
        let ctx = self.new_ctx();
        let selector = self.selector();
        let res = handle_create(req, ctx, selector).await?;

        Ok(Response::new(res))
    }
}

async fn handle_route(_req: RouteRequest) -> Result<RouteResponse> {
    todo!()
}

async fn handle_create(
    req: CreateRequest,
    ctx: Context,
    selector: SelectorRef,
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
    use crate::selector::{Namespace, Selector};
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
        let res = handle_create(req, ctx, selector).await.unwrap();

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
