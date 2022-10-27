use api::v1::meta::router_server;
use api::v1::meta::CreateRequest;
use api::v1::meta::Region;
use api::v1::meta::RegionRoute;
use api::v1::meta::ResponseHeader;
use api::v1::meta::RouteRequest;
use api::v1::meta::RouteResponse;
use api::v1::meta::Table;
use api::v1::meta::TableRoute;
use common_telemetry::info;
use snafu::OptionExt;
use tonic::Request;
use tonic::Response;

use super::store::helper;
use super::store::kv::KvStoreRef;
use super::GrpcResult;
use crate::error;
use crate::error::Result;
use crate::metasrv::MetaSrv;

#[async_trait::async_trait]
impl router_server::Router for MetaSrv {
    async fn route(&self, req: Request<RouteRequest>) -> GrpcResult<RouteResponse> {
        let req = req.into_inner();
        let kv_store = self.kv_store();
        let res = handle_route(req, kv_store).await?;

        Ok(Response::new(res))
    }

    async fn create(&self, req: Request<CreateRequest>) -> GrpcResult<RouteResponse> {
        let req = req.into_inner();
        let kv_store = self.kv_store();
        let res = handle_create(req, kv_store).await?;

        Ok(Response::new(res))
    }
}

async fn handle_route(_req: RouteRequest, _kv_store: KvStoreRef) -> Result<RouteResponse> {
    todo!()
}

async fn handle_create(req: CreateRequest, kv_store: KvStoreRef) -> Result<RouteResponse> {
    let CreateRequest {
        header,
        table_name,
        partitions,
    } = req;
    let table_name = table_name.context(error::EmptyTableNameSnafu)?;
    let cluster_id = header.as_ref().map_or(0, |h| h.cluster_id);
    // TODO(jiachun): now we just return all
    let peers = helper::find_all_datanodes(cluster_id, kv_store).await?;

    let table = Table {
        table_name: Some(table_name),
        ..Default::default() // TODO(jiachun) return schema
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
    let res_header = ResponseHeader {
        cluster_id,
        ..Default::default()
    };
    let res = RouteResponse {
        header: Some(res_header),
        peers,
        table_routes: vec![table_route],
    };

    info!("Create route response: {:?}", res);
    Ok(res)
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
            header: request_header((1, 1)),
            ..Default::default()
        };
        let req = req
            .add_table(TableName::new("catalog1", "schema1", "table1"))
            .add_table(TableName::new("catalog1", "schema1", "table2"))
            .add_table(TableName::new("catalog1", "schema1", "table3"));

        let _res = meta_srv.route(req.into_request()).await.unwrap();
    }

    #[tokio::test]
    async fn test_handle_create() {
        let kv_store = Arc::new(NoopKvStore {});
        let meta_srv = MetaSrv::new(MetaSrvOptions::default(), kv_store).await;

        let table_name = TableName::new("test_catalog", "test_db", "table1");
        let req = CreateRequest {
            header: request_header((1, 1)),
            table_name: Some(table_name),
            ..Default::default()
        };

        let p0 = Partition::new()
            .column_list(vec![b"col1".to_vec(), b"col2".to_vec()])
            .value_list(vec![b"v1".to_vec(), b"v2".to_vec()]);

        let p1 = Partition::new()
            .column_list(vec![b"col1".to_vec(), b"col2".to_vec()])
            .value_list(vec![b"v11".to_vec(), b"v22".to_vec()]);

        let req = req.add_partition(p0).add_partition(p1);

        let res = meta_srv.create(req.into_request()).await.unwrap();

        for r in res.into_inner().peers {
            assert_eq!("127.0.0.1:3000", r.addr);
        }
    }
}
