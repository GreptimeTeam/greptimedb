use api::v1::meta::router_server;
use api::v1::meta::CreateRequest;
use api::v1::meta::Peer;
use api::v1::meta::RouteRequest;
use api::v1::meta::RouteResponse;
use snafu::OptionExt;
use tonic::Request;
use tonic::Response;

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

async fn handle_create(req: CreateRequest, _kv_store: KvStoreRef) -> Result<RouteResponse> {
    let CreateRequest { table_name, .. } = req;
    let _table_name = table_name.context(error::EmptyTableNameSnafu)?;

    // TODO(jiachun):
    let peers = vec![Peer {
        id: 0,
        addr: "127.0.0.1:3000".to_string(),
    }];

    Ok(RouteResponse {
        peers,
        ..Default::default()
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

        let header = RequestHeader::new((1, 1));
        let req = RouteRequest {
            header: Some(header),
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

        let header = RequestHeader::new((1, 1));
        let table_name = TableName::new("test_catalog", "test_db", "table1");
        let req = CreateRequest {
            header: Some(header),
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
