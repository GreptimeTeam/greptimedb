use api::v1::meta::router_server;
use api::v1::meta::CreateRequest;
use api::v1::meta::CreateResponse;
use api::v1::meta::Peer;
use api::v1::meta::RouteRequest;
use api::v1::meta::RouteResponse;
use tonic::Request;
use tonic::Response;

use super::store::kv::KvStoreRef;
use super::GrpcResult;
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

    async fn create(&self, req: Request<CreateRequest>) -> GrpcResult<CreateResponse> {
        let req = req.into_inner();
        let kv_store = self.kv_store();
        let res = handle_create(req, kv_store).await?;

        Ok(Response::new(res))
    }
}

async fn handle_route(_req: RouteRequest, _kv_store: KvStoreRef) -> Result<RouteResponse> {
    todo!()
}

async fn handle_create(req: CreateRequest, _kv_store: KvStoreRef) -> Result<CreateResponse> {
    let CreateRequest { mut regions, .. } = req;

    // TODO(jiachun): route table
    for r in &mut regions {
        r.peer = Some(Peer::new(0, "127.0.0.1:3000"));
    }

    Ok(CreateResponse {
        regions,
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
    use crate::service::store::kv::KvStore;

    struct MockKvStore;

    #[async_trait::async_trait]
    impl KvStore for MockKvStore {
        async fn range(&self, _req: RangeRequest) -> crate::Result<RangeResponse> {
            unreachable!()
        }

        async fn put(&self, _req: PutRequest) -> crate::Result<PutResponse> {
            unreachable!()
        }

        async fn delete_range(
            &self,
            _req: DeleteRangeRequest,
        ) -> crate::Result<DeleteRangeResponse> {
            unreachable!()
        }
    }

    #[should_panic]
    #[tokio::test]
    async fn test_handle_route() {
        let kv_store = Arc::new(MockKvStore {});
        let meta_srv = MetaSrv::new(kv_store);

        let header = RequestHeader::new(1, 1);
        let req = RouteRequest::new(header);
        let req = req
            .add_table(TableName::new("catalog1", "schema1", "table1"))
            .add_table(TableName::new("catalog1", "schema1", "table2"))
            .add_table(TableName::new("catalog1", "schema1", "table3"));

        let _res = meta_srv.route(req.into_request()).await.unwrap();
    }

    #[tokio::test]
    async fn test_handle_create() {
        let kv_store = Arc::new(MockKvStore {});
        let meta_srv = MetaSrv::new(kv_store);

        let header = RequestHeader::new(1, 1);
        let table_name = TableName::new("test_catalog", "test_db", "table1");
        let req = CreateRequest::new(header, table_name);

        let p = region::Partition::new()
            .column_list(vec![b"col1".to_vec(), b"col2".to_vec()])
            .value_list(vec![b"v1".to_vec(), b"v2".to_vec()]);
        let r1 = Region::new(1, "region1", p);

        let p = region::Partition::new()
            .column_list(vec![b"col1".to_vec(), b"col2".to_vec()])
            .value_list(vec![b"v11".to_vec(), b"v22".to_vec()]);
        let r2 = Region::new(1, "region2", p);

        let req = req.add_region(r1).add_region(r2);

        let res = meta_srv.create(req.into_request()).await.unwrap();

        for r in res.into_inner().regions {
            assert_eq!("127.0.0.1:3000", r.peer.unwrap().endpoint.unwrap().addr);
        }
    }
}
