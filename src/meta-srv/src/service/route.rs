use api::v1::meta::route_server;
use api::v1::meta::CreateRequest;
use api::v1::meta::CreateResponse;
use api::v1::meta::Endpoint;
use api::v1::meta::Peer;
use api::v1::meta::RouteRequest;
use api::v1::meta::RouteResponse;
use tonic::Request;

use super::GrpcResult;
use crate::metasrv::MetaSrv;

#[async_trait::async_trait]
impl route_server::Route for MetaSrv {
    async fn route(&self, _req: Request<RouteRequest>) -> GrpcResult<RouteResponse> {
        todo!()
    }

    async fn create(&self, req: Request<CreateRequest>) -> GrpcResult<CreateResponse> {
        let CreateRequest { mut regions, .. } = req.into_inner();

        // TODO(jiachun)
        for r in &mut regions {
            r.peer = Some(Peer {
                endpoint: Some(Endpoint {
                    addr: "127.0.0.1:3000".to_string(),
                }),
                ..Default::default()
            });
        }

        Ok(tonic::Response::new(CreateResponse {
            regions,
            ..Default::default()
        }))
    }
}
