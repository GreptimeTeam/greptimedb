pub mod grpc;
pub mod http;

use grpc::GrpcServer;
use http::HttpServer;
use tokio::try_join;

use crate::datanode::DatanodeOptions;
use crate::error::Result;
use crate::instance::InstanceRef;

/// All rpc services.
pub struct Services {
    http_server: HttpServer,
    grpc_server: GrpcServer,
}

impl Services {
    pub fn new(instance: InstanceRef) -> Self {
        Self {
            http_server: HttpServer::new(instance.clone()),
            grpc_server: GrpcServer::new(instance),
        }
    }

    pub async fn start(&self, opts: &DatanodeOptions) -> Result<()> {
        try_join!(
            self.http_server.start(&opts.http_addr),
            self.grpc_server.start(&opts.rpc_addr)
        )?;
        Ok(())
    }
}
