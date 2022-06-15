pub mod grpc;
pub mod http;

use http::HttpServer;

use crate::datanode::DatanodeOptions;
use crate::error::Result;
use crate::instance::InstanceRef;

/// All rpc services.
pub struct Services {
    http_server: HttpServer,
}

impl Services {
    pub fn new(instance: InstanceRef) -> Self {
        Self {
            http_server: HttpServer::new(instance),
        }
    }

    pub async fn start(&self, opts: &DatanodeOptions) -> Result<()> {
        self.http_server.start(opts.http_addr.clone()).await
    }
}
