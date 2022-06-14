pub mod grpc;
pub mod http;

use http::HttpServer;

use crate::datanode::DataNodeOptions;
use crate::error::Result;
use crate::instance::InstanceRef;

/// All rpc services.
pub struct Services {
    http_server: HttpServer,
}

impl Services {
    pub fn new(opts: &DataNodeOptions, instance: InstanceRef) -> Self {
        Self {
            http_server: HttpServer::new(opts.http_addr.clone(), instance),
        }
    }

    pub async fn start(&self) -> Result<()> {
        self.http_server.start().await
    }
}
