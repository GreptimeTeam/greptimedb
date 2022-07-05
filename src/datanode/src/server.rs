pub mod grpc;
pub mod http;

use common_options::GreptimeOptions;
use http::HttpServer;

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

    pub async fn start(&self, opts: &GreptimeOptions) -> Result<()> {
        self.http_server.start(&opts.http_addr).await
    }
}
