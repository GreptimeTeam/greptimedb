use crate::error::Result;

mod grpc;
mod http;

use http::HttpServer;

/// All rpc services.
pub struct Services {
    http_server: HttpServer,
}

impl Services {
    pub fn new() -> Self {
        Self {
            http_server: HttpServer {},
        }
    }

    pub async fn start(&self) -> Result<()> {
        self.http_server.start().await
    }

    pub async fn shutdown(&self) -> Result<()> {
        unimplemented!()
    }
}
