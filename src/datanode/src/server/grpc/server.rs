use api::v1::*;
use tonic::{Request, Response, Status};

use super::processors::BatchProcessor;

#[derive(Clone)]
pub struct Server {
    processor: BatchProcessor,
}

impl Server {
    pub fn new(processor: BatchProcessor) -> Self {
        Self { processor }
    }

    pub fn into_service(self) -> greptime_server::GreptimeServer<Self> {
        greptime_server::GreptimeServer::new(self)
    }
}

#[tonic::async_trait]
impl greptime_server::Greptime for Server {
    async fn batch(&self, req: Request<BatchRequest>) -> Result<Response<BatchResponse>, Status> {
        let req = req.into_inner();
        let res = self.processor.batch(req).await?;
        Ok(Response::new(res))
    }
}
