use api::prometheus::remote::{ReadRequest, WriteRequest};
use async_trait::async_trait;
use servers::error::Result as ServerResult;
use servers::prometheus::Metrics;
use servers::query_handler::PrometheusProtocolHandler;

use crate::instance::Instance;

impl Instance {}

#[async_trait]
impl PrometheusProtocolHandler for Instance {
    async fn write(&self, request: &WriteRequest) -> ServerResult<()> {
        unimplemented!();
    }

    async fn read(&self, request: &ReadRequest) -> ServerResult<()> {
        unimplemented!();
    }

    async fn inject_metrics(&self, metrics: &Metrics) -> ServerResult<()> {
        unimplemented!();
    }
}
