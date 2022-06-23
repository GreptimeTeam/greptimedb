use api::v1::*;

use crate::{error::Result, instance::InstanceRef};

#[derive(Clone)]
pub struct BatchProcessor {}

impl BatchProcessor {
    pub fn new(_instance: InstanceRef) -> Self {
        Self {}
    }

    pub async fn batch(&self, mut batch_req: BatchRequest) -> Result<BatchResponse> {
        let batch_res = BatchResponse::default();
        let _databases = std::mem::take(&mut batch_req.databases);
        Ok(batch_res)
    }
}
