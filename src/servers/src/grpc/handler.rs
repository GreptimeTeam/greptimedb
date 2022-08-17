use api::v1::{BatchRequest, BatchResponse, DatabaseResponse};

use crate::error::Result;
use crate::query_handler::GrpcQueryHandlerRef;

#[derive(Clone)]
pub struct BatchHandler {
    query_handler: GrpcQueryHandlerRef,
}

impl BatchHandler {
    pub fn new(query_handler: GrpcQueryHandlerRef) -> Self {
        Self { query_handler }
    }

    pub async fn batch(&self, batch_req: BatchRequest) -> Result<BatchResponse> {
        let mut batch_resp = BatchResponse::default();
        let mut db_resp = DatabaseResponse::default();

        for db_req in batch_req.databases {
            for obj_expr in db_req.exprs {
                let object_resp = self.query_handler.do_query(obj_expr).await?;
                db_resp.results.push(object_resp);
            }
        }
        batch_resp.databases.push(db_resp);
        Ok(batch_resp)
    }
}
