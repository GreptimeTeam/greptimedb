use std::sync::Arc;

use api::v1::{AdminResponse, BatchRequest, BatchResponse, DatabaseResponse};
use common_runtime::Runtime;
use tokio::sync::oneshot;

use crate::context::Context;
use crate::error::Result;
use crate::query_handler::{GrpcAdminHandlerRef, GrpcQueryHandlerRef};

#[derive(Clone)]
pub struct BatchHandler {
    query_handler: GrpcQueryHandlerRef,
    admin_handler: GrpcAdminHandlerRef,
    runtime: Arc<Runtime>,
}

impl BatchHandler {
    pub fn new(
        query_handler: GrpcQueryHandlerRef,
        admin_handler: GrpcAdminHandlerRef,
        runtime: Arc<Runtime>,
    ) -> Self {
        Self {
            query_handler,
            admin_handler,
            runtime,
        }
    }

    pub async fn batch(&self, batch_req: BatchRequest) -> Result<BatchResponse> {
        let mut batch_resp = BatchResponse::default();
        let mut admin_resp = AdminResponse::default();
        let mut db_resp = DatabaseResponse::default();

        for admin_req in batch_req.admins {
            for admin_expr in admin_req.exprs {
                let admin_result = self
                    .admin_handler
                    .exec_admin_request(admin_expr, &Context::new())
                    .await?;
                admin_resp.results.push(admin_result);
            }
        }
        batch_resp.admins.push(admin_resp);

        let (tx, rx) = oneshot::channel();
        let query_handler = self.query_handler.clone();
        let _ = self.runtime.spawn(async move {
            // execute request in another runtime to prevent the execution from being cancelled unexpected by tonic runtime.
            let mut result = vec![];
            for db_req in batch_req.databases {
                for obj_expr in db_req.exprs {
                    let object_resp = query_handler.do_query(obj_expr, &Context::new()).await;

                    result.push(object_resp);
                }
            }
            // Ignore send result. Usually an error indicates the rx is dropped (request timeouted).
            let _ = tx.send(result);
        });
        // Safety: An early-dropped tx usually indicates a serious problem (like panic). This unwrap
        // is used to poison the upper layer.
        db_resp.results = rx.await.unwrap().into_iter().collect::<Result<_>>()?;
        batch_resp.databases.push(db_resp);
        Ok(batch_resp)
    }
}
