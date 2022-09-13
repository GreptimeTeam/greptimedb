use api::v1::{AdminResponse, BatchRequest, BatchResponse, DatabaseResponse};

use crate::error::Result;
use crate::query_handler::{GrpcAdminHandlerRef, GrpcQueryHandlerRef};

#[derive(Clone)]
pub struct BatchHandler {
    query_handler: GrpcQueryHandlerRef,
    admin_handler: GrpcAdminHandlerRef,
}

impl BatchHandler {
    pub fn new(query_handler: GrpcQueryHandlerRef, admin_handler: GrpcAdminHandlerRef) -> Self {
        Self {
            query_handler,
            admin_handler,
        }
    }

    pub async fn batch(&self, batch_req: BatchRequest) -> Result<BatchResponse> {
        let mut batch_resp = BatchResponse::default();
        let mut admin_resp = AdminResponse::default();
        let mut db_resp = DatabaseResponse::default();

        for admin_req in batch_req.admins {
            for admin_expr in admin_req.exprs {
                let admin_result = self.admin_handler.exec_admin_request(admin_expr).await?;
                admin_resp.results.push(admin_result);
            }
        }
        batch_resp.admins.push(admin_resp);

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
