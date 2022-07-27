use api::v1::*;
use query::Output;

use crate::server::grpc::server::PROTOCOL_VERSION;
use crate::{error::Result, instance::InstanceRef};

#[derive(Clone)]
pub struct BatchHandler {
    instance: InstanceRef,
}

impl BatchHandler {
    pub fn new(instance: InstanceRef) -> Self {
        Self { instance }
    }

    pub async fn batch(&self, batch_req: BatchRequest) -> Result<BatchResponse> {
        let mut batch_resp = BatchResponse::default();
        let mut db_resp = DatabaseResponse::default();
        let databases = batch_req.databases;

        for req in databases {
            let exprs = req.exprs;

            for obj_expr in exprs {
                let mut object_resp = ObjectResult::default();

                match obj_expr.expr {
                    Some(object_expr::Expr::Insert(insert_expr)) => {
                        match self.instance.execute_grpc_insert(insert_expr).await {
                            Ok(Output::AffectedRows(rows)) => {
                                object_resp.header = Some(ResultHeader {
                                    version: PROTOCOL_VERSION,
                                    success: rows as u32,
                                    ..Default::default()
                                });
                            }
                            Err(err) => {
                                object_resp.header = Some(ResultHeader {
                                    version: PROTOCOL_VERSION,
                                    err_msg: err.to_string(),
                                    // TODO(fys): failure count
                                    ..Default::default()
                                })
                            }
                            _ => unreachable!(),
                        }
                    }
                    _ => unimplemented!(),
                }

                db_resp.results.push(object_resp);
            }
        }
        batch_resp.databases.push(db_resp);
        Ok(batch_resp)
    }
}
