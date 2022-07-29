use api::v1::*;
use common_recordbatch::{util, RecordBatch};
use query::Output;

use crate::server::grpc::server::PROTOCOL_VERSION;
use crate::{error::Result, instance::InstanceRef};

// TODO(fys): Only one success code (200) was provided
// in the early stage and we will refine it later
pub const SUCCESS: u32 = 200;

// TODO(fys): Only one error code (500) was provided
// in the early stage and we will refine it later
pub const ERROR: u32 = 500;

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
                let object_resp = match obj_expr.expr {
                    Some(object_expr::Expr::Insert(insert_expr)) => {
                        self.handle_insert(insert_expr).await
                    }
                    Some(object_expr::Expr::Select(select_expr)) => {
                        self.handle_select(select_expr).await
                    }
                    _ => unimplemented!(),
                };

                db_resp.results.push(object_resp);
            }
        }
        batch_resp.databases.push(db_resp);
        Ok(batch_resp)
    }

    pub async fn handle_insert(&self, insert_expr: InsertExpr) -> ObjectResult {
        let mut object_resp = ObjectResult::default();
        match self.instance.execute_grpc_insert(insert_expr).await {
            Ok(Output::AffectedRows(rows)) => {
                object_resp.header = Some(ResultHeader {
                    version: PROTOCOL_VERSION,
                    code: SUCCESS,
                    success: rows as u32,
                    ..Default::default()
                });
            }
            Err(err) => {
                object_resp.header = Some(ResultHeader {
                    version: PROTOCOL_VERSION,
                    code: ERROR,
                    err_msg: err.to_string(),
                    // TODO(fys): failure count
                    ..Default::default()
                })
            }
            _ => unreachable!(),
        }
        object_resp
    }

    pub async fn handle_select(&self, select_expr: SelectExpr) -> ObjectResult {
        let expr = match select_expr.expr {
            Some(expr) => expr,
            None => return ObjectResult::default(),
        };
        match expr {
            select_expr::Expr::Sql(sql) => {
                let result = self.instance.execute_sql(&sql).await;
                Self::select_result(result).await
            }
        }
    }

    async fn select_result(select_result: Result<Output>) -> ObjectResult {
        let mut object_resp = ObjectResult::default();

        let mut header = ResultHeader {
            version: PROTOCOL_VERSION,
            ..Default::default()
        };

        match select_result {
            Ok(output) => match output {
                Output::AffectedRows(rows) => {
                    header.code = SUCCESS;
                    header.success = rows as u32;
                }
                Output::RecordBatch(stream) => match util::collect(stream).await {
                    Ok(record_batches) => {
                        let select_result = convert_record_batches_to_select_result(record_batches);
                        header.code = SUCCESS;
                        object_resp.results = select_result.into();
                    }
                    Err(err) => {
                        header.code = ERROR;
                        header.err_msg = err.to_string();
                    }
                },
            },
            Err(err) => {
                header.code = ERROR;
                header.err_msg = err.to_string();
            }
        }
        object_resp.header = Some(header);
        object_resp
    }
}

fn convert_record_batches_to_select_result(_record_batches: Vec<RecordBatch>) -> SelectResult {
    unimplemented!()
}
