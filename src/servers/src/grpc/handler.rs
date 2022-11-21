// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use api::v1::{AdminResponse, BatchRequest, BatchResponse, DatabaseResponse};
use common_runtime::Runtime;
use tokio::sync::oneshot;

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
        let (tx, rx) = oneshot::channel();
        let query_handler = self.query_handler.clone();
        let admin_handler = self.admin_handler.clone();

        let future = async move {
            let mut batch_resp = BatchResponse::default();
            let mut admin_resp = AdminResponse::default();
            let mut db_resp = DatabaseResponse::default();

            for admin_req in batch_req.admins {
                admin_resp.results.reserve(admin_req.exprs.len());

                for admin_expr in admin_req.exprs {
                    let admin_result = admin_handler.exec_admin_request(admin_expr).await?;
                    admin_resp.results.push(admin_result);
                }
            }
            batch_resp.admins.push(admin_resp);

            for db_req in batch_req.databases {
                db_resp.results.reserve(db_req.exprs.len());

                for obj_expr in db_req.exprs {
                    let object_resp = query_handler.do_query(obj_expr).await?;

                    db_resp.results.push(object_resp);
                }
            }
            batch_resp.databases.push(db_resp);

            Ok(batch_resp)
        };

        // Executes requests in another runtime to
        // 1. prevent the execution from being cancelled unexpected by tonic runtime.
        // 2. avoid the handler blocks the gRPC runtime because `exec_admin_request` may block
        // the caller thread.
        let _ = self.runtime.spawn(async move {
            let result = future.await;

            // Ignore send result. Usually an error indicates the rx is dropped (request timeouted).
            let _ = tx.send(result);
        });
        // Safety: An early-dropped tx usually indicates a serious problem (like panic). This unwrap
        // is used to poison the upper layer.
        rx.await.unwrap()
    }
}
