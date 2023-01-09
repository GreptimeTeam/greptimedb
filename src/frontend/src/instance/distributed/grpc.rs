// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use api::v1::ddl_request::Expr as DdlExpr;
use api::v1::greptime_request::Request;
use api::v1::GreptimeRequest;
use async_trait::async_trait;
use common_error::prelude::BoxedError;
use common_query::Output;
use servers::query_handler::GrpcQueryHandler;
use snafu::{OptionExt, ResultExt};

use crate::error::{self, Result};
use crate::instance::distributed::DistInstance;

impl DistInstance {
    async fn handle_grpc_query(&self, query: GreptimeRequest) -> Result<Output> {
        let request = query.request.context(error::IncompleteGrpcResultSnafu {
            err_msg: "Missing 'request' in GreptimeRequest",
        })?;
        let output = match request {
            Request::Insert(request) => self.handle_dist_insert(request).await?,
            Request::Query(_) => {
                unreachable!("Query should have been handled directly in Frontend Instance!")
            }
            Request::Ddl(request) => {
                let expr = request.expr.context(error::IncompleteGrpcResultSnafu {
                    err_msg: "Missing 'expr' in DDL request",
                })?;
                match expr {
                    DdlExpr::CreateDatabase(expr) => self.handle_create_database(expr).await?,
                    DdlExpr::CreateTable(mut expr) => {
                        // TODO(LFC): Support creating distributed table through GRPC interface.
                        // Currently only SQL supports it; how to design the fields in CreateTableExpr?
                        self.create_table(&mut expr, None).await?
                    }
                    DdlExpr::Alter(expr) => self.handle_alter_table(expr).await?,
                    DdlExpr::DropTable(_) => {
                        // TODO(LFC): Implement distributed drop table.
                        // Seems the whole "drop table through GRPC interface" feature is not implemented?
                        unimplemented!()
                    }
                }
            }
        };
        Ok(output)
    }
}

#[async_trait]
impl GrpcQueryHandler for DistInstance {
    async fn do_query(&self, query: GreptimeRequest) -> servers::error::Result<Output> {
        self.handle_grpc_query(query)
            .await
            .map_err(BoxedError::new)
            .context(servers::error::ExecuteGrpcQuerySnafu)
    }
}
