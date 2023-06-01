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

use api::v1::InsertRequests;
use async_trait::async_trait;
use common_error::prelude::BoxedError;
use servers::error as server_error;
use servers::opentsdb::codec::DataPoint;
use servers::query_handler::OpentsdbProtocolHandler;
use session::context::QueryContextRef;
use snafu::prelude::*;

use crate::instance::Instance;

#[async_trait]
impl OpentsdbProtocolHandler for Instance {
    async fn exec(&self, data_point: &DataPoint, ctx: QueryContextRef) -> server_error::Result<()> {
        let requests = InsertRequests {
            inserts: vec![data_point.as_grpc_insert()],
        };
        self.handle_inserts(requests, ctx)
            .await
            .map_err(BoxedError::new)
            .with_context(|_| server_error::ExecuteQuerySnafu {
                query: format!("{data_point:?}"),
            })?;
        Ok(())
    }
}
