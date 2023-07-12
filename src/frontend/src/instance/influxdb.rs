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

use async_trait::async_trait;
use common_error::ext::BoxedError;
use servers::influxdb::InfluxdbRequest;
use servers::query_handler::InfluxdbLineProtocolHandler;
use session::context::QueryContextRef;
use snafu::ResultExt;

use crate::instance::Instance;

#[async_trait]
impl InfluxdbLineProtocolHandler for Instance {
    async fn exec(
        &self,
        request: &InfluxdbRequest,
        ctx: QueryContextRef,
    ) -> servers::error::Result<()> {
        let requests = request.try_into()?;
        let _ = self
            .handle_inserts(requests, ctx)
            .await
            .map_err(BoxedError::new)
            .context(servers::error::ExecuteGrpcQuerySnafu)?;
        Ok(())
    }
}
