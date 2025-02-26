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
use auth::{PermissionChecker, PermissionCheckerRef, PermissionReq};
use client::Output;
use common_error::ext::BoxedError;
use servers::error::{AuthSnafu, Error, InFlightWriteBytesExceededSnafu};
use servers::influxdb::InfluxdbRequest;
use servers::interceptor::{LineProtocolInterceptor, LineProtocolInterceptorRef};
use servers::query_handler::InfluxdbLineProtocolHandler;
use session::context::QueryContextRef;
use snafu::ResultExt;

use crate::instance::Instance;

#[async_trait]
impl InfluxdbLineProtocolHandler for Instance {
    async fn exec(
        &self,
        request: InfluxdbRequest,
        ctx: QueryContextRef,
    ) -> servers::error::Result<Output> {
        self.plugins
            .get::<PermissionCheckerRef>()
            .as_ref()
            .check_permission(ctx.current_user(), PermissionReq::LineProtocol)
            .context(AuthSnafu)?;

        let interceptor_ref = self.plugins.get::<LineProtocolInterceptorRef<Error>>();
        interceptor_ref.pre_execute(&request.lines, ctx.clone())?;

        let requests = request.try_into()?;
        let requests = interceptor_ref
            .post_lines_conversion(requests, ctx.clone())
            .await?;

        let _guard = if let Some(limiter) = &self.limiter {
            let result = limiter.limit_row_inserts(&requests);
            if result.is_none() {
                return InFlightWriteBytesExceededSnafu.fail();
            }
            result
        } else {
            None
        };

        self.handle_influx_row_inserts(requests, ctx)
            .await
            .map_err(BoxedError::new)
            .context(servers::error::ExecuteGrpcQuerySnafu)
    }
}
