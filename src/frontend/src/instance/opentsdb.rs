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

use std::sync::Arc;

use async_trait::async_trait;
use auth::{
    PermissionChecker, PermissionCheckerRef, PermissionReq, PermissionTableTarget,
    PermissionTableTargets,
};
use common_error::ext::BoxedError;
use common_telemetry::tracing;
use servers::error::{self as server_error, AuthSnafu, ExecuteGrpcQuerySnafu};
use servers::opentsdb::codec::DataPoint;
use servers::opentsdb::data_point_to_grpc_row_insert_requests;
use servers::query_handler::OpentsdbProtocolHandler;
use session::context::QueryContextRef;
use snafu::prelude::*;
use table::requests::{SEMANTIC_SIGNAL_TYPE, SEMANTIC_SOURCE, SIGNAL_TYPE_METRIC, SOURCE_OPENTSDB};

use crate::instance::Instance;

fn permission_targets(data_points: &[DataPoint], ctx: &QueryContextRef) -> PermissionTableTargets {
    let catalog = ctx.current_catalog();
    let schema = ctx.current_schema();
    PermissionTableTargets::resolved(
        data_points
            .iter()
            .map(|data_point| PermissionTableTarget::new(catalog, &schema, data_point.metric()))
            .collect(),
    )
}

#[async_trait]
impl OpentsdbProtocolHandler for Instance {
    async fn preflight(
        &self,
        data_points: &[DataPoint],
        ctx: QueryContextRef,
    ) -> server_error::Result<()> {
        self.check_table_permission(
            &ctx,
            PermissionReq::Opentsdb,
            permission_targets(data_points, &ctx),
        )
        .context(AuthSnafu)?;
        Ok(())
    }

    #[tracing::instrument(skip_all, fields(protocol = "opentsdb"))]
    async fn exec(
        &self,
        data_points: Vec<DataPoint>,
        ctx: QueryContextRef,
    ) -> server_error::Result<usize> {
        self.plugins
            .get::<PermissionCheckerRef>()
            .as_ref()
            .check_permission(ctx.current_user(), PermissionReq::Opentsdb)
            .context(AuthSnafu)?;

        let (requests, _) = data_point_to_grpc_row_insert_requests(data_points)?;
        self.check_row_insert_permission(&requests, &ctx, PermissionReq::Opentsdb)
            .context(AuthSnafu)?;

        let ctx = {
            let mut c = (*ctx).clone();
            c.set_extension(SEMANTIC_SIGNAL_TYPE, SIGNAL_TYPE_METRIC);
            c.set_extension(SEMANTIC_SOURCE, SOURCE_OPENTSDB);
            Arc::new(c)
        };

        // OpenTSDB is single value.
        let output = self
            .handle_row_inserts(requests, ctx, true, true)
            .await
            .map_err(BoxedError::new)
            .context(ExecuteGrpcQuerySnafu)?;

        Ok(match output.data {
            common_query::OutputData::AffectedRows(rows) => rows,
            _ => unreachable!(),
        })
    }
}

#[cfg(test)]
mod tests {
    use session::context::QueryContext;

    use super::*;

    #[test]
    fn test_permission_targets_do_not_require_row_conversion() {
        let data_points = [
            DataPoint::new("cpu".to_string(), 0, 1.0, vec![]),
            DataPoint::new(
                "mem".to_string(),
                0,
                1.0,
                vec![("greptime_value".to_string(), "tag".to_string())],
            ),
        ];
        assert!(data_point_to_grpc_row_insert_requests(data_points.to_vec()).is_err());

        assert_eq!(
            PermissionTableTargets::Resolved(vec![
                PermissionTableTarget::new("greptime", "public", "cpu"),
                PermissionTableTarget::new("greptime", "public", "mem"),
            ]),
            permission_targets(&data_points, &QueryContext::arc())
        );
    }
}
