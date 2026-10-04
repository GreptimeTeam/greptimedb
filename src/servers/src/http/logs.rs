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
use std::time::Instant;

use axum::extract::State;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use common_telemetry::tracing;
use log_query::{Limit, LogQuery};
use session::context::{Channel, QueryContext};

use crate::http::result::greptime_result_v1::GreptimedbV1Response;
use crate::query_handler::LogQueryHandlerRef;

const DEFAULT_FETCH: usize = 1000;

fn apply_default_fetch(limit: &mut Limit) {
    limit.fetch.get_or_insert(DEFAULT_FETCH);
}

#[axum_macros::debug_handler]
#[tracing::instrument(skip_all, fields(protocol = "http", request_type = "logs"))]
pub async fn logs(
    State(handler): State<LogQueryHandlerRef>,
    Extension(mut query_ctx): Extension<QueryContext>,
    Json(mut params): Json<LogQuery>,
) -> Response {
    let exec_start = Instant::now();
    let db = query_ctx.get_db_string();

    query_ctx.set_channel(Channel::Log);
    let query_ctx = Arc::new(query_ctx);
    apply_default_fetch(&mut params.limit);

    let _timer = crate::metrics::METRIC_HTTP_LOGS_ELAPSED
        .with_label_values(&[db.as_str()])
        .start_timer();

    let output = handler.query(params, query_ctx).await;
    let resp = GreptimedbV1Response::from_output(vec![output]).await;

    resp.with_execution_time(exec_start.elapsed().as_millis() as u64)
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_apply_default_fetch() {
        let mut limit = Limit {
            skip: None,
            fetch: None,
        };
        apply_default_fetch(&mut limit);
        assert_eq!(limit.fetch, Some(DEFAULT_FETCH));

        let mut limit = Limit {
            skip: Some(10),
            fetch: None,
        };
        apply_default_fetch(&mut limit);
        assert_eq!(limit.skip, Some(10));
        assert_eq!(limit.fetch, Some(DEFAULT_FETCH));

        let mut limit = Limit {
            skip: Some(10),
            fetch: Some(42),
        };
        apply_default_fetch(&mut limit);
        assert_eq!(limit.skip, Some(10));
        assert_eq!(limit.fetch, Some(42));
    }
}
