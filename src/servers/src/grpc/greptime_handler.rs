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

//! Handler for Greptime Database service. It's implemented by frontend.

use std::str::FromStr;
use std::time::Instant;

use api::helper::request_type;
use api::v1::{GreptimeRequest, RequestHeader};
use auth::UserProviderRef;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_catalog::parse_catalog_and_schema_from_db_string;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_grpc::flight::do_put::DoPutResponse;
use common_query::Output;
use common_runtime::Runtime;
use common_runtime::runtime::RuntimeTrait;
use common_session::ReadPreference;
use common_telemetry::tracing_context::{FutureExt, TracingContext};
use common_telemetry::{debug, error, tracing, warn};
use common_time::timezone::parse_timezone;
use futures_util::StreamExt;
use session::context::{Channel, QueryContextBuilder, QueryContextRef};
use session::hints::READ_PREFERENCE_HINT;
use snafu::{OptionExt, ResultExt};
use table::TableRef;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;

use crate::error::{InvalidQuerySnafu, JoinTaskSnafu, Result, UnknownHintSnafu};
use crate::grpc::flight::{PutRecordBatchRequest, PutRecordBatchRequestStream};
use crate::grpc::{FlightCompression, TonicResult, context_auth};
use crate::metrics;
use crate::metrics::METRIC_SERVER_GRPC_DB_REQUEST_TIMER;
use crate::query_handler::grpc::ServerGrpcQueryHandlerRef;

#[derive(Clone)]
pub struct GreptimeRequestHandler {
    handler: ServerGrpcQueryHandlerRef,
    pub(crate) user_provider: Option<UserProviderRef>,
    runtime: Option<Runtime>,
    pub(crate) flight_compression: FlightCompression,
}

impl GreptimeRequestHandler {
    pub fn new(
        handler: ServerGrpcQueryHandlerRef,
        user_provider: Option<UserProviderRef>,
        runtime: Option<Runtime>,
        flight_compression: FlightCompression,
    ) -> Self {
        Self {
            handler,
            user_provider,
            runtime,
            flight_compression,
        }
    }

    #[tracing::instrument(skip_all, fields(protocol = "grpc", request_type = get_request_type(&request)))]
    pub(crate) async fn handle_request(
        &self,
        request: GreptimeRequest,
        hints: Vec<(String, String)>,
    ) -> Result<Output> {
        let query = request.request.context(InvalidQuerySnafu {
            reason: "Expecting non-empty GreptimeRequest.",
        })?;

        let header = request.header.as_ref();
        let query_ctx = create_query_context(Channel::Grpc, header, hints)?;
        let user_info = context_auth::auth(self.user_provider.clone(), header, &query_ctx).await?;
        query_ctx.set_current_user(user_info);

        let handler = self.handler.clone();
        let request_type = request_type(&query).to_string();
        let db = query_ctx.get_db_string();
        let timer = RequestTimer::new(db.clone(), request_type);
        let tracing_context = TracingContext::from_current_span();

        let result_future = async move {
            handler
                .do_query(query, query_ctx)
                .trace(tracing_context.attach(tracing::info_span!(
                    "GreptimeRequestHandler::handle_request_runtime"
                )))
                .await
                .map_err(|e| {
                    if e.status_code().should_log_error() {
                        let root_error = e.root_cause().unwrap_or(&e);
                        error!(e; "Failed to handle request, error: {}", root_error.to_string());
                    } else {
                        // Currently, we still print a debug log.
                        debug!("Failed to handle request, err: {:?}", e);
                    }
                    e
                })
        };

        match &self.runtime {
            Some(runtime) => {
                // Executes requests in another runtime to
                // 1. prevent the execution from being cancelled unexpected by Tonic runtime;
                //   - Refer to our blog for the rational behind it:
                //     https://www.greptime.com/blogs/2023-01-12-hidden-control-flow.html
                //   - Obtaining a `JoinHandle` to get the panic message (if there's any).
                //     From its docs, `JoinHandle` is cancel safe. The task keeps running even it's handle been dropped.
                // 2. avoid the handler blocks the gRPC runtime incidentally.
                runtime
                    .spawn(result_future)
                    .await
                    .context(JoinTaskSnafu)
                    .inspect_err(|e| {
                        timer.record(e.status_code());
                    })?
            }
            None => result_future.await,
        }
    }

    pub(crate) async fn put_record_batches(
        &self,
        mut stream: PutRecordBatchRequestStream,
        result_sender: mpsc::Sender<TonicResult<DoPutResponse>>,
        query_ctx: QueryContextRef,
    ) {
        let handler = self.handler.clone();
        let runtime = self
            .runtime
            .clone()
            .unwrap_or_else(common_runtime::global_runtime);
        runtime.spawn(async move {
            // Cached table ref
            let mut table_ref: Option<TableRef> = None;

            while let Some(request) = stream.next().await {
                let request = match request {
                    Ok(request) => request,
                    Err(e) => {
                        let _ = result_sender.try_send(Err(e));
                        break;
                    }
                };
                let request_id = request.request_id;


                let timer = metrics::GRPC_BULK_INSERT_ELAPSED.start_timer();
                let result = handler
                    .put_record_batch(request, &mut table_ref, query_ctx.clone())
                    .await
                    .inspect_err(|e| error!(e; "Failed to handle flight record batches"));
                timer.observe_duration();
                let result = result
                    .map(|x| DoPutResponse::new(request_id, x))
                    .map_err(Into::into);
                if let Err(e)= result_sender.try_send(result)
                    && let TrySendError::Closed(_) = e {
                    warn!(r#""DoPut" client with request_id {} maybe unreachable, abort handling its message"#, request_id);
                    break;
                }
            }
        });
    }
}

pub fn get_request_type(request: &GreptimeRequest) -> &'static str {
    request
        .request
        .as_ref()
        .map(request_type)
        .unwrap_or_default()
}

/// Creates a new `QueryContext` from the provided request header and extensions.
/// Strongly recommend setting an appropriate channel, as this is very helpful for statistics.
pub(crate) fn create_query_context(
    channel: Channel,
    header: Option<&RequestHeader>,
    mut extensions: Vec<(String, String)>,
) -> Result<QueryContextRef> {
    let (catalog, schema) = header
        .map(|header| {
            // We provide dbname field in newer versions of protos/sdks
            // parse dbname from header in priority
            if !header.dbname.is_empty() {
                parse_catalog_and_schema_from_db_string(&header.dbname)
            } else {
                (
                    if !header.catalog.is_empty() {
                        header.catalog.to_lowercase()
                    } else {
                        DEFAULT_CATALOG_NAME.to_string()
                    },
                    if !header.schema.is_empty() {
                        header.schema.to_lowercase()
                    } else {
                        DEFAULT_SCHEMA_NAME.to_string()
                    },
                )
            }
        })
        .unwrap_or_else(|| {
            (
                DEFAULT_CATALOG_NAME.to_string(),
                DEFAULT_SCHEMA_NAME.to_string(),
            )
        });
    let timezone = parse_timezone(header.map(|h| h.timezone.as_str()));
    let mut ctx_builder = QueryContextBuilder::default()
        .current_catalog(catalog)
        .current_schema(schema)
        .timezone(timezone)
        .channel(channel);

    if let Some(x) = extensions
        .iter()
        .position(|(k, _)| k == READ_PREFERENCE_HINT)
    {
        let (k, v) = extensions.swap_remove(x);
        let Ok(read_preference) = ReadPreference::from_str(&v) else {
            return UnknownHintSnafu {
                hint: format!("{k}={v}"),
            }
            .fail();
        };
        ctx_builder = ctx_builder.read_preference(read_preference);
    }

    for (key, value) in extensions {
        ctx_builder = ctx_builder.set_extension(key, value);
    }
    Ok(ctx_builder.build().into())
}

/// Histogram timer for handling gRPC request.
///
/// The timer records the elapsed time with [StatusCode::Success] on drop.
pub(crate) struct RequestTimer {
    start: Instant,
    db: String,
    request_type: String,
    status_code: StatusCode,
}

impl RequestTimer {
    /// Returns a new timer.
    pub fn new(db: String, request_type: String) -> RequestTimer {
        RequestTimer {
            start: Instant::now(),
            db,
            request_type,
            status_code: StatusCode::Success,
        }
    }

    /// Consumes the timer and record the elapsed time with specific `status_code`.
    pub fn record(mut self, status_code: StatusCode) {
        self.status_code = status_code;
    }
}

impl Drop for RequestTimer {
    fn drop(&mut self) {
        METRIC_SERVER_GRPC_DB_REQUEST_TIMER
            .with_label_values(&[
                self.db.as_str(),
                self.request_type.as_str(),
                self.status_code.as_ref(),
            ])
            .observe(self.start.elapsed().as_secs_f64());
    }
}

#[cfg(test)]
mod tests {
    use chrono::FixedOffset;
    use common_time::Timezone;

    use super::*;

    #[test]
    fn test_create_query_context() {
        let header = RequestHeader {
            catalog: "cat-a-log".to_string(),
            timezone: "+01:00".to_string(),
            ..Default::default()
        };
        let query_context = create_query_context(
            Channel::Unknown,
            Some(&header),
            vec![
                ("auto_create_table".to_string(), "true".to_string()),
                ("read_preference".to_string(), "leader".to_string()),
            ],
        )
        .unwrap();
        assert_eq!(query_context.current_catalog(), "cat-a-log");
        assert_eq!(query_context.current_schema(), DEFAULT_SCHEMA_NAME);
        assert_eq!(
            query_context.timezone(),
            Timezone::Offset(FixedOffset::east_opt(3600).unwrap())
        );
        assert!(matches!(
            query_context.read_preference(),
            ReadPreference::Leader
        ));
        assert_eq!(
            query_context.extensions().into_iter().collect::<Vec<_>>(),
            vec![("auto_create_table".to_string(), "true".to_string())]
        );
    }
}
