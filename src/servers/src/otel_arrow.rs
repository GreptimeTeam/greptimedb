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

use auth::UserProviderRef;
use common_error::ext::ErrorExt;
use common_error::status_code::status_to_tonic_code;
use common_telemetry::error;
use futures::SinkExt;
use otel_arrow_rust::Consumer;
use otel_arrow_rust::proto::opentelemetry::arrow::v1::arrow_metrics_service_server::ArrowMetricsService;
use otel_arrow_rust::proto::opentelemetry::arrow::v1::{
    BatchArrowRecords, BatchStatus, StatusCode as ArrowStatusCode,
};
use session::protocol_ctx::{OtlpMetricCtx, ProtocolCtx};
use tonic::metadata::{Entry, MetadataValue};
use tonic::service::Interceptor;
use tonic::{Request, Response, Status, Streaming};

use crate::error;
use crate::grpc::context_auth;
use crate::query_handler::{MetricsIngestOutcome, OpenTelemetryProtocolHandlerRef};

pub struct OtelArrowServiceHandler<T> {
    handler: T,
    user_provider: Option<UserProviderRef>,
    experimental_enable_exponential_histogram: bool,
}

impl<T> OtelArrowServiceHandler<T> {
    pub fn new(
        handler: T,
        user_provider: Option<UserProviderRef>,
        experimental_enable_exponential_histogram: bool,
    ) -> Self {
        Self {
            handler,
            user_provider,
            experimental_enable_exponential_histogram,
        }
    }

    fn metric_ctx(&self) -> OtlpMetricCtx {
        OtlpMetricCtx {
            experimental_enable_exponential_histogram: self
                .experimental_enable_exponential_histogram,
            ..Default::default()
        }
    }
}

fn batch_status(batch_id: i64, outcome: MetricsIngestOutcome) -> BatchStatus {
    let status_code = if outcome.accepted_data_points == 0 && outcome.rejected_data_points > 0 {
        ArrowStatusCode::InvalidArgument
    } else {
        ArrowStatusCode::Ok
    };
    BatchStatus {
        batch_id,
        status_code: status_code as i32,
        status_message: outcome.error_message.unwrap_or_default(),
    }
}

#[async_trait::async_trait]
impl ArrowMetricsService for OtelArrowServiceHandler<OpenTelemetryProtocolHandlerRef> {
    type ArrowMetricsStream = futures::channel::mpsc::Receiver<Result<BatchStatus, Status>>;
    async fn arrow_metrics(
        &self,
        request: Request<Streaming<BatchArrowRecords>>,
    ) -> Result<Response<Self::ArrowMetricsStream>, Status> {
        let (mut sender, receiver) = futures::channel::mpsc::channel(100);

        let (headers, _, mut incoming_requests) = request.into_parts();

        let query_ctx = context_auth::create_query_context_from_grpc_metadata(&headers)?;
        context_auth::check_auth(self.user_provider.clone(), &headers, query_ctx.clone()).await?;
        let query_ctx = {
            let mut ctx = query_ctx.fork();
            ctx.set_protocol_ctx(ProtocolCtx::OtlpMetric(self.metric_ctx()));
            Arc::new(ctx)
        };

        let handler = self.handler.clone();

        // handles incoming requests
        common_runtime::spawn_global(async move {
            let mut consumer = Consumer::default();
            while let Some(batch_res) = incoming_requests.message().await.transpose() {
                let mut batch = match batch_res {
                    Ok(batch) => batch,
                    Err(e) => {
                        error!(
                            "Failed to receive batch from otel-arrow client, error: {}",
                            e
                        );
                        let _ = sender.send(Err(e)).await;
                        return;
                    }
                };
                let batch_id = batch.batch_id;
                let request = match consumer.consume_metrics_batches(&mut batch).map_err(|e| {
                    error::HandleOtelArrowRequestSnafu {
                        err_msg: e.to_string(),
                    }
                    .build()
                }) {
                    Ok(request) => request,
                    Err(e) => {
                        let _ = sender
                            .send(Err(Status::new(
                                status_to_tonic_code(e.status_code()),
                                e.to_string(),
                            )))
                            .await;
                        error!(e;
                            "Failed to consume batch from otel-arrow client"
                        );
                        return;
                    }
                };
                let outcome = match handler.metrics(request, query_ctx.clone()).await {
                    Ok(outcome) => outcome,
                    Err(e) => {
                        let _ = sender
                            .send(Err(Status::new(
                                status_to_tonic_code(e.status_code()),
                                e.to_string(),
                            )))
                            .await;
                        error!(e; "Failed to ingest metrics from otel-arrow");
                        return;
                    }
                };
                let batch_status = batch_status(batch_id, outcome);
                let _ = sender.send(Ok(batch_status)).await;
            }
        });
        Ok(Response::new(receiver))
    }
}

/// This serves as a workaround for otel-arrow collector's custom header.
#[derive(Clone)]
pub struct HeaderInterceptor;

impl Interceptor for HeaderInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Ok(Entry::Occupied(mut e)) = request.metadata_mut().entry("grpc-encoding") {
            // This works as a workaround to handle customized compression type (zstdarrow*) in otel-arrow.
            if e.get().as_bytes().starts_with(b"zstdarrow") {
                e.insert(MetadataValue::from_static("zstd"));
            }
        }
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configured_metric_context_preserves_metric_engine_choice() {
        let handler = OtelArrowServiceHandler::new((), None, true);
        let metric_ctx = handler.metric_ctx();

        assert!(metric_ctx.experimental_enable_exponential_histogram);
        assert!(!metric_ctx.with_metric_engine);
    }

    #[test]
    fn batch_status_maps_partial_and_all_rejected_outcomes() {
        let partial = batch_status(
            1,
            MetricsIngestOutcome {
                accepted_data_points: 1,
                rejected_data_points: 2,
                error_message: Some("partial".to_string()),
                ..Default::default()
            },
        );
        assert_eq!(partial.status_code, ArrowStatusCode::Ok as i32);
        assert_eq!(partial.status_message, "partial");

        let rejected = batch_status(
            2,
            MetricsIngestOutcome {
                rejected_data_points: 2,
                error_message: Some("rejected".to_string()),
                ..Default::default()
            },
        );
        assert_eq!(
            rejected.status_code,
            ArrowStatusCode::InvalidArgument as i32
        );
        assert_eq!(rejected.status_message, "rejected");
    }
}
