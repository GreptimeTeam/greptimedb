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
use otel_arrow_rust::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
use otel_arrow_rust::proto::opentelemetry::metrics::v1::metric;
use session::protocol_ctx::{OtlpMetricCtx, ProtocolCtx};
use tonic::metadata::{Entry, MetadataValue};
use tonic::service::Interceptor;
use tonic::{Request, Response, Status, Streaming};

use crate::error;
use crate::grpc::context_auth;
use crate::query_handler::{MetricsIngestOutcome, OpenTelemetryProtocolHandlerRef};

const EXPONENTIAL_HISTOGRAM_UNSUPPORTED: &str = "OTel Arrow exponential histograms are unsupported because the Arrow wire format omits zero_threshold";

pub struct OtelArrowServiceHandler<T> {
    handler: T,
    user_provider: Option<UserProviderRef>,
}

impl<T> OtelArrowServiceHandler<T> {
    pub fn new(handler: T, user_provider: Option<UserProviderRef>) -> Self {
        Self {
            handler,
            user_provider,
        }
    }
}

/// Removes unsupported Arrow histograms before ingestion, preserving other metrics.
fn remove_exponential_histograms(request: &mut ExportMetricsServiceRequest) -> bool {
    let mut has_data_points = false;
    for scope in request
        .resource_metrics
        .iter_mut()
        .flat_map(|resource| &mut resource.scope_metrics)
    {
        scope.metrics.retain(|item| {
            if let Some(metric::Data::ExponentialHistogram(histogram)) = &item.data {
                has_data_points |= !histogram.data_points.is_empty();
                false
            } else {
                true
            }
        });
    }
    has_data_points
}

fn batch_status(
    batch_id: i64,
    outcome: MetricsIngestOutcome,
    has_exponential_histogram_data_points: bool,
) -> BatchStatus {
    let status_code = if outcome.accepted_data_points == 0
        && (outcome.rejected_data_points > 0 || has_exponential_histogram_data_points)
    {
        ArrowStatusCode::InvalidArgument
    } else {
        ArrowStatusCode::Ok
    };
    let status_message = if has_exponential_histogram_data_points {
        EXPONENTIAL_HISTOGRAM_UNSUPPORTED.to_string()
    } else {
        outcome.error_message.unwrap_or_default()
    };
    BatchStatus {
        batch_id,
        status_code: status_code as i32,
        status_message,
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

        let (headers, extensions, mut incoming_requests) = request.into_parts();

        // OTEL Arrow is currently used only for external ingestion. Its service bypasses
        // the frontend router middleware, so even the internal listener defaults to
        // Channel::Grpc and remains rate-limited. Before using this path internally,
        // propagate the server-owned Channel::Internal marker to this service.
        let query_ctx =
            context_auth::create_query_context_from_grpc_metadata(&headers, &extensions)?;
        context_auth::check_auth(self.user_provider.clone(), &headers, query_ctx.clone()).await?;
        let query_ctx = {
            let mut ctx = query_ctx.fork();
            ctx.set_protocol_ctx(ProtocolCtx::OtlpMetric(OtlpMetricCtx::default()));
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
                let mut request = match consumer.consume_metrics_batches(&mut batch).map_err(|e| {
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
                let has_exponential_histogram_data_points =
                    remove_exponential_histograms(&mut request);
                let outcome = match handler.metrics(request, query_ctx.clone()).await {
                    Ok(outcome) => outcome,
                    Err(error::Error::InvalidOtlpMetricInput { reason }) => {
                        let _ = sender
                            .send(Ok(BatchStatus {
                                batch_id,
                                status_code: ArrowStatusCode::InvalidArgument as i32,
                                status_message: reason,
                            }))
                            .await;
                        continue;
                    }
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
                let batch_status =
                    batch_status(batch_id, outcome, has_exponential_histogram_data_points);
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
    fn removes_arrow_exponential_histograms_and_preserves_other_metrics() {
        use otel_arrow_rust::proto::opentelemetry::metrics::v1::{
            ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Metric, ResourceMetrics,
            ScopeMetrics,
        };

        let mut request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![
                        Metric {
                            data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                                data_points: vec![ExponentialHistogramDataPoint::default()],
                                ..Default::default()
                            })),
                            ..Default::default()
                        },
                        Metric {
                            data: Some(metric::Data::Gauge(Gauge::default())),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        assert!(remove_exponential_histograms(&mut request));
        let metrics = &request.resource_metrics[0].scope_metrics[0].metrics;
        assert_eq!(metrics.len(), 1);
        assert!(matches!(metrics[0].data, Some(metric::Data::Gauge(_))));
        assert!(!remove_exponential_histograms(&mut request));
    }

    #[test]
    fn batch_status_explains_arrow_exponential_histogram_limit() {
        for accepted_data_points in [0, 1] {
            let status = batch_status(
                7,
                MetricsIngestOutcome {
                    accepted_data_points,
                    ..Default::default()
                },
                true,
            );
            assert_eq!(7, status.batch_id);
            assert_eq!(
                if accepted_data_points == 0 {
                    ArrowStatusCode::InvalidArgument
                } else {
                    ArrowStatusCode::Ok
                } as i32,
                status.status_code
            );
            assert_eq!(EXPONENTIAL_HISTOGRAM_UNSUPPORTED, status.status_message);
        }
    }
}
