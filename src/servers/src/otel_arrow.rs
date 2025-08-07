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

use common_error::ext::ErrorExt;
use common_error::status_code::status_to_tonic_code;
use common_telemetry::error;
use futures::SinkExt;
use otel_arrow_rust::opentelemetry::{ArrowMetricsService, BatchArrowRecords, BatchStatus};
use otel_arrow_rust::Consumer;
use session::context::QueryContext;
use tonic::metadata::{Entry, MetadataValue};
use tonic::service::Interceptor;
use tonic::{Request, Response, Status, Streaming};

use crate::error;
use crate::query_handler::OpenTelemetryProtocolHandlerRef;

pub struct OtelArrowServiceHandler<T>(pub T);

impl<T> OtelArrowServiceHandler<T> {
    pub fn new(handler: T) -> Self {
        Self(handler)
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
        let mut incoming_requests = request.into_inner();
        let handler = self.0.clone();
        let query_context = QueryContext::arc();
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
                let batch_status = BatchStatus {
                    batch_id: batch.batch_id,
                    status_code: 0,
                    status_message: Default::default(),
                };
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
                // use metric engine by default
                if let Err(e) = handler.metrics(request, query_context.clone()).await {
                    let _ = sender
                        .send(Err(Status::new(
                            status_to_tonic_code(e.status_code()),
                            e.to_string(),
                        )))
                        .await;
                    error!(e; "Failed to ingest metrics from otel-arrow");
                    return;
                }
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
