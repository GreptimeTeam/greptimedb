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

use futures::SinkExt;
use otel_arrow_rust::opentelemetry::{ArrowMetricsService, BatchArrowRecords, BatchStatus};
use otel_arrow_rust::Consumer;
use session::context::QueryContext;
use tonic::{Request, Response, Status, Streaming};

use crate::query_handler::OpenTelemetryProtocolHandler;

pub struct OtelArrowServiceHandler<T>(pub T);

#[async_trait::async_trait]
impl<T> ArrowMetricsService for OtelArrowServiceHandler<T>
where
    T: OpenTelemetryProtocolHandler + Send + Sync + Clone + 'static,
{
    type ArrowMetricsStream = futures::channel::mpsc::Receiver<Result<BatchStatus, Status>>;
    async fn arrow_metrics(
        &self,
        request: Request<Streaming<BatchArrowRecords>>,
    ) -> std::result::Result<Response<Self::ArrowMetricsStream>, Status> {
        let (mut sender, receiver) = futures::channel::mpsc::channel(100);
        let mut incoming_requests = request.into_inner();
        let handler = self.0.clone();
        let query_context = QueryContext::arc();
        // handles incoming requests
        common_runtime::spawn_global(async move {
            let mut consumer = Consumer::default();
            while let Some(mut batch) = incoming_requests.message().await.unwrap() {
                let batch_status = BatchStatus {
                    batch_id: batch.batch_id,
                    status_code: 0,
                    status_message: Default::default(),
                };
                let request = consumer.consume_batches(&mut batch).unwrap();
                handler
                    .metrics(request, query_context.clone())
                    .await
                    .unwrap();
                sender.send(Ok(batch_status)).await.unwrap();
            }
        });
        Ok(Response::new(receiver))
    }
}
