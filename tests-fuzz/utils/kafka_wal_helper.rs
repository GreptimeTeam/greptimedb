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

use std::net::SocketAddr;

use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use clap::Parser;
use common_telemetry::info;
use rskafka::client::ClientBuilder;
use rskafka::client::partition::{OffsetAt, UnknownTopicHandling};
use serde::Serialize;
use tests_fuzz::utils::kafka_wal_http::{
    DeleteRecordsRequest, DeleteRecordsResponse, RecordOffsetsRequest, RecordOffsetsResponse,
    TopicOffset,
};

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

const DEFAULT_DELETE_RECORDS_TIMEOUT_MS: i32 = 5000;

#[derive(Debug, Parser)]
#[command(about = "Kafka WAL helper for Remote WAL fuzz tests")]
struct Args {
    /// HTTP listen address.
    #[arg(long, default_value = "0.0.0.0:8080")]
    addr: SocketAddr,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
}

#[tokio::main]
async fn main() -> Result<()> {
    common_telemetry::init_default_ut_logging();
    let args = Args::parse();
    serve(args.addr).await
}

async fn serve(addr: SocketAddr) -> Result<()> {
    let app = Router::new()
        .route("/health", get(health))
        .route("/record-offsets", post(record_offsets_handler))
        .route("/delete-records", post(delete_records_handler));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!("Kafka WAL helper listening on {}", listener.local_addr()?);
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

async fn record_offsets_handler(
    Json(request): Json<RecordOffsetsRequest>,
) -> HttpResult<RecordOffsetsResponse> {
    record_offsets(request)
        .await
        .map(Json)
        .map_err(to_http_error)
}

async fn delete_records_handler(
    Json(request): Json<DeleteRecordsRequest>,
) -> HttpResult<DeleteRecordsResponse> {
    delete_records(request)
        .await
        .map(Json)
        .map_err(to_http_error)
}

type HttpResult<T> = std::result::Result<Json<T>, (StatusCode, String)>;

fn to_http_error(error: Error) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
}

async fn record_offsets(request: RecordOffsetsRequest) -> Result<RecordOffsetsResponse> {
    let client = ClientBuilder::new(request.broker_endpoints).build().await?;
    let mut offsets = Vec::with_capacity(request.num_topics);

    for idx in 0..request.num_topics {
        let topic = format!("{}_{}", request.topic_prefix, idx);
        let partition_client = client
            .partition_client(&topic, request.partition, UnknownTopicHandling::Retry)
            .await?;
        let offset = partition_client.get_offset(OffsetAt::Latest).await?;
        info!(
            "Recorded Kafka WAL offset, topic: {}, partition: {}, offset: {}",
            topic, request.partition, offset
        );
        offsets.push(TopicOffset {
            topic,
            partition: request.partition,
            offset,
        });
    }

    Ok(RecordOffsetsResponse { offsets })
}

async fn delete_records(request: DeleteRecordsRequest) -> Result<DeleteRecordsResponse> {
    let client = ClientBuilder::new(request.broker_endpoints).build().await?;
    let timeout_ms = request
        .timeout_ms
        .unwrap_or(DEFAULT_DELETE_RECORDS_TIMEOUT_MS);

    for offset in &request.offsets {
        let partition_client = client
            .partition_client(&offset.topic, offset.partition, UnknownTopicHandling::Retry)
            .await?;
        info!(
            "Deleting Kafka WAL records, topic: {}, partition: {}, offset: {}",
            offset.topic, offset.partition, offset.offset
        );
        partition_client
            .delete_records(offset.offset, timeout_ms)
            .await?;
        info!(
            "Deleted Kafka WAL records, topic: {}, partition: {}, offset: {}",
            offset.topic, offset.partition, offset.offset
        );
    }

    Ok(DeleteRecordsResponse {
        deleted: request.offsets,
    })
}
