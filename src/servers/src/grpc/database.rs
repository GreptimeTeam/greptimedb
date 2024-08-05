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

use api::v1::greptime_database_server::GreptimeDatabase;
use api::v1::greptime_response::Response as RawResponse;
use api::v1::{AffectedRows, GreptimeRequest, GreptimeResponse, ResponseHeader};
use async_trait::async_trait;
use common_error::status_code::StatusCode;
use common_query::OutputData;
use common_telemetry::{debug, warn};
use futures::StreamExt;
use tonic::metadata::{KeyAndValueRef, MetadataMap};
use tonic::{Request, Response, Status, Streaming};

use crate::grpc::greptime_handler::GreptimeRequestHandler;
use crate::grpc::{cancellation, TonicResult};

pub const GREPTIME_DB_HEADER_HINT_PREFIX: &str = "x-greptime-hint-";

pub(crate) struct DatabaseService {
    handler: GreptimeRequestHandler,
}

impl DatabaseService {
    pub(crate) fn new(handler: GreptimeRequestHandler) -> Self {
        Self { handler }
    }
}

#[async_trait]
impl GreptimeDatabase for DatabaseService {
    async fn handle(
        &self,
        request: Request<GreptimeRequest>,
    ) -> TonicResult<Response<GreptimeResponse>> {
        let remote_addr = request.remote_addr();
        let hints = extract_hints(request.metadata());
        debug!(
            "GreptimeDatabase::Handle: request from {:?} with hints: {:?}",
            remote_addr, hints
        );
        let handler = self.handler.clone();
        let request_future = async move {
            let request = request.into_inner();
            let output = handler.handle_request(request, hints).await?;
            let message = match output.data {
                OutputData::AffectedRows(rows) => GreptimeResponse {
                    header: Some(ResponseHeader {
                        status: Some(api::v1::Status {
                            status_code: StatusCode::Success as _,
                            ..Default::default()
                        }),
                    }),
                    response: Some(RawResponse::AffectedRows(AffectedRows { value: rows as _ })),
                },
                OutputData::Stream(_) | OutputData::RecordBatches(_) => {
                    return Err(Status::unimplemented("GreptimeDatabase::Handle for query"));
                }
            };

            Ok(Response::new(message))
        };

        let cancellation_future = async move {
            warn!(
                "GreptimeDatabase::Handle: request from {:?} cancelled by client",
                remote_addr
            );
            // If this future is executed it means the request future was dropped,
            // so it doesn't actually matter what is returned here
            Err(Status::cancelled(
                "GreptimeDatabase::Handle: request cancelled by client",
            ))
        };
        cancellation::with_cancellation_handler(request_future, cancellation_future).await
    }

    async fn handle_requests(
        &self,
        request: Request<Streaming<GreptimeRequest>>,
    ) -> Result<Response<GreptimeResponse>, Status> {
        let remote_addr = request.remote_addr();
        let hints = extract_hints(request.metadata());
        debug!(
            "GreptimeDatabase::HandleRequests: request from {:?} with hints: {:?}",
            remote_addr, hints
        );
        let handler = self.handler.clone();
        let request_future = async move {
            let mut affected_rows = 0;

            let mut stream = request.into_inner();
            while let Some(request) = stream.next().await {
                let request = request?;
                let output = handler.handle_request(request, hints.clone()).await?;
                match output.data {
                    OutputData::AffectedRows(rows) => affected_rows += rows,
                    OutputData::Stream(_) | OutputData::RecordBatches(_) => {
                        return Err(Status::unimplemented(
                            "GreptimeDatabase::HandleRequests for query",
                        ));
                    }
                }
            }
            let message = GreptimeResponse {
                header: Some(ResponseHeader {
                    status: Some(api::v1::Status {
                        status_code: StatusCode::Success as _,
                        ..Default::default()
                    }),
                }),
                response: Some(RawResponse::AffectedRows(AffectedRows {
                    value: affected_rows as u32,
                })),
            };

            Ok(Response::new(message))
        };

        let cancellation_future = async move {
            warn!(
                "GreptimeDatabase::HandleRequests: request from {:?} cancelled by client",
                remote_addr
            );
            // If this future is executed it means the request future was dropped,
            // so it doesn't actually matter what is returned here
            Err(Status::cancelled(
                "GreptimeDatabase::HandleRequests: request cancelled by client",
            ))
        };
        cancellation::with_cancellation_handler(request_future, cancellation_future).await
    }
}

fn extract_hints(metadata: &MetadataMap) -> Vec<(String, String)> {
    metadata
        .iter()
        .filter_map(|kv| {
            let KeyAndValueRef::Ascii(key, value) = kv else {
                return None;
            };
            let key = key.as_str();
            let new_key = key.strip_prefix(GREPTIME_DB_HEADER_HINT_PREFIX)?;
            let Ok(value) = value.to_str() else {
                // Simply return None for non-string values.
                return None;
            };
            Some((new_key.to_string(), value.trim().to_string()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use tonic::metadata::MetadataValue;

    use super::*;

    #[test]
    fn test_extract_hints() {
        let mut metadata = MetadataMap::new();
        let prev = metadata.insert(
            "x-greptime-hint-append_mode",
            MetadataValue::from_static("true"),
        );
        metadata.insert("test-key", MetadataValue::from_static("test-value"));
        assert!(prev.is_none());
        let hints = extract_hints(&metadata);
        assert_eq!(hints, vec![("append_mode".to_string(), "true".to_string())]);
    }

    #[test]
    fn extract_hints_ignores_non_ascii_metadata() {
        let mut metadata = MetadataMap::new();
        metadata.insert_bin(
            "x-greptime-hint-merge_mode-bin",
            MetadataValue::from_bytes(b"last_non_null"),
        );
        let hints = extract_hints(&metadata);
        assert!(hints.is_empty());
    }
}
