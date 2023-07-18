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

use api::v1::greptime_database_server::GreptimeDatabase;
use api::v1::greptime_response::Response as RawResponse;
use api::v1::{AffectedRows, GreptimeRequest, GreptimeResponse, ResponseHeader};
use async_trait::async_trait;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_query::Output;
use futures::StreamExt;
use tonic::{Request, Response, Status, Streaming};

use crate::grpc::handler::GreptimeRequestHandler;
use crate::grpc::TonicResult;

pub(crate) struct DatabaseService {
    handler: Arc<GreptimeRequestHandler>,
}

impl DatabaseService {
    pub(crate) fn new(handler: Arc<GreptimeRequestHandler>) -> Self {
        Self { handler }
    }
}

#[async_trait]
impl GreptimeDatabase for DatabaseService {
    async fn handle(
        &self,
        request: Request<GreptimeRequest>,
    ) -> TonicResult<Response<GreptimeResponse>> {
        let request = request.into_inner();
        let output = self.handler.handle_request(request).await?;
        let response = match output {
            Ok(Output::AffectedRows(rows)) => GreptimeResponse {
                header: Some(ResponseHeader {
                    status: Some(api::v1::Status {
                        status_code: StatusCode::Success as _,
                        ..Default::default()
                    }),
                }),
                response: Some(RawResponse::AffectedRows(AffectedRows { value: rows as _ })),
            },
            Ok(Output::Stream(_)) | Ok(Output::RecordBatches(_)) => {
                return Err(Status::unimplemented("GreptimeDatabase::Handle for query"));
            }
            Err(e) => GreptimeResponse {
                header: Some(ResponseHeader {
                    status: Some(api::v1::Status {
                        status_code: e.status_code() as _,
                        err_msg: e.to_string(),
                    }),
                }),
                response: None,
            },
        };
        Ok(Response::new(response))
    }

    async fn handle_requests(
        &self,
        request: Request<Streaming<GreptimeRequest>>,
    ) -> Result<Response<GreptimeResponse>, Status> {
        let mut affected_rows = 0;

        let mut stream = request.into_inner();
        while let Some(request) = stream.next().await {
            let request = request?;
            let output = self.handler.handle_request(request).await?;
            match output {
                Ok(Output::AffectedRows(rows)) => affected_rows += rows,
                Ok(Output::Stream(_)) | Ok(Output::RecordBatches(_)) => {
                    return Err(Status::unimplemented(
                        "GreptimeDatabase::HandleRequests for query",
                    ));
                }
                Err(e) => {
                    // We directly convert it to a tonic error and fail immediately in stream.
                    return Err(e.into());
                }
            }
        }

        let response = GreptimeResponse {
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
        Ok(Response::new(response))
    }
}
