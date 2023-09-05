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

use api::v1::region::{region_request, RegionRequest, RegionRequestHeader, RegionResponse};
use api::v1::ResponseHeader;
use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_error::status_code::StatusCode;
use common_meta::datanode_manager::{AffectedRows, Datanode};
use common_meta::error::{self as meta_error, Result as MetaResult};
use common_telemetry::timer;
use snafu::{location, Location, OptionExt};

use crate::error::Error::FlightGet;
use crate::error::{IllegalDatabaseResponseSnafu, MissingFieldSnafu, Result, ServerSnafu};
use crate::{metrics, Client};

#[derive(Debug)]
pub struct RegionRequester {
    client: Client,
}

#[async_trait]
impl Datanode for RegionRequester {
    async fn handle(&self, request: RegionRequest) -> MetaResult<AffectedRows> {
        self.handle_inner(request).await.map_err(|err| {
            if matches!(err, FlightGet { .. }) {
                meta_error::Error::RetryLater {
                    source: BoxedError::new(err),
                }
            } else {
                meta_error::Error::OperateRegion {
                    source: BoxedError::new(err),
                    location: location!(),
                }
            }
        })
    }
}

impl RegionRequester {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    async fn handle_inner(&self, request: RegionRequest) -> Result<AffectedRows> {
        let request_type = request
            .body
            .as_ref()
            .with_context(|| MissingFieldSnafu { field: "body" })?
            .as_ref()
            .to_string();

        let _timer = timer!(
            metrics::METRIC_REGION_REQUEST_GRPC,
            &[("request_type", request_type)]
        );

        let mut client = self.client.raw_region_client()?;

        let RegionResponse {
            header,
            affected_rows,
        } = client.handle(request).await?.into_inner();

        check_response_header(header)?;

        Ok(affected_rows)
    }

    pub async fn handle(&self, request: region_request::Body) -> Result<AffectedRows> {
        let request = RegionRequest {
            header: Some(RegionRequestHeader {
                trace_id: 0,
                span_id: 0,
            }),
            body: Some(request),
        };
        self.handle_inner(request).await
    }
}

fn check_response_header(header: Option<ResponseHeader>) -> Result<()> {
    let status = header
        .and_then(|header| header.status)
        .context(IllegalDatabaseResponseSnafu {
            err_msg: "either response header or status is missing",
        })?;

    if StatusCode::is_success(status.status_code) {
        Ok(())
    } else {
        let code =
            StatusCode::from_u32(status.status_code).context(IllegalDatabaseResponseSnafu {
                err_msg: format!("unknown server status: {:?}", status),
            })?;
        ServerSnafu {
            code,
            msg: status.err_msg,
        }
        .fail()
    }
}

#[cfg(test)]
mod test {
    use api::v1::Status as PbStatus;

    use super::*;
    use crate::Error::{IllegalDatabaseResponse, Server};

    #[test]
    fn test_check_response_header() {
        let result = check_response_header(None);
        assert!(matches!(
            result.unwrap_err(),
            IllegalDatabaseResponse { .. }
        ));

        let result = check_response_header(Some(ResponseHeader { status: None }));
        assert!(matches!(
            result.unwrap_err(),
            IllegalDatabaseResponse { .. }
        ));

        let result = check_response_header(Some(ResponseHeader {
            status: Some(PbStatus {
                status_code: StatusCode::Success as u32,
                err_msg: "".to_string(),
            }),
        }));
        assert!(result.is_ok());

        let result = check_response_header(Some(ResponseHeader {
            status: Some(PbStatus {
                status_code: u32::MAX,
                err_msg: "".to_string(),
            }),
        }));
        assert!(matches!(
            result.unwrap_err(),
            IllegalDatabaseResponse { .. }
        ));

        let result = check_response_header(Some(ResponseHeader {
            status: Some(PbStatus {
                status_code: StatusCode::Internal as u32,
                err_msg: "blabla".to_string(),
            }),
        }));
        let Server { code, msg } = result.unwrap_err() else {
            unreachable!()
        };
        assert_eq!(code, StatusCode::Internal);
        assert_eq!(msg, "blabla");
    }
}
