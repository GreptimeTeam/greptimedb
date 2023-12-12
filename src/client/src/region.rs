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

use api::v1::region::{QueryRequest, RegionRequest, RegionResponse};
use api::v1::ResponseHeader;
use arrow_flight::Ticket;
use async_stream::stream;
use async_trait::async_trait;
use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_grpc::flight::{FlightDecoder, FlightMessage};
use common_meta::datanode_manager::{AffectedRows, Datanode};
use common_meta::error::{self as meta_error, Result as MetaResult};
use common_recordbatch::error::ExternalSnafu;
use common_recordbatch::{RecordBatchStreamWrapper, SendableRecordBatchStream};
use common_telemetry::error;
use prost::Message;
use snafu::{location, Location, OptionExt, ResultExt};
use tokio_stream::StreamExt;

use crate::error::{
    self, ConvertFlightDataSnafu, IllegalDatabaseResponseSnafu, IllegalFlightMessagesSnafu,
    MissingFieldSnafu, Result, ServerSnafu,
};
use crate::{metrics, Client, Error};

#[derive(Debug)]
pub struct RegionRequester {
    client: Client,
}

#[async_trait]
impl Datanode for RegionRequester {
    async fn handle(&self, request: RegionRequest) -> MetaResult<AffectedRows> {
        self.handle_inner(request).await.map_err(|err| {
            if err.should_retry() {
                meta_error::Error::RetryLater {
                    source: BoxedError::new(err),
                }
            } else {
                meta_error::Error::External {
                    source: BoxedError::new(err),
                    location: location!(),
                }
            }
        })
    }

    async fn handle_query(&self, request: QueryRequest) -> MetaResult<SendableRecordBatchStream> {
        let ticket = Ticket {
            ticket: request.encode_to_vec().into(),
        };
        self.do_get_inner(ticket)
            .await
            .map_err(BoxedError::new)
            .context(meta_error::ExternalSnafu)
    }
}

impl RegionRequester {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    pub async fn do_get_inner(&self, ticket: Ticket) -> Result<SendableRecordBatchStream> {
        let mut flight_client = self.client.make_flight_client()?;
        let response = flight_client
            .mut_inner()
            .do_get(ticket)
            .await
            .map_err(|e| {
                let tonic_code = e.code();
                let e: error::Error = e.into();
                let code = e.status_code();
                let msg = e.to_string();
                let error = Error::FlightGet {
                    tonic_code,
                    addr: flight_client.addr().to_string(),
                    source: BoxedError::new(ServerSnafu { code, msg }.build()),
                };
                error!(
                    e; "Failed to do Flight get, addr: {}, code: {}",
                    flight_client.addr(),
                    tonic_code
                );
                error
            })?;

        let flight_data_stream = response.into_inner();
        let mut decoder = FlightDecoder::default();

        let mut flight_message_stream = flight_data_stream.map(move |flight_data| {
            flight_data
                .map_err(Error::from)
                .and_then(|data| decoder.try_decode(data).context(ConvertFlightDataSnafu))
        });

        let Some(first_flight_message) = flight_message_stream.next().await else {
            return IllegalFlightMessagesSnafu {
                reason: "Expect the response not to be empty",
            }
            .fail();
        };
        let FlightMessage::Schema(schema) = first_flight_message? else {
            return IllegalFlightMessagesSnafu {
                reason: "Expect schema to be the first flight message",
            }
            .fail();
        };

        let stream = Box::pin(stream!({
            while let Some(flight_message) = flight_message_stream.next().await {
                let flight_message = flight_message
                    .map_err(BoxedError::new)
                    .context(ExternalSnafu)?;
                let FlightMessage::Recordbatch(record_batch) = flight_message else {
                    yield IllegalFlightMessagesSnafu {
                            reason: "A Schema message must be succeeded exclusively by a set of RecordBatch messages"
                        }
                        .fail()
                        .map_err(BoxedError::new)
                        .context(ExternalSnafu);
                    break;
                };
                yield Ok(record_batch);
            }
        }));
        let record_batch_stream = RecordBatchStreamWrapper {
            schema,
            stream,
            output_ordering: None,
        };
        Ok(Box::pin(record_batch_stream))
    }

    async fn handle_inner(&self, request: RegionRequest) -> Result<AffectedRows> {
        let request_type = request
            .body
            .as_ref()
            .with_context(|| MissingFieldSnafu { field: "body" })?
            .as_ref()
            .to_string();
        let _timer = metrics::METRIC_REGION_REQUEST_GRPC
            .with_label_values(&[request_type.as_str()])
            .start_timer();

        let mut client = self.client.raw_region_client()?;

        let RegionResponse {
            header,
            affected_rows,
        } = client
            .handle(request)
            .await
            .map_err(|e| {
                let code = e.code();
                let err: error::Error = e.into();
                // Uses `Error::RegionServer` instead of `Error::Server`
                error::Error::RegionServer {
                    code,
                    source: BoxedError::new(err),
                }
            })?
            .into_inner();

        check_response_header(header)?;

        Ok(affected_rows)
    }

    pub async fn handle(&self, request: RegionRequest) -> Result<AffectedRows> {
        self.handle_inner(request).await
    }
}

pub fn check_response_header(header: Option<ResponseHeader>) -> Result<()> {
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
