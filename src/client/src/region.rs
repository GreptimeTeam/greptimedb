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

use api::region::RegionResponse;
use api::v1::ResponseHeader;
use api::v1::region::RegionRequest;
use arc_swap::ArcSwapOption;
use arrow_flight::Ticket;
use async_stream::stream;
use common_error::ext::BoxedError;
use common_error::status_code::StatusCode;
use common_grpc::flight::{FlightDecoder, FlightMessage};
use common_meta::error::{self as meta_error, Result as MetaResult};
use common_query::request::QueryRequest;
use common_recordbatch::error::ExternalSnafu;
use common_recordbatch::{RecordBatch, RecordBatchStreamWrapper, SendableRecordBatchStream};
use common_telemetry::error;
use common_telemetry::tracing::Span;
use common_telemetry::tracing_context::TracingContext;
use prost::Message;
use query::query_engine::DefaultSerializer;
use snafu::{OptionExt, ResultExt, location};
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
use tokio_stream::StreamExt;

use crate::error::{
    self, ConvertFlightDataSnafu, FlightGetSnafu, IllegalDatabaseResponseSnafu,
    IllegalFlightMessagesSnafu, MissingFieldSnafu, Result, ServerSnafu,
};
use crate::{Client, Error, metrics};

#[derive(Debug)]
pub struct RegionRequester {
    client: Client,
    send_compression: bool,
    accept_compression: bool,
}

impl RegionRequester {
    pub fn new(client: Client, send_compression: bool, accept_compression: bool) -> Self {
        Self {
            client,
            send_compression,
            accept_compression,
        }
    }

    pub async fn handle_region(&self, request: RegionRequest) -> MetaResult<RegionResponse> {
        self.handle_inner(request).await.map_err(|err| {
            if err.should_retry() {
                meta_error::Error::RetryLater {
                    source: BoxedError::new(err),
                    clean_poisons: false,
                }
            } else {
                meta_error::Error::External {
                    source: BoxedError::new(err),
                    location: location!(),
                }
            }
        })
    }

    pub async fn handle_query(
        &self,
        request: QueryRequest,
    ) -> MetaResult<SendableRecordBatchStream> {
        let plan = DFLogicalSubstraitConvertor
            .encode(&request.plan, DefaultSerializer)
            .map_err(BoxedError::new)
            .context(meta_error::ExternalSnafu)?
            .to_vec();
        let request = api::v1::region::QueryRequest {
            header: request.header,
            region_id: request.region_id.as_u64(),
            plan,
        };

        let ticket = Ticket {
            ticket: request.encode_to_vec().into(),
        };
        self.do_get_inner(ticket)
            .await
            .map_err(BoxedError::new)
            .context(meta_error::ExternalSnafu)
    }

    async fn do_get_inner(&self, ticket: Ticket) -> Result<SendableRecordBatchStream> {
        let mut flight_client = self
            .client
            .make_flight_client(self.send_compression, self.accept_compression)?;
        let response = flight_client
            .mut_inner()
            .do_get(ticket)
            .await
            .or_else(|e| {
                let tonic_code = e.code();
                let e: error::Error = e.into();
                error!(
                    e; "Failed to do Flight get, addr: {}, code: {}",
                    flight_client.addr(),
                    tonic_code
                );
                Err(BoxedError::new(e)).with_context(|_| FlightGetSnafu {
                    addr: flight_client.addr().to_string(),
                    tonic_code,
                })
            })?;

        let flight_data_stream = response.into_inner();
        let mut decoder = FlightDecoder::default();

        let mut flight_message_stream = flight_data_stream.map(move |flight_data| {
            flight_data
                .map_err(Error::from)
                .and_then(|data| decoder.try_decode(&data).context(ConvertFlightDataSnafu))?
                .context(IllegalFlightMessagesSnafu {
                    reason: "none message",
                })
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

        let metrics = Arc::new(ArcSwapOption::from(None));
        let metrics_ref = metrics.clone();

        let tracing_context = TracingContext::from_current_span();

        let schema = Arc::new(
            datatypes::schema::Schema::try_from(schema).context(error::ConvertSchemaSnafu)?,
        );
        let schema_cloned = schema.clone();
        let stream = Box::pin(stream!({
            let _span = tracing_context.attach(common_telemetry::tracing::info_span!(
                "poll_flight_data_stream"
            ));

            let mut buffered_message: Option<FlightMessage> = None;
            let mut stream_ended = false;

            while !stream_ended {
                // get the next message from the buffered message or read from the flight message stream
                let flight_message_item = if let Some(msg) = buffered_message.take() {
                    Some(Ok(msg))
                } else {
                    flight_message_stream.next().await
                };

                let flight_message = match flight_message_item {
                    Some(Ok(message)) => message,
                    Some(Err(e)) => {
                        yield Err(BoxedError::new(e)).context(ExternalSnafu);
                        break;
                    }
                    None => break,
                };

                match flight_message {
                    FlightMessage::RecordBatch(record_batch) => {
                        let result_to_yield =
                            RecordBatch::from_df_record_batch(schema_cloned.clone(), record_batch);

                        // get the next message from the stream. normally it should be a metrics message.
                        if let Some(next_flight_message_result) = flight_message_stream.next().await
                        {
                            match next_flight_message_result {
                                Ok(FlightMessage::Metrics(s)) => {
                                    let m = serde_json::from_str(&s).ok().map(Arc::new);
                                    metrics_ref.swap(m);
                                }
                                Ok(FlightMessage::RecordBatch(rb)) => {
                                    // for some reason it's not a metrics message, so we need to buffer this record batch
                                    // and yield it in the next iteration.
                                    buffered_message = Some(FlightMessage::RecordBatch(rb));
                                }
                                Ok(_) => {
                                    yield IllegalFlightMessagesSnafu {
                                        reason: "A RecordBatch message can only be succeeded by a Metrics message or another RecordBatch message"
                                    }
                                    .fail()
                                    .map_err(BoxedError::new)
                                    .context(ExternalSnafu);
                                    break;
                                }
                                Err(e) => {
                                    yield Err(BoxedError::new(e)).context(ExternalSnafu);
                                    break;
                                }
                            }
                        } else {
                            // the stream has ended
                            stream_ended = true;
                        }

                        yield Ok(result_to_yield);
                    }
                    FlightMessage::Metrics(s) => {
                        // just a branch in case of some metrics message comes after other things.
                        let m = serde_json::from_str(&s).ok().map(Arc::new);
                        metrics_ref.swap(m);
                        break;
                    }
                    _ => {
                        yield IllegalFlightMessagesSnafu {
                            reason: "A Schema message must be succeeded exclusively by a set of RecordBatch messages"
                        }
                        .fail()
                        .map_err(BoxedError::new)
                        .context(ExternalSnafu);
                        break;
                    }
                }
            }
        }));
        let record_batch_stream = RecordBatchStreamWrapper {
            schema,
            stream,
            output_ordering: None,
            metrics,
            span: Span::current(),
        };
        Ok(Box::pin(record_batch_stream))
    }

    async fn handle_inner(&self, request: RegionRequest) -> Result<RegionResponse> {
        let request_type = request
            .body
            .as_ref()
            .with_context(|| MissingFieldSnafu { field: "body" })?
            .as_ref()
            .to_string();
        let _timer = metrics::METRIC_REGION_REQUEST_GRPC
            .with_label_values(&[request_type.as_str()])
            .start_timer();

        let (addr, mut client) = self.client.raw_region_client()?;

        let response = client
            .handle(request)
            .await
            .map_err(|e| {
                let code = e.code();
                // Uses `Error::RegionServer` instead of `Error::Server`
                error::Error::RegionServer {
                    addr,
                    code,
                    source: BoxedError::new(error::Error::from(e)),
                    location: location!(),
                }
            })?
            .into_inner();

        check_response_header(&response.header)?;

        Ok(RegionResponse::from_region_response(response))
    }
}

pub fn check_response_header(header: &Option<ResponseHeader>) -> Result<()> {
    let status = header
        .as_ref()
        .and_then(|header| header.status.as_ref())
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
            msg: status.err_msg.clone(),
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
        let result = check_response_header(&None);
        assert!(matches!(
            result.unwrap_err(),
            IllegalDatabaseResponse { .. }
        ));

        let result = check_response_header(&Some(ResponseHeader { status: None }));
        assert!(matches!(
            result.unwrap_err(),
            IllegalDatabaseResponse { .. }
        ));

        let result = check_response_header(&Some(ResponseHeader {
            status: Some(PbStatus {
                status_code: StatusCode::Success as u32,
                err_msg: String::default(),
            }),
        }));
        assert!(result.is_ok());

        let result = check_response_header(&Some(ResponseHeader {
            status: Some(PbStatus {
                status_code: u32::MAX,
                err_msg: String::default(),
            }),
        }));
        assert!(matches!(
            result.unwrap_err(),
            IllegalDatabaseResponse { .. }
        ));

        let result = check_response_header(&Some(ResponseHeader {
            status: Some(PbStatus {
                status_code: StatusCode::Internal as u32,
                err_msg: "blabla".to_string(),
            }),
        }));
        let Server { code, msg, .. } = result.unwrap_err() else {
            unreachable!()
        };
        assert_eq!(code, StatusCode::Internal);
        assert_eq!(msg, "blabla");
    }
}
