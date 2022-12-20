// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use api::result::ObjectResultBuilder;
use api::v1::ObjectResult;
use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_flight::FlightData;
use common_error::prelude::StatusCode;
use common_recordbatch::{RecordBatch, RecordBatches};
use datatypes::arrow::datatypes::Schema as ArrowSchema;
use datatypes::arrow::ipc::{root_as_message, MessageHeader};
use datatypes::schema::{Schema, SchemaRef};
use futures::TryStreamExt;
use prost::Message;
use snafu::{OptionExt, ResultExt};
use tonic::codegen::futures_core::Stream;
use tonic::Response;

use crate::error::{self, InvalidFlightDataSnafu, Result};

type TonicResult<T> = std::result::Result<T, tonic::Status>;
type TonicStream<T> = Pin<Box<dyn Stream<Item = TonicResult<T>> + Send + Sync + 'static>>;

#[derive(Debug)]
pub enum FlightMessage {
    Schema(SchemaRef),
    Recordbatch(RecordBatch),
}

#[derive(Default)]
pub struct FlightDecoder {
    schema: Option<SchemaRef>,
}

impl FlightDecoder {
    pub fn try_decode(&mut self, flight_data: FlightData) -> Result<FlightMessage> {
        let message = root_as_message(flight_data.data_header.as_slice()).map_err(|e| {
            InvalidFlightDataSnafu {
                reason: e.to_string(),
            }
            .build()
        })?;
        match message.header_type() {
            MessageHeader::Schema => {
                let arrow_schema = ArrowSchema::try_from(&flight_data).map_err(|e| {
                    InvalidFlightDataSnafu {
                        reason: e.to_string(),
                    }
                    .build()
                })?;
                let schema = Arc::new(
                    Schema::try_from(arrow_schema).context(error::ConvertArrowSchemaSnafu)?,
                );

                self.schema = Some(schema.clone());

                Ok(FlightMessage::Schema(schema))
            }
            MessageHeader::RecordBatch => {
                let schema = self.schema.clone().context(InvalidFlightDataSnafu {
                    reason: "Should have decoded schema first!",
                })?;
                let arrow_schema = schema.arrow_schema().clone();

                let arrow_batch =
                    flight_data_to_arrow_batch(&flight_data, arrow_schema, &HashMap::new())
                        .map_err(|e| {
                            InvalidFlightDataSnafu {
                                reason: e.to_string(),
                            }
                            .build()
                        })?;
                let recordbatch = RecordBatch::try_from_df_record_batch(schema, arrow_batch)
                    .context(error::CreateRecordBatchSnafu)?;
                Ok(FlightMessage::Recordbatch(recordbatch))
            }
            other => {
                let name = other.variant_name().unwrap_or("UNKNOWN");
                InvalidFlightDataSnafu {
                    reason: format!("Unsupported FlightData type: {name}"),
                }
                .fail()
            }
        }
    }
}

// TODO(LFC): Remove it once we completely get rid of old GRPC interface.
pub async fn flight_data_to_object_result(
    response: Response<TonicStream<FlightData>>,
) -> Result<ObjectResult> {
    let stream = response.into_inner();
    let result: TonicResult<Vec<FlightData>> = stream.try_collect().await;
    match result {
        Ok(flight_data) => {
            let flight_data = flight_data
                .into_iter()
                .map(|x| x.encode_to_vec())
                .collect::<Vec<Vec<u8>>>();
            Ok(ObjectResultBuilder::new()
                .status_code(StatusCode::Success as u32)
                .flight_data(flight_data)
                .build())
        }
        Err(e) => Ok(ObjectResultBuilder::new()
            .status_code(StatusCode::Internal as _)
            .err_msg(e.to_string())
            .build()),
    }
}

pub fn raw_flight_data_to_message(raw_data: Vec<Vec<u8>>) -> Result<Vec<FlightMessage>> {
    let flight_data = raw_data
        .into_iter()
        .map(|x| FlightData::decode(x.as_slice()).context(error::DecodeFlightDataSnafu))
        .collect::<Result<Vec<FlightData>>>()?;

    let decoder = &mut FlightDecoder::default();
    flight_data
        .into_iter()
        .map(|x| decoder.try_decode(x))
        .collect()
}

pub fn flight_messages_to_recordbatches(messages: Vec<FlightMessage>) -> Result<RecordBatches> {
    if messages.is_empty() {
        Ok(RecordBatches::empty())
    } else {
        let mut recordbatches = Vec::with_capacity(messages.len() - 1);

        let schema = match &messages[0] {
            FlightMessage::Schema(schema) => schema.clone(),
            _ => {
                return InvalidFlightDataSnafu {
                    reason: "First Flight Message must be schema!",
                }
                .fail()
            }
        };

        for message in messages.into_iter().skip(1) {
            match message {
                FlightMessage::Recordbatch(recordbatch) => recordbatches.push(recordbatch),
                _ => {
                    return InvalidFlightDataSnafu {
                        reason: "Expect the following Flight Messages are all Recordbatches!",
                    }
                    .fail()
                }
            }
        }

        RecordBatches::try_new(schema, recordbatches).context(error::CreateRecordBatchSnafu)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_try_decode() {
        let decoder = &mut FlightDecoder::default();
        assert!(decoder.schema.is_none());
    }
}
