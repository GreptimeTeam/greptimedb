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

use std::collections::HashMap;
use std::sync::Arc;

use api::v1::{AffectedRows, FlightMetadata};
use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_flight::{FlightData, SchemaAsIpc};
use common_base::bytes::Bytes;
use common_recordbatch::{RecordBatch, RecordBatches};
use datatypes::arrow;
use datatypes::arrow::datatypes::Schema as ArrowSchema;
use datatypes::arrow::ipc::{root_as_message, writer, MessageHeader};
use datatypes::schema::{Schema, SchemaRef};
use flatbuffers::FlatBufferBuilder;
use prost::bytes::Bytes as ProstBytes;
use prost::Message;
use snafu::{OptionExt, ResultExt};

use crate::error::{
    ConvertArrowSchemaSnafu, CreateRecordBatchSnafu, DecodeFlightDataSnafu, InvalidFlightDataSnafu,
    Result,
};

#[derive(Debug, Clone)]
pub enum FlightMessage {
    Schema(SchemaRef),
    Recordbatch(RecordBatch),
    AffectedRows(usize),
}

pub struct FlightEncoder {
    write_options: writer::IpcWriteOptions,
    data_gen: writer::IpcDataGenerator,
    dictionary_tracker: writer::DictionaryTracker,
}

impl Default for FlightEncoder {
    fn default() -> Self {
        Self {
            write_options: writer::IpcWriteOptions::default(),
            data_gen: writer::IpcDataGenerator::default(),
            dictionary_tracker: writer::DictionaryTracker::new(false),
        }
    }
}

impl FlightEncoder {
    pub fn encode(&mut self, flight_message: FlightMessage) -> FlightData {
        match flight_message {
            FlightMessage::Schema(schema) => {
                SchemaAsIpc::new(schema.arrow_schema(), &self.write_options).into()
            }
            FlightMessage::Recordbatch(recordbatch) => {
                let (encoded_dictionaries, encoded_batch) = self
                    .data_gen
                    .encoded_batch(
                        recordbatch.df_record_batch(),
                        &mut self.dictionary_tracker,
                        &self.write_options,
                    )
                    .expect("DictionaryTracker configured above to not fail on replacement");

                // TODO(LFC): Handle dictionary as FlightData here, when we supported Arrow's Dictionary DataType.
                // Currently we don't have a datatype corresponding to Arrow's Dictionary DataType,
                // so there won't be any "dictionaries" here. Assert to be sure about it, and
                // perform a "testing guard" in case we forgot to handle the possible "dictionaries"
                // here in the future.
                debug_assert_eq!(encoded_dictionaries.len(), 0);

                encoded_batch.into()
            }
            FlightMessage::AffectedRows(rows) => {
                let metadata = FlightMetadata {
                    affected_rows: Some(AffectedRows { value: rows as _ }),
                }
                .encode_to_vec();
                FlightData {
                    flight_descriptor: None,
                    data_header: build_none_flight_msg().into(),
                    app_metadata: metadata.into(),
                    data_body: ProstBytes::default(),
                }
            }
        }
    }
}

#[derive(Default)]
pub struct FlightDecoder {
    schema: Option<SchemaRef>,
}

impl FlightDecoder {
    pub fn try_decode(&mut self, flight_data: FlightData) -> Result<FlightMessage> {
        let bytes = flight_data.data_header.slice(..);
        let message = root_as_message(&bytes).map_err(|e| {
            InvalidFlightDataSnafu {
                reason: e.to_string(),
            }
            .build()
        })?;
        match message.header_type() {
            MessageHeader::NONE => {
                let metadata = FlightMetadata::decode(flight_data.app_metadata)
                    .context(DecodeFlightDataSnafu)?;
                if let Some(AffectedRows { value }) = metadata.affected_rows {
                    return Ok(FlightMessage::AffectedRows(value as _));
                }
                InvalidFlightDataSnafu {
                    reason: "Expecting FlightMetadata have some meaningful content.",
                }
                .fail()
            }
            MessageHeader::Schema => {
                let arrow_schema = ArrowSchema::try_from(&flight_data).map_err(|e| {
                    InvalidFlightDataSnafu {
                        reason: e.to_string(),
                    }
                    .build()
                })?;
                let schema =
                    Arc::new(Schema::try_from(arrow_schema).context(ConvertArrowSchemaSnafu)?);

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
                    .context(CreateRecordBatchSnafu)?;
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

        RecordBatches::try_new(schema, recordbatches).context(CreateRecordBatchSnafu)
    }
}

fn build_none_flight_msg() -> Bytes {
    let mut builder = FlatBufferBuilder::new();

    let mut message = arrow::ipc::MessageBuilder::new(&mut builder);
    message.add_version(arrow::ipc::MetadataVersion::V5);
    message.add_header_type(MessageHeader::NONE);
    message.add_bodyLength(0);

    let data = message.finish();
    builder.finish(data, None);

    builder.finished_data().into()
}

#[cfg(test)]
mod test {
    use arrow_flight::utils::batches_to_flight_data;
    use datatypes::arrow::datatypes::{DataType, Field};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use datatypes::vectors::Int32Vector;

    use super::*;
    use crate::Error;

    #[test]
    fn test_try_decode() {
        let arrow_schema = ArrowSchema::new(vec![Field::new("n", DataType::Int32, true)]);
        let schema = Arc::new(Schema::try_from(arrow_schema.clone()).unwrap());

        let batch1 = RecordBatch::new(
            schema.clone(),
            vec![Arc::new(Int32Vector::from(vec![Some(1), None, Some(3)])) as _],
        )
        .unwrap();
        let batch2 = RecordBatch::new(
            schema.clone(),
            vec![Arc::new(Int32Vector::from(vec![None, Some(5)])) as _],
        )
        .unwrap();

        let flight_data = batches_to_flight_data(
            &arrow_schema,
            vec![
                batch1.clone().into_df_record_batch(),
                batch2.clone().into_df_record_batch(),
            ],
        )
        .unwrap();
        assert_eq!(flight_data.len(), 3);
        let [d1, d2, d3] = flight_data.as_slice() else {
            unreachable!()
        };

        let decoder = &mut FlightDecoder::default();
        assert!(decoder.schema.is_none());

        let result = decoder.try_decode(d2.clone());
        assert!(matches!(result, Err(Error::InvalidFlightData { .. })));
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Should have decoded schema first!"));

        let message = decoder.try_decode(d1.clone()).unwrap();
        assert!(matches!(message, FlightMessage::Schema(_)));
        let FlightMessage::Schema(decoded_schema) = message else {
            unreachable!()
        };
        assert_eq!(decoded_schema, schema);

        let _ = decoder.schema.as_ref().unwrap();

        let message = decoder.try_decode(d2.clone()).unwrap();
        assert!(matches!(message, FlightMessage::Recordbatch(_)));
        let FlightMessage::Recordbatch(actual_batch) = message else {
            unreachable!()
        };
        assert_eq!(actual_batch, batch1);

        let message = decoder.try_decode(d3.clone()).unwrap();
        assert!(matches!(message, FlightMessage::Recordbatch(_)));
        let FlightMessage::Recordbatch(actual_batch) = message else {
            unreachable!()
        };
        assert_eq!(actual_batch, batch2);
    }

    #[test]
    fn test_flight_messages_to_recordbatches() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "m",
            ConcreteDataType::int32_datatype(),
            true,
        )]));
        let batch1 = RecordBatch::new(
            schema.clone(),
            vec![Arc::new(Int32Vector::from(vec![Some(2), None, Some(4)])) as _],
        )
        .unwrap();
        let batch2 = RecordBatch::new(
            schema.clone(),
            vec![Arc::new(Int32Vector::from(vec![None, Some(6)])) as _],
        )
        .unwrap();
        let recordbatches =
            RecordBatches::try_new(schema.clone(), vec![batch1.clone(), batch2.clone()]).unwrap();

        let m1 = FlightMessage::Schema(schema);
        let m2 = FlightMessage::Recordbatch(batch1);
        let m3 = FlightMessage::Recordbatch(batch2);

        let result = flight_messages_to_recordbatches(vec![m2.clone(), m1.clone(), m3.clone()]);
        assert!(matches!(result, Err(Error::InvalidFlightData { .. })));
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("First Flight Message must be schema!"));

        let result = flight_messages_to_recordbatches(vec![m1.clone(), m2.clone(), m1.clone()]);
        assert!(matches!(result, Err(Error::InvalidFlightData { .. })));
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Expect the following Flight Messages are all Recordbatches!"));

        let actual = flight_messages_to_recordbatches(vec![m1, m2, m3]).unwrap();
        assert_eq!(actual, recordbatches);
    }
}
