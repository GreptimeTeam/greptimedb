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

pub mod do_put;

use std::collections::HashMap;
use std::sync::Arc;

use api::v1::{AffectedRows, FlightMetadata, Metrics};
use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_flight::{FlightData, SchemaAsIpc};
use common_base::bytes::Bytes;
use common_recordbatch::DfRecordBatch;
use datatypes::arrow;
use datatypes::arrow::buffer::Buffer;
use datatypes::arrow::datatypes::{Schema as ArrowSchema, SchemaRef};
use datatypes::arrow::error::ArrowError;
use datatypes::arrow::ipc::{convert, reader, root_as_message, writer, MessageHeader};
use flatbuffers::FlatBufferBuilder;
use prost::bytes::Bytes as ProstBytes;
use prost::Message;
use snafu::{OptionExt, ResultExt};

use crate::error;
use crate::error::{DecodeFlightDataSnafu, InvalidFlightDataSnafu, Result};

#[derive(Debug, Clone)]
pub enum FlightMessage {
    Schema(SchemaRef),
    RecordBatch(DfRecordBatch),
    AffectedRows(usize),
    Metrics(String),
}

pub struct FlightEncoder {
    write_options: writer::IpcWriteOptions,
    data_gen: writer::IpcDataGenerator,
    dictionary_tracker: writer::DictionaryTracker,
}

impl Default for FlightEncoder {
    fn default() -> Self {
        let write_options = writer::IpcWriteOptions::default()
            .try_with_compression(Some(arrow::ipc::CompressionType::LZ4_FRAME))
            .unwrap();

        Self {
            write_options,
            data_gen: writer::IpcDataGenerator::default(),
            dictionary_tracker: writer::DictionaryTracker::new(false),
        }
    }
}

impl FlightEncoder {
    /// Creates new [FlightEncoder] with compression disabled.
    pub fn with_compression_disabled() -> Self {
        let write_options = writer::IpcWriteOptions::default()
            .try_with_compression(None)
            .unwrap();

        Self {
            write_options,
            data_gen: writer::IpcDataGenerator::default(),
            dictionary_tracker: writer::DictionaryTracker::new(false),
        }
    }

    pub fn encode(&mut self, flight_message: FlightMessage) -> FlightData {
        match flight_message {
            FlightMessage::Schema(schema) => SchemaAsIpc::new(&schema, &self.write_options).into(),
            FlightMessage::RecordBatch(record_batch) => {
                let (encoded_dictionaries, encoded_batch) = self
                    .data_gen
                    .encoded_batch(
                        &record_batch,
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
                    metrics: None,
                }
                .encode_to_vec();
                FlightData {
                    flight_descriptor: None,
                    data_header: build_none_flight_msg().into(),
                    app_metadata: metadata.into(),
                    data_body: ProstBytes::default(),
                }
            }
            FlightMessage::Metrics(s) => {
                let metadata = FlightMetadata {
                    affected_rows: None,
                    metrics: Some(Metrics {
                        metrics: s.as_bytes().to_vec(),
                    }),
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
    schema_bytes: Option<bytes::Bytes>,
}

impl FlightDecoder {
    /// Build a [FlightDecoder] instance from provided schema bytes.
    pub fn try_from_schema_bytes(schema_bytes: &bytes::Bytes) -> Result<Self> {
        let arrow_schema = convert::try_schema_from_flatbuffer_bytes(&schema_bytes[..])
            .context(error::ArrowSnafu)?;
        Ok(Self {
            schema: Some(Arc::new(arrow_schema)),
            schema_bytes: Some(schema_bytes.clone()),
        })
    }

    pub fn try_decode_record_batch(
        &mut self,
        data_header: &bytes::Bytes,
        data_body: &bytes::Bytes,
    ) -> Result<DfRecordBatch> {
        let schema = self
            .schema
            .as_ref()
            .context(InvalidFlightDataSnafu {
                reason: "Should have decoded schema first!",
            })?
            .clone();
        let message = root_as_message(&data_header[..])
            .map_err(|err| {
                ArrowError::ParseError(format!("Unable to get root as message: {err:?}"))
            })
            .context(error::ArrowSnafu)?;
        let result = message
            .header_as_record_batch()
            .ok_or_else(|| {
                ArrowError::ParseError(
                    "Unable to convert flight data header to a record batch".to_string(),
                )
            })
            .and_then(|batch| {
                reader::read_record_batch(
                    &Buffer::from(data_body.as_ref()),
                    batch,
                    schema,
                    &HashMap::new(),
                    None,
                    &message.version(),
                )
            })
            .context(error::ArrowSnafu)?;
        Ok(result)
    }

    pub fn try_decode(&mut self, flight_data: &FlightData) -> Result<FlightMessage> {
        let message = root_as_message(&flight_data.data_header).map_err(|e| {
            InvalidFlightDataSnafu {
                reason: e.to_string(),
            }
            .build()
        })?;
        match message.header_type() {
            MessageHeader::NONE => {
                let metadata = FlightMetadata::decode(flight_data.app_metadata.clone())
                    .context(DecodeFlightDataSnafu)?;
                if let Some(AffectedRows { value }) = metadata.affected_rows {
                    return Ok(FlightMessage::AffectedRows(value as _));
                }
                if let Some(Metrics { metrics }) = metadata.metrics {
                    return Ok(FlightMessage::Metrics(
                        String::from_utf8_lossy(&metrics).to_string(),
                    ));
                }
                InvalidFlightDataSnafu {
                    reason: "Expecting FlightMetadata have some meaningful content.",
                }
                .fail()
            }
            MessageHeader::Schema => {
                let arrow_schema = Arc::new(ArrowSchema::try_from(flight_data).map_err(|e| {
                    InvalidFlightDataSnafu {
                        reason: e.to_string(),
                    }
                    .build()
                })?);
                self.schema = Some(arrow_schema.clone());
                self.schema_bytes = Some(flight_data.data_header.clone());
                Ok(FlightMessage::Schema(arrow_schema))
            }
            MessageHeader::RecordBatch => {
                let schema = self.schema.clone().context(InvalidFlightDataSnafu {
                    reason: "Should have decoded schema first!",
                })?;
                let arrow_batch =
                    flight_data_to_arrow_batch(flight_data, schema.clone(), &HashMap::new())
                        .map_err(|e| {
                            InvalidFlightDataSnafu {
                                reason: e.to_string(),
                            }
                            .build()
                        })?;
                Ok(FlightMessage::RecordBatch(arrow_batch))
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

    pub fn schema(&self) -> Option<&SchemaRef> {
        self.schema.as_ref()
    }

    pub fn schema_bytes(&self) -> Option<bytes::Bytes> {
        self.schema_bytes.clone()
    }
}

pub fn flight_messages_to_recordbatches(
    messages: Vec<FlightMessage>,
) -> Result<Vec<DfRecordBatch>> {
    if messages.is_empty() {
        Ok(vec![])
    } else {
        let mut recordbatches = Vec::with_capacity(messages.len() - 1);

        match &messages[0] {
            FlightMessage::Schema(_schema) => {}
            _ => {
                return InvalidFlightDataSnafu {
                    reason: "First Flight Message must be schema!",
                }
                .fail()
            }
        };

        for message in messages.into_iter().skip(1) {
            match message {
                FlightMessage::RecordBatch(recordbatch) => recordbatches.push(recordbatch),
                _ => {
                    return InvalidFlightDataSnafu {
                        reason: "Expect the following Flight Messages are all Recordbatches!",
                    }
                    .fail()
                }
            }
        }

        Ok(recordbatches)
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
    use datatypes::arrow::array::Int32Array;
    use datatypes::arrow::datatypes::{DataType, Field, Schema};

    use super::*;
    use crate::Error;

    #[test]
    fn test_try_decode() {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "n",
            DataType::Int32,
            true,
        )]));

        let batch1 = DfRecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])) as _],
        )
        .unwrap();
        let batch2 = DfRecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![None, Some(5)])) as _],
        )
        .unwrap();

        let flight_data =
            batches_to_flight_data(&schema, vec![batch1.clone(), batch2.clone()]).unwrap();
        assert_eq!(flight_data.len(), 3);
        let [d1, d2, d3] = flight_data.as_slice() else {
            unreachable!()
        };

        let decoder = &mut FlightDecoder::default();
        assert!(decoder.schema.is_none());

        let result = decoder.try_decode(d2);
        assert!(matches!(result, Err(Error::InvalidFlightData { .. })));
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Should have decoded schema first!"));

        let message = decoder.try_decode(d1).unwrap();
        assert!(matches!(message, FlightMessage::Schema(_)));
        let FlightMessage::Schema(decoded_schema) = message else {
            unreachable!()
        };
        assert_eq!(decoded_schema, schema);

        let _ = decoder.schema.as_ref().unwrap();

        let message = decoder.try_decode(d2).unwrap();
        assert!(matches!(message, FlightMessage::RecordBatch(_)));
        let FlightMessage::RecordBatch(actual_batch) = message else {
            unreachable!()
        };
        assert_eq!(actual_batch, batch1);

        let message = decoder.try_decode(d3).unwrap();
        assert!(matches!(message, FlightMessage::RecordBatch(_)));
        let FlightMessage::RecordBatch(actual_batch) = message else {
            unreachable!()
        };
        assert_eq!(actual_batch, batch2);
    }

    #[test]
    fn test_flight_messages_to_recordbatches() {
        let schema = Arc::new(Schema::new(vec![Field::new("m", DataType::Int32, true)]));
        let batch1 = DfRecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![Some(2), None, Some(4)])) as _],
        )
        .unwrap();
        let batch2 = DfRecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![None, Some(6)])) as _],
        )
        .unwrap();
        let recordbatches = vec![batch1.clone(), batch2.clone()];

        let m1 = FlightMessage::Schema(schema);
        let m2 = FlightMessage::RecordBatch(batch1);
        let m3 = FlightMessage::RecordBatch(batch2);

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
