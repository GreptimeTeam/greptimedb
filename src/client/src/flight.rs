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

use std::pin::Pin;

use arrow_flight::FlightData;
use common_error::ext::{BoxedError, ErrorExt};
use common_grpc::flight::{FlightDecoder, FlightMessage};
use common_telemetry::error;
use futures_util::stream::Peekable;
use futures_util::{Stream, StreamExt};
use snafu::{OptionExt, ResultExt};

use crate::Result;
use crate::error::{ConvertFlightDataSnafu, Error, IllegalFlightMessagesSnafu};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FlightMessageKind {
    Schema,
    RecordBatch,
    AffectedRows,
    Metrics,
}

impl From<&FlightMessage> for FlightMessageKind {
    fn from(message: &FlightMessage) -> Self {
        match message {
            FlightMessage::Schema(_) => Self::Schema,
            FlightMessage::RecordBatch(_) => Self::RecordBatch,
            FlightMessage::AffectedRows { .. } => Self::AffectedRows,
            FlightMessage::Metrics(_) => Self::Metrics,
        }
    }
}

pub(crate) struct FlightMessageReader<S: Stream + Unpin> {
    /// Remote Flight peer used to decorate response-stream errors.
    remote_addr: String,
    messages: Peekable<S>,
}

impl<S> FlightMessageReader<S>
where
    S: Stream<Item = Result<FlightMessage>> + Unpin,
{
    pub(crate) fn new(remote_addr: impl Into<String>, messages: S) -> Self {
        Self {
            remote_addr: remote_addr.into(),
            messages: messages.peekable(),
        }
    }

    pub(crate) fn remote_addr(&self) -> &str {
        &self.remote_addr
    }

    pub(crate) async fn read_first(&mut self) -> Result<FlightMessage> {
        self.read_next()
            .await?
            .context(IllegalFlightMessagesSnafu {
                reason: "Expect the response not to be empty",
            })
            .map_err(|error| wrap_flight_stream_error(self.remote_addr(), error))
    }

    pub(crate) async fn read_next(&mut self) -> Result<Option<FlightMessage>> {
        self.messages
            .next()
            .await
            .transpose()
            .map_err(|error| wrap_flight_stream_error(&self.remote_addr, error))
    }

    pub(crate) async fn peek_next_message_kind(&mut self) -> Result<Option<FlightMessageKind>> {
        match Pin::new(&mut self.messages).peek().await {
            Some(Ok(message)) => Ok(Some(message.into())),
            None => Ok(None),
            Some(Err(_)) => match self.read_next().await {
                // `peek` only borrows the error; consume it to preserve the source error.
                Err(error) => Err(error),
                Ok(_) => IllegalFlightMessagesSnafu {
                    reason: "Flight stream changed after peek".to_string(),
                }
                .fail(),
            },
        }
    }
}

pub(crate) fn wrap_flight_stream_error(remote_addr: &str, error: Error) -> Error {
    let tonic_code = error.tonic_code().unwrap_or(tonic::Code::Unknown);
    if error.status_code().should_log_error() {
        error!(
            error; "Failed to receive Flight data, addr: {}, code: {}",
            remote_addr,
            tonic_code
        );
    }

    Error::FlightGet {
        addr: remote_addr.to_string(),
        tonic_code,
        source: BoxedError::new(error),
    }
}

pub(crate) fn decode_flight_data(
    decoder: &mut FlightDecoder,
    flight_data: std::result::Result<FlightData, tonic::Status>,
) -> Option<Result<FlightMessage>> {
    flight_data
        .map_err(Error::from)
        .and_then(|data| decoder.try_decode(&data).context(ConvertFlightDataSnafu))
        .transpose()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_grpc::flight::FlightEncoder;
    use datatypes::arrow::array::{DictionaryArray, StringArray, UInt32Array};
    use datatypes::arrow::datatypes::{DataType, Field, Schema, UInt32Type};
    use datatypes::arrow::record_batch::RecordBatch;

    use super::*;

    #[test]
    fn test_decode_flight_data_skips_dictionary_batches() {
        let schema = Arc::new(Schema::new(vec![Field::new_dictionary(
            "host",
            DataType::UInt32,
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(DictionaryArray::<UInt32Type>::new(
                UInt32Array::from(vec![0, 1, 0]),
                Arc::new(StringArray::from(vec!["host-a", "host-b"])),
            ))],
        )
        .unwrap();

        let mut encoder = FlightEncoder::default();
        let mut flight_data = Vec::new();
        flight_data.extend(encoder.encode(FlightMessage::Schema(schema.clone())));
        let encoded_batch = encoder.encode(FlightMessage::RecordBatch(batch.clone()));
        assert_eq!(2, encoded_batch.len());
        flight_data.extend(encoded_batch);

        let mut decoder = FlightDecoder::default();
        let messages = flight_data
            .into_iter()
            .filter_map(|data| decode_flight_data(&mut decoder, Ok(data)))
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(2, messages.len());
        assert!(matches!(&messages[0], FlightMessage::Schema(actual) if actual == &schema));
        assert!(matches!(&messages[1], FlightMessage::RecordBatch(actual) if actual == &batch));
    }
}
