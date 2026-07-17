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

use arrow_flight::FlightData;
use common_grpc::flight::{FlightDecoder, FlightMessage};
use snafu::ResultExt;

use crate::Result;
use crate::error::{ConvertFlightDataSnafu, Error};

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
