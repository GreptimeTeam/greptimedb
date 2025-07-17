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

use common_grpc::flight::{FlightEncoder, FlightMessage};
use common_grpc::FlightData;
use common_recordbatch::DfRecordBatch;

/// Encodes record batch to a Schema message and a RecordBatch message.
pub fn encode_to_flight_data(rb: DfRecordBatch) -> (FlightData, FlightData) {
    let mut encoder = FlightEncoder::default();
    let schema = encoder.encode_schema(rb.schema_ref().as_ref());
    let [data] = encoder
        .encode(FlightMessage::RecordBatch(rb))
        .try_into()
        .unwrap();
    (schema, data)
}
