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

use arrow_flight::FlightData;
use bytes::Bytes;
use common_grpc::flight::{FlightDecoder, FlightEncoder, FlightMessage};
use common_recordbatch::DfRecordBatch;
use criterion::{criterion_group, criterion_main, Criterion};
use datatypes::arrow;
use datatypes::arrow::array::{ArrayRef, Int64Array, StringArray, TimestampMillisecondArray};
use datatypes::arrow::datatypes::DataType;
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema};
use prost::Message;

fn schema() -> arrow::datatypes::SchemaRef {
    let schema = Schema::new(vec![
        ColumnSchema::new("k0", ConcreteDataType::string_datatype(), false),
        ColumnSchema::new("k1", ConcreteDataType::string_datatype(), false),
        ColumnSchema::new(
            "ts",
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        ),
        ColumnSchema::new("v0", ConcreteDataType::int64_datatype(), false),
        ColumnSchema::new("v1", ConcreteDataType::int64_datatype(), false),
    ]);
    schema.arrow_schema().clone()
}

/// Generate record batch according to provided schema and num rows.
fn prepare_random_record_batch(
    schema: arrow::datatypes::SchemaRef,
    num_rows: usize,
) -> DfRecordBatch {
    let tag_candidates = (0..10000).map(|i| i.to_string()).collect::<Vec<_>>();

    let columns: Vec<ArrayRef> = schema
        .fields
        .iter()
        .map(|col| match col.data_type() {
            DataType::Utf8 => {
                let array = StringArray::from(
                    (0..num_rows)
                        .map(|_| {
                            let idx: usize = rand::random_range(0..10000);
                            format!("tag-{}", tag_candidates[idx])
                        })
                        .collect::<Vec<_>>(),
                );
                Arc::new(array) as ArrayRef
            }
            DataType::Timestamp(_, _) => {
                let now = common_time::util::current_time_millis();
                let array = TimestampMillisecondArray::from(
                    (0..num_rows).map(|i| now + i as i64).collect::<Vec<_>>(),
                );
                Arc::new(array) as ArrayRef
            }
            DataType::Int64 => {
                let array = Int64Array::from((0..num_rows).map(|i| i as i64).collect::<Vec<_>>());
                Arc::new(array) as ArrayRef
            }
            _ => unreachable!(),
        })
        .collect();

    DfRecordBatch::try_new(schema, columns).unwrap()
}

fn prepare_flight_data(num_rows: usize) -> (FlightData, FlightData) {
    let schema = schema();
    let mut encoder = FlightEncoder::default();
    let schema_data = encoder.encode_schema(schema.as_ref());
    let rb = prepare_random_record_batch(schema, num_rows);
    let [rb_data] = encoder
        .encode(FlightMessage::RecordBatch(rb))
        .try_into()
        .unwrap();
    (schema_data, rb_data)
}

fn decode_flight_data_from_protobuf(schema: &Bytes, payload: &Bytes) -> DfRecordBatch {
    let schema = FlightData::decode(&schema[..]).unwrap();
    let payload = FlightData::decode(&payload[..]).unwrap();
    let mut decoder = FlightDecoder::default();
    let _schema = decoder.try_decode(&schema).unwrap();
    let message = decoder.try_decode(&payload).unwrap();
    let Some(FlightMessage::RecordBatch(batch)) = message else {
        unreachable!("unexpected message");
    };
    batch
}

fn decode_flight_data_from_header_and_body(
    schema: &Bytes,
    data_header: &Bytes,
    data_body: &Bytes,
) -> DfRecordBatch {
    let mut decoder = FlightDecoder::try_from_schema_bytes(schema).unwrap();
    decoder
        .try_decode_record_batch(data_header, data_body)
        .unwrap()
}

fn bench_decode_flight_data(c: &mut Criterion) {
    let row_counts = [100000, 200000, 1000000];

    for row_count in row_counts {
        let (schema, payload) = prepare_flight_data(row_count);

        // arguments for decode_flight_data_from_protobuf
        let schema_bytes = Bytes::from(schema.encode_to_vec());
        let payload_bytes = Bytes::from(payload.encode_to_vec());

        let mut group = c.benchmark_group(format!("flight_decoder_{}_rows", row_count));
        group.bench_function("decode_from_protobuf", |b| {
            b.iter(|| decode_flight_data_from_protobuf(&schema_bytes, &payload_bytes));
        });

        group.bench_function("decode_from_header_and_body", |b| {
            b.iter(|| {
                decode_flight_data_from_header_and_body(
                    &schema.data_header,
                    &payload.data_header,
                    &payload.data_body,
                )
            });
        });

        group.finish();
    }
}

criterion_group!(benches, bench_decode_flight_data);
criterion_main!(benches);
