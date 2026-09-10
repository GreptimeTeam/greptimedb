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

//! Benchmarks `WalEntry` encoding: prost's `encode_to_vec` (which recomputes
//! the length of every nested message) against [`WalEntryEncoder`] (which
//! caches each message body length and writes in a single pass).
//!
//! Run with:
//! ```sh
//! cargo bench -p mito2 --bench bench_wal_encode
//! ```

use std::hint::black_box;

use api::v1::helper::{field_column_schema, tag_column_schema, time_index_column_schema};
use api::v1::{ColumnDataType, Mutation, OpType, Row, Rows, Value, WalEntry, value};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use mito2::wal::encoder::WalEntryEncoder;
use prost::Message;

/// Builds a `Rows` with 3 string tags + timestamp + 3 float fields, mirroring a
/// typical metrics write batch.
fn build_rows(num_rows: usize, mutation_idx: usize) -> Rows {
    let schema = vec![
        tag_column_schema("host", ColumnDataType::String),
        tag_column_schema("region", ColumnDataType::String),
        tag_column_schema("dc", ColumnDataType::String),
        time_index_column_schema("ts", ColumnDataType::TimestampMillisecond),
        field_column_schema("usage", ColumnDataType::Float64),
        field_column_schema("sys", ColumnDataType::Float64),
        field_column_schema("idle", ColumnDataType::Float64),
    ];
    let rows = (0..num_rows)
        .map(|i| Row {
            values: vec![
                Value {
                    value_data: Some(value::ValueData::StringValue(format!(
                        "w{mutation_idx}_h{}",
                        i % 2000
                    ))),
                },
                Value {
                    value_data: Some(value::ValueData::StringValue(format!("r{}", i % 8))),
                },
                Value {
                    value_data: Some(value::ValueData::StringValue(format!("dc{}", i % 4))),
                },
                Value {
                    value_data: Some(value::ValueData::TimestampMillisecondValue(
                        1_700_000_000_000 + i as i64,
                    )),
                },
                Value {
                    value_data: Some(value::ValueData::F64Value(i as f64 * 0.7)),
                },
                Value {
                    value_data: Some(value::ValueData::F64Value(i as f64 * 0.3)),
                },
                Value {
                    value_data: Some(value::ValueData::F64Value(100.0 - i as f64 * 0.7)),
                },
            ],
        })
        .collect();
    Rows { schema, rows }
}

/// Builds a `WalEntry` of `num_mutations` mutations, `rows_per_mutation` rows
/// each, mirroring a region write batch.
fn build_wal_entry(num_mutations: usize, rows_per_mutation: usize) -> WalEntry {
    let mutations = (0..num_mutations)
        .map(|m| Mutation {
            op_type: OpType::Put as i32,
            sequence: 1 + (m * rows_per_mutation) as u64,
            rows: Some(build_rows(rows_per_mutation, m)),
            write_hint: None,
        })
        .collect();
    WalEntry {
        mutations,
        bulk_entries: vec![],
    }
}

fn bench_wal_encode(c: &mut Criterion) {
    let total_rows = 10_000;
    let entry = build_wal_entry(4, total_rows / 4);

    let mut group = c.benchmark_group("wal_encode");
    group.throughput(Throughput::Elements(total_rows as u64));

    group.bench_function("prost_encode_to_vec", |b| {
        b.iter(|| black_box(entry.encode_to_vec()));
    });

    group.bench_function("wal_entry_encoder", |b| {
        let mut encoder = WalEntryEncoder::new();
        b.iter(|| black_box(encoder.encode_to_vec(&entry)));
    });

    group.finish();
}

criterion_group!(benches, bench_wal_encode);
criterion_main!(benches);
