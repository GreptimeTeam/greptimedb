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

use std::hint::black_box;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion};
use datatypes::prelude::ValueRef;
use mito_codec::row_converter::sparse::{RESERVED_COLUMN_ID_TABLE_ID, RESERVED_COLUMN_ID_TSID};
use mito_codec::row_converter::SparsePrimaryKeyCodec;

fn encode_sparse(c: &mut Criterion) {
    let num_tags = 10;
    let codec = SparsePrimaryKeyCodec::from_columns(0..num_tags);

    let dummy_table_id = 1024;
    let dummy_ts_id = 42;
    let internal_columns = [
        (
            RESERVED_COLUMN_ID_TABLE_ID,
            ValueRef::UInt32(dummy_table_id),
        ),
        (RESERVED_COLUMN_ID_TSID, ValueRef::UInt64(dummy_ts_id)),
    ];

    let tags: Vec<_> = (0..num_tags)
        .map(|idx| {
            let tag_value = idx.to_string().repeat(10);
            (idx, tag_value)
        })
        .collect();

    let mut group = c.benchmark_group("encode");
    group.bench_function("encode_to_vec", |b| {
        b.iter(|| {
            let mut buffer = Vec::new();
            codec
                .encode_to_vec(internal_columns.into_iter(), &mut buffer)
                .unwrap();
            codec
                .encode_to_vec(
                    tags.iter()
                        .map(|(col_id, tag_value)| (*col_id, ValueRef::String(tag_value))),
                    &mut buffer,
                )
                .unwrap();
            black_box(buffer);
        });
    });

    let tags: Vec<_> = tags
        .into_iter()
        .map(|(col_id, tag_value)| (col_id, Bytes::copy_from_slice(tag_value.as_bytes())))
        .collect();

    group.bench_function("encode_by_raw", |b| {
        b.iter(|| {
            let mut buffer_by_raw_encoding = Vec::new();
            codec
                .encode_internal(dummy_table_id, dummy_ts_id, &mut buffer_by_raw_encoding)
                .unwrap();
            codec
                .encode_raw_tag_value(
                    tags.iter().map(|(c, b)| (*c, b)),
                    &mut buffer_by_raw_encoding,
                )
                .unwrap();
            black_box(buffer_by_raw_encoding);
        });
    });
    group.finish();
}

criterion_group!(benches, encode_sparse);
criterion_main!(benches);
