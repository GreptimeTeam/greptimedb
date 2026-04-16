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
use std::hint::black_box;

use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use datatypes::prelude::ValueRef;
use mito_codec::row_converter::sparse::{RESERVED_COLUMN_ID_TABLE_ID, RESERVED_COLUMN_ID_TSID};
use mito_codec::row_converter::{SparseOffsetsCache, SparsePrimaryKeyCodec};

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
                .encode_to_vec(internal_columns.clone().into_iter(), &mut buffer)
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
                    tags.iter().map(|(c, b)| (*c, &b[..])),
                    &mut buffer_by_raw_encoding,
                )
                .unwrap();
            black_box(buffer_by_raw_encoding);
        });
    });
    group.finish();
}

/// Encodes a primary key with the given number of tags.
fn encode_pk(num_tags: u32) -> (SparsePrimaryKeyCodec, Vec<u8>) {
    // Use schemaless() so all columns (including reserved ones) are recognized during parsing.
    let codec = SparsePrimaryKeyCodec::schemaless();
    let dummy_table_id = 1024u32;
    let dummy_tsid = 42u64;

    let tags: Vec<_> = (0..num_tags)
        .map(|idx| {
            let tag_value = idx.to_string().repeat(10);
            (idx, Bytes::copy_from_slice(tag_value.as_bytes()))
        })
        .collect();

    let mut buffer = Vec::new();
    codec
        .encode_internal(dummy_table_id, dummy_tsid, &mut buffer)
        .unwrap();
    codec
        .encode_raw_tag_value(tags.iter().map(|(c, b)| (*c, &b[..])), &mut buffer)
        .unwrap();

    (codec, buffer)
}

fn bench_has_column(c: &mut Criterion) {
    for num_tags in [5, 10, 50, 100] {
        let (codec, pk) = encode_pk(num_tags);
        let mut group = c.benchmark_group(format!("has_column/{num_tags}_tags"));

        group.bench_function("table_id", |b| {
            b.iter(|| {
                let mut offsets_map = SparseOffsetsCache::new();
                black_box(codec.has_column(&pk, &mut offsets_map, RESERVED_COLUMN_ID_TABLE_ID));
            });
        });

        group.bench_function("tsid", |b| {
            b.iter(|| {
                let mut offsets_map = SparseOffsetsCache::new();
                black_box(codec.has_column(&pk, &mut offsets_map, RESERVED_COLUMN_ID_TSID));
            });
        });

        group.bench_function("first_tag", |b| {
            b.iter(|| {
                let mut offsets_map = SparseOffsetsCache::new();
                black_box(codec.has_column(&pk, &mut offsets_map, 0));
            });
        });

        group.bench_function("middle_tag", |b| {
            b.iter(|| {
                let mut offsets_map = SparseOffsetsCache::new();
                black_box(codec.has_column(&pk, &mut offsets_map, num_tags / 2));
            });
        });

        group.bench_function("last_tag", |b| {
            b.iter(|| {
                let mut offsets_map = SparseOffsetsCache::new();
                black_box(codec.has_column(&pk, &mut offsets_map, num_tags - 1));
            });
        });

        group.finish();
    }
}

/// Benchmarks Vec linear scan vs HashMap lookup at various collection sizes
/// to find the optimal `SPARSE_OFFSETS_INLINE_CAP` threshold.
fn bench_inline_threshold(c: &mut Criterion) {
    for size in [4, 8, 12, 16, 20, 24, 32, 48, 64] {
        let vec: Vec<(u32, usize)> = (0..size).map(|i| (i as u32, i * 8)).collect();
        let map: HashMap<u32, usize> = vec.iter().copied().collect();

        let last_id = (size - 1) as u32;
        let missing_id = size as u32;

        let mut group = c.benchmark_group(format!("inline_threshold/{size}"));

        // Vec: best case (first element)
        group.bench_function("vec_first", |b| {
            b.iter(|| {
                let target = black_box(0u32);
                for entry in &vec {
                    if entry.0 == target {
                        return black_box(Some(entry.1));
                    }
                }
                black_box(None)
            });
        });

        // Vec: worst case (last element)
        group.bench_function("vec_last", |b| {
            b.iter(|| {
                let target = black_box(last_id);
                for entry in &vec {
                    if entry.0 == target {
                        return black_box(Some(entry.1));
                    }
                }
                black_box(None)
            });
        });

        // Vec: miss
        group.bench_function("vec_miss", |b| {
            b.iter(|| {
                let target = black_box(missing_id);
                for entry in &vec {
                    if entry.0 == target {
                        return black_box(Some(entry.1));
                    }
                }
                black_box(None)
            });
        });

        // HashMap: hit (last element)
        group.bench_function("map_hit", |b| {
            b.iter(|| black_box(map.get(&black_box(last_id)).copied()));
        });

        // HashMap: miss
        group.bench_function("map_miss", |b| {
            b.iter(|| black_box(map.get(&black_box(missing_id)).copied()));
        });

        group.finish();
    }
}

criterion_group!(benches, encode_sparse, bench_has_column, bench_inline_threshold);
criterion_main!(benches);
