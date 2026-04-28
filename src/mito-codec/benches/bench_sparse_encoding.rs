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
use criterion::measurement::WallTime;
use criterion::{BenchmarkGroup, Criterion, criterion_group, criterion_main};
use datatypes::prelude::ValueRef;
use datatypes::value::Value;
use mito_codec::row_converter::sparse::{RESERVED_COLUMN_ID_TABLE_ID, RESERVED_COLUMN_ID_TSID};
use mito_codec::row_converter::{SparseOffsetsCache, SparsePrimaryKeyCodec, SparseValues};
use store_api::storage::ColumnId;

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

/// PoC: lookup-strategy comparison for `SparseValues`-shaped collections.
///
/// `SparseValues` is currently a `HashMap<ColumnId, Value>`. The companion
/// `SparseOffsetsCache` switched to a small-vec fast path with
/// `SPARSE_OFFSETS_INLINE_CAP = 32` entries, but its payload is `usize` —
/// `Value` is a much fatter enum, so the linear-scan-vs-HashMap crossover
/// will likely land at a different inline cap. The benches below measure
/// both the crossover (`bench_sparse_values_threshold`) and the workload
/// impact at realistic primary-key widths (`bench_sparse_values_lookup`).
fn sample_value(idx: u32) -> Value {
    Value::String(idx.to_string().repeat(10).into())
}

/// Common lookup surface for the variants under test.
trait ValueLookup {
    fn lookup_get(&self, col: ColumnId) -> Option<&Value>;
    fn lookup_or_null(&self, col: ColumnId) -> &Value;
}

impl ValueLookup for SparseValues {
    fn lookup_get(&self, col: ColumnId) -> Option<&Value> {
        self.get(&col)
    }
    fn lookup_or_null(&self, col: ColumnId) -> &Value {
        self.get_or_null(col)
    }
}

impl ValueLookup for Vec<(ColumnId, Value)> {
    fn lookup_get(&self, col: ColumnId) -> Option<&Value> {
        for entry in self {
            if entry.0 == col {
                return Some(&entry.1);
            }
        }
        None
    }
    fn lookup_or_null(&self, col: ColumnId) -> &Value {
        for entry in self {
            if entry.0 == col {
                return &entry.1;
            }
        }
        &Value::Null
    }
}

/// Inline-Vec + HashMap-overflow mirror of `SparseOffsetsCache`'s layout,
/// parameterized so the bench can sweep candidate inline caps.
struct HybridValues<const CAP: usize> {
    inline: Vec<(ColumnId, Value)>,
    overflow: HashMap<ColumnId, Value>,
}

impl<const CAP: usize> HybridValues<CAP> {
    fn new() -> Self {
        Self {
            inline: Vec::with_capacity(CAP),
            overflow: HashMap::new(),
        }
    }

    fn insert(&mut self, col: ColumnId, value: Value) {
        if self.inline.len() < CAP {
            self.inline.push((col, value));
        } else {
            self.overflow.insert(col, value);
        }
    }
}

impl<const CAP: usize> ValueLookup for HybridValues<CAP> {
    fn lookup_get(&self, col: ColumnId) -> Option<&Value> {
        for entry in &self.inline {
            if entry.0 == col {
                return Some(&entry.1);
            }
        }
        if self.overflow.is_empty() {
            return None;
        }
        self.overflow.get(&col)
    }
    fn lookup_or_null(&self, col: ColumnId) -> &Value {
        self.lookup_get(col).unwrap_or(&Value::Null)
    }
}

fn build_sparse_values(pairs: &[(ColumnId, Value)]) -> SparseValues {
    let mut sv = SparseValues::new(HashMap::with_capacity(pairs.len()));
    for (c, v) in pairs {
        sv.insert(*c, v.clone());
    }
    sv
}

fn build_vec_values(pairs: &[(ColumnId, Value)]) -> Vec<(ColumnId, Value)> {
    pairs.to_vec()
}

fn build_hybrid_values<const CAP: usize>(
    pairs: &[(ColumnId, Value)],
) -> HybridValues<CAP> {
    let mut h = HybridValues::<CAP>::new();
    for (c, v) in pairs {
        h.insert(*c, v.clone());
    }
    h
}

/// Runs the five workloads (lookup_first/last/miss, iter_all, build_then_iter)
/// for one variant inside a `sparse_values/{size}` group.
fn run_lookup_workloads<V, B>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    name: &str,
    column_ids: &[ColumnId],
    last_id: ColumnId,
    missing_id: ColumnId,
    build: B,
) where
    V: ValueLookup,
    B: Fn() -> V,
{
    let built = build();

    group.bench_function(format!("{name}/lookup_first"), |b| {
        b.iter(|| {
            let target = black_box(0 as ColumnId);
            black_box(built.lookup_or_null(target))
        });
    });

    group.bench_function(format!("{name}/lookup_last"), |b| {
        b.iter(|| {
            let target = black_box(last_id);
            black_box(built.lookup_or_null(target))
        });
    });

    group.bench_function(format!("{name}/lookup_miss"), |b| {
        b.iter(|| {
            let target = black_box(missing_id);
            black_box(built.lookup_get(target))
        });
    });

    group.bench_function(format!("{name}/iter_all"), |b| {
        b.iter(|| {
            for col in column_ids {
                black_box(built.lookup_or_null(*col));
            }
        });
    });

    group.bench_function(format!("{name}/build_then_iter"), |b| {
        b.iter(|| {
            let c = build();
            for col in column_ids {
                black_box(c.lookup_or_null(*col));
            }
            c
        });
    });
}

/// Sweeps collection sizes to find the linear-scan-vs-HashMap crossover for
/// `(ColumnId, Value)` entries — i.e. the natural inline cap for a
/// `SparseValues`-shaped hybrid.
fn bench_sparse_values_threshold(c: &mut Criterion) {
    for size in [2usize, 4, 8, 12, 16, 20, 24, 32, 48, 64] {
        let vec: Vec<(ColumnId, Value)> = (0..size as u32)
            .map(|i| (i as ColumnId, sample_value(i)))
            .collect();
        let map: HashMap<ColumnId, Value> = vec.iter().cloned().collect();
        let last_id = (size - 1) as ColumnId;
        let missing_id = size as ColumnId;

        let mut group = c.benchmark_group(format!("sparse_values_threshold/{size}"));

        group.bench_function("vec_first", |b| {
            b.iter(|| {
                let target = black_box(0 as ColumnId);
                for entry in &vec {
                    if entry.0 == target {
                        return black_box(Some(&entry.1));
                    }
                }
                black_box(None)
            });
        });

        group.bench_function("vec_last", |b| {
            b.iter(|| {
                let target = black_box(last_id);
                for entry in &vec {
                    if entry.0 == target {
                        return black_box(Some(&entry.1));
                    }
                }
                black_box(None)
            });
        });

        group.bench_function("vec_miss", |b| {
            b.iter(|| {
                let target = black_box(missing_id);
                for entry in &vec {
                    if entry.0 == target {
                        return black_box(Some(&entry.1));
                    }
                }
                black_box(None)
            });
        });

        group.bench_function("map_hit_last", |b| {
            b.iter(|| black_box(map.get(&black_box(last_id))));
        });

        group.bench_function("map_miss", |b| {
            b.iter(|| black_box(map.get(&black_box(missing_id))));
        });

        group.finish();
    }
}

/// Workload comparison at realistic primary-key widths. Compares the current
/// `HashMap`-backed `SparseValues` against a pure `Vec` linear scan and two
/// hybrid candidates. The hybrid caps are picked to bracket the likely
/// crossover (smaller than the offsets-cache's 32, since `Value` is fatter
/// than `usize`); refine after reading `bench_sparse_values_threshold`.
fn bench_sparse_values_lookup(c: &mut Criterion) {
    for size in [2usize, 4, 8, 16, 32, 50] {
        let pairs: Vec<(ColumnId, Value)> = (0..size as u32)
            .map(|i| (i as ColumnId, sample_value(i)))
            .collect();
        let column_ids: Vec<ColumnId> = pairs.iter().map(|(c, _)| *c).collect();
        let last_id = (size - 1) as ColumnId;
        let missing_id = size as ColumnId;

        let mut group = c.benchmark_group(format!("sparse_values/{size}"));

        run_lookup_workloads(
            &mut group,
            "hashmap",
            &column_ids,
            last_id,
            missing_id,
            || build_sparse_values(&pairs),
        );

        run_lookup_workloads(
            &mut group,
            "vec",
            &column_ids,
            last_id,
            missing_id,
            || build_vec_values(&pairs),
        );

        run_lookup_workloads(
            &mut group,
            "hybrid_8",
            &column_ids,
            last_id,
            missing_id,
            || build_hybrid_values::<8>(&pairs),
        );

        run_lookup_workloads(
            &mut group,
            "hybrid_32",
            &column_ids,
            last_id,
            missing_id,
            || build_hybrid_values::<32>(&pairs),
        );

        group.finish();
    }
}

criterion_group!(
    benches,
    encode_sparse,
    bench_has_column,
    bench_inline_threshold,
    bench_sparse_values_threshold,
    bench_sparse_values_lookup
);
criterion_main!(benches);
