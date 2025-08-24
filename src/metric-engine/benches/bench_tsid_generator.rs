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

use api::prom_store::remote::WriteRequest;
use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion};
use metric_engine::row_modifier::{FxHashTsidGenerator, TsidGenerator};
use prost::Message;

type Mur3HashGenerator = TsidGenerator<mur3::Hasher128>;

const TSID_HASH_SEED: u32 = 846793005;

#[allow(clippy::type_complexity)]
fn prepare_labels() -> (Vec<(String, String)>, Vec<(Bytes, Bytes)>) {
    let mut d = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    d.push("..");
    d.push("servers");
    d.push("benches");
    d.push("write_request.pb.data");
    let data = std::fs::read(d).unwrap();
    let request = WriteRequest::decode(&*data).unwrap();
    let bytes_labels = request
        .timeseries
        .iter()
        .flat_map(|t| {
            t.labels.iter().map(|c| {
                (
                    Bytes::copy_from_slice(c.name.as_bytes()),
                    Bytes::copy_from_slice(c.value.as_bytes()),
                )
            })
        })
        .collect::<Vec<_>>();
    let string_labels = request
        .timeseries
        .into_iter()
        .flat_map(|t| t.labels.into_iter().map(|c| (c.name, c.value)))
        .collect::<Vec<_>>();

    (string_labels, bytes_labels)
}

fn encode_ts_id(c: &mut Criterion) {
    let (strings, bytes) = prepare_labels();

    let mut group = c.benchmark_group("encode_ts_id");
    group.bench_function("mur3-string", |b| {
        b.iter(|| {
            let mut generator =
                Mur3HashGenerator::with_hasher(mur3::Hasher128::with_seed(TSID_HASH_SEED));
            for (k, v) in &strings {
                generator.write_label(k, v);
            }
            black_box(generator.finish());
        });
    });

    group.bench_function("mur3-bytes", |b| {
        b.iter(|| {
            let mut generator =
                Mur3HashGenerator::with_hasher(mur3::Hasher128::with_seed(TSID_HASH_SEED));
            for (k, v) in &bytes {
                generator.write_label_bytes(k, v);
            }
            black_box(generator.finish());
        });
    });

    group.bench_function("fxhash-string", |b| {
        b.iter(|| {
            let mut generator = FxHashTsidGenerator::default();
            for (k, v) in &strings {
                generator.write_label(k, v);
            }
            black_box(generator.finish());
        });
    });

    group.bench_function("fxhash-bytes", |b| {
        b.iter(|| {
            let mut generator = FxHashTsidGenerator::default();
            for (k, v) in &bytes {
                generator.write_label_bytes(k, v);
            }
            black_box(generator.finish());
        });
    });

    group.finish();
}

criterion_group!(benches, encode_ts_id);
criterion_main!(benches);
