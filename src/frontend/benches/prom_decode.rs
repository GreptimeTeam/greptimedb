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

use std::time::Duration;

use api::prom_store::remote::WriteRequest;
use base64::Engine;
use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion};
use frontend::proto::PromWriteRequest;
use prost::Message;

fn bench_decode_prom_request(c: &mut Criterion) {
    let data = base64::engine::general_purpose::STANDARD
        .decode(std::fs::read("/tmp/prom-data/1709380539690445357").unwrap())
        .unwrap();
    let mut request = PromWriteRequest::default();

    // let mut request = WriteRequest::default();
    // let mut request = greptime_proto_bytes::prometheus::remote::WriteRequest::default();

    let data = Bytes::from(data);
    c.benchmark_group("decode_prom")
        .measurement_time(Duration::from_secs(1))
        .bench_function("merge", |b| {
            b.iter(|| {
                let data = data.clone();
                request.clear();
                request.merge(data).unwrap();
            });
        });
}

criterion_group!(benches, bench_decode_prom_request);
criterion_main!(benches);
