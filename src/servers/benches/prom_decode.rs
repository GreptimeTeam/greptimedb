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
use prost::Message;
use servers::prom_store::to_grpc_row_insert_requests;
use servers::proto::PromWriteRequest;

fn bench_decode_prom_request(c: &mut Criterion) {
    let data = base64::engine::general_purpose::STANDARD
        .decode(std::fs::read("/tmp/prom-data/1709380539690445357").unwrap())
        .unwrap();
    let mut prom_request = PromWriteRequest::default();
    let mut request = WriteRequest::default();

    let data = Bytes::from(data);
    c.benchmark_group("decode_prom")
        .measurement_time(Duration::from_secs(10))
        .bench_function("decode_write_request", |b| {
            b.iter(|| {
                let data = data.clone();
                request.clear();
                request.merge(data).unwrap();
                let _ = to_grpc_row_insert_requests(&request).unwrap();
            });
        })
        .bench_function("decode_prom_write_request", |b| {
            b.iter(|| {
                let data = data.clone();
                prom_request.clear();
                prom_request.merge(data).unwrap();
                // todo: convert to RowInsertRequests.
            });
        });
}

criterion_group!(benches, bench_decode_prom_request);
criterion_main!(benches);
