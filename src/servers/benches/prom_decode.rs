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
use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion};
use prost::Message;
use servers::prom_store::to_grpc_row_insert_requests;
use servers::proto::{PromSeriesProcessor, PromWriteRequest};

fn bench_decode_prom_request_without_strict_mode(c: &mut Criterion) {
    let mut d = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    d.push("benches");
    d.push("write_request.pb.data");

    let data = Bytes::from(std::fs::read(d).unwrap());

    let mut request = WriteRequest::default();
    let mut prom_request = PromWriteRequest::default();
    let is_strict_mode = false;
    let mut p = PromSeriesProcessor::default_processor();

    c.benchmark_group("decode")
        .measurement_time(Duration::from_secs(3))
        .bench_function("write_request", |b| {
            b.iter(|| {
                request.clear();
                let data = data.clone();
                request.merge(data).unwrap();
                to_grpc_row_insert_requests(&request).unwrap();
            });
        })
        .bench_function("prom_write_request", |b| {
            b.iter(|| {
                let data = data.clone();
                prom_request.merge(data, is_strict_mode, &mut p).unwrap();
                prom_request.as_row_insert_requests();
            });
        });
}

fn bench_decode_prom_request_with_strict_mode(c: &mut Criterion) {
    let mut d = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    d.push("benches");
    d.push("write_request.pb.data");

    let data = Bytes::from(std::fs::read(d).unwrap());

    let mut request = WriteRequest::default();
    let mut prom_request = PromWriteRequest::default();
    let is_strict_mode = true;
    let mut p = PromSeriesProcessor::default_processor();

    c.benchmark_group("decode")
        .measurement_time(Duration::from_secs(3))
        .bench_function("write_request", |b| {
            b.iter(|| {
                request.clear();
                let data = data.clone();
                request.merge(data).unwrap();
                to_grpc_row_insert_requests(&request).unwrap();
            });
        })
        .bench_function("prom_write_request", |b| {
            b.iter(|| {
                let data = data.clone();
                prom_request.merge(data, is_strict_mode, &mut p).unwrap();
                prom_request.as_row_insert_requests();
            });
        });
}

criterion_group!(
    benches,
    bench_decode_prom_request_without_strict_mode,
    bench_decode_prom_request_with_strict_mode
);
criterion_main!(benches);
