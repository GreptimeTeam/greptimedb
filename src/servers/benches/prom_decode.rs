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
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use prost::Message;
use servers::http::{PromValidationMode, validate_label_name};
use servers::prom_store::to_grpc_row_insert_requests;
use servers::proto::{PromSeriesProcessor, PromWriteRequest};

fn bench_decode_prom_request(c: &mut Criterion) {
    let mut d = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    d.push("benches");
    d.push("write_request.pb.data");

    let data = Bytes::from(std::fs::read(d).unwrap());

    let mut group = c.benchmark_group("decode_prom_request");
    group.measurement_time(Duration::from_secs(3));

    // Benchmark standard WriteRequest decoding as a baseline
    let mut request = WriteRequest::default();
    group.bench_function("standard_write_request", |b| {
        b.iter(|| {
            let data = data.clone();
            request.merge(data).unwrap();
            to_grpc_row_insert_requests(&request).unwrap();
        });
    });

    // Benchmark each validation mode
    for mode in [
        PromValidationMode::Strict,
        PromValidationMode::Lossy,
        PromValidationMode::Unchecked,
    ] {
        let mut prom_request = PromWriteRequest::default();
        let mut p = PromSeriesProcessor::default_processor();
        group.bench_with_input(
            BenchmarkId::new("validation_mode", format!("{:?}", mode)),
            &mode,
            |b, &mode| {
                b.iter(|| {
                    let data = data.clone();
                    prom_request.decode(data, mode, &mut p).unwrap();
                    prom_request.as_row_insert_requests();
                });
            },
        );
    }

    group.finish();
}

/// Benchmark comparing UTF-8 string validation (`decode_string`) vs
/// direct byte-level Prometheus label name validation (`decode_label_name`).
fn bench_label_name_validation(c: &mut Criterion) {
    let mut group = c.benchmark_group("label_name_validation");
    group.measurement_time(Duration::from_secs(3));

    // Test inputs: typical Prometheus label names of varying lengths.
    let test_names: Vec<(&str, &[u8])> = vec![
        ("short", b"__name__"),
        ("medium", b"http_request_duration_seconds"),
        (
            "long",
            b"very_long_label_name_that_might_appear_in_a_real_prometheus_metric_configuration",
        ),
    ];

    let strict = PromValidationMode::Strict;

    for (label, name_bytes) in &test_names {
        // Benchmark decode_string (UTF-8 validation only)
        group.bench_with_input(
            BenchmarkId::new("decode_string", label),
            name_bytes,
            |b, bytes| {
                b.iter(|| {
                    black_box(strict.decode_string(black_box(bytes)).unwrap());
                });
            },
        );

        // Benchmark decode_label_name (byte-level ASCII check + unchecked conversion)
        group.bench_with_input(
            BenchmarkId::new("decode_label_name", label),
            name_bytes,
            |b, bytes| {
                b.iter(|| black_box(strict.decode_label_name(black_box(bytes)).unwrap()));
            },
        );

        // Benchmark is_valid_prom_label_name_bytes alone (byte check only, no String allocation)
        group.bench_with_input(
            BenchmarkId::new("is_valid_prom_label_name_bytes", label),
            name_bytes,
            |b, bytes| {
                b.iter(|| {
                    black_box(validate_label_name(black_box(bytes)));
                });
            },
        );
    }

    group.finish();
}

/// Benchmark comparing `std::str::from_utf8` vs `simdutf8::basic::from_utf8`
/// across varying input data lengths.
fn bench_utf8_validation(c: &mut Criterion) {
    let mut group = c.benchmark_group("utf8_validation");
    group.measurement_time(Duration::from_secs(3));

    // Generate valid ASCII/UTF-8 byte slices of varying lengths.
    // Uses a repeating pattern of typical label characters.
    let pattern = b"http_request_duration_seconds_total_bucket";
    let lengths: Vec<usize> = vec![8, 32, 64, 128, 256, 512, 1024, 4096, 16384, 65536];

    for &len in &lengths {
        let data: Vec<u8> = pattern.iter().copied().cycle().take(len).collect();

        group.bench_with_input(BenchmarkId::new("std_from_utf8", len), &data, |b, data| {
            b.iter(|| {
                black_box(std::str::from_utf8(black_box(data)).unwrap());
            });
        });

        group.bench_with_input(
            BenchmarkId::new("simdutf8_basic_from_utf8", len),
            &data,
            |b, data| {
                b.iter(|| {
                    black_box(simdutf8::basic::from_utf8(black_box(data)).unwrap());
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_decode_prom_request,
    bench_label_name_validation,
    bench_utf8_validation
);
criterion_main!(benches);
