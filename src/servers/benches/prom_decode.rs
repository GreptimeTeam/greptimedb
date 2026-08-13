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
use std::time::Duration;

use api::greptime_proto::io::prometheus::write::v2 as write_v2;
use api::prom_store::remote::{self as write_v1, WriteRequest};
use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main,
};
use prost::Message;
use servers::prom_remote_write::decode::{PromSeriesProcessor, PromWriteRequest};
use servers::prom_remote_write::v2::test_util as remote_write_v2;
use servers::prom_remote_write::validation::{PromValidationMode, validate_label_name};
use servers::prom_store::to_grpc_row_insert_requests;

fn load_fixture_v1_bytes() -> Vec<u8> {
    let mut d = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    d.push("benches");
    d.push("write_request.pb.data");
    std::fs::read(d).expect("read write_request.pb.data fixture")
}

/// Convert a PRW v1 `WriteRequest` into an equivalent PRW v2 `Request`.
///
/// Labels are interned into the v2 symbol table so payloads with repeated label
/// names/values exercise the main on-wire advantage of v2.
fn write_request_v1_to_v2(v1: &WriteRequest) -> write_v2::Request {
    let mut symbols = vec![String::new()];
    let mut symbol_index: HashMap<String, u32> = HashMap::new();
    symbol_index.insert(String::new(), 0);

    let mut intern = |value: &str| -> u32 {
        if let Some(&idx) = symbol_index.get(value) {
            return idx;
        }
        let idx = symbols.len() as u32;
        symbols.push(value.to_string());
        symbol_index.insert(value.to_string(), idx);
        idx
    };

    let timeseries = v1
        .timeseries
        .iter()
        .map(|series| {
            let mut labels: Vec<&write_v1::Label> = series.labels.iter().collect();
            // PRW v2 requires lexicographically sorted label pairs.
            labels.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.value.cmp(&b.value)));

            let mut labels_refs = Vec::with_capacity(labels.len() * 2);
            for label in labels {
                labels_refs.push(intern(&label.name));
                labels_refs.push(intern(&label.value));
            }

            write_v2::TimeSeries {
                labels_refs,
                samples: series
                    .samples
                    .iter()
                    .map(|sample| write_v2::Sample {
                        value: sample.value,
                        timestamp: sample.timestamp,
                        start_timestamp: 0,
                    })
                    .collect(),
                histograms: Vec::new(),
                exemplars: Vec::new(),
                metadata: None,
            }
        })
        .collect();

    write_v2::Request {
        symbols,
        timeseries,
    }
}

/// Synthetic sample workload with controllable label reuse.
///
/// `shared_labels` keeps common label names/values across series (v2-friendly).
/// Each series also gets a unique `series_id` label so cardinality stays high.
fn generate_sample_workload(
    series_count: usize,
    samples_per_series: usize,
    shared_label_count: usize,
) -> (WriteRequest, write_v2::Request) {
    let shared_labels: Vec<write_v1::Label> = (0..shared_label_count)
        .map(|i| write_v1::Label {
            name: format!("label_{i}"),
            value: format!("value_{}", i % 16),
        })
        .collect();

    let timeseries = (0..series_count)
        .map(|series_idx| {
            let mut labels = Vec::with_capacity(shared_label_count + 2);
            labels.push(write_v1::Label {
                name: "__name__".to_string(),
                value: format!("metric_{}", series_idx % 32),
            });
            labels.extend(shared_labels.iter().cloned());
            labels.push(write_v1::Label {
                name: "series_id".to_string(),
                value: series_idx.to_string(),
            });

            write_v1::TimeSeries {
                labels,
                samples: (0..samples_per_series)
                    .map(|sample_idx| write_v1::Sample {
                        value: (series_idx * 1000 + sample_idx) as f64,
                        timestamp: 1_700_000_000_000 + sample_idx as i64 * 1_000,
                    })
                    .collect(),
                exemplars: Vec::new(),
                histograms: Vec::new(),
            }
        })
        .collect();

    let v1 = WriteRequest {
        timeseries,
        metadata: Vec::new(),
    };
    let v2 = write_request_v1_to_v2(&v1);
    (v1, v2)
}

fn bench_decode_prom_request(c: &mut Criterion) {
    let data = load_fixture_v1_bytes();

    let mut group = c.benchmark_group("decode_prom_request");
    group.measurement_time(Duration::from_secs(3));

    // Benchmark standard WriteRequest decoding as a baseline
    let mut request = WriteRequest::default();
    group.bench_function("standard_write_request", |b| {
        b.iter(|| {
            request.merge(data.as_slice()).unwrap();
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

/// Compare Prometheus remote-write v1 vs v2 decoding efficiency.
///
/// Two layers are measured on equivalent logical payloads:
/// 1. protobuf decode only (`WriteRequest` / v2 `Request`)
/// 2. full Greptime path to row-insert requests
///
/// Workloads:
/// - `fixture`: existing `write_request.pb.data` converted v1 -> v2
/// - `synthetic_*`: generated series with shared labels (symbol-table friendly)
#[allow(clippy::print_stderr)]
fn bench_prom_v1_vs_v2_decode(c: &mut Criterion) {
    let fixture_v1_bytes = load_fixture_v1_bytes();
    let fixture_v1 = WriteRequest::decode(fixture_v1_bytes.as_slice()).unwrap();
    let fixture_v2 = write_request_v1_to_v2(&fixture_v1);
    let fixture_v2_bytes = fixture_v2.encode_to_vec();

    let synthetic = [
        (
            "synthetic_1k_series_1_sample",
            generate_sample_workload(1_000, 1, 8),
        ),
        (
            "synthetic_1k_series_10_samples",
            generate_sample_workload(1_000, 10, 8),
        ),
        (
            "synthetic_5k_series_1_sample",
            generate_sample_workload(5_000, 1, 8),
        ),
    ];

    let mut workloads: Vec<(&str, Vec<u8>, Vec<u8>)> =
        vec![("fixture", fixture_v1_bytes, fixture_v2_bytes)];
    for (name, (v1, v2)) in &synthetic {
        workloads.push((*name, v1.encode_to_vec(), v2.encode_to_vec()));
    }

    eprintln!("\nprom remote-write v1 vs v2 payload sizes:");
    for (name, v1_bytes, v2_bytes) in &workloads {
        let ratio = v2_bytes.len() as f64 / v1_bytes.len() as f64;
        eprintln!(
            "  {name}: v1={} B, v2={} B, v2/v1={ratio:.3}",
            v1_bytes.len(),
            v2_bytes.len()
        );
    }

    // --- protobuf decode only ---
    {
        let mut group = c.benchmark_group("prom_rw_protobuf_decode");
        group.measurement_time(Duration::from_secs(3));

        for (name, v1_bytes, v2_bytes) in &workloads {
            group.throughput(Throughput::Bytes(v1_bytes.len() as u64));
            group.bench_with_input(BenchmarkId::new("v1", name), v1_bytes, |b, bytes| {
                b.iter(|| {
                    black_box(WriteRequest::decode(black_box(bytes.as_slice())).unwrap());
                });
            });

            group.throughput(Throughput::Bytes(v2_bytes.len() as u64));
            group.bench_with_input(BenchmarkId::new("v2", name), v2_bytes, |b, bytes| {
                b.iter(|| {
                    black_box(write_v2::Request::decode(black_box(bytes.as_slice())).unwrap());
                });
            });
        }

        group.finish();
    }

    // --- full path: protobuf -> Greptime row inserts ---
    {
        let mut group = c.benchmark_group("prom_rw_decode_to_rows");
        group.sample_size(50);
        group.measurement_time(Duration::from_secs(5));

        for (name, v1_bytes, v2_bytes) in &workloads {
            group.throughput(Throughput::Bytes(v1_bytes.len() as u64));
            group.bench_with_input(BenchmarkId::new("v1_custom", name), v1_bytes, |b, bytes| {
                let mut prom_request = PromWriteRequest::default();
                let mut processor = PromSeriesProcessor::default_processor();
                b.iter_batched(
                    || bytes.clone(),
                    |bytes| {
                        prom_request
                            .decode(black_box(bytes), PromValidationMode::Strict, &mut processor)
                            .unwrap();
                        let rows = prom_request.as_row_insert_requests();
                        black_box(&rows);
                    },
                    BatchSize::LargeInput,
                );
            });

            // Baseline: stock prost WriteRequest + existing converter.
            group.throughput(Throughput::Bytes(v1_bytes.len() as u64));
            group.bench_with_input(BenchmarkId::new("v1_prost", name), v1_bytes, |b, bytes| {
                b.iter(|| {
                    let request = WriteRequest::decode(black_box(bytes.as_slice())).unwrap();
                    black_box(to_grpc_row_insert_requests(&request).unwrap());
                });
            });

            group.throughput(Throughput::Bytes(v2_bytes.len() as u64));
            group.bench_with_input(BenchmarkId::new("v2_manual", name), v2_bytes, |b, bytes| {
                b.iter(|| {
                    black_box(
                        remote_write_v2::decode_uncompressed_write_requests(
                            black_box(bytes.as_slice()),
                            true,
                        )
                        .unwrap(),
                    );
                });
            });
        }

        group.finish();
    }
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
    bench_prom_v1_vs_v2_decode,
    bench_label_name_validation,
    bench_utf8_validation
);
criterion_main!(benches);
