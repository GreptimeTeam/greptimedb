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

#[path = "otlp_metrics/fixtures.rs"]
mod fixtures;

use std::hint::black_box;
use std::time::Duration;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use servers::otlp::metrics::to_grpc_insert_requests;
use session::protocol_ctx::OtlpMetricCtx;

fn bench_conversion(c: &mut Criterion) {
    let fixtures = fixtures::fixtures();
    for (group_name, delta) in [
        ("otlp_conversion/common", false),
        ("otlp_conversion/delta", true),
    ] {
        let mut group = c.benchmark_group(group_name);
        for fixture in fixtures
            .iter()
            .filter(|fixture| fixture.name.starts_with("delta") == delta)
        {
            group.throughput(Throughput::Elements(fixture.points));
            for resource_info in [false, true] {
                let ctx = OtlpMetricCtx {
                    resource_info,
                    with_metric_engine: true,
                    ..Default::default()
                };
                group.bench_function(
                    BenchmarkId::new(
                        fixture.name,
                        if resource_info {
                            "descriptors_on"
                        } else {
                            "descriptors_off"
                        },
                    ),
                    |b| {
                        let output =
                            to_grpc_insert_requests(fixture.request.clone(), &mut ctx.clone())
                                .unwrap();
                        assert_eq!(
                            output.outcome.accepted_data_points,
                            fixture.points as i64 - fixture.rejected
                        );
                        assert_eq!(output.outcome.rejected_data_points, fixture.rejected);
                        assert_eq!(output.resource_info.is_some(), resource_info);
                        b.iter_batched(
                            || fixture.request.clone(),
                            |request| {
                                black_box(
                                    to_grpc_insert_requests(request, &mut ctx.clone()).unwrap(),
                                )
                            },
                            BatchSize::LargeInput,
                        );
                    },
                );
            }
        }
        group.finish();
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(20).warm_up_time(Duration::from_secs(1)).measurement_time(Duration::from_secs(3));
    targets = bench_conversion
}
criterion_main!(benches);
