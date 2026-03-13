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

use std::collections::BTreeMap;
use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use servers::error::Result;
use servers::http::loki::parse_loki_labels;

// cargo bench loki_labels

fn json5_parse(input: &str) -> Result<BTreeMap<String, String>> {
    let input = input.replace("=", ":");
    let result: BTreeMap<String, String> = json5::from_str(&input).unwrap();
    Ok(result)
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("loki_labels");
    let input = r#"{job="foobar", cluster="foo-central1", namespace="bar", container_name="buzz"}"#;

    group.bench_function("json5", |b| b.iter(|| json5_parse(black_box(input))));
    group.bench_function("hand_parse", |b| {
        b.iter(|| parse_loki_labels(black_box(input)))
    });
    group.finish(); // Important to call finish() on the group
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
