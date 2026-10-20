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

//! Benchmarks compacting L0 files into one output through the engine.
//!
//! Every input file holds all series over its own time span, as consecutive
//! flushes do. Short series are where splitting the inputs by series hands the
//! SST writer one small batch per series and file.
//!
//! Run with:
//! ```sh
//! cargo bench -p mito2 --features test --bench bench_compaction_merge
//! ```

use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, Instant};

use api::v1::Rows;
use api::v1::region::{StrictWindow, compact_request};
use common_test_util::temp_dir::create_temp_dir;
use criterion::{Criterion, SamplingMode, Throughput, criterion_group, criterion_main};
use mito2::config::MitoConfig;
use mito2::engine::MitoEngine;
use mito2::test_util::{
    CreateRequestBuilder, TestEnv, build_rows_for_key, flush_region, rows_schema,
};
use store_api::region_engine::{RegionEngine, RegionRole};
use store_api::region_request::{
    PathType, RegionCompactRequest, RegionOpenRequest, RegionPutRequest, RegionRequest,
};
use store_api::storage::RegionId;

const REGION_ID: RegionId = RegionId::new(1, 1);
/// Series written by each put request.
const SERIES_PER_PUT: usize = 1000;
/// The inputs of every shape fit in one day, so each flush writes one file and
/// one compaction window covers all of them.
const WINDOW_SECONDS: i64 = 24 * 3600;

struct Shape {
    name: &'static str,
    num_files: usize,
    num_series: usize,
    rows_per_series: usize,
}

impl Shape {
    fn num_rows(&self) -> usize {
        self.num_files * self.num_series * self.rows_per_series
    }
}

const SHAPES: [Shape; 2] = [
    // Few enough series for scans to split the inputs, each far shorter than a batch.
    Shape {
        name: "10k_series_x_30_rows",
        num_files: 16,
        num_series: 10_000,
        rows_per_series: 30,
    },
    // Series long enough to fill most of a batch.
    Shape {
        name: "60_series_x_5000_rows",
        num_files: 16,
        num_series: 60,
        rows_per_series: 5_000,
    },
];

/// L0 files of a shape and the options to open the region holding them.
struct Inputs {
    env: TestEnv,
    table_dir: String,
    path_type: PathType,
    options: HashMap<String, String>,
}

impl Inputs {
    /// Opens a copy of the inputs in a new engine, so every compaction starts from the same files.
    async fn open_copy(&self) -> (TestEnv, MitoEngine) {
        let data_home = create_temp_dir("bench_compaction_merge");
        copy_dir(self.env.data_home(), data_home.path());
        let mut env = TestEnv::with_data_home(data_home).await;
        let engine = env.create_engine(engine_config()).await;
        let request = RegionOpenRequest {
            engine: String::new(),
            table_dir: self.table_dir.clone(),
            path_type: self.path_type,
            options: self.options.clone(),
            skip_wal_replay: false,
            checkpoint: None,
            requirements: Default::default(),
        };
        engine
            .handle_request(REGION_ID, RegionRequest::Open(request))
            .await
            .unwrap();
        engine
            .set_region_role(REGION_ID, RegionRole::Leader)
            .unwrap();
        (env, engine)
    }
}

fn engine_config() -> MitoConfig {
    MitoConfig {
        // Keeps automatic compaction from merging the inputs.
        min_compaction_interval: Duration::from_secs(3600),
        ..Default::default()
    }
}

/// Writes `shape.num_files` L0 files, each with every series over its own time span.
async fn write_inputs(shape: &Shape) -> Inputs {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(engine_config()).await;
    let request = CreateRequestBuilder::new().build();
    let schema = rows_schema(&request);
    let table_dir = request.table_dir.clone();
    let path_type = request.path_type;
    let options = request.options.clone();
    engine
        .handle_request(REGION_ID, RegionRequest::Create(request))
        .await
        .unwrap();

    for file in 0..shape.num_files {
        let start = file * shape.rows_per_series;
        let end = start + shape.rows_per_series;
        for first in (0..shape.num_series).step_by(SERIES_PER_PUT) {
            let last = (first + SERIES_PER_PUT).min(shape.num_series);
            let rows = (first..last)
                .flat_map(|series| build_rows_for_key(&format!("host-{series:05}"), start, end, 0))
                .collect();
            let request = RegionPutRequest {
                rows: Rows {
                    schema: schema.clone(),
                    rows,
                },
                // The rows only need to reach the SSTs.
                skip_wal: true,
                hint: None,
                partition_expr_version: None,
            };
            engine
                .handle_request(REGION_ID, RegionRequest::Put(request))
                .await
                .unwrap();
        }
        flush_region(&engine, REGION_ID, None).await;
    }
    let statistic = engine.get_region_statistic(REGION_ID).unwrap();
    assert_eq!(
        shape.num_files as u64, statistic.sst_num,
        "every flush must leave its own input file"
    );
    engine.stop().await.unwrap();

    Inputs {
        env,
        table_dir,
        path_type,
        options,
    }
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

fn compact_request() -> RegionRequest {
    RegionRequest::Compact(RegionCompactRequest {
        options: compact_request::Options::StrictWindow(StrictWindow {
            window_seconds: WINDOW_SECONDS,
        }),
        ..Default::default()
    })
}

fn bench_compaction_merge(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("compaction_merge");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(20));
    for shape in &SHAPES {
        let inputs = runtime.block_on(write_inputs(shape));
        group.throughput(Throughput::Elements(shape.num_rows() as u64));
        group.bench_function(shape.name, |b| {
            b.iter_custom(|iterations| {
                runtime.block_on(async {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iterations {
                        let (_env, engine) = inputs.open_copy().await;
                        let start = Instant::now();
                        engine
                            .handle_request(REGION_ID, compact_request())
                            .await
                            .unwrap();
                        elapsed += start.elapsed();
                        let statistic = engine.get_region_statistic(REGION_ID).unwrap();
                        assert_eq!(1, statistic.sst_num, "inputs must merge into one output");
                        engine.stop().await.unwrap();
                    }
                    elapsed
                })
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_compaction_merge);
criterion_main!(benches);
