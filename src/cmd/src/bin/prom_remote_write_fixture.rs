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

#![allow(clippy::print_stdout)]

use std::time::{Duration, Instant};

use api::prom_store::remote::{Label, Sample, TimeSeries, WriteRequest};
use clap::Parser;
use prost::Message;
use serde_json::json;
use servers::prom_store::snappy_compress;

#[derive(Debug, Parser)]
#[command(about = "Generate a deterministic Prometheus remote-write fixture")]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:4000/v1/prometheus/write")]
    endpoint: String,
    #[arg(long, default_value = "public")]
    database: String,
    #[arg(long)]
    metric: String,
    #[arg(long, default_value = "greptime_physical_table")]
    physical_table: String,
    #[arg(long, default_value_t = 8)]
    series_count: u64,
    #[arg(long, default_value_t = 30)]
    samples_per_series: u64,
    #[arg(long, default_value_t = 1_704_067_200_000)]
    start_unix_millis: i64,
    #[arg(long, default_value_t = 15_000)]
    step_millis: i64,
    #[arg(long, alias = "batch-size", default_value_t = 8)]
    chunk_series_count: u64,
    #[arg(long, default_value_t = 60)]
    timeout_seconds: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let started = Instant::now();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(args.timeout_seconds))
        .build()?;
    let mut batches = 0_u64;
    let mut rows = 0_u64;
    let mut http_statuses = Vec::new();

    let chunk = args.chunk_series_count.max(1);
    for first in (0..args.series_count).step_by(chunk as usize) {
        let last = (first + chunk).min(args.series_count);
        let timeseries = (first..last)
            .map(|series_idx| make_series(&args, series_idx))
            .collect::<Vec<_>>();
        rows += timeseries
            .iter()
            .map(|ts| ts.samples.len() as u64)
            .sum::<u64>();
        let body = snappy_compress(
            &WriteRequest {
                timeseries,
                ..Default::default()
            }
            .encode_to_vec(),
        )?;
        let status = client
            .post(&args.endpoint)
            .header("Content-Encoding", "snappy")
            .body(body)
            .send()
            .await?
            .status();
        http_statuses.push(status.as_u16());
        if !status.is_success() {
            return Err(format!("remote-write failed with status {status}").into());
        }
        batches += 1;
    }

    println!(
        "{}",
        json!({
            "status": "ok",
            "endpoint": args.endpoint,
            "database": args.database,
            "metric": args.metric,
            "physical_table": args.physical_table,
            "series_count": args.series_count,
            "samples_per_series": args.samples_per_series,
            "rows": rows,
            "samples_written": rows,
            "batches": batches,
            "elapsed_seconds": started.elapsed().as_secs_f64(),
            "http_statuses": http_statuses,
        })
    );
    Ok(())
}

fn make_series(args: &Args, series_idx: u64) -> TimeSeries {
    let mut labels = vec![
        label("__name__", &args.metric),
        label("x_greptime_database", &args.database),
        label("x_greptime_physical_table", &args.physical_table),
        label("host", &format!("host{:04}", series_idx % 1024)),
        label("instance", &format!("instance{:06}", series_idx)),
    ];
    labels.sort_unstable_by(|left, right| left.name.cmp(&right.name));
    let samples = (0..args.samples_per_series)
        .map(|sample_idx| Sample {
            value: deterministic_value(series_idx, sample_idx),
            timestamp: args.start_unix_millis + (sample_idx as i64 * args.step_millis),
        })
        .collect();
    TimeSeries {
        labels,
        samples,
        ..Default::default()
    }
}

fn label(name: &str, value: &str) -> Label {
    Label {
        name: name.to_string(),
        value: value.to_string(),
    }
}

fn deterministic_value(series_idx: u64, sample_idx: u64) -> f64 {
    ((series_idx % 97) as f64) + (sample_idx as f64 * 0.125)
}
