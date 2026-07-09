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

use clap::Args as ClapArgs;
use prost::Message;
use serde_json::json;
use servers::prom_store::snappy_compress;

use crate::query_perf_fixture::case::ValuePattern;

#[derive(Debug, ClapArgs)]
pub(super) struct PromRemoteWriteArgs {
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
    #[arg(long, default_value_t = ValuePattern::Linear)]
    value_pattern: ValuePattern,
    #[arg(long, default_value_t = 0.0)]
    value_base: f64,
    #[arg(long, default_value_t = 0.125)]
    value_step: f64,
    #[arg(long, default_value_t = 97)]
    value_cardinality: u64,
    #[arg(long, default_value_t = 0)]
    value_seed: u64,
    #[arg(long, default_value_t = 8)]
    value_run_length: u64,
    #[arg(long, default_value_t = 100)]
    value_stall_every: u64,
    #[arg(long, default_value_t = 16)]
    value_stall_length: u64,
    #[arg(long, default_value_t = 5)]
    value_mixed_every: u64,
    #[arg(long, default_value_t = 0)]
    value_sample_offset: u64,
    #[arg(long)]
    value_total_samples_per_series: Option<u64>,
}

pub(super) async fn run_prom_remote_write(
    args: PromRemoteWriteArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    use api::prom_store::remote::WriteRequest;
    let started = std::time::Instant::now();
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(args.timeout_seconds))
        .build()?;
    let mut batches = 0_u64;
    let mut rows = 0_u64;
    let mut http_statuses = Vec::new();
    for first in (0..args.series_count).step_by(args.chunk_series_count.max(1) as usize) {
        let last = (first + args.chunk_series_count.max(1)).min(args.series_count);
        let timeseries = (first..last)
            .map(|series_idx| make_prom_series(&args, series_idx))
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
        let response = client
            .post(&args.endpoint)
            .header("Content-Encoding", "snappy")
            .body(body)
            .send()
            .await?;
        let status = response.status();
        http_statuses.push(status.as_u16());
        if !status.is_success() {
            return Err(format!(
                "remote-write failed with status {status}: {}",
                response.text().await.unwrap_or_default()
            )
            .into());
        }
        batches += 1;
    }
    println!(
        "{}",
        json!({"status":"ok","endpoint":args.endpoint,"database":args.database,"metric":args.metric,"physical_table":args.physical_table,"series_count":args.series_count,"samples_per_series":args.samples_per_series,"rows":rows,"samples_written":rows,"batches":batches,"elapsed_seconds":started.elapsed().as_secs_f64(),"http_statuses":http_statuses,"value":{"pattern":args.value_pattern.to_string(),"base":args.value_base,"step":args.value_step,"cardinality":args.value_cardinality,"seed":args.value_seed,"run_length":args.value_run_length,"stall_every":args.value_stall_every,"stall_length":args.value_stall_length,"mixed_every":args.value_mixed_every,"sample_offset":args.value_sample_offset,"total_samples_per_series":args.value_total_samples_per_series.unwrap_or(args.samples_per_series)}})
    );
    Ok(())
}

fn make_prom_series(
    args: &PromRemoteWriteArgs,
    series_idx: u64,
) -> api::prom_store::remote::TimeSeries {
    let mut labels = vec![
        prom_label("__name__", &args.metric),
        prom_label("x_greptime_database", &args.database),
        prom_label("x_greptime_physical_table", &args.physical_table),
        prom_label("host", &format!("host{:04}", series_idx % 1024)),
        prom_label("instance", &format!("instance{:06}", series_idx)),
    ];
    labels.sort_unstable_by(|l, r| l.name.cmp(&r.name));
    let samples = (0..args.samples_per_series)
        .map(|sample_idx| api::prom_store::remote::Sample {
            value: deterministic_prom_value(args, series_idx, sample_idx),
            timestamp: args.start_unix_millis + (sample_idx as i64 * args.step_millis),
        })
        .collect();
    api::prom_store::remote::TimeSeries {
        labels,
        samples,
        ..Default::default()
    }
}
fn prom_label(name: &str, value: &str) -> api::prom_store::remote::Label {
    api::prom_store::remote::Label {
        name: name.to_string(),
        value: value.to_string(),
    }
}
fn deterministic_prom_value(args: &PromRemoteWriteArgs, series_idx: u64, sample_idx: u64) -> f64 {
    let ordinal = series_idx
        * args
            .value_total_samples_per_series
            .unwrap_or(args.samples_per_series)
        + args.value_sample_offset
        + sample_idx;
    let local = args.value_sample_offset + sample_idx;
    match args.value_pattern {
        ValuePattern::Linear => {
            args.value_base + (series_idx % 97) as f64 + local as f64 * args.value_step
        }
        ValuePattern::Constant => args.value_base,
        ValuePattern::Modulo => {
            args.value_base + (ordinal % args.value_cardinality.max(1)) as f64 * args.value_step
        }
        ValuePattern::Unique => args.value_base + ordinal as f64 * args.value_step,
        ValuePattern::SeededRandom => {
            args.value_base
                + (splitmix64(ordinal ^ args.value_seed) % args.value_cardinality.max(1)) as f64
                    * args.value_step
        }
        ValuePattern::RunLength => {
            args.value_base
                + ((ordinal / args.value_run_length.max(1)) % args.value_cardinality.max(1)) as f64
                    * args.value_step
        }
        ValuePattern::QuantizedSignal => {
            args.value_base
                + (((series_idx % args.value_cardinality.max(1))
                    + (local / args.value_run_length.max(1)))
                    % args.value_cardinality.max(1)) as f64
                    * args.value_step
        }
        ValuePattern::SignalWithSporadicStalls => {
            let every = args.value_stall_every.max(1);
            let phase = local % every;
            let effective = if phase < args.value_stall_length.min(every) {
                local - phase
            } else {
                local
            };
            args.value_base + (series_idx % 97) as f64 + effective as f64 * args.value_step
        }
        ValuePattern::MixedSignalRepeated => {
            if local.is_multiple_of(args.value_mixed_every.max(1)) {
                args.value_base
            } else {
                args.value_base + (series_idx % 97) as f64 + local as f64 * args.value_step
            }
        }
    }
}
fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e3779b97f4a7c15);
    let mut mixed = value;
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d049bb133111eb);
    mixed ^ (mixed >> 31)
}
