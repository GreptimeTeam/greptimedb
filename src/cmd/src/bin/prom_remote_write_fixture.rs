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
use clap::{Parser, ValueEnum};
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "snake_case")]
enum ValuePattern {
    Linear,
    Constant,
    Modulo,
    Unique,
    SeededRandom,
    RunLength,
    QuantizedSignal,
    SignalWithSporadicStalls,
    MixedSignalRepeated,
}

impl std::fmt::Display for ValuePattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Linear => write!(f, "linear"),
            Self::Constant => write!(f, "constant"),
            Self::Modulo => write!(f, "modulo"),
            Self::Unique => write!(f, "unique"),
            Self::SeededRandom => write!(f, "seeded_random"),
            Self::RunLength => write!(f, "run_length"),
            Self::QuantizedSignal => write!(f, "quantized_signal"),
            Self::SignalWithSporadicStalls => write!(f, "signal_with_sporadic_stalls"),
            Self::MixedSignalRepeated => write!(f, "mixed_signal_repeated"),
        }
    }
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
        let response = client
            .post(&args.endpoint)
            .header("Content-Encoding", "snappy")
            .body(body)
            .send()
            .await?;
        let status = response.status();
        http_statuses.push(status.as_u16());
        if !status.is_success() {
            let body_text = response.text().await.unwrap_or_default();
            return Err(format!("remote-write failed with status {status}: {body_text}").into());
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
            "value": {
                "pattern": args.value_pattern.to_string(),
                "base": args.value_base,
                "step": args.value_step,
                "cardinality": args.value_cardinality,
                "seed": args.value_seed,
                "run_length": args.value_run_length,
                "stall_every": args.value_stall_every,
                "stall_length": args.value_stall_length,
                "mixed_every": args.value_mixed_every,
                "sample_offset": args.value_sample_offset,
                "total_samples_per_series": args.value_total_samples_per_series.unwrap_or(args.samples_per_series),
            },
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
            value: deterministic_value(args, series_idx, sample_idx),
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

fn deterministic_value(args: &Args, series_idx: u64, sample_idx: u64) -> f64 {
    let ordinal = value_ordinal(args, series_idx, sample_idx);
    let series_sample_ordinal = value_series_sample_ordinal(args, sample_idx);
    match args.value_pattern {
        ValuePattern::Linear => linear_value(args, series_idx, series_sample_ordinal),
        ValuePattern::Constant => args.value_base,
        ValuePattern::Modulo => {
            args.value_base + (ordinal % args.value_cardinality.max(1)) as f64 * args.value_step
        }
        ValuePattern::Unique => args.value_base + ordinal as f64 * args.value_step,
        ValuePattern::SeededRandom => {
            let bucket = splitmix64(ordinal ^ args.value_seed) % args.value_cardinality.max(1);
            args.value_base + bucket as f64 * args.value_step
        }
        ValuePattern::RunLength => {
            let bucket = (ordinal / args.value_run_length.max(1)) % args.value_cardinality.max(1);
            args.value_base + bucket as f64 * args.value_step
        }
        ValuePattern::QuantizedSignal => {
            let bucket = ((series_idx % args.value_cardinality.max(1))
                + (series_sample_ordinal / args.value_run_length.max(1)))
                % args.value_cardinality.max(1);
            args.value_base + bucket as f64 * args.value_step
        }
        ValuePattern::SignalWithSporadicStalls => {
            let stall_every = args.value_stall_every.max(1);
            let stall_length = args.value_stall_length.min(stall_every);
            let phase = series_sample_ordinal % stall_every;
            if phase < stall_length {
                linear_value(args, series_idx, series_sample_ordinal - phase)
            } else {
                linear_value(args, series_idx, series_sample_ordinal)
            }
        }
        ValuePattern::MixedSignalRepeated => {
            if series_sample_ordinal.is_multiple_of(args.value_mixed_every.max(1)) {
                args.value_base
            } else {
                linear_value(args, series_idx, series_sample_ordinal)
            }
        }
    }
}

fn linear_value(args: &Args, series_idx: u64, series_sample_ordinal: u64) -> f64 {
    args.value_base + (series_idx % 97) as f64 + series_sample_ordinal as f64 * args.value_step
}

fn value_ordinal(args: &Args, series_idx: u64, sample_idx: u64) -> u64 {
    let total_samples_per_series = args
        .value_total_samples_per_series
        .unwrap_or(args.samples_per_series);
    series_idx * total_samples_per_series + args.value_sample_offset + sample_idx
}

fn value_series_sample_ordinal(args: &Args, sample_idx: u64) -> u64 {
    args.value_sample_offset + sample_idx
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e3779b97f4a7c15);
    let mut mixed = value;
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d049bb133111eb);
    mixed ^ (mixed >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(value_pattern: ValuePattern) -> Args {
        Args {
            endpoint: "http://127.0.0.1:4000/v1/prometheus/write".to_string(),
            database: "public".to_string(),
            metric: "metric".to_string(),
            physical_table: "greptime_physical_table".to_string(),
            series_count: 8,
            samples_per_series: 30,
            start_unix_millis: 1_704_067_200_000,
            step_millis: 15_000,
            chunk_series_count: 8,
            timeout_seconds: 60,
            value_pattern,
            value_base: 0.0,
            value_step: 0.125,
            value_cardinality: 97,
            value_seed: 0,
            value_run_length: 8,
            value_stall_every: 100,
            value_stall_length: 16,
            value_mixed_every: 5,
            value_sample_offset: 0,
            value_total_samples_per_series: None,
        }
    }

    #[test]
    fn default_linear_matches_old_formula() {
        let args = args(ValuePattern::Linear);
        for series_idx in [0, 1, 96, 97, 194] {
            for sample_idx in [0, 1, 10, 29] {
                assert_eq!(
                    deterministic_value(&args, series_idx, sample_idx),
                    ((series_idx % 97) as f64) + (sample_idx as f64 * 0.125)
                );
            }
        }
    }

    #[test]
    fn constant_uses_base() {
        let mut args = args(ValuePattern::Constant);
        args.value_base = 42.5;
        assert_eq!(deterministic_value(&args, 7, 11), 42.5);
    }

    #[test]
    fn modulo_uses_global_ordinal_and_cardinality() {
        let mut args = args(ValuePattern::Modulo);
        args.samples_per_series = 3;
        args.value_base = 10.0;
        args.value_step = 2.0;
        args.value_cardinality = 4;
        assert_eq!(deterministic_value(&args, 1, 2), 12.0);
        assert_eq!(deterministic_value(&args, 2, 0), 14.0);
    }

    #[test]
    fn unique_uses_global_ordinal() {
        let mut args = args(ValuePattern::Unique);
        args.samples_per_series = 10;
        args.value_base = 1.0;
        args.value_step = 0.5;
        assert_eq!(deterministic_value(&args, 2, 3), 12.5);
    }

    #[test]
    fn unique_uses_chunk_offset_and_total_samples() {
        let mut args = args(ValuePattern::Unique);
        args.samples_per_series = 3;
        args.value_total_samples_per_series = Some(10);
        args.value_sample_offset = 3;
        args.value_step = 1.0;
        assert_eq!(deterministic_value(&args, 2, 0), 23.0);
    }

    #[test]
    fn seeded_random_is_deterministic() {
        let mut args = args(ValuePattern::SeededRandom);
        args.value_cardinality = 5;
        args.value_seed = 123;
        let first = deterministic_value(&args, 4, 5);
        assert_eq!(first, deterministic_value(&args, 4, 5));
        args.value_seed = 124;
        assert_ne!(first, deterministic_value(&args, 4, 5));
    }

    #[test]
    fn run_length_repeats_each_bucket_for_configured_window() {
        let mut args = args(ValuePattern::RunLength);
        args.samples_per_series = 10;
        args.value_step = 1.0;
        args.value_cardinality = 4;
        args.value_run_length = 3;
        assert_eq!(deterministic_value(&args, 0, 0), 0.0);
        assert_eq!(deterministic_value(&args, 0, 2), 0.0);
        assert_eq!(deterministic_value(&args, 0, 3), 1.0);
        assert_eq!(deterministic_value(&args, 1, 0), 3.0);
    }

    #[test]
    fn quantized_signal_uses_series_local_quantized_steps() {
        let mut args = args(ValuePattern::QuantizedSignal);
        args.value_base = 10.0;
        args.value_step = 2.0;
        args.value_cardinality = 5;
        args.value_run_length = 4;
        assert_eq!(deterministic_value(&args, 2, 0), 14.0);
        assert_eq!(deterministic_value(&args, 2, 3), 14.0);
        assert_eq!(deterministic_value(&args, 2, 4), 16.0);
    }

    #[test]
    fn quantized_signal_respects_chunk_offset() {
        let mut args = args(ValuePattern::QuantizedSignal);
        args.value_step = 1.0;
        args.value_cardinality = 8;
        args.value_run_length = 4;
        args.value_sample_offset = 4;
        assert_eq!(deterministic_value(&args, 0, 0), 1.0);
        assert_eq!(deterministic_value(&args, 0, 3), 1.0);
        assert_eq!(deterministic_value(&args, 0, 4), 2.0);
    }

    #[test]
    fn sporadic_stalls_hold_value_within_stall_window() {
        let mut args = args(ValuePattern::SignalWithSporadicStalls);
        args.value_step = 1.0;
        args.value_stall_every = 5;
        args.value_stall_length = 2;
        assert_eq!(deterministic_value(&args, 0, 0), 0.0);
        assert_eq!(deterministic_value(&args, 0, 1), 0.0);
        assert_eq!(deterministic_value(&args, 0, 2), 2.0);
        assert_eq!(deterministic_value(&args, 0, 5), 5.0);
        assert_eq!(deterministic_value(&args, 0, 6), 5.0);
    }

    #[test]
    fn mixed_signal_repeated_inserts_periodic_base_value() {
        let mut args = args(ValuePattern::MixedSignalRepeated);
        args.value_base = 100.0;
        args.value_step = 1.0;
        args.value_mixed_every = 3;
        assert_eq!(deterministic_value(&args, 0, 0), 100.0);
        assert_eq!(deterministic_value(&args, 0, 1), 101.0);
        assert_eq!(deterministic_value(&args, 0, 2), 102.0);
        assert_eq!(deterministic_value(&args, 0, 3), 100.0);
    }
}
