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

use crate::query_perf_fixture::case::{ValuePattern, validate_mixed_series_selector};

const MAX_EXACT_F64_INTEGER: u64 = 1 << 53;

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
    #[arg(long, default_value_t = 20)]
    value_series_mix_every: u64,
    #[arg(long, default_value_t = 19)]
    value_gauge_residue: u64,
    #[arg(long, default_value_t = 0.125)]
    value_fractional_step: f64,
    #[arg(long, default_value_t = 0)]
    value_sample_offset: u64,
    #[arg(long)]
    value_total_samples_per_series: Option<u64>,
}

pub(super) async fn run_prom_remote_write(
    args: PromRemoteWriteArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    use api::prom_store::remote::WriteRequest;
    validate_prom_remote_write_args(&args)?;
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
        json!({"status":"ok","endpoint":args.endpoint,"database":args.database,"metric":args.metric,"physical_table":args.physical_table,"series_count":args.series_count,"samples_per_series":args.samples_per_series,"rows":rows,"samples_written":rows,"batches":batches,"elapsed_seconds":started.elapsed().as_secs_f64(),"http_statuses":http_statuses,"value":{"pattern":args.value_pattern.to_string(),"base":args.value_base,"step":args.value_step,"cardinality":args.value_cardinality,"seed":args.value_seed,"run_length":args.value_run_length,"stall_every":args.value_stall_every,"stall_length":args.value_stall_length,"mixed_every":args.value_mixed_every,"series_mix_every":args.value_series_mix_every,"gauge_residue":args.value_gauge_residue,"fractional_step":args.value_fractional_step,"sample_offset":args.value_sample_offset,"total_samples_per_series":args.value_total_samples_per_series.unwrap_or(args.samples_per_series)}})
    );
    Ok(())
}

fn validate_prom_remote_write_args(args: &PromRemoteWriteArgs) -> Result<(), String> {
    if args.value_pattern != ValuePattern::MixedSeries {
        return Ok(());
    }

    validate_mixed_series_selector(
        args.value_series_mix_every,
        args.value_gauge_residue,
        args.value_fractional_step,
    )?;
    let total = args.value_total_samples_per_series.ok_or_else(|| {
        "mixed_series requires --value-total-samples-per-series for stable disjoint counter ranges"
            .to_string()
    })?;
    if total == 0 {
        return Err("mixed_series requires --value-total-samples-per-series > 0".to_string());
    }
    let end = args
        .value_sample_offset
        .checked_add(args.samples_per_series)
        .ok_or_else(|| {
            "mixed_series sample offset plus samples per series overflows u64".to_string()
        })?;
    if end > total {
        return Err(
            "mixed_series sample offset plus samples per series exceeds total samples per series"
                .to_string(),
        );
    }
    if total - 1 > MAX_EXACT_F64_INTEGER {
        return Err(
            "mixed_series total samples per series exceeds the exact f64 integer range".to_string(),
        );
    }
    if args.series_count > 0 && args.samples_per_series > 0 {
        let max_counter = (args.series_count - 1)
            .checked_mul(total)
            .and_then(|value| value.checked_add(end - 1))
            .ok_or_else(|| "mixed_series counter range overflows u64".to_string())?;
        if max_counter > MAX_EXACT_F64_INTEGER {
            return Err(
                "mixed_series counter range exceeds the exact f64 integer range".to_string(),
            );
        }
    }
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
        ValuePattern::MixedSeries => {
            if series_idx % args.value_series_mix_every == args.value_gauge_residue {
                (series_idx % 97) as f64 + local as f64 * args.value_fractional_step
            } else {
                let total = args
                    .value_total_samples_per_series
                    .expect("mixed series arguments are validated before generation");
                series_idx
                    .checked_mul(total)
                    .and_then(|value| value.checked_add(local))
                    .expect("mixed series counter range is validated before generation")
                    as f64
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

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    fn args(pattern: ValuePattern) -> PromRemoteWriteArgs {
        PromRemoteWriteArgs {
            endpoint: "http://127.0.0.1:4000/v1/prometheus/write".to_string(),
            database: "public".to_string(),
            metric: "test_metric".to_string(),
            physical_table: "greptime_physical_table".to_string(),
            series_count: 2,
            samples_per_series: 64,
            start_unix_millis: 1_704_067_200_000,
            step_millis: 60_000,
            chunk_series_count: 2,
            timeout_seconds: 60,
            value_pattern: pattern,
            value_base: 0.0,
            value_step: 1.0,
            value_cardinality: 1_024,
            value_seed: 8_444,
            value_run_length: 16,
            value_stall_every: 100,
            value_stall_length: 16,
            value_mixed_every: 5,
            value_series_mix_every: 20,
            value_gauge_residue: 19,
            value_fractional_step: 0.125,
            value_sample_offset: 0,
            value_total_samples_per_series: None,
        }
    }

    #[test]
    fn seeded_random_is_deterministic_in_range_and_highly_distinct() {
        let mut args = args(ValuePattern::SeededRandom);
        args.samples_per_series = 4_096;
        args.value_cardinality = 29_491_200;

        let values = (0..args.samples_per_series)
            .map(|sample_idx| deterministic_prom_value(&args, 0, sample_idx))
            .collect::<Vec<_>>();
        let repeated = (0..args.samples_per_series)
            .map(|sample_idx| deterministic_prom_value(&args, 0, sample_idx))
            .collect::<Vec<_>>();

        assert_eq!(values, repeated);
        assert!(values.iter().all(|value| {
            (args.value_base..=args.value_base + (args.value_cardinality - 1) as f64)
                .contains(value)
        }));
        assert!(
            values
                .iter()
                .map(|value| value.to_bits())
                .collect::<HashSet<_>>()
                .len()
                > 4_000
        );
    }

    #[test]
    fn run_length_values_have_exact_sixteen_sample_runs() {
        let args = args(ValuePattern::RunLength);

        for sample_idx in 0..64 {
            assert_eq!(
                deterministic_prom_value(&args, 0, sample_idx),
                (sample_idx / 16) as f64
            );
        }
    }

    #[test]
    fn mixed_every_five_repeats_only_the_expected_positions() {
        let mut args = args(ValuePattern::MixedSignalRepeated);
        args.value_step = 0.125;

        for sample_idx in 0..20 {
            let expected = if sample_idx % 5 == 0 {
                0.0
            } else {
                1.0 + sample_idx as f64 * 0.125
            };
            assert_eq!(deterministic_prom_value(&args, 1, sample_idx), expected);
        }
    }

    #[test]
    fn chunked_values_match_monolithic_values_through_offsets() {
        let mut monolithic = args(ValuePattern::SeededRandom);
        monolithic.samples_per_series = 14_400;
        monolithic.value_total_samples_per_series = Some(14_400);

        for series_idx in 0..2 {
            let expected = (0..monolithic.samples_per_series)
                .map(|sample_idx| deterministic_prom_value(&monolithic, series_idx, sample_idx))
                .collect::<Vec<_>>();
            for chunk_idx in 0..10 {
                let mut chunk = args(ValuePattern::SeededRandom);
                chunk.samples_per_series = 1_440;
                chunk.value_sample_offset = chunk_idx * 1_440;
                chunk.value_total_samples_per_series = Some(14_400);
                for sample_idx in 0..chunk.samples_per_series {
                    assert_eq!(
                        deterministic_prom_value(&chunk, series_idx, sample_idx),
                        expected[(chunk.value_sample_offset + sample_idx) as usize]
                    );
                }
            }
        }
    }

    #[test]
    fn values_continue_across_series_boundaries() {
        let mut args = args(ValuePattern::Unique);
        args.samples_per_series = 16;
        args.value_total_samples_per_series = Some(16);
        args.value_base = 1.0;
        args.value_step = 0.5;

        assert_eq!(deterministic_prom_value(&args, 0, 15), 8.5);
        assert_eq!(deterministic_prom_value(&args, 1, 0), 9.0);
    }

    #[test]
    fn mixed_series_has_expected_counter_and_gauge_classification() {
        let args = mixed_series_args();
        for (series_count, expected_gauges) in [(200, 10), (2_000, 100)] {
            let gauges = (0..series_count)
                .filter(|series_idx| {
                    series_idx % args.value_series_mix_every == args.value_gauge_residue
                })
                .count() as u64;
            assert_eq!(gauges, expected_gauges);
            assert_eq!(series_count - gauges, series_count / 20 * 19);
        }
        assert_eq!(
            (0..100)
                .filter(|series_idx| series_idx % args.value_series_mix_every
                    == args.value_gauge_residue)
                .collect::<Vec<_>>(),
            vec![19, 39, 59, 79, 99]
        );
    }

    #[test]
    fn mixed_series_counters_are_integral_increasing_and_disjoint() {
        let args = mixed_series_args();
        let total = args.value_total_samples_per_series.unwrap();
        for series_idx in [0, 1, 18, 20] {
            let values = (0..8)
                .map(|sample_idx| deterministic_prom_value(&args, series_idx, sample_idx))
                .collect::<Vec<_>>();
            assert!(values.iter().all(|value| value.fract() == 0.0));
            assert_eq!(
                values,
                (0..8)
                    .map(|idx| (series_idx * total + idx) as f64)
                    .collect::<Vec<_>>()
            );
        }
        assert!(
            deterministic_prom_value(&args, 0, total - 1) < deterministic_prom_value(&args, 1, 0)
        );
    }

    #[test]
    fn mixed_series_gauges_follow_fractional_formula() {
        let args = mixed_series_args();
        assert_eq!(deterministic_prom_value(&args, 19, 0), 19.0);
        assert_eq!(deterministic_prom_value(&args, 19, 1), 19.125);
        assert_eq!(deterministic_prom_value(&args, 39, 3), 39.375);
        assert_ne!(deterministic_prom_value(&args, 19, 1).fract(), 0.0);
    }

    #[test]
    fn mixed_series_is_deterministic_and_chunking_preserves_values() {
        let monolithic = mixed_series_args();
        for series_idx in [18, 19] {
            let expected = (0..monolithic.samples_per_series)
                .map(|sample_idx| deterministic_prom_value(&monolithic, series_idx, sample_idx))
                .collect::<Vec<_>>();
            assert_eq!(
                expected,
                (0..monolithic.samples_per_series)
                    .map(|sample_idx| deterministic_prom_value(&monolithic, series_idx, sample_idx))
                    .collect::<Vec<_>>()
            );
            for chunk_idx in 0..10 {
                let mut chunk = mixed_series_args();
                chunk.samples_per_series = 1_440;
                chunk.value_sample_offset = chunk_idx * 1_440;
                for sample_idx in 0..chunk.samples_per_series {
                    assert_eq!(
                        deterministic_prom_value(&chunk, series_idx, sample_idx),
                        expected[(chunk.value_sample_offset + sample_idx) as usize]
                    );
                }
            }
        }
    }

    #[test]
    fn mixed_series_rejects_invalid_selector_step_and_total() {
        let mut invalid = mixed_series_args();
        invalid.value_series_mix_every = 0;
        assert!(
            validate_prom_remote_write_args(&invalid)
                .unwrap_err()
                .contains("series_mix_every")
        );
        invalid = mixed_series_args();
        invalid.value_gauge_residue = invalid.value_series_mix_every;
        assert!(
            validate_prom_remote_write_args(&invalid)
                .unwrap_err()
                .contains("gauge_residue")
        );
        invalid = mixed_series_args();
        invalid.value_fractional_step = f64::INFINITY;
        assert!(
            validate_prom_remote_write_args(&invalid)
                .unwrap_err()
                .contains("fractional_step")
        );
        invalid = mixed_series_args();
        invalid.value_total_samples_per_series = None;
        assert!(
            validate_prom_remote_write_args(&invalid)
                .unwrap_err()
                .contains("total-samples")
        );
    }

    fn mixed_series_args() -> PromRemoteWriteArgs {
        let mut args = args(ValuePattern::MixedSeries);
        args.series_count = 2_000;
        args.samples_per_series = 14_400;
        args.value_total_samples_per_series = Some(14_400);
        args
    }
}
