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

use prost::Message;
use servers::prom_store::snappy_compress;

use crate::query_perf::case::{ValuePattern, validate_remote_write_totals};
use crate::query_perf::error::{Error, Result};
use crate::query_perf::fixture::{RemoteWriteRequest, RemoteWriteSummary};
use crate::query_perf::manifest::EndpointUrl;

pub async fn run_prom_remote_write(args: RemoteWriteRequest) -> Result<RemoteWriteSummary> {
    use api::prom_store::remote::WriteRequest;
    validate_args(&args)?;
    let started = std::time::Instant::now();
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(args.timeout_seconds))
        .build()
        .map_err(|err| Error::new(format!("failed to build remote-write client: {err}")))?;
    let mut batches = 0_u64;
    let mut rows = 0_u64;
    let mut http_statuses = Vec::new();
    let chunk_size = usize::try_from(args.chunk_series_count)
        .map_err(|_| Error::new("chunk_series_count exceeds usize"))?;
    for first in (0..args.series_count).step_by(chunk_size) {
        let last = first
            .checked_add(args.chunk_series_count)
            .ok_or_else(|| Error::new("remote-write chunk range overflows"))?
            .min(args.series_count);
        let timeseries = (first..last)
            .map(|series_idx| make_prom_series(&args, series_idx))
            .collect::<Result<Vec<_>>>()?;
        let batch_rows = timeseries
            .iter()
            .map(|ts| ts.samples.len() as u64)
            .try_fold(0_u64, |total, count| total.checked_add(count))
            .ok_or_else(|| Error::new("remote-write batch row count overflows"))?;
        rows = rows
            .checked_add(batch_rows)
            .ok_or_else(|| Error::new("remote-write row count overflows"))?;
        let body = snappy_compress(
            &WriteRequest {
                timeseries,
                ..Default::default()
            }
            .encode_to_vec(),
        )
        .map_err(|err| Error::new(format!("failed to compress remote-write request: {err}")))?;
        let response = client
            .post(&args.endpoint)
            .header("Content-Encoding", "snappy")
            .body(body)
            .send()
            .await
            .map_err(|err| Error::new(format!("remote-write request failed: {err}")))?;
        let status = response.status();
        http_statuses.push(status.as_u16());
        if !status.is_success() {
            let body = response
                .text()
                .await
                .unwrap_or_else(|err| format!("<failed to read body: {err}>"));
            return Err(Error::new(format!(
                "remote-write failed with status {status}: {body}"
            )));
        }
        batches += 1;
    }
    Ok(RemoteWriteSummary {
        endpoint: args.endpoint,
        database: args.database,
        metric: args.metric,
        physical_table: args.physical_table,
        series_count: args.series_count,
        samples_per_series: args.samples_per_series,
        rows,
        batches,
        elapsed_seconds: started.elapsed().as_secs_f64(),
        http_statuses,
    })
}

fn make_prom_series(
    args: &RemoteWriteRequest,
    series_idx: u64,
) -> Result<api::prom_store::remote::TimeSeries> {
    let mut labels = vec![
        prom_label("__name__", &args.metric),
        prom_label("x_greptime_database", &args.database),
        prom_label("x_greptime_physical_table", &args.physical_table),
        prom_label("host", &format!("host{:04}", series_idx % 1024)),
        prom_label("instance", &format!("instance{:06}", series_idx)),
    ];
    labels.sort_unstable_by(|l, r| l.name.cmp(&r.name));
    let samples = (0..args.samples_per_series)
        .map(|sample_idx| -> Result<_> {
            let sample_step = i64::try_from(sample_idx)
                .map_err(|_| Error::new("remote-write sample index exceeds i64"))?
                .checked_mul(args.step_millis)
                .ok_or_else(|| Error::new("remote-write timestamp multiplication overflows"))?;
            Ok(api::prom_store::remote::Sample {
                value: deterministic_prom_value(args, series_idx, sample_idx)?,
                timestamp: args
                    .start_unix_millis
                    .checked_add(sample_step)
                    .ok_or_else(|| Error::new("remote-write timestamp range overflows"))?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(api::prom_store::remote::TimeSeries {
        labels,
        samples,
        ..Default::default()
    })
}
fn prom_label(name: &str, value: &str) -> api::prom_store::remote::Label {
    api::prom_store::remote::Label {
        name: name.to_string(),
        value: value.to_string(),
    }
}
fn deterministic_prom_value(
    args: &RemoteWriteRequest,
    series_idx: u64,
    sample_idx: u64,
) -> Result<f64> {
    let total = args
        .value_total_samples_per_series
        .unwrap_or(args.samples_per_series);
    let ordinal = series_idx
        .checked_mul(total)
        .and_then(|value| value.checked_add(args.value_sample_offset))
        .and_then(|value| value.checked_add(sample_idx))
        .ok_or_else(|| Error::new("remote-write ordinal overflows"))?;
    let local = args
        .value_sample_offset
        .checked_add(sample_idx)
        .ok_or_else(|| Error::new("remote-write sample offset overflows"))?;
    Ok(match args.value_pattern {
        ValuePattern::Linear => {
            args.value_base + (series_idx % 97) as f64 + local as f64 * args.value_step
        }
        ValuePattern::Constant => args.value_base,
        ValuePattern::Modulo => {
            args.value_base + (ordinal % args.value_cardinality) as f64 * args.value_step
        }
        ValuePattern::Unique => args.value_base + ordinal as f64 * args.value_step,
        ValuePattern::SeededRandom => {
            args.value_base
                + (splitmix64(ordinal ^ args.value_seed) % args.value_cardinality) as f64
                    * args.value_step
        }
        ValuePattern::RunLength => {
            args.value_base
                + ((ordinal / args.value_run_length) % args.value_cardinality) as f64
                    * args.value_step
        }
        ValuePattern::QuantizedSignal => {
            args.value_base
                + (((series_idx % args.value_cardinality) + (local / args.value_run_length))
                    % args.value_cardinality) as f64
                    * args.value_step
        }
        ValuePattern::SignalWithSporadicStalls => {
            let every = args.value_stall_every;
            let phase = local % every;
            let effective = if phase < args.value_stall_length.min(every) {
                local - phase
            } else {
                local
            };
            args.value_base + (series_idx % 97) as f64 + effective as f64 * args.value_step
        }
        ValuePattern::MixedSignalRepeated => {
            if local.is_multiple_of(args.value_mixed_every) {
                args.value_base
            } else {
                args.value_base + (series_idx % 97) as f64 + local as f64 * args.value_step
            }
        }
    })
}

fn validate_args(args: &RemoteWriteRequest) -> Result<()> {
    EndpointUrl::parse(args.endpoint.clone())?;
    for (name, value) in [
        ("database", &args.database),
        ("metric", &args.metric),
        ("physical_table", &args.physical_table),
    ] {
        if value.trim().is_empty()
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        {
            return Err(Error::new(format!("{name} must be a non-empty identifier")));
        }
    }
    for (name, value) in [
        ("series_count", args.series_count),
        ("samples_per_series", args.samples_per_series),
        ("chunk_series_count", args.chunk_series_count),
        ("timeout_seconds", args.timeout_seconds),
        ("value_cardinality", args.value_cardinality),
        ("value_run_length", args.value_run_length),
        ("value_stall_every", args.value_stall_every),
        ("value_stall_length", args.value_stall_length),
        ("value_mixed_every", args.value_mixed_every),
    ] {
        if value == 0 {
            return Err(Error::new(format!("{name} must be positive")));
        }
    }
    if args.step_millis <= 0 {
        return Err(Error::new("step_millis must be positive"));
    }
    let span = i64::try_from(args.samples_per_series - 1)
        .map_err(|_| Error::new("samples_per_series exceeds i64"))?
        .checked_mul(args.step_millis)
        .ok_or_else(|| Error::new("remote-write timestamp multiplication overflows"))?;
    args.start_unix_millis
        .checked_add(span)
        .ok_or_else(|| Error::new("remote-write timestamp range overflows"))?;
    if !args.value_base.is_finite() || !args.value_step.is_finite() {
        return Err(Error::new("value_base and value_step must be finite"));
    }
    validate_remote_write_totals(
        args.series_count,
        args.samples_per_series,
        args.value_sample_offset,
        args.value_total_samples_per_series
            .unwrap_or(args.samples_per_series),
    )?;
    Ok(())
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

    fn args(pattern: ValuePattern) -> RemoteWriteRequest {
        RemoteWriteRequest {
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
            .map(|sample_idx| {
                deterministic_prom_value(&args, 0, sample_idx)
                    .unwrap_or_else(|err| panic!("value: {err}"))
            })
            .collect::<Vec<_>>();
        let repeated = (0..args.samples_per_series)
            .map(|sample_idx| {
                deterministic_prom_value(&args, 0, sample_idx)
                    .unwrap_or_else(|err| panic!("value: {err}"))
            })
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
                deterministic_prom_value(&args, 0, sample_idx)
                    .unwrap_or_else(|err| panic!("value: {err}")),
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
            assert_eq!(
                deterministic_prom_value(&args, 1, sample_idx)
                    .unwrap_or_else(|err| panic!("value: {err}")),
                expected
            );
        }
    }

    #[test]
    fn chunked_values_match_monolithic_values_through_offsets() {
        let mut monolithic = args(ValuePattern::SeededRandom);
        monolithic.samples_per_series = 14_400;
        monolithic.value_total_samples_per_series = Some(14_400);

        for series_idx in 0..2 {
            let expected = (0..monolithic.samples_per_series)
                .map(|sample_idx| {
                    deterministic_prom_value(&monolithic, series_idx, sample_idx)
                        .unwrap_or_else(|err| panic!("value: {err}"))
                })
                .collect::<Vec<_>>();
            for chunk_idx in 0..10 {
                let mut chunk = args(ValuePattern::SeededRandom);
                chunk.samples_per_series = 1_440;
                chunk.value_sample_offset = chunk_idx * 1_440;
                chunk.value_total_samples_per_series = Some(14_400);
                for sample_idx in 0..chunk.samples_per_series {
                    assert_eq!(
                        deterministic_prom_value(&chunk, series_idx, sample_idx)
                            .unwrap_or_else(|err| panic!("value: {err}")),
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

        assert_eq!(
            deterministic_prom_value(&args, 0, 15).unwrap_or_else(|err| panic!("value: {err}")),
            8.5
        );
        assert_eq!(
            deterministic_prom_value(&args, 1, 0).unwrap_or_else(|err| panic!("value: {err}")),
            9.0
        );
    }

    #[test]
    fn rejects_remote_write_arithmetic_overflow() {
        let mut request = args(ValuePattern::Unique);
        request.series_count = u64::MAX;
        request.samples_per_series = 2;
        assert!(validate_args(&request).is_err());

        let mut request = args(ValuePattern::Unique);
        request.start_unix_millis = i64::MAX;
        request.samples_per_series = 2;
        assert!(validate_args(&request).is_err());
    }
}
