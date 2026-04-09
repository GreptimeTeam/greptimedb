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

use std::env;
use std::process::ExitCode;

use tests_fuzz::gc_model::runner::{
    FaultInjection, FuzzInput, collect_replay, regression_smoke_cases,
};

fn main() -> ExitCode {
    match parse_args(env::args().skip(1).collect()) {
        Ok(Command::Replay(input)) => {
            let replay = collect_replay(input.clone());
            println!(
                "replay seed={} actions={} max_regions={} linger_ms={} mismatch_rate={}",
                input.seed, input.actions, input.max_regions, input.linger_ms, input.mismatch_rate
            );
            for step in replay {
                println!(
                    "#{:03} action={:?} summary={} snapshot={{now={}, live_regions={}, manifest_files={}, removed_files={}, temp_refs={}, cross_edges={}, total_files={}}}",
                    step.idx,
                    step.action,
                    step.summary,
                    step.snapshot.now,
                    step.snapshot.live_region_count,
                    step.snapshot.manifest_file_count,
                    step.snapshot.removed_file_count,
                    step.snapshot.temp_ref_count,
                    step.snapshot.cross_region_edge_count,
                    step.snapshot.total_file_count,
                );
            }
            ExitCode::SUCCESS
        }
        Ok(Command::SmokeSuite) => {
            for case in regression_smoke_cases() {
                println!(
                    "smoke-case name={} seed={} actions={} max_regions={} linger_ms={} mismatch_rate={} note={}",
                    case.name,
                    case.seed,
                    case.actions,
                    case.max_regions,
                    case.linger_ms,
                    case.mismatch_rate,
                    case.note,
                );
                let replay = collect_replay(case.as_input());
                println!("  replay-steps={}", replay.len());
            }
            ExitCode::SUCCESS
        }
        Err(message) => {
            eprintln!("{message}");
            ExitCode::from(2)
        }
    }
}

enum Command {
    Replay(FuzzInput),
    SmokeSuite,
}

fn parse_args(args: Vec<String>) -> Result<Command, String> {
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        return Err(
            "usage: cargo run -p tests-fuzz --bin replay_gc_model_cross_region -- [--seed <u64> [--actions <usize>] [--max-regions <u8>] [--linger-ms <u32>] [--mismatch-rate <u8>] [--ignore-temp-refs] [--ignore-follower-protection] [--ignore-cross-region-refs] [--ignore-lingering]] [--smoke-suite]".to_string(),
        );
    }

    let mut seed = None;
    let mut actions = 64usize;
    let mut max_regions = 8u8;
    let mut linger_ms = 1000u32;
    let mut mismatch_rate = 5u8;
    let mut fault_injection = FaultInjection::default();

    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--smoke-suite" => return Ok(Command::SmokeSuite),
            "--ignore-temp-refs" => {
                fault_injection.ignore_temp_refs = true;
                continue;
            }
            "--ignore-follower-protection" => {
                fault_injection.ignore_follower_protection = true;
                continue;
            }
            "--ignore-cross-region-refs" => {
                fault_injection.ignore_cross_region_refs = true;
                continue;
            }
            "--ignore-lingering" => {
                fault_injection.ignore_lingering = true;
                continue;
            }
            _ => {}
        }

        let value = iter
            .next()
            .ok_or_else(|| format!("missing value for {arg}"))?;
        match arg.as_str() {
            "--seed" => seed = Some(parse_value(&arg, &value)?),
            "--actions" => actions = parse_value(&arg, &value)?,
            "--max-regions" => max_regions = parse_value(&arg, &value)?,
            "--linger-ms" => linger_ms = parse_value(&arg, &value)?,
            "--mismatch-rate" => mismatch_rate = parse_value(&arg, &value)?,
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }

    Ok(Command::Replay(FuzzInput {
        seed: seed.ok_or_else(|| "--seed is required".to_string())?,
        actions,
        max_regions,
        linger_ms,
        mismatch_rate,
        fault_injection,
    }))
}

fn parse_value<T>(flag: &str, value: &str) -> Result<T, String>
where
    T: std::str::FromStr,
{
    value
        .parse()
        .map_err(|_| format!("invalid value for {flag}: {value}"))
}
