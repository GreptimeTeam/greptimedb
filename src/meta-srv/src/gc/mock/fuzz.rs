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

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::gc::mock::fuzz_input::Phase2MockFuzzInput;
use crate::gc::mock::fuzz_oracle::{Phase2MockFuzzEvidence, validate_phase2_mock_evidence};
use crate::gc::mock::fuzz_scenarios::ScenarioFixtureBuilder;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Phase2MockFuzzStep {
    full_file_listing: bool,
    inject_retry: bool,
    dropped_region_bias_hit: bool,
    route_override_bias_hit: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Phase2CoverageTag {
    Active,
    ActiveFullListing,
    Retry,
    Dropped,
    DroppedRetry,
    RouteOverride,
    RouteOverrideRetry,
}

#[derive(Debug, Default)]
struct Phase2CoverageSummary {
    tags: HashSet<Phase2CoverageTag>,
}

impl Phase2CoverageSummary {
    fn record(&mut self, step: Phase2MockFuzzStep) {
        let is_dropped = step.dropped_region_bias_hit || step.route_override_bias_hit;
        if is_dropped {
            self.tags.insert(Phase2CoverageTag::Dropped);
        } else {
            self.tags.insert(Phase2CoverageTag::Active);
        }
        if step.full_file_listing && !is_dropped {
            self.tags.insert(Phase2CoverageTag::ActiveFullListing);
        }
        if step.inject_retry && !is_dropped {
            self.tags.insert(Phase2CoverageTag::Retry);
        }
        if step.inject_retry && is_dropped {
            self.tags.insert(Phase2CoverageTag::DroppedRetry);
        }
        if step.route_override_bias_hit {
            self.tags.insert(Phase2CoverageTag::RouteOverride);
        }
        if step.route_override_bias_hit && step.inject_retry {
            self.tags.insert(Phase2CoverageTag::RouteOverrideRetry);
        }
    }

    fn ensure_minimum_coverage(&self) -> Result<(), String> {
        let required = [
            Phase2CoverageTag::Active,
            Phase2CoverageTag::Retry,
            Phase2CoverageTag::Dropped,
            Phase2CoverageTag::RouteOverride,
        ];
        let missing: Vec<_> = required
            .into_iter()
            .filter(|tag| !self.tags.contains(tag))
            .collect();
        if !missing.is_empty() {
            return Err(format!("missing weighted coverage tags: {missing:?}"));
        }
        Ok(())
    }
}

fn sample_step(input: Phase2MockFuzzInput, rng: &mut StdRng) -> Phase2MockFuzzStep {
    let roll = |bias: u8, rng: &mut StdRng| rng.random_range(0..100) < bias;

    Phase2MockFuzzStep {
        full_file_listing: roll(input.full_listing_bias, rng),
        inject_retry: roll(input.retry_bias, rng),
        dropped_region_bias_hit: roll(input.dropped_region_bias, rng),
        route_override_bias_hit: roll(input.route_override_bias, rng),
    }
}

async fn run_phase2_mock_fuzz(input: Phase2MockFuzzInput) -> Result<(), String> {
    let mut rng = StdRng::seed_from_u64(input.seed);
    let mut coverage = Phase2CoverageSummary::default();

    for step_idx in 0..input.action_count {
        let step = sample_step(input, &mut rng);
        coverage.record(step);
        let mut builder = ScenarioFixtureBuilder::new(step_idx as u64 + input.seed);
        let request_multi_region = step_idx % 4 == 0;
        if step.dropped_region_bias_hit || step.route_override_bias_hit {
            builder = builder.with_dropped_region();
        }
        if step.route_override_bias_hit {
            builder = builder.with_route_override();
        }
        if step.inject_retry {
            builder = builder.with_retry_once();
        }
        if request_multi_region {
            builder = builder.with_multi_region();
        }

        let fixture = builder.build();
        let requested_regions = if step.dropped_region_bias_hit || step.route_override_bias_hit {
            if request_multi_region {
                vec![fixture.active_region_id, fixture.dropped_region_id]
            } else {
                vec![fixture.dropped_region_id]
            }
        } else if request_multi_region {
            vec![fixture.active_region_id, fixture.extra_active_region_ids[0]]
        } else {
            vec![fixture.active_region_id]
        };
        let requested_full_listing = request_multi_region || step.full_file_listing;
        let report = fixture
            .scheduler
            .handle_manual_gc(
                Some(requested_regions.clone()),
                Some(requested_full_listing),
                Some(Duration::from_secs(1)),
            )
            .await
            .map_err(|error| {
                format!(
                    "phase2-mock-fuzz error: {} step={} step_state={:?} error={error}",
                    input.summary(),
                    step_idx,
                    step,
                )
            })?;

        match report {
            crate::gc::GcJobReport::Combined { report } => {
                if step.inject_retry
                    && report
                        .need_retry_regions
                        .intersection(&requested_regions.iter().copied().collect())
                        .next()
                        .is_none()
                {
                    return Err(format!(
                        "phase2-mock-fuzz missing retry marker: {} step={} step_state={:?} report={:?}",
                        input.summary(),
                        step_idx,
                        step,
                        report,
                    ));
                }
                if !step.inject_retry
                    && requested_regions
                        .iter()
                        .any(|region_id| !report.deleted_files.contains_key(region_id))
                {
                    return Err(format!(
                        "phase2-mock-fuzz missing deleted files: {} step={} step_state={:?} report={:?}",
                        input.summary(),
                        step_idx,
                        step,
                        report,
                    ));
                }

                let full_listing_calls = fixture
                    .ctx
                    .last_gc_regions_full_file_listing
                    .lock()
                    .unwrap();
                let last_full_listing = full_listing_calls.last().copied();
                let route_overrides = fixture.ctx.last_gc_regions_route_overrides.lock().unwrap();
                let last_override = route_overrides.last().cloned().unwrap_or_default();

                if step.dropped_region_bias_hit || step.route_override_bias_hit {
                    if last_full_listing != Some(true) {
                        return Err(format!(
                            "phase2-mock-fuzz dropped path missed forced full listing: {} step={} step_state={:?} full_listing_calls={:?}",
                            input.summary(),
                            step_idx,
                            step,
                            *full_listing_calls,
                        ));
                    }
                    if !last_override.contains_key(&fixture.dropped_region_id) {
                        return Err(format!(
                            "phase2-mock-fuzz dropped path missed route override: {} step={} step_state={:?} route_override={:?}",
                            input.summary(),
                            step_idx,
                            step,
                            last_override,
                        ));
                    }
                } else if last_full_listing != Some(requested_full_listing) {
                    return Err(format!(
                        "phase2-mock-fuzz active path full listing mismatch: {} step={} step_state={:?} full_listing_calls={:?}",
                        input.summary(),
                        step_idx,
                        step,
                        *full_listing_calls,
                    ));
                }

                let allowed_override_regions =
                    if step.dropped_region_bias_hit || step.route_override_bias_hit {
                        [fixture.dropped_region_id].into()
                    } else {
                        HashSet::new()
                    };
                let evidence = Phase2MockFuzzEvidence {
                    seed: input.seed,
                    action_index: step_idx,
                    deleted_files: report.deleted_files.clone(),
                    need_retry_regions: report.need_retry_regions.clone(),
                    processed_regions: report.processed_regions.clone(),
                    reachable_files: fixture.reachable_files.clone(),
                    protected_files: fixture.protected_files.clone(),
                    route_overrides: last_override.clone(),
                    allowed_override_regions,
                    expected_target_regions: requested_regions.iter().copied().collect(),
                };
                validate_phase2_mock_evidence(&evidence).map_err(|error| {
                    format!(
                        "phase2-mock-fuzz oracle failure: {} step_state={:?} evidence_summary={} report={:?}",
                        error,
                        step,
                        evidence.concise_summary(),
                        report,
                    )
                })?;
            }
            other => {
                return Err(format!(
                    "phase2-mock-fuzz unexpected report kind: {} step={} step_state={:?} report={:?}",
                    input.summary(),
                    step_idx,
                    step,
                    other,
                ));
            }
        }
    }

    coverage.ensure_minimum_coverage().map_err(|error| {
        format!(
            "phase2-mock-fuzz coverage failure: {} input={} seen={:?}",
            error,
            input.summary(),
            coverage.tags,
        )
    })?;

    Ok(())
}

fn default_phase2_seed_corpus_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests-fuzz/corpus/phase2_mock")
}

fn load_phase2_seed_corpus_inputs() -> Result<Vec<Phase2MockFuzzInput>, String> {
    let seed_dir = std::env::var("GT_PHASE2_SEED_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| default_phase2_seed_corpus_dir());
    let entries = fs::read_dir(&seed_dir).map_err(|error| {
        format!(
            "failed to read phase2 seed dir {}: {error}",
            seed_dir.display()
        )
    })?;

    let mut paths = entries
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            format!(
                "failed to iterate phase2 seed dir {}: {error}",
                seed_dir.display()
            )
        })?
        .into_iter()
        .map(|entry| entry.path())
        .filter(|path| path.is_file())
        .collect::<Vec<_>>();
    paths.sort();

    let mut inputs = Vec::new();
    for path in paths {
        let content = fs::read_to_string(&path).map_err(|error| {
            format!(
                "failed to read phase2 seed file {}: {error}",
                path.display()
            )
        })?;
        let input = Phase2MockFuzzInput::from_seed_metadata(&content).map_err(|error| {
            format!(
                "failed to parse phase2 seed file {}: {error}",
                path.display()
            )
        })?;
        inputs.push(input);
    }

    if inputs.is_empty() {
        return Err(format!(
            "no phase2 seed files found in {}",
            seed_dir.display()
        ));
    }

    Ok(inputs)
}

#[tokio::test]
async fn test_phase2_mock_fuzz_smoke_seed_replay() {
    let input = Phase2MockFuzzInput::smoke(7);
    if let Err(context) = run_phase2_mock_fuzz(input).await {
        panic!("{context}");
    }
}

#[tokio::test]
async fn test_phase2_mock_fuzz_multi_seed_smoke() {
    let inputs = [
        Phase2MockFuzzInput {
            seed: 7,
            action_count: 8,
            dropped_region_bias: 35,
            route_override_bias: 45,
            retry_bias: 50,
            full_listing_bias: 60,
        },
        Phase2MockFuzzInput {
            seed: 11,
            action_count: 10,
            dropped_region_bias: 60,
            route_override_bias: 30,
            retry_bias: 40,
            full_listing_bias: 20,
        },
        Phase2MockFuzzInput {
            seed: 19,
            action_count: 12,
            dropped_region_bias: 20,
            route_override_bias: 70,
            retry_bias: 55,
            full_listing_bias: 50,
        },
    ];

    for input in inputs {
        if let Err(context) = run_phase2_mock_fuzz(input).await {
            panic!("{context}");
        }
    }
}

#[tokio::test]
async fn test_phase2_mock_fuzz_replay_corpus() {
    let inputs = load_phase2_seed_corpus_inputs().unwrap();

    for input in inputs {
        if let Err(context) = run_phase2_mock_fuzz(input).await {
            panic!("phase2 corpus replay failed: {context}");
        }
    }
}
