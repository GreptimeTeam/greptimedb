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

#![no_main]

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaChaRng;
use tests_fuzz::gc_model::runner::{self, FaultInjection, FuzzInput};
use tests_fuzz::utils::get_fuzz_override;

#[derive(Clone, Copy, Debug)]
struct TargetInput {
    seed: u64,
    actions: usize,
    max_regions: u8,
    linger_ms: u32,
    mismatch_rate: u8,
}

impl Arbitrary<'_> for TargetInput {
    fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        let seed = get_fuzz_override::<u64>("SEED").unwrap_or(u.int_in_range(u64::MIN..=u64::MAX)?);
        let mut rng = ChaChaRng::seed_from_u64(seed);
        let actions =
            get_fuzz_override::<usize>("ACTIONS").unwrap_or_else(|| rng.random_range(20..=256));
        let max_regions =
            get_fuzz_override::<u8>("MAX_REGIONS").unwrap_or_else(|| rng.random_range(2..=16));
        let linger_ms =
            get_fuzz_override::<u32>("LINGER_MS").unwrap_or_else(|| rng.random_range(1..=10_000));
        let mismatch_rate =
            get_fuzz_override::<u8>("MISMATCH_RATE").unwrap_or_else(|| rng.random_range(0..=100));

        Ok(Self {
            seed,
            actions,
            max_regions,
            linger_ms,
            mismatch_rate,
        })
    }
}

fuzz_target!(|input: TargetInput| {
    common_telemetry::init_default_ut_logging();
    runner::run(FuzzInput {
        seed: input.seed,
        actions: input.actions,
        max_regions: input.max_regions,
        linger_ms: input.linger_ms,
        mismatch_rate: input.mismatch_rate,
        fault_injection: FaultInjection {
            ignore_temp_refs: get_fuzz_override::<bool>("IGNORE_TEMP_REFS").unwrap_or(false),
            ignore_follower_protection: get_fuzz_override::<bool>("IGNORE_FOLLOWER_PROTECTION")
                .unwrap_or(false),
            ignore_cross_region_refs: get_fuzz_override::<bool>("IGNORE_CROSS_REGION_REFS")
                .unwrap_or(false),
            ignore_lingering: get_fuzz_override::<bool>("IGNORE_LINGERING").unwrap_or(false),
        },
    });
});
