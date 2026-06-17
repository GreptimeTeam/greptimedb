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
use tests_fuzz::gc_e2e::phase3_harness::{
    self, Phase3E2eInput, Phase3E2eScenarioKind, Phase3E2eTableShape,
};
use tests_fuzz::utils::get_fuzz_override;

#[derive(Clone, Copy, Debug)]
struct TargetInput {
    seed: u64,
    flush_rounds: usize,
    full_file_listing: bool,
    compaction_wait_secs: u64,
    multi_region: bool,
    repartition_like: bool,
    follower_like: bool,
    pre_gc_protection: bool,
    admin_gc_dropped: bool,
    post_migration_gc: bool,
}

impl Arbitrary<'_> for TargetInput {
    fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        let seed = get_fuzz_override::<u64>("SEED").unwrap_or(u.arbitrary()?);
        let mut rng = ChaChaRng::seed_from_u64(seed);
        let post_migration_gc_override = get_fuzz_override::<bool>("POST_MIGRATION_GC");
        let admin_gc_dropped_override = get_fuzz_override::<bool>("ADMIN_GC_DROPPED");
        let pre_gc_protection_override = get_fuzz_override::<bool>("PRE_GC_PROTECTION");

        let post_migration_gc = post_migration_gc_override.unwrap_or_else(|| {
            !admin_gc_dropped_override.unwrap_or(false)
                && !pre_gc_protection_override.unwrap_or(false)
                && rng.random_bool(0.10)
        });
        let admin_gc_dropped = admin_gc_dropped_override
            .unwrap_or_else(|| !post_migration_gc && rng.random_bool(0.12));
        let pre_gc_protection = pre_gc_protection_override
            .unwrap_or_else(|| !post_migration_gc && !admin_gc_dropped && rng.random_bool(0.20));

        Ok(Self {
            seed,
            flush_rounds: get_fuzz_override::<usize>("FLUSH_ROUNDS")
                .unwrap_or_else(|| rng.random_range(2..=4)),
            full_file_listing: get_fuzz_override::<bool>("FULL_FILE_LISTING")
                .unwrap_or_else(|| rng.random_bool(0.5)),
            compaction_wait_secs: get_fuzz_override::<u64>("COMPACTION_WAIT_SECS")
                .unwrap_or_else(|| rng.random_range(2..=3)),
            multi_region: get_fuzz_override::<bool>("MULTI_REGION")
                .unwrap_or_else(|| rng.random_bool(0.5)),
            repartition_like: get_fuzz_override::<bool>("REPARTITION_LIKE")
                .unwrap_or_else(|| rng.random_bool(0.25)),
            follower_like: get_fuzz_override::<bool>("FOLLOWER_LIKE")
                .unwrap_or_else(|| rng.random_bool(0.25)),
            pre_gc_protection,
            admin_gc_dropped,
            post_migration_gc,
        })
    }
}

fuzz_target!(|input: TargetInput| {
    common_telemetry::init_default_ut_logging();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(phase3_harness::run_phase3_e2e_gc_cycle(Phase3E2eInput {
            seed: input.seed,
            flush_rounds: if input.post_migration_gc {
                input.flush_rounds.max(4)
            } else {
                input.flush_rounds
            },
            full_file_listing: input.full_file_listing
                || input.pre_gc_protection
                || input.admin_gc_dropped
                || input.post_migration_gc,
            compaction_wait_secs: input.compaction_wait_secs,
            table_shape: if input.multi_region
                || input.pre_gc_protection
                || input.admin_gc_dropped
                || input.post_migration_gc
            {
                Phase3E2eTableShape::MultiRegion
            } else {
                Phase3E2eTableShape::SingleRegion
            },
            scenario_kind: if input.pre_gc_protection {
                Phase3E2eScenarioKind::RepartitionPreGcProtection
            } else if input.admin_gc_dropped {
                Phase3E2eScenarioKind::RepartitionAdminGcDroppedRegion
            } else if input.post_migration_gc {
                Phase3E2eScenarioKind::PostMigrationAdminGc
            } else if input.repartition_like && input.multi_region {
                Phase3E2eScenarioKind::RepartitionLike
            } else if input.follower_like && input.multi_region {
                Phase3E2eScenarioKind::FollowerLike
            } else {
                Phase3E2eScenarioKind::CompactGc
            },
        }));
});
