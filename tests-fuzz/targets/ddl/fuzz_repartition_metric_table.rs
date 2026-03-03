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
use common_telemetry::info;
use libfuzzer_sys::fuzz_target;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaChaRng;
use sqlx::{MySql, Pool};
use tests_fuzz::error::Result;
use tests_fuzz::utils::{
    Connections, get_fuzz_override, get_gt_fuzz_input_max_alter_actions,
    init_greptime_connections_via_env,
};

struct FuzzContext {
    greptime: Pool<MySql>,
}

impl FuzzContext {
    async fn close(self) {
        self.greptime.close().await;
    }
}

#[derive(Clone, Debug)]
struct FuzzInput {
    seed: u64,
    actions: usize,
    partitions: usize,
}

impl Arbitrary<'_> for FuzzInput {
    fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        let seed = get_fuzz_override::<u64>("SEED").unwrap_or(u.int_in_range(u64::MIN..=u64::MAX)?);
        let mut rng = ChaChaRng::seed_from_u64(seed);
        let partitions =
            get_fuzz_override::<usize>("PARTITIONS").unwrap_or_else(|| rng.random_range(2..8));
        let max_actions = get_gt_fuzz_input_max_alter_actions();
        let actions = get_fuzz_override::<usize>("ACTIONS")
            .unwrap_or_else(|| rng.random_range(1..max_actions));

        Ok(FuzzInput {
            seed,
            actions,
            partitions,
        })
    }
}

async fn execute_repartition_metric_table(ctx: FuzzContext, input: FuzzInput) -> Result<()> {
    info!("fuzz_repartition_metric_table skeleton input: {input:?}");
    info!(
        "commit-1 skeleton only: seed={}, actions={}, partitions={}",
        input.seed, input.actions, input.partitions
    );

    ctx.close().await;
    Ok(())
}

fuzz_target!(|input: FuzzInput| {
    common_telemetry::init_default_ut_logging();
    common_runtime::block_on_global(async {
        let Connections { mysql } = init_greptime_connections_via_env().await;
        let ctx = FuzzContext {
            greptime: mysql.expect("mysql connection init must be succeed"),
        };
        execute_repartition_metric_table(ctx, input)
            .await
            .unwrap_or_else(|err| panic!("fuzz test must be succeed: {err:?}"));
    })
});
