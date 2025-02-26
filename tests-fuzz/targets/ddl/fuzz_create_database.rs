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

use common_telemetry::info;
use libfuzzer_sys::arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaChaRng;
use snafu::ResultExt;
use sqlx::{MySql, Pool};
use tests_fuzz::error::{self, Result};
use tests_fuzz::fake::{
    merge_two_word_map_fn, random_capitalize_map, uppercase_and_keyword_backtick_map,
    MappedGenerator, WordGenerator,
};
use tests_fuzz::generator::create_expr::CreateDatabaseExprGeneratorBuilder;
use tests_fuzz::generator::Generator;
use tests_fuzz::ir::CreateDatabaseExpr;
use tests_fuzz::translator::mysql::create_expr::CreateDatabaseExprTranslator;
use tests_fuzz::translator::DslTranslator;
use tests_fuzz::utils::{init_greptime_connections_via_env, Connections};

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
}

impl Arbitrary<'_> for FuzzInput {
    fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        let seed = u.int_in_range(u64::MIN..=u64::MAX)?;
        Ok(FuzzInput { seed })
    }
}

fn generate_expr(input: FuzzInput) -> Result<CreateDatabaseExpr> {
    let mut rng = ChaChaRng::seed_from_u64(input.seed);
    let if_not_exists = rng.gen_bool(0.5);
    let create_database_generator = CreateDatabaseExprGeneratorBuilder::default()
        .name_generator(Box::new(MappedGenerator::new(
            WordGenerator,
            merge_two_word_map_fn(random_capitalize_map, uppercase_and_keyword_backtick_map),
        )))
        .if_not_exists(if_not_exists)
        .build()
        .unwrap();
    create_database_generator.generate(&mut rng)
}

async fn execute_create_database(ctx: FuzzContext, input: FuzzInput) -> Result<()> {
    info!("input: {input:?}");
    let expr = generate_expr(input)?;
    let translator = CreateDatabaseExprTranslator;
    let sql = translator.translate(&expr)?;
    let result = sqlx::query(&sql)
        .execute(&ctx.greptime)
        .await
        .context(error::ExecuteQuerySnafu { sql: &sql })?;
    info!("Create database: {sql}, result: {result:?}");

    // Cleans up
    let sql = format!("DROP DATABASE {}", expr.database_name);
    let result = sqlx::query(&sql)
        .execute(&ctx.greptime)
        .await
        .context(error::ExecuteQuerySnafu { sql })?;
    info!("Drop database: {}, result: {result:?}", expr.database_name);
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
        execute_create_database(ctx, input)
            .await
            .unwrap_or_else(|err| panic!("fuzz test must be succeed: {err:?}"));
    })
});
