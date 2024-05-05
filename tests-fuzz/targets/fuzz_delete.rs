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
#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

use std::sync::Arc;

use common_telemetry::info;
use libfuzzer_sys::arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaChaRng;
use snafu::{ensure, ResultExt};
use sqlx::{Executor, MySql, Pool};
use tests_fuzz::context::{TableContext, TableContextRef};
use tests_fuzz::error::{self, Result};
use tests_fuzz::fake::{
    merge_two_word_map_fn, random_capitalize_map, uppercase_and_keyword_backtick_map,
    MappedGenerator, WordGenerator,
};
use tests_fuzz::generator::create_expr::CreateTableExprGeneratorBuilder;
use tests_fuzz::generator::insert_expr::InsertExprGeneratorBuilder;
use tests_fuzz::generator::Generator;
use tests_fuzz::ir::{CreateTableExpr, InsertIntoExpr};
use tests_fuzz::translator::mysql::create_expr::CreateTableExprTranslator;
use tests_fuzz::translator::mysql::insert_expr::InsertIntoExprTranslator;
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

#[derive(Copy, Clone, Debug)]
struct FuzzInput {
    seed: u64,
    columns: usize,
    rows: usize,
}

impl Arbitrary<'_> for FuzzInput {
    fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        let seed = u.int_in_range(u64::MIN..=u64::MAX)?;
        let mut rng = ChaChaRng::seed_from_u64(seed);
        let columns = rng.gen_range(2..30);
        let rows = rng.gen_range(1..4096);
        Ok(FuzzInput {
            columns,
            rows,
            seed,
        })
    }
}

fn generate_create_expr<R: Rng + 'static>(
    input: FuzzInput,
    rng: &mut R,
) -> Result<CreateTableExpr> {
    let create_table_generator = CreateTableExprGeneratorBuilder::default()
        .name_generator(Box::new(MappedGenerator::new(
            WordGenerator,
            merge_two_word_map_fn(random_capitalize_map, uppercase_and_keyword_backtick_map),
        )))
        .columns(input.columns)
        .engine("mito")
        .build()
        .unwrap();
    create_table_generator.generate(rng)
}

fn generate_insert_expr<R: Rng + 'static>(
    input: FuzzInput,
    rng: &mut R,
    table_ctx: TableContextRef,
) -> Result<InsertIntoExpr> {
    let omit_column_list = rng.gen_bool(0.2);

    let insert_generator = InsertExprGeneratorBuilder::default()
        .table_ctx(table_ctx)
        .omit_column_list(omit_column_list)
        .rows(input.rows)
        .build()
        .unwrap();
    insert_generator.generate(rng)
}

async fn execute_delete(ctx: FuzzContext, input: FuzzInput) -> Result<()> {
    info!("input: {input:?}");

    Ok(())
}

fuzz_target!(|input: FuzzInput| {
    common_telemetry::init_default_ut_logging();
    common_runtime::block_on_write(async {
        let Connections { mysql } = init_greptime_connections_via_env().await;
        let ctx = FuzzContext {
            greptime: mysql.expect("mysql connection init must be succeed"),
        };
        execute_delete(ctx, input)
            .await
            .unwrap_or_else(|err| panic!("fuzz test must be succeed: {err:?}"));
    })
});
