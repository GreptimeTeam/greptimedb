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

use std::sync::Arc;

use common_telemetry::info;
use libfuzzer_sys::arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaChaRng;
use snafu::ResultExt;
use sqlx::{MySql, Pool};
use tests_fuzz::context::{TableContext, TableContextRef};
use tests_fuzz::error::{self, Result};
use tests_fuzz::fake::{
    merge_two_word_map_fn, random_capitalize_map, uppercase_and_keyword_backtick_map,
    MappedGenerator, WordGenerator,
};
use tests_fuzz::generator::create_expr::{
    CreateLogicalTableExprGeneratorBuilder, CreateTableExprGeneratorBuilder,
};
use tests_fuzz::generator::Generator;
use tests_fuzz::ir::CreateTableExpr;
use tests_fuzz::translator::mysql::create_expr::CreateTableExprTranslator;
use tests_fuzz::translator::DslTranslator;
use tests_fuzz::utils::{init_greptime_connections, Connections};
use tests_fuzz::validator;

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
}

impl Arbitrary<'_> for FuzzInput {
    fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        let seed = u.int_in_range(u64::MIN..=u64::MAX)?;
        let mut rng = ChaChaRng::seed_from_u64(seed);
        let actions = rng.gen_range(2..30);
        Ok(FuzzInput { actions, seed })
    }
}

fn generate_create_physical_table_expr<R: Rng + 'static>(rng: &mut R) -> Result<CreateTableExpr> {
    let if_not_exists = rng.gen_bool(0.5);
    let columns = rng.gen_range(2..30);
    let create_physical_table_generator = CreateTableExprGeneratorBuilder::default()
        .name_generator(Box::new(MappedGenerator::new(
            WordGenerator,
            merge_two_word_map_fn(random_capitalize_map, uppercase_and_keyword_backtick_map),
        )))
        .columns(columns)
        .engine("metric")
        .if_not_exists(if_not_exists)
        .with_clause([("physical_metric_table".to_string(), "".to_string())])
        .build()
        .unwrap();
    create_physical_table_generator.generate(rng)
}

fn generate_create_logical_table_expr<R: Rng + 'static>(
    table_ctx: TableContextRef,
    rng: &mut R,
) -> Result<CreateTableExpr> {
    let if_not_exists = rng.gen_bool(0.5);
    let columns = rng.gen_range(2..30);
    let overlapped_columns = rng.gen_range(0..table_ctx.columns.len() - 1);
    let create_table_generator = CreateTableExprGeneratorBuilder::default()
        .name_generator(Box::new(MappedGenerator::new(
            WordGenerator,
            merge_two_word_map_fn(random_capitalize_map, uppercase_and_keyword_backtick_map),
        )))
        .columns(columns)
        .engine("metric")
        .if_not_exists(if_not_exists)
        .with_clause([("on_physical_table".to_string(), table_ctx.name.to_string())])
        .build()
        .unwrap();

    let create_logical_table_generator = CreateLogicalTableExprGeneratorBuilder::default()
        .table_ctx(table_ctx)
        .table_generator(create_table_generator)
        .overlapped_columns(overlapped_columns)
        .build()
        .unwrap();
    create_logical_table_generator.generate(rng)
}

async fn execute_create_logic_table(ctx: FuzzContext, input: FuzzInput) -> Result<()> {
    info!("input: {input:?}");
    let mut rng = ChaChaRng::seed_from_u64(input.seed);

    // Create physical table
    let create_physical_table_expr = generate_create_physical_table_expr(&mut rng)?;
    let translator = CreateTableExprTranslator;
    let sql = translator.translate(&create_physical_table_expr)?;
    let result = sqlx::query(&sql)
        .execute(&ctx.greptime)
        .await
        .context(error::ExecuteQuerySnafu { sql: &sql })?;
    info!("Create physical table: {sql}, result: {result:?}");

    // Create logical table
    let table_ctx = Arc::new(TableContext::from(&create_physical_table_expr));
    for _ in 0..input.actions {
        let create_logical_table_expr =
            generate_create_logical_table_expr(table_ctx.clone(), &mut rng)?;
        let translator = CreateTableExprTranslator;
        let sql = translator.translate(&create_logical_table_expr)?;
        let result = sqlx::query(&sql)
            .execute(&ctx.greptime)
            .await
            .context(error::ExecuteQuerySnafu { sql: &sql })?;
        info!("Create logical table: {sql}, result: {result:?}");

        // Validate columns
        let mut column_entries = validator::column::fetch_columns(
            &ctx.greptime,
            "public".into(),
            create_logical_table_expr.table_name.clone(),
        )
        .await?;
        column_entries.sort_by(|a, b| a.column_name.cmp(&b.column_name));
        let mut columns = create_logical_table_expr.columns.clone();
        columns.sort_by(|a, b| a.name.value.cmp(&b.name.value));
        validator::column::assert_eq(&column_entries, &columns)?;

        // Cleans up
        let sql = format!("DROP TABLE {}", create_logical_table_expr.table_name);
        let result = sqlx::query(&sql)
            .execute(&ctx.greptime)
            .await
            .context(error::ExecuteQuerySnafu { sql: &sql })?;
        info!(
            "Drop table: {}, result: {result:?}",
            create_logical_table_expr.table_name
        );
    }
    ctx.close().await;

    Ok(())
}

fuzz_target!(|input: FuzzInput| {
    common_telemetry::init_default_ut_logging();
    common_runtime::block_on_write(async {
        let Connections { mysql } = init_greptime_connections().await;
        let ctx = FuzzContext {
            greptime: mysql.expect("mysql connection init must be succeed"),
        };
        execute_create_logic_table(ctx, input)
            .await
            .unwrap_or_else(|err| panic!("fuzz test must be succeed: {err:?}"));
    })
});
