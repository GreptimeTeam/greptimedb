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

use arbitrary::{Arbitrary, Unstructured};
use common_telemetry::info;
use libfuzzer_sys::fuzz_target;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaChaRng;
use snafu::ResultExt;
use sqlx::{MySql, Pool};
use tests_fuzz::context::{TableContext, TableContextRef};
use tests_fuzz::error::{self, Result};
use tests_fuzz::fake::{
    MappedGenerator, WordGenerator, merge_two_word_map_fn, random_capitalize_map,
    uppercase_and_keyword_backtick_map,
};
use tests_fuzz::generator::Generator;
use tests_fuzz::generator::create_expr::CreateTableExprGeneratorBuilder;
use tests_fuzz::generator::repartition_expr::{
    MergePartitionExprGeneratorBuilder, SplitPartitionExprGeneratorBuilder,
};
use tests_fuzz::ir::{CreateTableExpr, MySQLTsColumnTypeGenerator, RepartitionExpr};
use tests_fuzz::translator::DslTranslator;
use tests_fuzz::translator::mysql::create_expr::CreateTableExprTranslator;
use tests_fuzz::translator::mysql::repartition_expr::RepartitionExprTranslator;
use tests_fuzz::utils::{
    Connections, get_fuzz_override, get_gt_fuzz_input_max_alter_actions,
    init_greptime_connections_via_env,
};
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

fn generate_create_expr<R: Rng + 'static>(
    input: &FuzzInput,
    rng: &mut R,
) -> Result<CreateTableExpr> {
    let create_table_generator = CreateTableExprGeneratorBuilder::default()
        .name_generator(Box::new(MappedGenerator::new(
            WordGenerator,
            merge_two_word_map_fn(random_capitalize_map, uppercase_and_keyword_backtick_map),
        )))
        .columns(5)
        .partition(input.partitions)
        .engine("mito")
        .ts_column_type_generator(Box::new(MySQLTsColumnTypeGenerator))
        .build()
        .unwrap();
    create_table_generator.generate(rng)
}

async fn execute_repartition_table(ctx: FuzzContext, input: FuzzInput) -> Result<()> {
    info!("input: {input:?}");
    let mut rng = ChaChaRng::seed_from_u64(input.seed);
    // Create table
    let expr = generate_create_expr(&input, &mut rng).unwrap();
    let translator = CreateTableExprTranslator;
    let sql = translator.translate(&expr)?;
    let result = sqlx::query(&sql)
        .execute(&ctx.greptime)
        .await
        .context(error::ExecuteQuerySnafu { sql: &sql })?;
    info!("Create table: {sql}, result: {result:?}");

    // Repartition table
    let mut table_ctx = Arc::new(TableContext::from(&expr));
    for i in 0..input.actions {
        let partition_num = table_ctx.partition.as_ref().unwrap().exprs.len();
        info!(
            "partition_num: {partition_num}, action: {}/{}",
            i + 1,
            input.actions,
        );

        let expr = repartition_operation(&table_ctx, &mut rng)?;
        let translator = RepartitionExprTranslator;
        let sql = translator.translate(&expr)?;
        info!("Repartition sql: {sql}");
        let result = sqlx::query(&sql)
            .execute(&ctx.greptime)
            .await
            .context(error::ExecuteQuerySnafu { sql: &sql })?;
        info!("result: {result:?}");
        table_ctx = Arc::new(Arc::unwrap_or_clone(table_ctx).repartition(expr).unwrap());

        // Validates partition expression
        let partition_entries = validator::partition::fetch_partitions_info_schema(
            &ctx.greptime,
            "public".into(),
            &table_ctx.name,
        )
        .await?;

        validator::partition::assert_partitions(
            table_ctx.partition.as_ref().unwrap(),
            &partition_entries,
        )?;
        // TODO(weny): inserts data and validates the data
    }

    // Cleans up
    let table_name = table_ctx.name.clone();
    let sql = format!("DROP TABLE {}", table_name);
    let result = sqlx::query(&sql)
        .execute(&ctx.greptime)
        .await
        .context(error::ExecuteQuerySnafu { sql })?;
    info!("Drop table: {}, result: {result:?}", table_name);
    ctx.close().await;

    Ok(())
}

fn repartition_operation<R: Rng + 'static>(
    table_ctx: &TableContextRef,
    rng: &mut R,
) -> Result<RepartitionExpr> {
    let split = rng.random_bool(0.5);
    // If partition expression count is less than 2, we split it intorst.
    if table_ctx.partition.as_ref().unwrap().exprs.len() <= 2 || split {
        let expr = SplitPartitionExprGeneratorBuilder::default()
            .table_ctx(table_ctx.clone())
            .build()
            .unwrap()
            .generate(rng)?;

        Ok(RepartitionExpr::Split(expr))
    } else {
        let expr = MergePartitionExprGeneratorBuilder::default()
            .table_ctx(table_ctx.clone())
            .build()
            .unwrap()
            .generate(rng)?;

        Ok(RepartitionExpr::Merge(expr))
    }
}

fuzz_target!(|input: FuzzInput| {
    common_telemetry::init_default_ut_logging();
    common_runtime::block_on_global(async {
        let Connections { mysql } = init_greptime_connections_via_env().await;
        let ctx = FuzzContext {
            greptime: mysql.expect("mysql connection init must be succeed"),
        };
        execute_repartition_table(ctx, input)
            .await
            .unwrap_or_else(|err| panic!("fuzz test must be succeed: {err:?}"));
    })
});
