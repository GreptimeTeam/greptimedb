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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arbitrary::{Arbitrary, Unstructured};
use common_telemetry::info;
use libfuzzer_sys::fuzz_target;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaChaRng;
use snafu::ResultExt;
use sqlx::{MySql, Pool};
use strum::EnumIter;
use tests_fuzz::context::{TableContext, TableContextRef};
use tests_fuzz::error::{self, Result};
use tests_fuzz::fake::{
    merge_two_word_map_fn, random_capitalize_map, uppercase_and_keyword_backtick_map,
    MappedGenerator, WordGenerator,
};
use tests_fuzz::generator::alter_expr::AlterExprAddColumnGeneratorBuilder;
use tests_fuzz::generator::create_expr::CreateTableExprGeneratorBuilder;
use tests_fuzz::generator::Generator;
use tests_fuzz::ir::{AlterTableExpr, AlterTableOperation, CreateTableExpr};
use tests_fuzz::translator::mysql::alter_expr::AlterTableExprTranslator;
use tests_fuzz::translator::mysql::create_expr::CreateTableExprTranslator;
use tests_fuzz::translator::DslTranslator;
use tests_fuzz::utils::{
    get_gt_fuzz_input_max_columns, init_greptime_connections_via_env, Connections,
};
use tests_fuzz::validator;
use tokio::task::JoinSet;
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

#[derive(Debug, EnumIter)]
enum AlterTableOption {
    AddColumn,
    DropColumn,
    RenameTable,
    ModifyDataType,
}

fn generate_create_table_expr<R: Rng + 'static>(rng: &mut R) -> Result<CreateTableExpr> {
    let max_columns = get_gt_fuzz_input_max_columns();
    let columns = rng.gen_range(2..max_columns);
    let mut with_clause = HashMap::new();
    if rng.gen_bool(0.5) {
        with_clause.insert("append_mode".to_string(), "true".to_string());
    }
    let create_table_generator = CreateTableExprGeneratorBuilder::default()
        .name_generator(Box::new(MappedGenerator::new(
            WordGenerator,
            merge_two_word_map_fn(random_capitalize_map, uppercase_and_keyword_backtick_map),
        )))
        .columns(columns)
        .engine("mito")
        .with_clause(with_clause)
        .build()
        .unwrap();
    create_table_generator.generate(rng)
}

fn generate_alter_table_add_column_expr<R: Rng + 'static>(
    table_ctx: TableContextRef,
    rng: &mut R,
) -> Result<AlterTableExpr> {
    let location = rng.gen_bool(0.5);
    let expr_generator = AlterExprAddColumnGeneratorBuilder::default()
        // random capitalize column name
        .name_generator(Box::new(MappedGenerator::new(
            WordGenerator,
            merge_two_word_map_fn(random_capitalize_map, uppercase_and_keyword_backtick_map),
        )))
        .table_ctx(table_ctx)
        .location(location)
        .build()
        .unwrap();
    expr_generator.generate(rng)
}

impl Arbitrary<'_> for FuzzInput {
    fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        let seed = u.int_in_range(u64::MIN..=u64::MAX)?;
        let mut rng = ChaChaRng::seed_from_u64(seed);
        let actions = rng.gen_range(1..256);

        Ok(FuzzInput { seed, actions })
    }
}

/// Parallel alter table fuzz target
async fn unstable_execute_parallel_alter_table(ctx: FuzzContext, input: FuzzInput) -> Result<()> {
    info!("input: {input:?}");
    let mut rng = ChaChaRng::seed_from_u64(input.seed);

    // Create table
    let expr = generate_create_table_expr(&mut rng).unwrap();
    let translator = CreateTableExprTranslator;
    let sql = translator.translate(&expr)?;
    let result = sqlx::query(&sql)
        .execute(&ctx.greptime)
        .await
        .context(error::ExecuteQuerySnafu { sql: &sql })?;
    info!("Create table: {sql}, result: {result:?}");

    // Parallel add columns at once, and validate columns only after all columns are added
    let table_ctx = Arc::new(TableContext::from(&expr));
    let mut after_all_alter_table_ctx = table_ctx.clone();
    let mut pool: JoinSet<Result<()>> = JoinSet::new();

    let mut used_column_names = HashSet::new();
    let mut action = 0;
    while action < input.actions {
        let expr = generate_alter_table_add_column_expr(table_ctx.clone(), &mut rng).unwrap();
        if let AlterTableOperation::AddColumn { column, .. } = &expr.alter_kinds {
            if used_column_names.contains(&column.name) {
                info!("Column name already used: {}, retrying", column.name.value);
                continue;
            }
            used_column_names.insert(column.name.clone());
            action += 1;
        }
        let translator = AlterTableExprTranslator;
        let sql = translator.translate(&expr)?;
        if let AlterTableOperation::AddColumn { .. } = expr.alter_kinds {
            // Applies changes
            after_all_alter_table_ctx = Arc::new(
                Arc::unwrap_or_clone(after_all_alter_table_ctx)
                    .alter(expr.clone())
                    .unwrap(),
            );
            let conn = ctx.greptime.clone();
            pool.spawn(async move {
                let result = sqlx::query(&sql)
                    .execute(&conn)
                    .await
                    .context(error::ExecuteQuerySnafu { sql: &sql })?;
                info!("Alter table: {sql}, result: {result:?}");
                Ok(())
            });
            continue;
        }
    }

    // run concurrently
    let output = pool.join_all().await;
    for (idx, err) in output.iter().enumerate().filter_map(|(i, u)| match u {
        Ok(_) => None,
        Err(e) => Some((i, e)),
    }) {
        // found first error and return
        // but also print all errors for debugging
        common_telemetry::error!("Error at parallel task {}: {err:?}", idx);
    }

    // return first error if exist
    if let Some(err) = output.into_iter().filter_map(|u| u.err()).next() {
        return Err(err);
    }

    let table_ctx = after_all_alter_table_ctx;
    // Validates columns
    let mut column_entries =
        validator::column::fetch_columns(&ctx.greptime, "public".into(), table_ctx.name.clone())
            .await?;
    column_entries.sort_by(|a, b| a.column_name.cmp(&b.column_name));
    let mut columns = table_ctx.columns.clone();
    columns.sort_by(|a, b| a.name.value.cmp(&b.name.value));
    validator::column::assert_eq(&column_entries, &columns)?;

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

fuzz_target!(|input: FuzzInput| {
    common_telemetry::init_default_ut_logging();
    common_runtime::block_on_global(async {
        let Connections { mysql, .. } = init_greptime_connections_via_env().await;
        let ctx = FuzzContext {
            greptime: mysql.expect("mysql connection init must be succeed"),
        };
        unstable_execute_parallel_alter_table(ctx, input)
            .await
            .unwrap_or_else(|err| panic!("fuzz test must be succeed: {err:?}"));
    })
});
