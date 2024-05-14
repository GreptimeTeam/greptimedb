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
use snafu::{ensure, ResultExt};
use sqlx::{Executor, MySql, Pool};
use tests_fuzz::context::{TableContext, TableContextRef};
use tests_fuzz::error::{self, Result};
use tests_fuzz::fake::WordGenerator;
use tests_fuzz::generator::alter_expr::{
    AlterExprAddColumnGeneratorBuilder, AlterExprDropColumnGeneratorBuilder,
    AlterExprRenameGeneratorBuilder,
};
use tests_fuzz::generator::create_expr::CreateTableExprGeneratorBuilder;
use tests_fuzz::generator::insert_expr::InsertExprGeneratorBuilder;
use tests_fuzz::generator::Generator;
use tests_fuzz::ir::{droppable_columns, AlterTableExpr, CreateTableExpr, InsertIntoExpr};
use tests_fuzz::translator::mysql::alter_expr::AlterTableExprTranslator;
use tests_fuzz::translator::mysql::create_expr::CreateTableExprTranslator;
use tests_fuzz::translator::mysql::insert_expr::InsertIntoExprTranslator;
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

#[derive(Clone, Debug, Copy)]
struct FuzzInput {
    seed: u64,
    actions: usize,
    rows: usize,
}

fn generate_create_table_expr<R: Rng + 'static>(rng: &mut R) -> Result<CreateTableExpr> {
    let columns = rng.gen_range(2..30);
    let create_table_generator = CreateTableExprGeneratorBuilder::default()
        .name_generator(Box::new(WordGenerator))
        .columns(columns)
        .engine("mito")
        .build()
        .unwrap();
    create_table_generator.generate(rng)
}

fn generate_alter_table_expr<R: Rng + 'static>(
    table_ctx: TableContextRef,
    rng: &mut R,
) -> Result<AlterTableExpr> {
    let rename = rng.gen_bool(0.2);
    if rename {
        let expr_generator = AlterExprRenameGeneratorBuilder::default()
            .table_ctx(table_ctx)
            .name_generator(Box::new(WordGenerator))
            .build()
            .unwrap();
        expr_generator.generate(rng)
    } else {
        let drop_column = rng.gen_bool(0.5) && !droppable_columns(&table_ctx.columns).is_empty();
        if drop_column {
            let expr_generator = AlterExprDropColumnGeneratorBuilder::default()
                .table_ctx(table_ctx)
                .build()
                .unwrap();
            expr_generator.generate(rng)
        } else {
            let location = rng.gen_bool(0.5);
            let expr_generator = AlterExprAddColumnGeneratorBuilder::default()
                .table_ctx(table_ctx)
                .location(location)
                .build()
                .unwrap();
            expr_generator.generate(rng)
        }
    }
}

impl Arbitrary<'_> for FuzzInput {
    fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        let seed = u.int_in_range(u64::MIN..=u64::MAX)?;
        let mut rng = ChaChaRng::seed_from_u64(seed);
        let actions = rng.gen_range(1..256);
        let insertions = rng.gen_range(1..1024);

        Ok(FuzzInput {
            seed,
            actions,
            rows: insertions,
        })
    }
}

fn generate_insert_expr<R: Rng + 'static>(
    input: FuzzInput,
    rng: &mut R,
    table_ctx: TableContextRef,
) -> Result<InsertIntoExpr> {
    let insert_generator = InsertExprGeneratorBuilder::default()
        .table_ctx(table_ctx)
        .rows(input.rows)
        .build()
        .unwrap();
    insert_generator.generate(rng)
}

async fn execute_alter_table(ctx: FuzzContext, input: FuzzInput) -> Result<()> {
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

    // Alter table actions
    let mut table_ctx = Arc::new(TableContext::from(&expr));
    for _ in 0..input.actions {
        let expr = generate_alter_table_expr(table_ctx.clone(), &mut rng).unwrap();
        let translator = AlterTableExprTranslator;
        let sql = translator.translate(&expr)?;
        let result = sqlx::query(&sql)
            .execute(&ctx.greptime)
            .await
            .context(error::ExecuteQuerySnafu { sql: &sql })?;
        info!("Alter table: {sql}, result: {result:?}");
        // Applies changes
        let mut t = Arc::unwrap_or_clone(table_ctx);
        t.alter(expr).unwrap();
        table_ctx = Arc::new(t);

        // Validates columns
        let mut column_entries = validator::column::fetch_columns_via_mysql(
            &ctx.greptime,
            "public".into(),
            table_ctx.name.clone(),
        )
        .await?;
        column_entries.sort_by(|a, b| a.column_name.cmp(&b.column_name));
        let mut columns = table_ctx.columns.clone();
        columns.sort_by(|a, b| a.name.value.cmp(&b.name.value));
        validator::column::assert_eq(&column_entries, &columns)?;

        // insertions
        let insert_expr = generate_insert_expr(input, &mut rng, table_ctx.clone())?;
        let translator = InsertIntoExprTranslator;
        let sql = translator.translate(&insert_expr)?;
        let result = ctx
            .greptime
            // unprepared query, see <https://github.com/GreptimeTeam/greptimedb/issues/3500>
            .execute(sql.as_str())
            .await
            .context(error::ExecuteQuerySnafu { sql: &sql })?;

        ensure!(
            result.rows_affected() == input.rows as u64,
            error::AssertSnafu {
                reason: format!(
                    "expected rows affected: {}, actual: {}",
                    input.rows,
                    result.rows_affected(),
                )
            }
        );
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

fuzz_target!(|input: FuzzInput| {
    common_telemetry::init_default_ut_logging();
    common_runtime::block_on_write(async {
        let Connections { mysql } = init_greptime_connections().await;
        let ctx = FuzzContext {
            greptime: mysql.expect("mysql connection init must be succeed"),
        };
        execute_alter_table(ctx, input)
            .await
            .unwrap_or_else(|err| panic!("fuzz test must be succeed: {err:?}"));
    })
});
