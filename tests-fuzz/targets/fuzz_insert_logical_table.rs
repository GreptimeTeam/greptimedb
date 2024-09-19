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

use std::collections::HashMap;
use std::sync::Arc;

use common_telemetry::info;
use common_time::util::current_time_millis;
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
use tests_fuzz::generator::create_expr::{
    CreateLogicalTableExprGeneratorBuilder, CreatePhysicalTableExprGeneratorBuilder,
};
use tests_fuzz::generator::insert_expr::InsertExprGeneratorBuilder;
use tests_fuzz::generator::Generator;
use tests_fuzz::ir::{
    generate_random_value, generate_unique_timestamp_for_mysql, replace_default,
    sort_by_primary_keys, CreateTableExpr, InsertIntoExpr,
};
use tests_fuzz::translator::mysql::create_expr::CreateTableExprTranslator;
use tests_fuzz::translator::mysql::insert_expr::InsertIntoExprTranslator;
use tests_fuzz::translator::DslTranslator;
use tests_fuzz::utils::{
    compact_table, flush_memtable, get_gt_fuzz_input_max_rows, get_gt_fuzz_input_max_tables,
    init_greptime_connections_via_env, Connections,
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

#[derive(Copy, Clone, Debug)]
struct FuzzInput {
    seed: u64,
    tables: usize,
    rows: usize,
}

impl Arbitrary<'_> for FuzzInput {
    fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        let seed = u.int_in_range(u64::MIN..=u64::MAX)?;
        let mut rng = ChaChaRng::seed_from_u64(seed);
        let max_tables = get_gt_fuzz_input_max_tables();
        let tables = rng.gen_range(1..max_tables);
        let max_row = get_gt_fuzz_input_max_rows();
        let rows = rng.gen_range(1..max_row);
        Ok(FuzzInput { tables, seed, rows })
    }
}

fn generate_create_physical_table_expr<R: Rng + 'static>(rng: &mut R) -> Result<CreateTableExpr> {
    let physical_table_if_not_exists = rng.gen_bool(0.5);
    let mut with_clause = HashMap::new();
    if rng.gen_bool(0.5) {
        with_clause.insert("append_mode".to_string(), "true".to_string());
    }
    let create_physical_table_expr = CreatePhysicalTableExprGeneratorBuilder::default()
        .name_generator(Box::new(MappedGenerator::new(
            WordGenerator,
            merge_two_word_map_fn(random_capitalize_map, uppercase_and_keyword_backtick_map),
        )))
        .if_not_exists(physical_table_if_not_exists)
        .with_clause(with_clause)
        .build()
        .unwrap();
    create_physical_table_expr.generate(rng)
}

fn generate_create_logical_table_expr<R: Rng + 'static>(
    physical_table_ctx: TableContextRef,
    rng: &mut R,
) -> Result<CreateTableExpr> {
    let labels = rng.gen_range(1..=5);
    let logical_table_if_not_exists = rng.gen_bool(0.5);

    let create_logical_table_expr = CreateLogicalTableExprGeneratorBuilder::default()
        .name_generator(Box::new(MappedGenerator::new(
            WordGenerator,
            merge_two_word_map_fn(random_capitalize_map, uppercase_and_keyword_backtick_map),
        )))
        .physical_table_ctx(physical_table_ctx)
        .labels(labels)
        .if_not_exists(logical_table_if_not_exists)
        .build()
        .unwrap();
    create_logical_table_expr.generate(rng)
}

fn generate_insert_expr<R: Rng + 'static>(
    rows: usize,
    rng: &mut R,
    table_ctx: TableContextRef,
) -> Result<InsertIntoExpr> {
    let insert_generator = InsertExprGeneratorBuilder::default()
        .omit_column_list(false)
        .table_ctx(table_ctx)
        .rows(rows)
        .value_generator(Box::new(generate_random_value))
        .ts_value_generator(generate_unique_timestamp_for_mysql(current_time_millis()))
        .build()
        .unwrap();
    insert_generator.generate(rng)
}

async fn create_physical_table<R: Rng + 'static>(
    ctx: &FuzzContext,
    rng: &mut R,
) -> Result<TableContextRef> {
    // Create a physical table and a logical table on top of it
    let create_physical_table_expr = generate_create_physical_table_expr(rng).unwrap();
    let translator = CreateTableExprTranslator;
    let sql = translator.translate(&create_physical_table_expr)?;
    let result = sqlx::query(&sql)
        .execute(&ctx.greptime)
        .await
        .context(error::ExecuteQuerySnafu { sql: &sql })?;
    info!("Create physical table: {sql}, result: {result:?}");

    Ok(Arc::new(TableContext::from(&create_physical_table_expr)))
}

async fn validate_values(
    ctx: &FuzzContext,
    logical_table_ctx: TableContextRef,
    insert_expr: &InsertIntoExpr,
) -> Result<()> {
    // Validate inserted rows
    // The order of inserted rows are random, so we need to sort the inserted rows by primary keys and time index for comparison
    let primary_keys_names = logical_table_ctx
        .columns
        .iter()
        .filter(|c| c.is_primary_key() || c.is_time_index())
        .map(|c| c.name.clone())
        .collect::<Vec<_>>();

    // Not all primary keys are in insert_expr
    let primary_keys_idxs_in_insert_expr = insert_expr
        .columns
        .iter()
        .enumerate()
        .filter(|(_, c)| primary_keys_names.contains(&c.name))
        .map(|(i, _)| i)
        .collect::<Vec<_>>();
    let primary_keys_column_list = primary_keys_idxs_in_insert_expr
        .iter()
        .map(|&i| insert_expr.columns[i].name.to_string())
        .collect::<Vec<_>>()
        .join(", ")
        .to_string();

    let column_list = insert_expr
        .columns
        .iter()
        .map(|c| c.name.to_string())
        .collect::<Vec<_>>()
        .join(", ")
        .to_string();

    let select_sql = format!(
        "SELECT {} FROM {} ORDER BY {}",
        column_list, logical_table_ctx.name, primary_keys_column_list
    );
    let fetched_rows = validator::row::fetch_values(&ctx.greptime, select_sql.as_str()).await?;
    let mut expected_rows =
        replace_default(&insert_expr.values_list, &logical_table_ctx, insert_expr);
    sort_by_primary_keys(&mut expected_rows, primary_keys_idxs_in_insert_expr);
    validator::row::assert_eq::<MySql>(&insert_expr.columns, &fetched_rows, &expected_rows)?;

    Ok(())
}

async fn insert_values<R: Rng + 'static>(
    rows: usize,
    ctx: &FuzzContext,
    rng: &mut R,
    logical_table_ctx: TableContextRef,
) -> Result<InsertIntoExpr> {
    let insert_expr = generate_insert_expr(rows, rng, logical_table_ctx.clone())?;
    let translator = InsertIntoExprTranslator;
    let sql = translator.translate(&insert_expr)?;
    let result = ctx
        .greptime
        // unprepared query, see <https://github.com/GreptimeTeam/greptimedb/issues/3500>
        .execute(sql.as_str())
        .await
        .context(error::ExecuteQuerySnafu { sql: &sql })?;

    ensure!(
        result.rows_affected() == rows as u64,
        error::AssertSnafu {
            reason: format!(
                "expected rows affected: {}, actual: {}",
                rows,
                result.rows_affected(),
            )
        }
    );

    Ok(insert_expr)
}

async fn execute_insert(ctx: FuzzContext, input: FuzzInput) -> Result<()> {
    info!("input: {input:?}");
    let mut rng = ChaChaRng::seed_from_u64(input.seed);
    let physical_table_ctx = create_physical_table(&ctx, &mut rng).await?;

    let mut tables = HashMap::with_capacity(input.tables);

    // Create logical tables
    for _ in 0..input.tables {
        let translator = CreateTableExprTranslator;
        let create_logical_table_expr =
            generate_create_logical_table_expr(physical_table_ctx.clone(), &mut rng).unwrap();
        if tables.contains_key(&create_logical_table_expr.table_name) {
            // Ignores same name logical table.
            continue;
        }
        let sql = translator.translate(&create_logical_table_expr)?;
        let result = sqlx::query(&sql)
            .execute(&ctx.greptime)
            .await
            .context(error::ExecuteQuerySnafu { sql: &sql })?;
        info!("Create logical table: {sql}, result: {result:?}");
        let logical_table_ctx = Arc::new(TableContext::from(&create_logical_table_expr));

        let insert_expr =
            insert_values(input.rows, &ctx, &mut rng, logical_table_ctx.clone()).await?;
        validate_values(&ctx, logical_table_ctx.clone(), &insert_expr).await?;
        tables.insert(logical_table_ctx.name.clone(), logical_table_ctx.clone());
        if rng.gen_bool(0.1) {
            flush_memtable(&ctx.greptime, &physical_table_ctx.name).await?;
            validate_values(&ctx, logical_table_ctx.clone(), &insert_expr).await?;
        }
        if rng.gen_bool(0.1) {
            compact_table(&ctx.greptime, &physical_table_ctx.name).await?;
            validate_values(&ctx, logical_table_ctx.clone(), &insert_expr).await?;
        }
    }

    // Clean up logical table
    for (table_name, _) in tables {
        let sql = format!("DROP TABLE {}", table_name);
        let result = sqlx::query(&sql)
            .execute(&ctx.greptime)
            .await
            .context(error::ExecuteQuerySnafu { sql: &sql })?;
        info!("Drop table: {}, result: {result:?}", table_name);
    }

    // Clean up physical table
    let sql = format!("DROP TABLE {}", physical_table_ctx.name);
    let result = sqlx::query(&sql)
        .execute(&ctx.greptime)
        .await
        .context(error::ExecuteQuerySnafu { sql })?;
    info!(
        "Drop table: {}, result: {result:?}",
        physical_table_ctx.name
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
        execute_insert(ctx, input)
            .await
            .unwrap_or_else(|err| panic!("fuzz test must be succeed: {err:?}"));
    })
});
