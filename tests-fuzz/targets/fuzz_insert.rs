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
use tests_fuzz::ir::{
    generate_random_value_for_mysql, replace_default, CreateTableExpr, InsertIntoExpr,
    MySQLTsColumnTypeGenerator,
};
use tests_fuzz::translator::mysql::create_expr::CreateTableExprTranslator;
use tests_fuzz::translator::mysql::insert_expr::InsertIntoExprTranslator;
use tests_fuzz::translator::DslTranslator;
use tests_fuzz::utils::{init_greptime_connections_via_env, Connections};
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
        .ts_column_type_generator(Box::new(MySQLTsColumnTypeGenerator))
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
        .value_generator(Box::new(generate_random_value_for_mysql))
        .build()
        .unwrap();
    insert_generator.generate(rng)
}

async fn execute_insert(ctx: FuzzContext, input: FuzzInput) -> Result<()> {
    info!("input: {input:?}");
    let mut rng = ChaChaRng::seed_from_u64(input.seed);

    let create_expr = generate_create_expr(input, &mut rng)?;
    let translator = CreateTableExprTranslator;
    let sql = translator.translate(&create_expr)?;
    let _result = sqlx::query(&sql)
        .execute(&ctx.greptime)
        .await
        .context(error::ExecuteQuerySnafu { sql: &sql })?;

    let table_ctx = Arc::new(TableContext::from(&create_expr));
    let insert_expr = generate_insert_expr(input, &mut rng, table_ctx)?;
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

    // Validate inserted rows
    // The order of inserted rows are random, so we need to sort the inserted rows by primary keys and time index for comparison
    let primary_keys_names = create_expr
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
        column_list, create_expr.table_name, primary_keys_column_list
    );
    let fetched_rows = validator::row::fetch_values(&ctx.greptime, select_sql.as_str()).await?;
    let mut expected_rows = replace_default(&insert_expr.values_list, &create_expr);
    expected_rows.sort_by(|a, b| {
        let a_keys: Vec<_> = primary_keys_idxs_in_insert_expr
            .iter()
            .map(|&i| &a[i])
            .collect();
        let b_keys: Vec<_> = primary_keys_idxs_in_insert_expr
            .iter()
            .map(|&i| &b[i])
            .collect();
        for (a_key, b_key) in a_keys.iter().zip(b_keys.iter()) {
            match a_key.cmp(b_key) {
                Some(std::cmp::Ordering::Equal) => continue,
                non_eq => return non_eq.unwrap(),
            }
        }
        std::cmp::Ordering::Equal
    });
    validator::row::assert_eq::<MySql>(&insert_expr.columns, &fetched_rows, &expected_rows)?;

    // Cleans up
    let sql = format!("DROP TABLE {}", create_expr.table_name);
    let result = sqlx::query(&sql)
        .execute(&ctx.greptime)
        .await
        .context(error::ExecuteQuerySnafu { sql })?;
    info!(
        "Drop table: {}\n\nResult: {result:?}\n\n",
        create_expr.table_name
    );
    ctx.close().await;

    Ok(())
}

fuzz_target!(|input: FuzzInput| {
    common_telemetry::init_default_ut_logging();
    common_runtime::block_on_write(async {
        let Connections { mysql } = init_greptime_connections_via_env().await;
        let ctx = FuzzContext {
            greptime: mysql.expect("mysql connection init must be succeed"),
        };
        execute_insert(ctx, input)
            .await
            .unwrap_or_else(|err| panic!("fuzz test must be succeed: {err:?}"));
    })
});
