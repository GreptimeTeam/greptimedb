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

use arbitrary::{Arbitrary, Unstructured};
use common_telemetry::info;
use datatypes::data_type::ConcreteDataType;
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
use tests_fuzz::generator::alter_expr::AlterExprAddColumnGeneratorBuilder;
use tests_fuzz::generator::create_expr::{
    CreateLogicalTableExprGeneratorBuilder, CreatePhysicalTableExprGeneratorBuilder,
};
use tests_fuzz::generator::Generator;
use tests_fuzz::ir::{
    primary_key_and_not_null_column_options_generator, primary_key_options_generator, Column,
    CreateTableExpr, StringColumnTypeGenerator,
};
use tests_fuzz::translator::mysql::alter_expr::AlterTableExprTranslator;
use tests_fuzz::translator::mysql::create_expr::CreateTableExprTranslator;
use tests_fuzz::translator::DslTranslator;
use tests_fuzz::utils::{
    get_gt_fuzz_input_max_alter_actions, init_greptime_connections_via_env, Connections,
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
}

impl Arbitrary<'_> for FuzzInput {
    fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        let seed = u.int_in_range(u64::MIN..=u64::MAX)?;
        let mut rng = ChaChaRng::seed_from_u64(seed);
        let max_actions = get_gt_fuzz_input_max_alter_actions();
        let actions = rng.gen_range(1..max_actions);

        Ok(FuzzInput { seed, actions })
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

async fn execute_alter_table(ctx: FuzzContext, input: FuzzInput) -> Result<()> {
    info!("input: {input:?}");
    let mut rng = ChaChaRng::seed_from_u64(input.seed);

    // Create a physical table and a logical table on top of it
    let create_physical_table_expr = generate_create_physical_table_expr(&mut rng).unwrap();
    let translator = CreateTableExprTranslator;
    let sql = translator.translate(&create_physical_table_expr)?;
    let result = sqlx::query(&sql)
        .execute(&ctx.greptime)
        .await
        .context(error::ExecuteQuerySnafu { sql: &sql })?;
    info!("Create physical table: {sql}, result: {result:?}");

    let mut physical_table_meta_columns = vec![];
    physical_table_meta_columns.push({
        let column_type = ConcreteDataType::uint64_datatype();
        let options = primary_key_and_not_null_column_options_generator(&mut rng, &column_type);
        Column {
            name: "__tsid".into(),
            column_type,
            options,
        }
    });
    physical_table_meta_columns.push({
        let column_type = ConcreteDataType::uint32_datatype();
        let options = primary_key_and_not_null_column_options_generator(&mut rng, &column_type);
        Column {
            name: "__table_id".into(),
            column_type,
            options,
        }
    });

    let physical_table_ctx = Arc::new(TableContext::from(&create_physical_table_expr));

    let create_logical_table_expr =
        generate_create_logical_table_expr(physical_table_ctx, &mut rng).unwrap();
    let sql = translator.translate(&create_logical_table_expr)?;
    let result = sqlx::query(&sql)
        .execute(&ctx.greptime)
        .await
        .context(error::ExecuteQuerySnafu { sql: &sql })?;
    info!("Create logical table: {sql}, result: {result:?}");

    // Alter table actions
    let mut logical_table_ctx = Arc::new(TableContext::from(&create_logical_table_expr));
    for _ in 0..input.actions {
        // Logical table only enable to add columns
        let expr = AlterExprAddColumnGeneratorBuilder::default()
            .table_ctx(logical_table_ctx.clone())
            .name_generator(Box::new(MappedGenerator::new(
                WordGenerator,
                merge_two_word_map_fn(random_capitalize_map, uppercase_and_keyword_backtick_map),
            )))
            .column_type_generator(Box::new(StringColumnTypeGenerator))
            .column_options_generator(Box::new(primary_key_options_generator))
            .build()
            .unwrap()
            .generate(&mut rng)
            .unwrap();
        let translator = AlterTableExprTranslator;
        let sql = translator.translate(&expr)?;
        let result = sqlx::query(&sql)
            .execute(&ctx.greptime)
            .await
            .context(error::ExecuteQuerySnafu { sql: &sql })?;
        info!("Alter table: {sql}, result: {result:?}");
        // Applies changes
        logical_table_ctx = Arc::new(Arc::unwrap_or_clone(logical_table_ctx).alter(expr).unwrap());

        // Validate columns in logical table
        let mut column_entries = validator::column::fetch_columns(
            &ctx.greptime,
            "public".into(),
            logical_table_ctx.name.clone(),
        )
        .await?;
        column_entries.sort_by(|a, b| a.column_name.cmp(&b.column_name));
        let mut columns = logical_table_ctx.columns.clone();
        columns.sort_by(|a, b| a.name.value.cmp(&b.name.value));
        validator::column::assert_eq(&column_entries, &columns)?;

        // Validate columns in physical table
        let mut column_entries = validator::column::fetch_columns(
            &ctx.greptime,
            "public".into(),
            create_physical_table_expr.table_name.clone(),
        )
        .await?;
        column_entries.sort_by(|a, b| a.column_name.cmp(&b.column_name));
        columns.append(&mut physical_table_meta_columns.clone());
        columns.sort_by(|a, b| a.name.value.cmp(&b.name.value));
        validator::column::assert_eq(&column_entries, &columns)?;
    }

    // Clean up logical table
    let sql = format!("DROP TABLE {}", create_logical_table_expr.table_name);
    let result = sqlx::query(&sql)
        .execute(&ctx.greptime)
        .await
        .context(error::ExecuteQuerySnafu { sql: &sql })?;
    info!(
        "Drop table: {}, result: {result:?}",
        create_logical_table_expr.table_name
    );

    // Clean up physical table
    let sql = format!("DROP TABLE {}", create_physical_table_expr.table_name);
    let result = sqlx::query(&sql)
        .execute(&ctx.greptime)
        .await
        .context(error::ExecuteQuerySnafu { sql })?;
    info!(
        "Drop table: {}, result: {result:?}",
        create_physical_table_expr.table_name
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
        execute_alter_table(ctx, input)
            .await
            .unwrap_or_else(|err| panic!("fuzz test must be succeed: {err:?}"));
    })
});
