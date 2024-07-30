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
use datatypes::data_type::ConcreteDataType;
use libfuzzer_sys::arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaChaRng;
use snafu::ResultExt;
use sqlx::{MySql, Pool};
use tests_fuzz::context::TableContext;
use tests_fuzz::error::{self, Result};
use tests_fuzz::fake::{
    merge_two_word_map_fn, random_capitalize_map, uppercase_and_keyword_backtick_map,
    MappedGenerator, WordGenerator,
};
use tests_fuzz::generator::create_expr::{
    CreateLogicalTableExprGeneratorBuilder, CreatePhysicalTableExprGeneratorBuilder,
};
use tests_fuzz::generator::Generator;
use tests_fuzz::ir::{primary_key_and_not_null_column_options_generator, Column};
use tests_fuzz::translator::mysql::create_expr::CreateTableExprTranslator;
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

async fn execute_create_logic_table(ctx: FuzzContext, input: FuzzInput) -> Result<()> {
    info!("input: {input:?}");
    let mut rng = ChaChaRng::seed_from_u64(input.seed);

    // Create physical table
    let physical_table_if_not_exists = rng.gen_bool(0.5);
    let create_physical_table_expr = CreatePhysicalTableExprGeneratorBuilder::default()
        .name_generator(Box::new(MappedGenerator::new(
            WordGenerator,
            merge_two_word_map_fn(random_capitalize_map, uppercase_and_keyword_backtick_map),
        )))
        .if_not_exists(physical_table_if_not_exists)
        .build()
        .unwrap()
        .generate(&mut rng)?;
    let translator = CreateTableExprTranslator;
    let sql = translator.translate(&create_physical_table_expr)?;
    let result = sqlx::query(&sql)
        .execute(&ctx.greptime)
        .await
        .context(error::ExecuteQuerySnafu { sql: &sql })?;
    info!("Create physical table: {sql}, result: {result:?}");

    let mut physical_table_columns = create_physical_table_expr.columns.clone();
    physical_table_columns.push({
        let column_type = ConcreteDataType::uint64_datatype();
        let options = primary_key_and_not_null_column_options_generator(&mut rng, &column_type);
        Column {
            name: "__tsid".into(),
            column_type,
            options,
        }
    });
    physical_table_columns.push({
        let column_type = ConcreteDataType::uint32_datatype();
        let options = primary_key_and_not_null_column_options_generator(&mut rng, &column_type);
        Column {
            name: "__table_id".into(),
            column_type,
            options,
        }
    });

    // Create logical table
    let physical_table_ctx = Arc::new(TableContext::from(&create_physical_table_expr));
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
        .unwrap()
        .generate(&mut rng)?;
    let sql = translator.translate(&create_logical_table_expr)?;
    let result = sqlx::query(&sql)
        .execute(&ctx.greptime)
        .await
        .context(error::ExecuteQuerySnafu { sql: &sql })?;
    info!("Create logical table: {sql}, result: {result:?}");

    // Validate columns in logical table
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

    // Validate columns in physical table
    columns.retain(|column| column.column_type == ConcreteDataType::string_datatype());
    physical_table_columns.append(&mut columns);
    physical_table_columns.sort_by(|a, b| a.name.value.cmp(&b.name.value));

    let mut column_entries = validator::column::fetch_columns(
        &ctx.greptime,
        "public".into(),
        create_physical_table_expr.table_name.clone(),
    )
    .await?;
    column_entries.sort_by(|a, b| a.column_name.cmp(&b.column_name));
    validator::column::assert_eq(&column_entries, &physical_table_columns)?;

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
        execute_create_logic_table(ctx, input)
            .await
            .unwrap_or_else(|err| panic!("fuzz test must be succeed: {err:?}"));
    })
});
