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

use arbitrary::{Arbitrary, Unstructured};
use common_telemetry::info;
use common_time::util::current_time_millis;
use libfuzzer_sys::fuzz_target;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaChaRng;
use snafu::ResultExt;
use snafu::ensure;
use sqlx::{Executor, MySql, Pool};
use std::collections::HashMap;
use std::sync::Arc;
use tests_fuzz::context::{TableContext, TableContextRef};
use tests_fuzz::error::{self, Result};
use tests_fuzz::fake::{
    MappedGenerator, WordGenerator, merge_two_word_map_fn, random_capitalize_map,
    uppercase_and_keyword_backtick_map,
};
use tests_fuzz::generator::Generator;
use tests_fuzz::generator::create_expr::{
    CreateLogicalTableExprGeneratorBuilder, CreatePhysicalTableExprGeneratorBuilder,
};
use tests_fuzz::generator::insert_expr::InsertExprGeneratorBuilder;
use tests_fuzz::generator::repartition_expr::{
    MergePartitionExprGeneratorBuilder, SplitPartitionExprGeneratorBuilder,
};
use tests_fuzz::ir::{
    CreateTableExpr, Ident, InsertIntoExpr, RepartitionExpr, generate_random_value,
    generate_unique_timestamp_for_mysql,
};
use tests_fuzz::translator::DslTranslator;
use tests_fuzz::translator::mysql::create_expr::CreateTableExprTranslator;
use tests_fuzz::translator::mysql::insert_expr::InsertIntoExprTranslator;
use tests_fuzz::translator::mysql::repartition_expr::RepartitionExprTranslator;
use tests_fuzz::utils::{
    Connections, get_fuzz_override, get_gt_fuzz_input_max_alter_actions,
    get_gt_fuzz_input_max_tables,
    init_greptime_connections_via_env,
};
use tests_fuzz::validator::row::count_values;

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
    tables: usize,
}

fn generate_create_physical_table_expr<R: Rng + 'static>(
    partitions: usize,
    rng: &mut R,
) -> Result<CreateTableExpr> {
    CreatePhysicalTableExprGeneratorBuilder::default()
        .name_generator(Box::new(MappedGenerator::new(
            WordGenerator,
            merge_two_word_map_fn(random_capitalize_map, uppercase_and_keyword_backtick_map),
        )))
        .if_not_exists(rng.random_bool(0.5))
        .partition(partitions)
        .build()
        .unwrap()
        .generate(rng)
}

fn generate_create_logical_table_expr<R: Rng + 'static>(
    physical_table_ctx: TableContextRef,
    include_partition_column: bool,
    rng: &mut R,
) -> Result<CreateTableExpr> {
    CreateLogicalTableExprGeneratorBuilder::default()
        .name_generator(Box::new(MappedGenerator::new(
            WordGenerator,
            merge_two_word_map_fn(random_capitalize_map, uppercase_and_keyword_backtick_map),
        )))
        .physical_table_ctx(physical_table_ctx)
        .labels(rng.random_range(1..=5))
        .if_not_exists(rng.random_bool(0.5))
        .include_partition_column(include_partition_column)
        .build()
        .unwrap()
        .generate(rng)
}

fn generate_insert_expr<R: Rng + 'static>(
    rows: usize,
    rng: &mut R,
    table_ctx: TableContextRef,
) -> Result<InsertIntoExpr> {
    InsertExprGeneratorBuilder::default()
        .omit_column_list(false)
        .table_ctx(table_ctx)
        .rows(rows)
        .value_generator(Box::new(generate_random_value))
        .ts_value_generator(generate_unique_timestamp_for_mysql(current_time_millis()))
        .build()
        .unwrap()
        .generate(rng)
}

async fn create_metric_tables<R: Rng + 'static>(
    ctx: &FuzzContext,
    rng: &mut R,
    partitions: usize,
    table_count: usize,
) -> Result<(TableContextRef, HashMap<Ident, TableContextRef>)> {
    let create_physical_expr = generate_create_physical_table_expr(partitions, rng)?;
    let translator = CreateTableExprTranslator;
    let create_physical_sql = translator.translate(&create_physical_expr)?;
    let result = sqlx::query(&create_physical_sql)
        .execute(&ctx.greptime)
        .await
        .context(error::ExecuteQuerySnafu {
            sql: &create_physical_sql,
        })?;
    info!("Create physical table: {create_physical_sql}, result: {result:?}");
    let physical_table_ctx = Arc::new(TableContext::from(&create_physical_expr));
    ensure!(
        physical_table_ctx.partition.is_some(),
        error::AssertSnafu {
            reason: "Physical metric table must have partition".to_string()
        }
    );

    let mut logical_tables = HashMap::with_capacity(table_count);
    let max_attempts = table_count * 3;
    for _ in 0..max_attempts {
        if logical_tables.len() >= table_count {
            break;
        }

        let include_partition_column = rng.random_bool(0.5);
        let create_logical_expr = generate_create_logical_table_expr(
            physical_table_ctx.clone(),
            include_partition_column,
            rng,
        )?;
        if logical_tables.contains_key(&create_logical_expr.table_name) {
            continue;
        }

        let create_logical_sql = translator.translate(&create_logical_expr)?;
        let result = sqlx::query(&create_logical_sql)
            .execute(&ctx.greptime)
            .await
            .context(error::ExecuteQuerySnafu {
                sql: &create_logical_sql,
            })?;
        info!("Create logical table: {create_logical_sql}, result: {result:?}");
        let logical_ctx = Arc::new(TableContext::from(&create_logical_expr));
        logical_tables.insert(logical_ctx.name.clone(), logical_ctx);
    }

    ensure!(
        !logical_tables.is_empty(),
        error::AssertSnafu {
            reason: "No logical table created".to_string()
        }
    );

    Ok((physical_table_ctx, logical_tables))
}

async fn insert_rows<R: Rng + 'static>(
    ctx: &FuzzContext,
    rng: &mut R,
    rows: usize,
    logical_tables: &HashMap<Ident, TableContextRef>,
) -> Result<HashMap<Ident, InsertIntoExpr>> {
    let mut inserts = HashMap::with_capacity(logical_tables.len());
    for table_ctx in logical_tables.values() {
        let insert_expr = generate_insert_expr(rows, rng, table_ctx.clone())?;
        let translator = InsertIntoExprTranslator;
        let sql = translator.translate(&insert_expr)?;
        let result = ctx
            .greptime
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
        inserts.insert(table_ctx.name.clone(), insert_expr);
    }

    Ok(inserts)
}

async fn validate_rows(
    ctx: &FuzzContext,
    logical_tables: &HashMap<Ident, TableContextRef>,
    inserts: &HashMap<Ident, InsertIntoExpr>,
) -> Result<()> {
    for (name, table_ctx) in logical_tables {
        let insert_expr = inserts.get(name).unwrap();
        let count_sql = format!("SELECT COUNT(1) AS count FROM {}", table_ctx.name);
        let count = count_values(&ctx.greptime, &count_sql).await?;
        let expected = insert_expr.values_list.len();
        assert_eq!(count.count as usize, expected);

        let distinct_count_sql = format!(
            "SELECT COUNT(DISTINCT {}) AS count FROM {}",
            table_ctx.timestamp_column().unwrap().name,
            table_ctx.name
        );
        let distinct_count = count_values(&ctx.greptime, &distinct_count_sql).await?;
        assert_eq!(distinct_count.count as usize, expected);
    }
    Ok(())
}

async fn cleanup_tables(
    ctx: &FuzzContext,
    physical_table_ctx: &TableContextRef,
    logical_tables: &HashMap<Ident, TableContextRef>,
) -> Result<()> {
    for table_ctx in logical_tables.values() {
        let drop_logical_sql = format!("DROP TABLE {}", table_ctx.name);
        let result = sqlx::query(&drop_logical_sql)
            .execute(&ctx.greptime)
            .await
            .context(error::ExecuteQuerySnafu {
                sql: &drop_logical_sql,
            })?;
        info!("Drop logical table: {drop_logical_sql}, result: {result:?}");
    }

    let drop_physical_sql = format!("DROP TABLE {}", physical_table_ctx.name);
    let result = sqlx::query(&drop_physical_sql)
        .execute(&ctx.greptime)
        .await
        .context(error::ExecuteQuerySnafu {
            sql: &drop_physical_sql,
        })?;
    info!("Drop physical table: {drop_physical_sql}, result: {result:?}");
    Ok(())
}

fn repartition_operation<R: Rng + 'static>(
    table_ctx: &TableContextRef,
    rng: &mut R,
) -> Result<RepartitionExpr> {
    let split = rng.random_bool(0.5);
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

impl Arbitrary<'_> for FuzzInput {
    fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        let seed = get_fuzz_override::<u64>("SEED").unwrap_or(u.int_in_range(u64::MIN..=u64::MAX)?);
        let mut rng = ChaChaRng::seed_from_u64(seed);
        let partitions =
            get_fuzz_override::<usize>("PARTITIONS").unwrap_or_else(|| rng.random_range(2..8));
        let max_tables = get_gt_fuzz_input_max_tables();
        let tables = get_fuzz_override::<usize>("TABLES")
            .unwrap_or_else(|| rng.random_range(1..=std::cmp::max(1, max_tables)));
        let max_actions = get_gt_fuzz_input_max_alter_actions();
        let actions = get_fuzz_override::<usize>("ACTIONS")
            .unwrap_or_else(|| rng.random_range(1..max_actions));

        Ok(FuzzInput {
            seed,
            actions,
            partitions,
            tables,
        })
    }
}

async fn execute_repartition_metric_table(ctx: FuzzContext, input: FuzzInput) -> Result<()> {
    info!("input: {input:?}");
    let mut rng = ChaChaRng::seed_from_u64(input.seed);

    let (mut physical_table_ctx, logical_tables) =
        create_metric_tables(&ctx, &mut rng, input.partitions, input.tables).await?;
    let inserts = insert_rows(&ctx, &mut rng, input.actions, &logical_tables).await?;

    for i in 0..input.actions {
        let partition_num = physical_table_ctx.partition.as_ref().unwrap().exprs.len();
        info!(
            "partition_num: {partition_num}, action: {}/{}, table: {}",
            i + 1,
            input.actions,
            physical_table_ctx.name
        );

        let repartition_expr = repartition_operation(&physical_table_ctx, &mut rng)?;
        let translator = RepartitionExprTranslator;
        let sql = translator.translate(&repartition_expr)?;
        info!("Repartition sql: {sql}");
        let result = sqlx::query(&sql)
            .execute(&ctx.greptime)
            .await
            .context(error::ExecuteQuerySnafu { sql: &sql })?;
        info!("Repartition result: {result:?}");

        physical_table_ctx = Arc::new(
            Arc::unwrap_or_clone(physical_table_ctx)
                .repartition(repartition_expr)
                .unwrap(),
        );

        let partition_entries = tests_fuzz::validator::partition::fetch_partitions_info_schema(
            &ctx.greptime,
            "public".into(),
            &physical_table_ctx.name,
        )
        .await?;
        tests_fuzz::validator::partition::assert_partitions(
            physical_table_ctx.partition.as_ref().unwrap(),
            &partition_entries,
        )?;
    }

    validate_rows(&ctx, &logical_tables, &inserts).await?;
    cleanup_tables(&ctx, &physical_table_ctx, &logical_tables).await?;

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
        execute_repartition_metric_table(ctx, input)
            .await
            .unwrap_or_else(|err| panic!("fuzz test must be succeed: {err:?}"));
    })
});
