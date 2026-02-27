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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arbitrary::{Arbitrary, Unstructured};
use common_telemetry::{info, warn};
use common_time::Timestamp;
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
use tests_fuzz::generator::insert_expr::InsertExprGeneratorBuilder;
use tests_fuzz::generator::repartition_expr::{
    MergePartitionExprGeneratorBuilder, SplitPartitionExprGeneratorBuilder,
};
use tests_fuzz::ir::{
    CreateTableExpr, InsertIntoExpr, MySQLTsColumnTypeGenerator, RepartitionExpr, RowValue,
    SimplePartitions, generate_partition_value, generate_unique_timestamp_for_mysql_with_clock,
};
use tests_fuzz::translator::DslTranslator;
use tests_fuzz::translator::mysql::create_expr::CreateTableExprTranslator;
use tests_fuzz::translator::mysql::insert_expr::InsertIntoExprTranslator;
use tests_fuzz::translator::mysql::repartition_expr::RepartitionExprTranslator;
use tests_fuzz::utils::{
    Connections, get_fuzz_override, get_gt_fuzz_input_max_alter_actions,
    init_greptime_connections_via_env,
};
use tests_fuzz::validator;
use tests_fuzz::validator::row::count_values;

#[derive(Clone)]
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

struct SharedState {
    table_ctx: TableContextRef,
    clock: Arc<Mutex<Timestamp>>,
    inserted_rows: u64,
    running: bool,
}

fn build_insert_expr<R: Rng + 'static>(
    table_ctx: &TableContextRef,
    rng: &mut R,
    partitions: &SimplePartitions,
    clock: &Arc<Mutex<Timestamp>>,
) -> InsertIntoExpr {
    let ts_value_generator = generate_unique_timestamp_for_mysql_with_clock(clock.clone());
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();
    let partition_len = table_ctx.partition.as_ref().unwrap().exprs.len();
    let row = rng.random_range(partition_len..partition_len * 2);

    let moved_partitions = partitions.clone();
    let insert_generator = InsertExprGeneratorBuilder::default()
        .table_ctx(table_ctx.clone())
        .omit_column_list(false)
        .rows(row)
        .required_columns(vec![partitions.column_name.clone()])
        .value_overrides(Some(Box::new(move |column, rng| {
            if column.name.value == moved_partitions.column_name.value {
                let bound_idx =
                    counter_clone.fetch_add(1, Ordering::Relaxed) % moved_partitions.bounds.len();

                return Some(RowValue::Value(generate_partition_value(
                    rng,
                    &column.column_type,
                    &moved_partitions.bounds,
                    bound_idx,
                )));
            }
            None
        })))
        .ts_value_generator(ts_value_generator)
        .build()
        .unwrap();
    insert_generator.generate(rng).unwrap()
}

async fn execute_insert_with_retry(ctx: &FuzzContext, sql: &str) -> Result<()> {
    let mut delay = Duration::from_millis(100);
    let mut attempt = 0;
    let max_attempts = 10;
    loop {
        match sqlx::query(sql)
            .persistent(false)
            .execute(&ctx.greptime)
            .await
        {
            Ok(_) => {
                return Ok(());
            }
            Err(err) => {
                tokio::time::sleep(delay).await;
                delay = std::cmp::min(delay * 2, Duration::from_secs(1));
                attempt += 1;
                warn!("Execute insert with retry: {sql}, attempt: {attempt}, error: {err:?}");
                if attempt >= max_attempts {
                    return Err(err).context(error::ExecuteQuerySnafu { sql });
                }
            }
        }
    }
}

async fn write_loop<R: Rng + 'static>(
    mut rng: R,
    ctx: FuzzContext,
    shared_state: Arc<Mutex<SharedState>>,
) -> Result<()> {
    info!("Start write loop");
    let clock = shared_state.lock().unwrap().clock.clone();
    loop {
        let (is_running, table_ctx) = {
            let state = shared_state.lock().unwrap();
            (state.running, state.table_ctx.clone())
        };

        if !is_running {
            break;
        }

        let partitions = SimplePartitions::from_table_ctx(&table_ctx).unwrap();
        let insert_expr = build_insert_expr(&table_ctx, &mut rng, &partitions, &clock);

        let new_inserted_rows = insert_expr.values_list.len() as u64;
        let translator = InsertIntoExprTranslator;
        let sql = translator.translate(&insert_expr)?;

        let now = Instant::now();
        execute_insert_with_retry(&ctx, &sql).await?;
        info!("Execute insert sql: {sql}, elapsed: {:?}", now.elapsed());
        {
            let mut state = shared_state.lock().unwrap();
            state.inserted_rows += new_inserted_rows;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    info!("Write loop ended");
    Ok(())
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
    let shared_state = Arc::new(Mutex::new(SharedState {
        table_ctx: table_ctx.clone(),
        clock: Arc::new(Mutex::new(Timestamp::current_millis())),
        running: true,
        inserted_rows: 0,
    }));

    let writer_rng = ChaChaRng::seed_from_u64(input.seed);
    let writer_task = tokio::spawn(write_loop(writer_rng, ctx.clone(), shared_state.clone()));
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
        // Updates the table partition in the shared state.
        shared_state.lock().unwrap().table_ctx = table_ctx.clone();

        // Validates partition expression
        let partition_entries = validator::partition::fetch_partitions_info_schema(
            &ctx.greptime,
            "public".into(),
            &table_ctx.name,
        )
        .await?;
        info!(
            "partition_entries:\n{}",
            partition_entries
                .iter()
                .map(|entry| {
                    format!(
                        r#"  - table_catalog: {}
    table_schema: {}
    table_name: {}
    partition_name: {}
    partition_expression: {}
    partition_description: {}
    greptime_partition_id: {}
    partition_ordinal_position: {}"#,
                        entry.table_catalog,
                        entry.table_schema,
                        entry.table_name,
                        entry.partition_name,
                        entry.partition_expression,
                        entry.partition_description,
                        entry.greptime_partition_id,
                        entry.partition_ordinal_position,
                    )
                })
                .collect::<Vec<_>>()
                .join("\n")
        );

        validator::partition::assert_partitions(
            table_ctx.partition.as_ref().unwrap(),
            &partition_entries,
        )?;
    }
    shared_state.lock().unwrap().running = false;
    writer_task.await.unwrap().unwrap();
    let count_sql = format!("SELECT COUNT(1) AS count FROM {}", table_ctx.name);
    let count = count_values(&ctx.greptime, &count_sql).await?;
    assert_eq!(
        count.count as usize,
        shared_state.lock().unwrap().inserted_rows as usize
    );
    let timestamp_column_name = table_ctx.timestamp_column().unwrap().name.clone();
    // Since each timestamp value is unique, the count of distinct timestamps should match the total row count.
    let distinct_count_sql = format!(
        "SELECT COUNT(DISTINCT {}) AS count FROM {}",
        timestamp_column_name, table_ctx.name
    );
    let distinct_count = count_values(&ctx.greptime, &distinct_count_sql).await?;
    assert_eq!(
        distinct_count.count as usize,
        shared_state.lock().unwrap().inserted_rows as usize
    );

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
