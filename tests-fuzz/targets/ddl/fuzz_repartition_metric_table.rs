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

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arbitrary::{Arbitrary, Unstructured};
use common_telemetry::{info, warn};
use common_time::Timestamp;
use libfuzzer_sys::fuzz_target;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaChaRng;
use snafu::{ResultExt, ensure};
use sqlx::{MySql, Pool};
use tests_fuzz::context::{TableContext, TableContextRef};
use tests_fuzz::error::{self, Result};
use tests_fuzz::fake::{
    ConstGenerator, MappedGenerator, WordGenerator, merge_two_word_map_fn, random_capitalize_map,
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
    generate_unique_timestamp_for_mysql_with_clock,
};
use tests_fuzz::translator::DslTranslator;
use tests_fuzz::translator::csv::InsertExprToCsvRecordsTranslator;
use tests_fuzz::translator::mysql::create_expr::CreateTableExprTranslator;
use tests_fuzz::translator::mysql::insert_expr::InsertIntoExprTranslator;
use tests_fuzz::translator::mysql::repartition_expr::RepartitionExprTranslator;
use tests_fuzz::utils::csv_dump_writer::{CsvDumpMetadata, CsvDumpSession};
use tests_fuzz::utils::{
    Connections, get_fuzz_override, get_gt_fuzz_input_max_alter_actions,
    get_gt_fuzz_input_max_tables, init_greptime_connections_via_env,
};
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
    tables: usize,
}

fn generate_create_physical_table_expr<R: Rng + 'static>(
    partitions: usize,
    rng: &mut R,
) -> Result<CreateTableExpr> {
    CreatePhysicalTableExprGeneratorBuilder::default()
        .name_generator(Box::new(ConstGenerator::new(Ident::new(
            "fuzz_repartition_metric_physical",
        ))))
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
    clock: Arc<Mutex<Timestamp>>,
) -> Result<InsertIntoExpr> {
    let ts_value_generator = generate_unique_timestamp_for_mysql_with_clock(clock);
    InsertExprGeneratorBuilder::default()
        .omit_column_list(false)
        .table_ctx(table_ctx)
        .rows(rows)
        .value_generator(Box::new(generate_random_value))
        .ts_value_generator(ts_value_generator)
        .build()
        .unwrap()
        .generate(rng)
}

async fn create_metric_tables<R: Rng + 'static>(
    ctx: &FuzzContext,
    rng: &mut R,
    partitions: usize,
    table_count: usize,
) -> Result<(TableContextRef, BTreeMap<Ident, TableContextRef>)> {
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

    let mut logical_tables = BTreeMap::new();
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
            Ok(_) => return Ok(()),
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

struct SharedState {
    clock: Arc<Mutex<Timestamp>>,
    inserted_rows: HashMap<String, u64>,
    csv_dump_session: Option<CsvDumpSession>,
    running: bool,
}

async fn write_loop<R: Rng + 'static>(
    mut rng: R,
    ctx: FuzzContext,
    logical_tables: BTreeMap<Ident, TableContextRef>,
    shared_state: Arc<Mutex<SharedState>>,
) -> Result<()> {
    info!("Start write loop");
    loop {
        let (running, clock) = {
            let state = shared_state.lock().unwrap();
            (state.running, state.clock.clone())
        };
        if !running {
            break;
        }

        for table_ctx in logical_tables.values() {
            let rows = rng.random_range(1..=3);
            let insert_expr =
                generate_insert_expr(rows, &mut rng, table_ctx.clone(), clock.clone())?;
            let translator = InsertIntoExprTranslator;
            let sql = translator.translate(&insert_expr)?;
            let inserted = insert_expr.values_list.len() as u64;
            let csv_records = InsertExprToCsvRecordsTranslator.translate(&insert_expr)?;
            let full_headers = table_ctx
                .columns
                .iter()
                .map(|column| column.name.value.to_string())
                .collect::<Vec<_>>();

            let now = Instant::now();
            execute_insert_with_retry(&ctx, &sql).await?;
            info!("Execute insert sql: {sql}, elapsed: {:?}", now.elapsed());

            let mut state = shared_state.lock().unwrap();
            if let Some(csv_dump_session) = state.csv_dump_session.as_mut() {
                csv_dump_session.append(csv_records, full_headers)?;
            }
            *state
                .inserted_rows
                .entry(table_ctx.name.to_string())
                .or_insert(0) += inserted;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    info!("Write loop ended");
    Ok(())
}

async fn validate_rows(
    ctx: &FuzzContext,
    logical_tables: &BTreeMap<Ident, TableContextRef>,
    inserted_rows: &HashMap<String, u64>,
) -> Result<()> {
    for table_ctx in logical_tables.values() {
        let expected = *inserted_rows.get(&table_ctx.name.to_string()).unwrap_or(&0) as usize;
        let count_sql = format!("SELECT COUNT(1) AS count FROM {}", table_ctx.name);
        let count = count_values(&ctx.greptime, &count_sql).await?;
        let distinct_count_sql = format!(
            "SELECT COUNT(DISTINCT {}) AS count FROM {}",
            table_ctx.timestamp_column().unwrap().name,
            table_ctx.name
        );
        let distinct_count = count_values(&ctx.greptime, &distinct_count_sql).await?;
        info!(
            "Validate rows for table: {}, expected: {}, count: {}, distinct_count: {}",
            table_ctx.name, expected, count.count as usize, distinct_count.count as usize
        );
        assert_eq!(count.count as usize, expected);

        assert_eq!(distinct_count.count as usize, expected);
    }
    Ok(())
}

async fn cleanup_tables(
    ctx: &FuzzContext,
    physical_table_ctx: &TableContextRef,
    logical_tables: &BTreeMap<Ident, TableContextRef>,
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
    let clock = Arc::new(Mutex::new(Timestamp::current_millis()));

    let (mut physical_table_ctx, logical_tables) =
        create_metric_tables(&ctx, &mut rng, input.partitions, input.tables).await?;

    let mut inserted_rows = HashMap::with_capacity(logical_tables.len());
    for table_ctx in logical_tables.values() {
        inserted_rows.insert(table_ctx.name.to_string(), 0);
    }
    let csv_dump_session = Some(CsvDumpSession::new(CsvDumpMetadata::new(
        "fuzz_repartition_metric_table",
        input.seed,
        input.actions,
        input.partitions,
        input.tables,
    ))?);

    let shared_state = Arc::new(Mutex::new(SharedState {
        clock,
        inserted_rows,
        csv_dump_session,
        running: true,
    }));
    let writer_rng = ChaChaRng::seed_from_u64(input.seed ^ 0xA5A5_A5A5_A5A5_A5A5);
    let writer_task = tokio::spawn(write_loop(
        writer_rng,
        ctx.clone(),
        logical_tables.clone(),
        shared_state.clone(),
    ));
    tokio::time::sleep(Duration::from_millis(100)).await;

    for i in 0..input.actions {
        let partition_num = physical_table_ctx.partition.as_ref().unwrap().exprs.len();
        info!(
            "partition_num: {partition_num}, action: {}/{}, table: {}, logical table num: {}",
            i + 1,
            input.actions,
            physical_table_ctx.name,
            logical_tables.len()
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

    shared_state.lock().unwrap().running = false;
    writer_task.await.unwrap().unwrap();
    let (inserted_rows, mut csv_dump_session) = {
        let mut state = shared_state.lock().unwrap();
        if let Some(csv_dump_session) = state.csv_dump_session.as_mut() {
            csv_dump_session.flush_all()?;
        }
        (state.inserted_rows.clone(), state.csv_dump_session.take())
    };

    let run_result = async {
        validate_rows(&ctx, &logical_tables, &inserted_rows).await?;
        cleanup_tables(&ctx, &physical_table_ctx, &logical_tables).await?;
        Ok(())
    }
    .await;

    if let Some(csv_dump_session) = csv_dump_session.take() {
        match &run_result {
            Ok(_) => {
                if let Err(err) = csv_dump_session.cleanup_on_success() {
                    warn!(
                        "Cleanup csv dump directory failed, path: {}, error: {:?}",
                        csv_dump_session.run_dir.display(),
                        err
                    );
                }
            }
            Err(_) => {
                warn!(
                    "Keep csv dump directory for failure analysis, path: {}",
                    csv_dump_session.run_dir.display()
                );
            }
        }
    }

    ctx.close().await;
    run_result
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
