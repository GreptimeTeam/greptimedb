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

use std::env;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arbitrary::{Arbitrary, Unstructured};
use common_telemetry::info;
use common_time::Timestamp;
use libfuzzer_sys::fuzz_target;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaChaRng;
use snafu::ResultExt;
use sqlx::{Executor, MySql, Pool};
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
use tests_fuzz::utils::cluster_info::{
    wait_for_all_datanode_offline, wait_for_all_datanode_online,
};
use tests_fuzz::utils::network_chaos::{
    inject_datanode_metasrv_network_partition, recover_network_chaos,
};
use tests_fuzz::utils::procedure::procedure_state as fetch_procedure_state_json;
use tests_fuzz::utils::{
    Connections, GT_FUZZ_CLUSTER_NAME, GT_FUZZ_CLUSTER_NAMESPACE, get_fuzz_override,
    get_gt_fuzz_input_max_rows, init_greptime_connections_via_env,
};
use tests_fuzz::validator;
use tests_fuzz::validator::row::count_values;

#[derive(Clone)]
struct FuzzContext {
    greptime: Pool<MySql>,
    kube: kube::client::Client,
    namespace: String,
    cluster_name: String,
}

const PROCEDURE_TIMEOUT: Duration = Duration::from_secs(300);
const NETWORK_CHAOS_DURATION_SECS: usize = 360;

impl FuzzContext {
    async fn close(self) {
        self.greptime.close().await;
    }
}

#[derive(Clone, Debug)]
struct FuzzInput {
    seed: u64,
    rows: usize,
    partitions: usize,
    chaos_delay_ms: u64,
    chaos_hold_secs: u64,
}

impl Arbitrary<'_> for FuzzInput {
    fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        let seed = get_fuzz_override::<u64>("SEED").unwrap_or(u.int_in_range(u64::MIN..=u64::MAX)?);
        let mut rng = ChaChaRng::seed_from_u64(seed);
        let rows = get_fuzz_override::<usize>("ROWS")
            .unwrap_or_else(|| rng.random_range(2..get_gt_fuzz_input_max_rows()));
        let partitions =
            get_fuzz_override::<usize>("PARTITIONS").unwrap_or_else(|| rng.random_range(2..8));
        let chaos_delay_ms =
            get_fuzz_override::<u64>("CHAOS_DELAY_MS").unwrap_or_else(|| rng.random_range(0..5000));
        let chaos_hold_secs =
            get_fuzz_override::<u64>("CHAOS_HOLD_SECS").unwrap_or_else(|| rng.random_range(10..20));

        Ok(FuzzInput {
            seed,
            rows,
            partitions,
            chaos_delay_ms,
            chaos_hold_secs,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProcedureTerminalState {
    Done,
    Failed,
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

fn build_insert_expr<R: Rng + 'static>(
    table_ctx: &TableContextRef,
    rng: &mut R,
    partitions: &SimplePartitions,
    clock: &Arc<Mutex<Timestamp>>,
    rows: usize,
) -> InsertIntoExpr {
    let ts_value_generator = generate_unique_timestamp_for_mysql_with_clock(clock.clone());
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();
    let partition_len = table_ctx.partition.as_ref().unwrap().exprs.len();
    let moved_partitions = partitions.clone();
    let insert_generator = InsertExprGeneratorBuilder::default()
        .table_ctx(table_ctx.clone())
        .omit_column_list(false)
        .rows(rows.max(partition_len))
        .required_columns(vec![partitions.column_name.clone()])
        .value_overrides(Some(Box::new(move |column, rng| {
            if column.name.value == moved_partitions.column_name.value {
                let bound_idx = counter_clone.fetch_add(1, Ordering::Relaxed) % partition_len;
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
        match ctx.greptime.execute(sql).await {
            Ok(_) => return Ok(()),
            Err(err) => {
                tokio::time::sleep(delay).await;
                delay = std::cmp::min(delay * 2, Duration::from_secs(1));
                attempt += 1;
                if attempt >= max_attempts {
                    return Err(err).context(error::ExecuteQuerySnafu { sql });
                }
            }
        }
    }
}

async fn create_table(ctx: &FuzzContext, expr: &CreateTableExpr) -> Result<TableContextRef> {
    let translator = CreateTableExprTranslator;
    let sql = translator.translate(expr)?;
    let result = sqlx::query(&sql)
        .execute(&ctx.greptime)
        .await
        .context(error::ExecuteQuerySnafu { sql: &sql })?;
    info!("Create table: {sql}, result: {result:?}");
    Ok(Arc::new(TableContext::from(expr)))
}

async fn insert_initial_rows<R: Rng + 'static>(
    ctx: &FuzzContext,
    table_ctx: &TableContextRef,
    rng: &mut R,
    rows: usize,
) -> Result<u64> {
    let partitions = SimplePartitions::from_table_ctx(table_ctx).unwrap();
    let clock = Arc::new(Mutex::new(Timestamp::current_millis()));
    let insert_expr = build_insert_expr(table_ctx, rng, &partitions, &clock, rows);
    let inserted_rows = insert_expr.values_list.len() as u64;
    let translator = InsertIntoExprTranslator;
    let sql = translator.translate(&insert_expr)?;
    execute_insert_with_retry(ctx, &sql).await?;
    info!("Insert initial rows: {sql}");
    Ok(inserted_rows)
}

async fn validate_table_rows(
    ctx: &FuzzContext,
    table_ctx: &TableContextRef,
    inserted_rows: u64,
) -> Result<()> {
    let count_sql = format!("SELECT COUNT(1) AS count FROM {}", table_ctx.name);
    let count = count_values(&ctx.greptime, &count_sql).await?;
    assert_eq!(count.count as u64, inserted_rows);

    let timestamp_column_name = table_ctx.timestamp_column().unwrap().name.clone();
    let distinct_count_sql = format!(
        "SELECT COUNT(DISTINCT {}) AS count FROM {}",
        timestamp_column_name, table_ctx.name
    );
    let distinct_count = count_values(&ctx.greptime, &distinct_count_sql).await?;
    assert_eq!(distinct_count.count as u64, inserted_rows);
    Ok(())
}

fn repartition_operation<R: Rng + 'static>(
    table_ctx: &TableContextRef,
    rng: &mut R,
    wait: bool,
) -> Result<RepartitionExpr> {
    let split = rng.random_bool(0.5);
    if table_ctx.partition.as_ref().unwrap().exprs.len() <= 2 || split {
        let expr = SplitPartitionExprGeneratorBuilder::default()
            .table_ctx(table_ctx.clone())
            .wait(wait)
            .build()
            .unwrap()
            .generate(rng)?;
        Ok(RepartitionExpr::Split(expr))
    } else {
        let expr = MergePartitionExprGeneratorBuilder::default()
            .table_ctx(table_ctx.clone())
            .wait(wait)
            .build()
            .unwrap()
            .generate(rng)?;
        Ok(RepartitionExpr::Merge(expr))
    }
}

async fn submit_repartition_procedure(ctx: &FuzzContext, expr: &RepartitionExpr) -> Result<String> {
    let translator = RepartitionExprTranslator;
    let sql = translator.translate(expr)?;
    let (procedure_id,) = sqlx::query_as::<_, (String,)>(&sql)
        .fetch_one(&ctx.greptime)
        .await
        .context(error::ExecuteQuerySnafu { sql: &sql })?;
    info!("Submit repartition procedure: {sql}, procedure_id: {procedure_id}");
    Ok(procedure_id)
}

async fn fetch_procedure_state(
    ctx: &FuzzContext,
    procedure_id: &str,
) -> Result<Option<ProcedureTerminalState>> {
    let output = fetch_procedure_state_json(&ctx.greptime, procedure_id).await;

    Ok(if output.contains("Done") {
        Some(ProcedureTerminalState::Done)
    } else if output.contains("Failed") {
        Some(ProcedureTerminalState::Failed)
    } else {
        None
    })
}

async fn wait_for_procedure_terminal_state(
    ctx: &FuzzContext,
    procedure_id: &str,
    timeout: Duration,
) -> Result<ProcedureTerminalState> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(state) = fetch_procedure_state(ctx, procedure_id).await? {
            info!("Procedure terminal state: {state:?}, procedure_id: {procedure_id}");
            return Ok(state);
        }

        if tokio::time::Instant::now() >= deadline {
            return error::AssertSnafu {
                reason: format!(
                    "procedure {procedure_id} did not reach terminal state before timeout"
                ),
            }
            .fail();
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn validate_terminal_metadata(
    ctx: &FuzzContext,
    table_name: &tests_fuzz::ir::Ident,
    terminal: ProcedureTerminalState,
    before_table_ctx: &TableContextRef,
    after_table_ctx: &TableContextRef,
) -> Result<()> {
    let partition_entries = validator::partition::fetch_partitions_info_schema(
        &ctx.greptime,
        "public".into(),
        table_name,
    )
    .await?;

    info!(
        "Validate terminal metadata: procedure_terminal_state={terminal:?}, partition_entries={partition_entries:?}"
    );

    match terminal {
        ProcedureTerminalState::Done => validator::partition::assert_partitions(
            after_table_ctx.partition.as_ref().unwrap(),
            &partition_entries,
        )?,
        ProcedureTerminalState::Failed => validator::partition::assert_partitions(
            before_table_ctx.partition.as_ref().unwrap(),
            &partition_entries,
        )?,
    }

    Ok(())
}

async fn drop_table(ctx: &FuzzContext, table_name: &tests_fuzz::ir::Ident) -> Result<()> {
    let sql = format!("DROP TABLE {}", table_name);
    let result = sqlx::query(&sql)
        .execute(&ctx.greptime)
        .await
        .context(error::ExecuteQuerySnafu { sql: &sql })?;
    info!("Drop table: {table_name}, result: {result:?}");
    Ok(())
}

async fn execute_repartition_chaos(ctx: FuzzContext, input: FuzzInput) -> Result<()> {
    info!("input: {input:?}");
    let mut rng = ChaChaRng::seed_from_u64(input.seed);

    let create_expr = generate_create_expr(&input, &mut rng)?;
    let before_table_ctx = create_table(&ctx, &create_expr).await?;
    let inserted_rows = insert_initial_rows(&ctx, &before_table_ctx, &mut rng, input.rows).await?;
    validate_table_rows(&ctx, &before_table_ctx, inserted_rows).await?;

    let before_entries = validator::partition::fetch_partitions_info_schema(
        &ctx.greptime,
        "public".into(),
        &before_table_ctx.name,
    )
    .await?;
    info!("Before repartition partition entries: {before_entries:?}");

    let repartition_expr = repartition_operation(&before_table_ctx, &mut rng, false)?;
    let after_table_ctx = Arc::new(
        Arc::unwrap_or_clone(before_table_ctx.clone())
            .repartition(repartition_expr.clone())
            .unwrap(),
    );

    let namespace = ctx.namespace.clone();
    let cluster_name = ctx.cluster_name.clone();
    let kube_client = ctx.kube.clone();
    let handle = tokio::spawn(async move {
        let chaos_name = inject_datanode_metasrv_network_partition(
            kube_client.clone(),
            &namespace,
            &cluster_name,
            NETWORK_CHAOS_DURATION_SECS,
        )
        .await
        .unwrap();
        info!(
            "Injected network chaos: {chaos_name}, holding for {} seconds",
            input.chaos_hold_secs
        );

        tokio::time::sleep(Duration::from_secs(input.chaos_hold_secs)).await;
        recover_network_chaos(kube_client.clone(), &namespace, &chaos_name)
            .await
            .unwrap();
    });

    wait_for_all_datanode_offline(ctx.greptime.clone(), Duration::from_secs(30)).await;
    info!(
        "Waiting for {} ms before submitting procedure",
        input.chaos_delay_ms
    );
    tokio::time::sleep(Duration::from_millis(input.chaos_delay_ms)).await;

    let procedure_id = submit_repartition_procedure(&ctx, &repartition_expr).await?;
    let terminal =
        wait_for_procedure_terminal_state(&ctx, &procedure_id, PROCEDURE_TIMEOUT).await?;
    handle.await.unwrap();

    wait_for_all_datanode_online(ctx.greptime.clone(), Duration::from_secs(30)).await;
    validate_terminal_metadata(
        &ctx,
        &before_table_ctx.name,
        terminal,
        &before_table_ctx,
        &after_table_ctx,
    )
    .await?;
    validate_table_rows(&ctx, &before_table_ctx, inserted_rows).await?;

    drop_table(&ctx, &before_table_ctx.name).await?;
    ctx.close().await;
    Ok(())
}

fuzz_target!(|input: FuzzInput| {
    common_telemetry::init_default_ut_logging();
    common_runtime::block_on_global(async {
        let Connections { mysql } = init_greptime_connections_via_env().await;
        let ctx = FuzzContext {
            greptime: mysql.expect("mysql connection init must be succeed"),
            kube: kube::client::Client::try_default()
                .await
                .expect("init kube client"),
            namespace: env::var(GT_FUZZ_CLUSTER_NAMESPACE).unwrap_or("my-greptimedb".to_string()),
            cluster_name: env::var(GT_FUZZ_CLUSTER_NAME).unwrap_or("my-greptimedb".to_string()),
        };
        execute_repartition_chaos(ctx, input)
            .await
            .unwrap_or_else(|err| panic!("fuzz test must be succeed: {err:?}"));
    })
});
