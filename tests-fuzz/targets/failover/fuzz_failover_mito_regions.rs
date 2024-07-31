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
use std::sync::Arc;
use std::time::Duration;

use arbitrary::{Arbitrary, Unstructured};
use common_telemetry::info;
use common_time::util::current_time_millis;
use futures::future::try_join_all;
use libfuzzer_sys::fuzz_target;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use rand_chacha::{ChaCha20Rng, ChaChaRng};
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
use tests_fuzz::generator::{Generator, Random};
use tests_fuzz::ir::{
    generate_random_value, generate_unique_timestamp_for_mysql, CreateTableExpr, Ident,
    InsertIntoExpr, MySQLTsColumnTypeGenerator,
};
use tests_fuzz::translator::mysql::create_expr::CreateTableExprTranslator;
use tests_fuzz::translator::mysql::insert_expr::InsertIntoExprTranslator;
use tests_fuzz::translator::DslTranslator;
use tests_fuzz::utils::cluster_info::wait_for_all_datanode_online;
use tests_fuzz::utils::partition::{
    fetch_partitions, pretty_print_region_distribution, region_distribution,
    wait_for_all_regions_evicted, Partition,
};
use tests_fuzz::utils::pod_failure::{inject_datanode_pod_failure, recover_pod_failure};
use tests_fuzz::utils::{
    compact_table, flush_memtable, get_gt_fuzz_input_max_columns,
    get_gt_fuzz_input_max_insert_actions, get_gt_fuzz_input_max_rows, get_gt_fuzz_input_max_tables,
    init_greptime_connections_via_env, Connections, GT_FUZZ_CLUSTER_NAME,
    GT_FUZZ_CLUSTER_NAMESPACE,
};
use tests_fuzz::validator::row::count_values;
use tokio::sync::Semaphore;

struct FuzzContext {
    greptime: Pool<MySql>,
    kube: kube::client::Client,
    namespace: String,
    cluster_name: String,
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
    tables: usize,
    inserts: usize,
}

impl Arbitrary<'_> for FuzzInput {
    fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        let seed = u.int_in_range(u64::MIN..=u64::MAX)?;
        let mut rng = ChaChaRng::seed_from_u64(seed);
        let max_columns = get_gt_fuzz_input_max_columns();
        let columns = rng.gen_range(2..max_columns);
        let max_rows = get_gt_fuzz_input_max_rows();
        let rows = rng.gen_range(2..max_rows);
        let max_tables = get_gt_fuzz_input_max_tables();
        let tables = rng.gen_range(2..max_tables);
        let max_inserts = get_gt_fuzz_input_max_insert_actions();
        let inserts = rng.gen_range(2..max_inserts);
        Ok(FuzzInput {
            columns,
            rows,
            seed,
            tables,
            inserts,
        })
    }
}

fn generate_create_exprs<R: Rng + 'static>(
    tables: usize,
    columns: usize,
    rng: &mut R,
) -> Result<Vec<CreateTableExpr>> {
    let name_generator = MappedGenerator::new(
        WordGenerator,
        merge_two_word_map_fn(random_capitalize_map, uppercase_and_keyword_backtick_map),
    );

    let base_table_name = name_generator.gen(rng);
    let min_column = columns / 2;
    let columns = rng.gen_range(min_column..columns);
    let mut exprs = Vec::with_capacity(tables);
    for i in 0..tables {
        let table_name = Ident {
            value: format!("{}_{i}", base_table_name.value),
            quote_style: base_table_name.quote_style,
        };
        let create_table_generator = CreateTableExprGeneratorBuilder::default()
            .name_generator(Box::new(MappedGenerator::new(
                WordGenerator,
                merge_two_word_map_fn(random_capitalize_map, uppercase_and_keyword_backtick_map),
            )))
            .name(table_name)
            .columns(columns)
            .engine("mito")
            .ts_column_type_generator(Box::new(MySQLTsColumnTypeGenerator))
            .build()
            .unwrap();
        let expr = create_table_generator.generate(rng)?;
        exprs.push(expr)
    }

    Ok(exprs)
}

async fn execute_create_exprs(
    ctx: &FuzzContext,
    exprs: Vec<CreateTableExpr>,
    parallelism: usize,
) -> Result<()> {
    let semaphore = Arc::new(Semaphore::new(parallelism));

    let tasks = exprs.iter().map(|expr| {
        let semaphore = semaphore.clone();
        let greptime = ctx.greptime.clone();
        async move {
            let _permit = semaphore.acquire().await.unwrap();
            let translator = CreateTableExprTranslator;
            let sql = translator.translate(expr).unwrap();
            sqlx::query(&sql)
                .execute(&greptime)
                .await
                .context(error::ExecuteQuerySnafu { sql: &sql })
        }
    });

    let _ = try_join_all(tasks).await?;
    Ok(())
}

fn generate_insert_exprs<R: Rng + 'static>(
    tables: &[TableContextRef],
    rng: &mut R,
    rows: usize,
    inserts: usize,
) -> Result<Vec<Vec<InsertIntoExpr>>> {
    let mut exprs = Vec::with_capacity(tables.len());
    for table_ctx in tables {
        let omit_column_list = rng.gen_bool(0.2);
        let min_rows = rows / 2;
        let rows = rng.gen_range(min_rows..rows);
        let min_inserts = inserts / 2;
        let inserts = rng.gen_range(min_inserts..inserts);

        let insert_generator = InsertExprGeneratorBuilder::default()
            .table_ctx(table_ctx.clone())
            .omit_column_list(omit_column_list)
            .rows(rows)
            .ts_value_generator(generate_unique_timestamp_for_mysql(current_time_millis()))
            .value_generator(Box::new(generate_random_value))
            .build()
            .unwrap();

        let inserts = (0..inserts)
            .map(|_| insert_generator.generate(rng))
            .collect::<Result<Vec<_>>>()?;
        exprs.push(inserts);
    }

    Ok(exprs)
}

async fn execute_insert_exprs<R: Rng + 'static>(
    ctx: &FuzzContext,
    inserts: Vec<Vec<InsertIntoExpr>>,
    parallelism: usize,
    rng: &mut R,
) -> Result<Vec<u64>> {
    let semaphore = Arc::new(Semaphore::new(parallelism));

    let tasks = inserts.into_iter().map(|inserts| {
        let flush_probability = rng.gen_range(0.0..1.0);
        let compact_probability = rng.gen_range(0.0..1.0);
        let seed: u64 = rng.gen();

        let semaphore = semaphore.clone();
        let greptime = ctx.greptime.clone();
        async move {
            let _permit = semaphore.acquire().await.unwrap();
            let mut total_affected = 0;
            let mut rng = ChaCha20Rng::seed_from_u64(seed);
            for insert_expr in inserts {
                let translator = InsertIntoExprTranslator;
                let sql = translator.translate(&insert_expr)?;
                let result = greptime
                    // unprepared query, see <https://github.com/GreptimeTeam/greptimedb/issues/3500>
                    .execute(sql.as_str())
                    .await
                    .context(error::ExecuteQuerySnafu { sql: &sql })?;
                ensure!(
                    result.rows_affected() == insert_expr.values_list.len() as u64,
                    error::AssertSnafu {
                        reason: format!(
                            "expected rows affected: {}, actual: {}",
                            insert_expr.values_list.len(),
                            result.rows_affected(),
                        )
                    }
                );
                if rng.gen_bool(flush_probability) {
                    flush_memtable(&greptime, &insert_expr.table_name).await?;
                }
                if rng.gen_bool(compact_probability) {
                    compact_table(&greptime, &insert_expr.table_name).await?;
                }
                total_affected += result.rows_affected();
            }

            Ok(total_affected)
        }
    });

    try_join_all(tasks).await
}

async fn collect_table_partitions(
    ctx: &FuzzContext,
    table_ctxs: &[TableContextRef],
) -> Result<Vec<Partition>> {
    let mut partitions = Vec::with_capacity(table_ctxs.len());
    for table_ctx in table_ctxs {
        let table_partitions = fetch_partitions(&ctx.greptime, table_ctx.name.clone()).await?;
        info!(
            "table: {}, partitions: {:?}",
            table_ctx.name, table_partitions
        );
        partitions.extend(table_partitions)
    }
    Ok(partitions)
}

async fn execute_failover(ctx: FuzzContext, input: FuzzInput) -> Result<()> {
    let mut rng = ChaCha20Rng::seed_from_u64(input.seed);
    info!("Generates {} tables", input.tables);
    let exprs = generate_create_exprs(input.tables, input.columns, &mut rng)?;
    let parallelism = 4;
    let table_ctxs = exprs
        .iter()
        .map(|expr| Arc::new(TableContext::from(expr)))
        .collect::<Vec<_>>();
    info!("Creates tables");
    execute_create_exprs(&ctx, exprs, parallelism).await?;
    let insert_exprs = generate_insert_exprs(&table_ctxs, &mut rng, input.rows, input.inserts)?;
    info!("Inserts value into tables");
    let affected_rows = execute_insert_exprs(&ctx, insert_exprs, parallelism, &mut rng).await?;
    let partitions = collect_table_partitions(&ctx, &table_ctxs).await?;
    let region_distribution = region_distribution(partitions);

    pretty_print_region_distribution(&region_distribution);
    let nodes = region_distribution.keys().cloned().collect::<Vec<_>>();
    let selected_datanode = nodes
        .choose_multiple(&mut rng, 1)
        .cloned()
        .collect::<Vec<_>>()
        .remove(0);
    let selected_regions = region_distribution
        .get(&selected_datanode)
        .cloned()
        .unwrap();

    // Injects pod failures
    info!("Injects pod failures to datanode: {selected_datanode}, regions: {selected_regions:?}");
    let chaos_name = inject_datanode_pod_failure(
        ctx.kube.clone(),
        &ctx.namespace,
        &ctx.cluster_name,
        selected_datanode,
        360,
    )
    .await?;

    // Waits for num of regions on `selected_datanode` become to 0.
    wait_for_all_regions_evicted(
        ctx.greptime.clone(),
        selected_datanode,
        Duration::from_secs(300),
    )
    .await;

    // Recovers pod failures
    recover_pod_failure(ctx.kube.clone(), &ctx.namespace, &chaos_name).await?;
    wait_for_all_datanode_online(ctx.greptime.clone(), Duration::from_secs(60)).await;

    // Validates value rows
    info!("Validates num of rows");
    for (table_ctx, expected_rows) in table_ctxs.iter().zip(affected_rows) {
        let sql = format!("select count(1) as count from {}", table_ctx.name);
        let values = count_values(&ctx.greptime, &sql).await?;
        assert_eq!(values.count as u64, expected_rows);
    }

    for table_ctx in table_ctxs {
        let sql = format!("DROP TABLE {}", table_ctx.name);
        let result = sqlx::query(&sql)
            .execute(&ctx.greptime)
            .await
            .context(error::ExecuteQuerySnafu { sql })?;
        info!("Drop table: {}\n\nResult: {result:?}\n\n", table_ctx.name);
    }

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
        execute_failover(ctx, input)
            .await
            .unwrap_or_else(|err| panic!("fuzz test must be succeed: {err:?}"));
    })
});
