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
use std::env;
use std::sync::Arc;
use std::time::Duration;

use arbitrary::{Arbitrary, Unstructured};
use common_telemetry::info;
use libfuzzer_sys::fuzz_target;
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
use tests_fuzz::generator::create_expr::{
    CreateLogicalTableExprGeneratorBuilder, CreatePhysicalTableExprGeneratorBuilder,
};
use tests_fuzz::generator::insert_expr::InsertExprGeneratorBuilder;
use tests_fuzz::generator::Generator;
use tests_fuzz::ir::{
    generate_random_timestamp_for_mysql, generate_random_value, CreateTableExpr, InsertIntoExpr,
};
use tests_fuzz::translator::mysql::create_expr::CreateTableExprTranslator;
use tests_fuzz::translator::mysql::insert_expr::InsertIntoExprTranslator;
use tests_fuzz::translator::DslTranslator;
use tests_fuzz::utils::cluster_info::wait_for_all_datanode_online;
use tests_fuzz::utils::partition::{
    fetch_partitions, region_distribution, wait_for_all_regions_evicted,
};
use tests_fuzz::utils::pod_failure::{inject_datanode_pod_failure, recover_pod_failure};
use tests_fuzz::utils::{
    compact_table, flush_memtable, get_gt_fuzz_input_max_rows, get_gt_fuzz_input_max_tables,
    init_greptime_connections_via_env, Connections, GT_FUZZ_CLUSTER_NAME,
    GT_FUZZ_CLUSTER_NAMESPACE,
};
use tests_fuzz::validator::row::count_values;

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
    rows: usize,
    tables: usize,
}

impl Arbitrary<'_> for FuzzInput {
    fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        let seed = u.int_in_range(u64::MIN..=u64::MAX)?;
        let mut rng = ChaChaRng::seed_from_u64(seed);
        let max_rows = get_gt_fuzz_input_max_rows();
        let rows = rng.gen_range(2..max_rows);
        let max_tables = get_gt_fuzz_input_max_tables();
        let tables = rng.gen_range(1..max_tables);
        Ok(FuzzInput { rows, seed, tables })
    }
}

fn generate_create_physical_table_expr<R: Rng + 'static>(rng: &mut R) -> Result<CreateTableExpr> {
    let physical_table_if_not_exists = rng.gen_bool(0.5);
    let create_physical_table_expr = CreatePhysicalTableExprGeneratorBuilder::default()
        .name_generator(Box::new(MappedGenerator::new(
            WordGenerator,
            merge_two_word_map_fn(random_capitalize_map, uppercase_and_keyword_backtick_map),
        )))
        .if_not_exists(physical_table_if_not_exists)
        .build()
        .unwrap();
    create_physical_table_expr.generate(rng)
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
        .ts_value_generator(Box::new(generate_random_timestamp_for_mysql))
        .build()
        .unwrap();
    insert_generator.generate(rng)
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

async fn execute_failover(ctx: FuzzContext, input: FuzzInput) -> Result<()> {
    let mut rng = ChaCha20Rng::seed_from_u64(input.seed);
    // Creates a physical table.
    let physical_table_ctx = create_physical_table(&ctx, &mut rng).await?;

    let mut tables = HashMap::with_capacity(input.tables);

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
        if rng.gen_bool(0.1) {
            flush_memtable(&ctx.greptime, &physical_table_ctx.name).await?;
        }
        if rng.gen_bool(0.1) {
            compact_table(&ctx.greptime, &physical_table_ctx.name).await?;
        }

        tables.insert(
            logical_table_ctx.name.clone(),
            (logical_table_ctx.clone(), insert_expr),
        );
    }

    let partitions = fetch_partitions(&ctx.greptime, physical_table_ctx.name.clone()).await?;
    let region_distribution = region_distribution(partitions);
    let selected_datanode = *region_distribution.keys().next().unwrap();
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

    for (table_ctx, insert_expr) in tables.values() {
        let sql = format!("select count(1) as count from {}", table_ctx.name);
        let values = count_values(&ctx.greptime, &sql).await?;
        let expected_rows = insert_expr.values_list.len() as u64;
        assert_eq!(
            values.count as u64, expected_rows,
            "Expected rows: {}, got: {}, table: {}",
            expected_rows, values.count, table_ctx.name
        );
    }

    // Clean up
    for (table_ctx, _) in tables.values() {
        let sql = format!("DROP TABLE {}", table_ctx.name);
        let result = sqlx::query(&sql)
            .execute(&ctx.greptime)
            .await
            .context(error::ExecuteQuerySnafu { sql })?;
        info!("Drop table: {}\n\nResult: {result:?}\n\n", table_ctx.name);
    }

    let sql = format!("DROP TABLE {}", physical_table_ctx.name);
    let result = sqlx::query(&sql)
        .execute(&ctx.greptime)
        .await
        .context(error::ExecuteQuerySnafu { sql })?;
    info!(
        "Drop table: {}\n\nResult: {result:?}\n\n",
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
