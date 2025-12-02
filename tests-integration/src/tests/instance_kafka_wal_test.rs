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

use std::assert_matches::assert_matches;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use client::DEFAULT_CATALOG_NAME;
use common_query::{Output, OutputData};
use datatypes::arrow::array::{ArrayRef, AsArray, TimestampMillisecondArray};
use datatypes::arrow::datatypes::TimestampMillisecondType;
use frontend::instance::Instance;
use itertools::Itertools;
use rand::Rng;
use rand::rngs::ThreadRng;
use rstest::rstest;
use rstest_reuse::apply;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::{QueryContext, QueryContextRef};
use tokio::sync::Mutex;

use crate::tests::test_util::*;

#[apply(both_instances_cases_with_kafka_wal)]
async fn test_create_database_and_insert_query(
    rebuildable_instance: Option<Box<dyn RebuildableMockInstance>>,
) {
    let Some(instance) = rebuildable_instance else {
        return;
    };
    let instance = instance.frontend();

    let output = execute_sql_with(
        &instance,
        "create database test",
        QueryContext::with(DEFAULT_CATALOG_NAME, "test").into(),
    )
    .await;
    assert_matches!(output.data, OutputData::AffectedRows(1));

    let output = execute_sql_with(
        &instance,
        r#"create table greptime.test.demo(
             host STRING,
             cpu DOUBLE,
             memory DOUBLE,
             ts timestamp,
             TIME INDEX(ts)
        )"#,
        QueryContext::with(DEFAULT_CATALOG_NAME, "test").into(),
    )
    .await;
    assert!(matches!(output.data, OutputData::AffectedRows(0)));

    let output = execute_sql_with(
        &instance,
        r#"insert into test.demo(host, cpu, memory, ts) values
                           ('host1', 66.6, 1024, 1655276557000),
                           ('host2', 88.8, 333.3, 1655276558000)
        "#,
        QueryContext::with(DEFAULT_CATALOG_NAME, "test").into(),
    )
    .await;
    assert!(matches!(output.data, OutputData::AffectedRows(2)));

    let query_output = execute_sql_with(
        &instance,
        "select ts from test.demo order by ts limit 1",
        QueryContext::with(DEFAULT_CATALOG_NAME, "test").into(),
    )
    .await;
    match query_output.data {
        OutputData::Stream(s) => {
            let batches = common_recordbatch::util::collect(s).await.unwrap();
            assert_eq!(1, batches[0].num_columns());
            let expected = Arc::new(TimestampMillisecondArray::from_iter_values(vec![
                1655276557000_i64,
            ])) as ArrayRef;
            assert_eq!(batches[0].column(0), &expected);
        }
        _ => unreachable!(),
    }
}

/// Maintains metadata of a table.
struct Table {
    name: String,
    logical_timer: AtomicU64,
    inserted: Mutex<Vec<u64>>,
}

/// Inserts some data to a collection of tables and checks if these data exist after restart.
#[apply(both_instances_cases_with_kafka_wal)]
async fn test_replay(rebuildable_instance: Option<Box<dyn RebuildableMockInstance>>) {
    let Some(mut rebuildable_instance) = rebuildable_instance else {
        return;
    };
    let instance = rebuildable_instance.frontend();

    let output = execute_sql_with(
        &instance,
        "create database test",
        QueryContext::with(DEFAULT_CATALOG_NAME, "test").into(),
    )
    .await;
    assert_matches!(output.data, OutputData::AffectedRows(1));

    let tables = create_tables("test_replay", &instance, 10).await;
    insert_data(&tables, &instance, 15).await;
    ensure_data_exists(&tables, &instance).await;

    // Rebuilds to emulate restart which triggers a replay.
    rebuildable_instance.rebuild().await;
    ensure_data_exists(&tables, &rebuildable_instance.frontend()).await;
}

/// Inserts some data to a collection of tables and sends alter table requests to force flushing each table.
/// Then checks if these data exist after restart.
#[apply(both_instances_cases_with_kafka_wal)]
async fn test_flush_then_replay(rebuildable_instance: Option<Box<dyn RebuildableMockInstance>>) {
    let Some(mut rebuildable_instance) = rebuildable_instance else {
        return;
    };
    let instance = rebuildable_instance.frontend();

    let output = execute_sql_with(
        &instance,
        "create database test",
        QueryContext::with(DEFAULT_CATALOG_NAME, "test").into(),
    )
    .await;
    assert_matches!(output.data, OutputData::AffectedRows(1));

    let tables = create_tables("test_flush_then_replay", &instance, 10).await;
    insert_data(&tables, &instance, 15).await;
    ensure_data_exists(&tables, &instance).await;

    // Alters tables to force flushing.
    futures::future::join_all(tables.iter().map(|table| {
        let instance = instance.clone();
        async move {
            assert_matches!(
                do_alter(&instance, &table.name).await.data,
                OutputData::AffectedRows(0)
            );
        }
    }))
    .await;

    // Inserts more data and check all data exists after flushing.
    insert_data(&tables, &instance, 15).await;
    ensure_data_exists(&tables, &instance).await;

    // Rebuilds to emulate restart which triggers a replay.
    rebuildable_instance.rebuild().await;
    ensure_data_exists(&tables, &rebuildable_instance.frontend()).await;
}

/// Creates a given number of tables.
async fn create_tables(test_name: &str, instance: &Arc<Instance>, num_tables: usize) -> Vec<Table> {
    futures::future::join_all((0..num_tables).map(|i| {
        let instance = instance.clone();
        async move {
            let table_name = format!("{}_{}", test_name, i);
            assert_matches!(
                do_create(&instance, &table_name).await.data,
                OutputData::AffectedRows(0)
            );
            Table {
                name: table_name,
                logical_timer: AtomicU64::new(1685508715000),
                inserted: Mutex::new(Vec::new()),
            }
        }
    }))
    .await
}

/// Inserts data to the tables in parallel.
/// The reason why the insertion is parallel is that we want to ensure the kafka wal works as expected under parallel write workloads.
async fn insert_data(tables: &[Table], instance: &Arc<Instance>, num_writers: usize) {
    // Each writer randomly chooses a table and inserts a sequence of rows into the table.
    futures::future::join_all((0..num_writers).map(|_| async {
        let mut rng = rand::rng();
        let table = &tables[rng.random_range(0..tables.len())];
        for _ in 0..10 {
            let ts = table.logical_timer.fetch_add(1000, Ordering::Relaxed);
            let row = make_row(ts, &mut rng);
            assert_matches!(
                do_insert(instance, &table.name, row).await.data,
                OutputData::AffectedRows(1)
            );
            {
                // Inserting into the `inserted` vector and inserting into the database are not atomic
                // which requires us to do a sorting upon checking data integrity.
                let mut inserted = table.inserted.lock().await;
                inserted.push(ts);
            }
        }
    }))
    .await;
}

/// Sends queries to ensure the data exists for each table.
async fn ensure_data_exists(tables: &[Table], instance: &Arc<Instance>) {
    futures::future::join_all(tables.iter().map(|table| async {
        let output = do_query(instance, &table.name).await;
        let OutputData::Stream(stream) = output.data else {
            unreachable!()
        };
        let record_batches = common_recordbatch::util::collect(stream).await.unwrap();
        let queried = record_batches
            .into_iter()
            .flat_map(|rb| {
                let array = rb.column(0);
                let array = array.as_primitive::<TimestampMillisecondType>();
                array.iter().flatten().map(|x| x as u64).collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let inserted = table
            .inserted
            .lock()
            .await
            .iter()
            .sorted()
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(queried, inserted);
    }))
    .await;
}

/// Sends a create table SQL.
async fn do_create(instance: &Arc<Instance>, table_name: &str) -> Output {
    execute_sql_with(
        instance,
        &format!(
            r#"create table greptime.test.{} (
                    host STRING,
                    cpu DOUBLE,
                    memory DOUBLE,
                    ts timestamp,
                    TIME INDEX(ts)
            )"#,
            table_name
        ),
        QueryContext::with(DEFAULT_CATALOG_NAME, "test").into(),
    )
    .await
}

/// Sends an alter table SQL.
async fn do_alter(instance: &Arc<Instance>, table_name: &str) -> Output {
    execute_sql_with(
        instance,
        &format!("alter table {} add column new_col STRING", table_name),
        QueryContext::with(DEFAULT_CATALOG_NAME, "test").into(),
    )
    .await
}

/// Sends a insert SQL.
async fn do_insert(instance: &Arc<Instance>, table_name: &str, row: String) -> Output {
    execute_sql_with(
        instance,
        &format!("insert into test.{table_name}(host, cpu, memory, ts) values {row}"),
        QueryContext::with(DEFAULT_CATALOG_NAME, "test").into(),
    )
    .await
}

/// Sends a query SQL.
async fn do_query(instance: &Arc<Instance>, table_name: &str) -> Output {
    execute_sql_with(
        instance,
        &format!("select ts from test.{table_name} order by ts"),
        QueryContext::with(DEFAULT_CATALOG_NAME, "test").into(),
    )
    .await
}

/// Sends a SQL with the given context which specifies the catalog name and schema name, aka. database name.
/// The query context is required since the tables are created in the `test` schema rather than the default `public` schema.
async fn execute_sql_with(
    instance: &Arc<Instance>,
    sql: &str,
    query_ctx: QueryContextRef,
) -> Output {
    instance.do_query(sql, query_ctx).await.remove(0).unwrap()
}

fn make_row(ts: u64, rng: &mut ThreadRng) -> String {
    let host = format!("host{}", rng.random_range(0..5));
    let cpu: f64 = rng.random_range(0.0..99.9);
    let memory: f64 = rng.random_range(0.0..999.9);
    format!("('{host}', {cpu}, {memory}, {ts})")
}
