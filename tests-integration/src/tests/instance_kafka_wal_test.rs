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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use client::{OutputData, DEFAULT_CATALOG_NAME};
use common_query::Output;
use datatypes::vectors::{TimestampMillisecondVector, VectorRef};
use frontend::instance::Instance;
use itertools::Itertools;
use rand::rngs::ThreadRng;
use rand::Rng;
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
        QueryContext::with(DEFAULT_CATALOG_NAME, "test"),
    )
    .await;
    assert_matches!(output, OutputData::AffectedRows(1));

    let output = execute_sql_with(
        &instance,
        r#"create table greptime.test.demo(
             host STRING,
             cpu DOUBLE,
             memory DOUBLE,
             ts timestamp,
             TIME INDEX(ts)
        )"#,
        QueryContext::with(DEFAULT_CATALOG_NAME, "test"),
    )
    .await;
    assert!(matches!(output.data, OutputData::AffectedRows(0)));

    let output = execute_sql_with(
        &instance,
        r#"insert into test.demo(host, cpu, memory, ts) values
                           ('host1', 66.6, 1024, 1655276557000),
                           ('host2', 88.8, 333.3, 1655276558000)
        "#,
        QueryContext::with(DEFAULT_CATALOG_NAME, "test"),
    )
    .await;
    assert!(matches!(output.data, OutputData::AffectedRows(2)));

    let query_output = execute_sql_with(
        &instance,
        "select ts from test.demo order by ts limit 1",
        QueryContext::with(DEFAULT_CATALOG_NAME, "test"),
    )
    .await;
    match query_output {
        OutputData::Stream(s) => {
            let batches = common_recordbatch::util::collect(s).await.unwrap();
            assert_eq!(1, batches[0].num_columns());
            assert_eq!(
                Arc::new(TimestampMillisecondVector::from_vec(vec![
                    1655276557000_i64
                ])) as VectorRef,
                *batches[0].column(0)
            );
        }
        _ => unreachable!(),
    }
}

struct Table {
    name: String,
    logical_timer: AtomicU64,
    inserted: Mutex<Vec<u64>>,
}

#[apply(both_instances_cases_with_kafka_wal)]
async fn test_replay(rebuildable_instance: Option<Box<dyn RebuildableMockInstance>>) {
    let Some(mut rebuildable_instance) = rebuildable_instance else {
        return;
    };
    let instance = rebuildable_instance.frontend();

    let output = execute_sql_with(
        &instance,
        "create database test",
        QueryContext::with(DEFAULT_CATALOG_NAME, "test"),
    )
    .await;
    assert_matches!(output, Output::AffectedRows(1));

    let tables = create_tables("test_replay", &instance, 10).await;
    insert_data(&tables, &instance, 15).await;
    ensure_data_exists(&tables, &instance).await;

    // Rebuilds to emulate restart which then triggers a replay.
    let instance = rebuildable_instance.rebuild().await;
    ensure_data_exists(&tables, &instance).await;
}

#[apply(both_instances_cases_with_kafka_wal)]
async fn test_flush_then_replay(rebuildable_instance: Option<Box<dyn RebuildableMockInstance>>) {
    let Some(mut rebuildable_instance) = rebuildable_instance else {
        return;
    };
    let instance = rebuildable_instance.frontend();

    let output = execute_sql_with(
        &instance,
        "create database test",
        QueryContext::with(DEFAULT_CATALOG_NAME, "test"),
    )
    .await;
    assert_matches!(output, Output::AffectedRows(1));

    let tables = create_tables("test_flush_then_replay", &instance, 10).await;
    insert_data(&tables, &instance, 15).await;
    ensure_data_exists(&tables, &instance).await;

    // Renames tables to force flushing each table.
    let tables = futures::future::join_all(tables.into_iter().map(|table| {
        let instance = instance.clone();
        async move {
            // Repeats the last char to construct a new table name.
            let new_table_name = format!("{}{}", table.name, table.name.chars().last().unwrap());
            assert_matches!(
                do_alter(&instance, &table.name, &new_table_name).await,
                Output::AffectedRows(0)
            );
            Table {
                name: new_table_name,
                logical_timer: AtomicU64::new(table.logical_timer.load(Ordering::Relaxed)),
                inserted: Mutex::new(table.inserted.lock().await.clone()),
            }
        }
    }))
    .await;
    ensure_data_exists(&tables, &instance).await;

    // Rebuilds to emulate restart which then triggers a replay.
    let instance = rebuildable_instance.rebuild().await;
    ensure_data_exists(&tables, &instance).await;
}

async fn create_tables(test_name: &str, instance: &Arc<Instance>, num_tables: usize) -> Vec<Table> {
    futures::future::join_all((0..num_tables).map(|i| {
        let instance = instance.clone();
        async move {
            let table_name = format!("{}_{}", test_name, i);
            assert_matches!(
                do_create(&instance, &table_name).await,
                Output::AffectedRows(0)
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

async fn insert_data(tables: &[Table], instance: &Arc<Instance>, num_writers: usize) {
    // Each writer randomly chooses a table and inserts a sequence of rows into the table.
    futures::future::join_all((0..num_writers).map(|_| async {
        let mut rng = rand::thread_rng();
        let table = &tables[rng.gen_range(0..tables.len())];
        for _ in 0..10 {
            let ts = table.logical_timer.fetch_add(1000, Ordering::Relaxed);
            let row = make_row(ts, &mut rng);
            assert_matches!(
                do_insert(instance, &table.name, row).await,
                Output::AffectedRows(1)
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

async fn ensure_data_exists(tables: &[Table], instance: &Arc<Instance>) {
    futures::future::join_all(tables.iter().map(|table| async {
        let output = do_query(instance, &table.name).await;
        let Output::Stream(stream) = output else {
            unreachable!()
        };
        let record_batches = common_recordbatch::util::collect(stream).await.unwrap();
        let queried = record_batches
            .into_iter()
            .flat_map(|rb| {
                rb.rows()
                    .map(|row| row[0].as_timestamp().unwrap().value() as u64)
                    .collect::<Vec<_>>()
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
        QueryContext::with(DEFAULT_CATALOG_NAME, "test"),
    )
    .await
}

async fn do_alter(instance: &Arc<Instance>, table_name: &str, new_table_name: &str) -> Output {
    execute_sql_with(
        instance,
        &format!("alter table {} rename {}", table_name, new_table_name),
        QueryContext::with(DEFAULT_CATALOG_NAME, "test"),
    )
    .await
}

async fn do_insert(instance: &Arc<Instance>, table_name: &str, row: String) -> Output {
    execute_sql_with(
        instance,
        &format!("insert into test.{table_name}(host, cpu, memory, ts) values {row}"),
        QueryContext::with(DEFAULT_CATALOG_NAME, "test"),
    )
    .await
}

async fn do_query(instance: &Arc<Instance>, table_name: &str) -> Output {
    execute_sql_with(
        instance,
        &format!("select ts from test.{table_name} order by ts"),
        QueryContext::with(DEFAULT_CATALOG_NAME, "test"),
    )
    .await
}

async fn execute_sql_with(
    instance: &Arc<Instance>,
    sql: &str,
    query_ctx: QueryContextRef,
) -> Output {
    instance.do_query(sql, query_ctx).await.remove(0).unwrap()
}

fn make_row(ts: u64, rng: &mut ThreadRng) -> String {
    let host = format!("host{}", rng.gen_range(0..5));
    let cpu: f64 = rng.gen_range(0.0..99.9);
    let memory: f64 = rng.gen_range(0.0..999.9);
    format!("('{host}', {cpu}, {memory}, {ts})")
}
