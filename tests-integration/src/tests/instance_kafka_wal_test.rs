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

use std::sync::Arc;

use common_query::Output;
use common_recordbatch::util;
use datatypes::vectors::{TimestampMillisecondVector, VectorRef};
use frontend::error::Result;
use frontend::instance::Instance;
use rstest::rstest;
use rstest_reuse::apply;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::{QueryContext, QueryContextRef};

use crate::tests::test_util::*;

#[apply(both_instances_cases_with_kafka_wal)]
async fn test_create_database_and_insert_query(instance: Option<Box<dyn RebuildableMockInstance>>) {
    let Some(instance) = instance else { return };

    let instance = instance.frontend();

    let output = execute_sql(&instance, "create database test").await;
    assert!(matches!(output, Output::AffectedRows(1)));

    let output = execute_sql(
        &instance,
        r#"create table greptime.test.demo(
             host STRING,
             cpu DOUBLE,
             memory DOUBLE,
             ts timestamp,
             TIME INDEX(ts)
)"#,
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(0)));

    let output = execute_sql(
        &instance,
        r#"insert into test.demo(host, cpu, memory, ts) values
                           ('host1', 66.6, 1024, 1655276557000),
                           ('host2', 88.8,  333.3, 1655276558000)
                           "#,
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(2)));

    let query_output = execute_sql(&instance, "select ts from test.demo order by ts limit 1").await;
    match query_output {
        Output::Stream(s) => {
            let batches = util::collect(s).await.unwrap();
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

async fn execute_sql(instance: &Arc<Instance>, sql: &str) -> Output {
    execute_sql_with(instance, sql, QueryContext::arc()).await
}

async fn try_execute_sql_with(
    instance: &Arc<Instance>,
    sql: &str,
    query_ctx: QueryContextRef,
) -> Result<Output> {
    instance.do_query(sql, query_ctx).await.remove(0)
}

async fn execute_sql_with(
    instance: &Arc<Instance>,
    sql: &str,
    query_ctx: QueryContextRef,
) -> Output {
    try_execute_sql_with(instance, sql, query_ctx)
        .await
        .unwrap()
}
