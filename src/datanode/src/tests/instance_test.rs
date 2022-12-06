// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use common_catalog::consts::DEFAULT_SCHEMA_NAME;
use common_query::Output;
use common_recordbatch::util;
use datafusion::arrow_print;
use datafusion_common::record_batch::RecordBatch as DfRecordBatch;
use datatypes::arrow::array::{Int64Array, UInt64Array, Utf8Array};
use datatypes::arrow_array::StringArray;
use datatypes::prelude::ConcreteDataType;
use session::context::QueryContext;

use crate::instance::Instance;
use crate::tests::test_util;

#[tokio::test(flavor = "multi_thread")]
async fn test_create_database_and_insert_query() {
    common_telemetry::init_default_ut_logging();

    let (opts, _guard) =
        test_util::create_tmp_dir_and_datanode_opts("create_database_and_insert_query");
    let instance = Instance::with_mock_meta_client(&opts).await.unwrap();
    instance.start().await.unwrap();

    let output = execute_sql(&instance, "create database test").await;
    assert!(matches!(output, Output::AffectedRows(1)));

    let output = execute_sql(
        &instance,
        r#"create table greptime.test.demo(
             host STRING,
             cpu DOUBLE,
             memory DOUBLE,
             ts bigint,
             TIME INDEX(ts)
)"#,
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(1)));

    let output = execute_sql(
        &instance,
        r#"insert into test.demo(host, cpu, memory, ts) values
                           ('host1', 66.6, 1024, 1655276557000),
                           ('host2', 88.8,  333.3, 1655276558000)
                           "#,
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(2)));

    let query_output = execute_sql(&instance, "select ts from test.demo order by ts").await;
    match query_output {
        Output::Stream(s) => {
            let batches = util::collect(s).await.unwrap();
            let columns = batches[0].df_recordbatch.columns();
            assert_eq!(1, columns.len());
            assert_eq!(
                &Int64Array::from_slice(&[1655276557000, 1655276558000]),
                columns[0].as_any().downcast_ref::<Int64Array>().unwrap()
            );
        }
        _ => unreachable!(),
    }
}
#[tokio::test(flavor = "multi_thread")]
async fn test_issue477_same_table_name_in_different_databases() {
    common_telemetry::init_default_ut_logging();

    let (opts, _guard) =
        test_util::create_tmp_dir_and_datanode_opts("create_database_and_insert_query");
    let instance = Instance::with_mock_meta_client(&opts).await.unwrap();
    instance.start().await.unwrap();

    // Create database a and b
    let output = execute_sql(&instance, "create database a").await;
    assert!(matches!(output, Output::AffectedRows(1)));
    let output = execute_sql(&instance, "create database b").await;
    assert!(matches!(output, Output::AffectedRows(1)));

    // Create table a.demo and b.demo
    let output = execute_sql(
        &instance,
        r#"create table a.demo(
             host STRING,
             ts bigint,
             TIME INDEX(ts)
)"#,
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(1)));

    let output = execute_sql(
        &instance,
        r#"create table b.demo(
             host STRING,
             ts bigint,
             TIME INDEX(ts)
)"#,
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(1)));

    // Insert different data into a.demo and b.demo
    let output = execute_sql(
        &instance,
        r#"insert into a.demo(host, ts) values
                           ('host1', 1655276557000)
                           "#,
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(1)));
    let output = execute_sql(
        &instance,
        r#"insert into b.demo(host, ts) values
                           ('host2',1655276558000)
                           "#,
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(1)));

    // Query data and assert
    assert_query_result(
        &instance,
        "select host,ts from a.demo order by ts",
        1655276557000,
        "host1",
    )
    .await;

    assert_query_result(
        &instance,
        "select host,ts from b.demo order by ts",
        1655276558000,
        "host2",
    )
    .await;
}

async fn assert_query_result(instance: &Instance, sql: &str, ts: i64, host: &str) {
    let query_output = execute_sql(instance, sql).await;
    match query_output {
        Output::Stream(s) => {
            let batches = util::collect(s).await.unwrap();
            let columns = batches[0].df_recordbatch.columns();
            assert_eq!(2, columns.len());
            assert_eq!(
                &Utf8Array::<i32>::from_slice(&[host]),
                columns[0]
                    .as_any()
                    .downcast_ref::<Utf8Array<i32>>()
                    .unwrap()
            );
            assert_eq!(
                &Int64Array::from_slice(&[ts]),
                columns[1].as_any().downcast_ref::<Int64Array>().unwrap()
            );
        }
        _ => unreachable!(),
    }
}

async fn setup_test_instance() -> Instance {
    common_telemetry::init_default_ut_logging();

    let (opts, _guard) = test_util::create_tmp_dir_and_datanode_opts("execute_insert");
    let instance = Instance::with_mock_meta_client(&opts).await.unwrap();
    instance.start().await.unwrap();

    test_util::create_test_table(
        instance.catalog_manager(),
        instance.sql_handler(),
        ConcreteDataType::timestamp_millisecond_datatype(),
    )
    .await
    .unwrap();

    instance
}

#[tokio::test(flavor = "multi_thread")]
async fn test_execute_insert() {
    let instance = setup_test_instance().await;
    let output = execute_sql(
        &instance,
        r#"insert into demo(host, cpu, memory, ts) values
                           ('host1', 66.6, 1024, 1655276557000),
                           ('host2', 88.8,  333.3, 1655276558000)
                           "#,
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(2)));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_execute_insert_query_with_i64_timestamp() {
    common_telemetry::init_default_ut_logging();

    let (opts, _guard) = test_util::create_tmp_dir_and_datanode_opts("insert_query_i64_timestamp");
    let instance = Instance::with_mock_meta_client(&opts).await.unwrap();
    instance.start().await.unwrap();

    test_util::create_test_table(
        instance.catalog_manager(),
        instance.sql_handler(),
        ConcreteDataType::int64_datatype(),
    )
    .await
    .unwrap();

    let output = execute_sql(
        &instance,
        r#"insert into demo(host, cpu, memory, ts) values
                           ('host1', 66.6, 1024, 1655276557000),
                           ('host2', 88.8,  333.3, 1655276558000)
                           "#,
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(2)));

    let query_output = execute_sql(&instance, "select ts from demo order by ts").await;
    match query_output {
        Output::Stream(s) => {
            let batches = util::collect(s).await.unwrap();
            let columns = batches[0].df_recordbatch.columns();
            assert_eq!(1, columns.len());
            assert_eq!(
                &Int64Array::from_slice(&[1655276557000, 1655276558000]),
                columns[0].as_any().downcast_ref::<Int64Array>().unwrap()
            );
        }
        _ => unreachable!(),
    }

    let query_output = execute_sql(&instance, "select ts as time from demo order by ts").await;
    match query_output {
        Output::Stream(s) => {
            let batches = util::collect(s).await.unwrap();
            let columns = batches[0].df_recordbatch.columns();
            assert_eq!(1, columns.len());
            assert_eq!(
                &Int64Array::from_slice(&[1655276557000, 1655276558000]),
                columns[0].as_any().downcast_ref::<Int64Array>().unwrap()
            );
        }
        _ => unreachable!(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_execute_query() {
    let (opts, _guard) = test_util::create_tmp_dir_and_datanode_opts("execute_query");
    let instance = Instance::with_mock_meta_client(&opts).await.unwrap();
    instance.start().await.unwrap();

    let output = execute_sql(&instance, "select sum(number) from numbers limit 20").await;
    match output {
        Output::Stream(recordbatch) => {
            let numbers = util::collect(recordbatch).await.unwrap();
            let columns = numbers[0].df_recordbatch.columns();
            assert_eq!(1, columns.len());
            assert_eq!(columns[0].len(), 1);

            assert_eq!(
                *columns[0].as_any().downcast_ref::<UInt64Array>().unwrap(),
                UInt64Array::from_slice(&[4950])
            );
        }
        _ => unreachable!(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_execute_show_databases_tables() {
    let (opts, _guard) =
        test_util::create_tmp_dir_and_datanode_opts("execute_show_databases_tables");
    let instance = Instance::with_mock_meta_client(&opts).await.unwrap();
    instance.start().await.unwrap();

    let output = execute_sql(&instance, "show databases").await;
    match output {
        Output::RecordBatches(databases) => {
            let databases = databases.take();
            let columns = databases[0].df_recordbatch.columns();
            assert_eq!(1, columns.len());
            assert_eq!(columns[0].len(), 1);

            assert_eq!(
                *columns[0].as_any().downcast_ref::<StringArray>().unwrap(),
                StringArray::from(vec![Some("public")])
            );
        }
        _ => unreachable!(),
    }

    let output = execute_sql(&instance, "show databases like '%bl%'").await;
    match output {
        Output::RecordBatches(databases) => {
            let databases = databases.take();
            let columns = databases[0].df_recordbatch.columns();
            assert_eq!(1, columns.len());
            assert_eq!(columns[0].len(), 1);

            assert_eq!(
                *columns[0].as_any().downcast_ref::<StringArray>().unwrap(),
                StringArray::from(vec![Some("public")])
            );
        }
        _ => unreachable!(),
    }

    let output = execute_sql(&instance, "show tables").await;
    match output {
        Output::RecordBatches(databases) => {
            let databases = databases.take();
            let columns = databases[0].df_recordbatch.columns();
            assert_eq!(1, columns.len());
            assert_eq!(columns[0].len(), 2);
        }
        _ => unreachable!(),
    }

    // creat a table
    test_util::create_test_table(
        instance.catalog_manager(),
        instance.sql_handler(),
        ConcreteDataType::timestamp_millisecond_datatype(),
    )
    .await
    .unwrap();

    let output = execute_sql(&instance, "show tables").await;
    match output {
        Output::RecordBatches(databases) => {
            let databases = databases.take();
            let columns = databases[0].df_recordbatch.columns();
            assert_eq!(1, columns.len());
            assert_eq!(columns[0].len(), 3);
        }
        _ => unreachable!(),
    }

    // show tables like [string]
    let output = execute_sql(&instance, "show tables like 'de%'").await;
    match output {
        Output::RecordBatches(databases) => {
            let databases = databases.take();
            let columns = databases[0].df_recordbatch.columns();
            assert_eq!(1, columns.len());
            assert_eq!(columns[0].len(), 1);

            assert_eq!(
                *columns[0].as_any().downcast_ref::<StringArray>().unwrap(),
                StringArray::from(vec![Some("demo")])
            );
        }
        _ => unreachable!(),
    }
}

#[tokio::test(flavor = "multi_thread")]
pub async fn test_execute_create() {
    common_telemetry::init_default_ut_logging();

    let (opts, _guard) = test_util::create_tmp_dir_and_datanode_opts("execute_create");
    let instance = Instance::with_mock_meta_client(&opts).await.unwrap();
    instance.start().await.unwrap();

    let output = execute_sql(
        &instance,
        r#"create table test_table(
                            host string,
                            ts timestamp,
                            cpu double default 0,
                            memory double,
                            TIME INDEX (ts),
                            PRIMARY KEY(host)
                        ) engine=mito with(regions=1);"#,
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(1)));
}

async fn check_output_stream(output: Output, expected: Vec<&str>) {
    let recordbatches = match output {
        Output::Stream(stream) => util::collect(stream).await.unwrap(),
        Output::RecordBatches(recordbatches) => recordbatches.take(),
        _ => unreachable!(),
    };
    let recordbatches = recordbatches
        .into_iter()
        .map(|r| r.df_recordbatch)
        .collect::<Vec<DfRecordBatch>>();
    let pretty_print = arrow_print::write(&recordbatches);
    let pretty_print = pretty_print.lines().collect::<Vec<&str>>();
    assert_eq!(pretty_print, expected);
}

#[tokio::test]
async fn test_alter_table() {
    let instance = Instance::new_mock().await.unwrap();
    instance.start().await.unwrap();

    test_util::create_test_table(
        instance.catalog_manager(),
        instance.sql_handler(),
        ConcreteDataType::timestamp_millisecond_datatype(),
    )
    .await
    .unwrap();
    // make sure table insertion is ok before altering table
    execute_sql(
        &instance,
        "insert into demo(host, cpu, memory, ts) values ('host1', 1.1, 100, 1000)",
    )
    .await;

    // Add column
    let output = execute_sql(&instance, "alter table demo add my_tag string null").await;
    assert!(matches!(output, Output::AffectedRows(0)));

    let output = execute_sql(
        &instance,
        "insert into demo(host, cpu, memory, ts, my_tag) values ('host2', 2.2, 200, 2000, 'hello')",
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(1)));
    let output = execute_sql(
        &instance,
        "insert into demo(host, cpu, memory, ts) values ('host3', 3.3, 300, 3000)",
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(1)));

    let output = execute_sql(&instance, "select * from demo order by ts").await;
    let expected = vec![
        "+-------+-----+--------+---------------------+--------+",
        "| host  | cpu | memory | ts                  | my_tag |",
        "+-------+-----+--------+---------------------+--------+",
        "| host1 | 1.1 | 100    | 1970-01-01 00:00:01 |        |",
        "| host2 | 2.2 | 200    | 1970-01-01 00:00:02 | hello  |",
        "| host3 | 3.3 | 300    | 1970-01-01 00:00:03 |        |",
        "+-------+-----+--------+---------------------+--------+",
    ];
    check_output_stream(output, expected).await;

    // Drop a column
    let output = execute_sql(&instance, "alter table demo drop column memory").await;
    assert!(matches!(output, Output::AffectedRows(0)));

    let output = execute_sql(&instance, "select * from demo order by ts").await;
    let expected = vec![
        "+-------+-----+---------------------+--------+",
        "| host  | cpu | ts                  | my_tag |",
        "+-------+-----+---------------------+--------+",
        "| host1 | 1.1 | 1970-01-01 00:00:01 |        |",
        "| host2 | 2.2 | 1970-01-01 00:00:02 | hello  |",
        "| host3 | 3.3 | 1970-01-01 00:00:03 |        |",
        "+-------+-----+---------------------+--------+",
    ];
    check_output_stream(output, expected).await;

    // insert a new row
    let output = execute_sql(
        &instance,
        "insert into demo(host, cpu, ts, my_tag) values ('host4', 400, 4000, 'world')",
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(1)));

    let output = execute_sql(&instance, "select * from demo order by ts").await;
    let expected = vec![
        "+-------+-----+---------------------+--------+",
        "| host  | cpu | ts                  | my_tag |",
        "+-------+-----+---------------------+--------+",
        "| host1 | 1.1 | 1970-01-01 00:00:01 |        |",
        "| host2 | 2.2 | 1970-01-01 00:00:02 | hello  |",
        "| host3 | 3.3 | 1970-01-01 00:00:03 |        |",
        "| host4 | 400 | 1970-01-01 00:00:04 | world  |",
        "+-------+-----+---------------------+--------+",
    ];
    check_output_stream(output, expected).await;
}

async fn test_insert_with_default_value_for_type(type_name: &str) {
    let (opts, _guard) = test_util::create_tmp_dir_and_datanode_opts("execute_create");
    let instance = Instance::with_mock_meta_client(&opts).await.unwrap();
    instance.start().await.unwrap();

    let create_sql = format!(
        r#"create table test_table(
        host string,
        ts {} DEFAULT CURRENT_TIMESTAMP,
        cpu double default 0,
        TIME INDEX (ts),
        PRIMARY KEY(host)
    ) engine=mito with(regions=1);"#,
        type_name
    );
    let output = execute_sql(&instance, &create_sql).await;
    assert!(matches!(output, Output::AffectedRows(1)));

    // Insert with ts.
    let output = execute_sql(
        &instance,
        "insert into test_table(host, cpu, ts) values ('host1', 1.1, 1000)",
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(1)));

    // Insert without ts, so it should be filled by default value.
    let output = execute_sql(
        &instance,
        "insert into test_table(host, cpu) values ('host2', 2.2)",
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(1)));

    let output = execute_sql(&instance, "select host, cpu from test_table").await;
    let expected = vec![
        "+-------+-----+",
        "| host  | cpu |",
        "+-------+-----+",
        "| host1 | 1.1 |",
        "| host2 | 2.2 |",
        "+-------+-----+",
    ];
    check_output_stream(output, expected).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_insert_with_default_value() {
    common_telemetry::init_default_ut_logging();

    test_insert_with_default_value_for_type("timestamp").await;
    test_insert_with_default_value_for_type("bigint").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_use_database() {
    let (opts, _guard) = test_util::create_tmp_dir_and_datanode_opts("use_database");
    let instance = Instance::with_mock_meta_client(&opts).await.unwrap();
    instance.start().await.unwrap();

    let output = execute_sql(&instance, "create database db1").await;
    assert!(matches!(output, Output::AffectedRows(1)));

    let output = execute_sql_in_db(
        &instance,
        "create table tb1(col_i32 int, ts bigint, TIME INDEX(ts))",
        "db1",
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(1)));

    let output = execute_sql_in_db(&instance, "show tables", "db1").await;
    let expected = vec![
        "+--------+",
        "| Tables |",
        "+--------+",
        "| tb1    |",
        "+--------+",
    ];
    check_output_stream(output, expected).await;

    let output = execute_sql_in_db(
        &instance,
        r#"insert into tb1(col_i32, ts) values (1, 1655276557000)"#,
        "db1",
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(1)));

    let output = execute_sql_in_db(&instance, "select col_i32 from tb1", "db1").await;
    let expected = vec![
        "+---------+",
        "| col_i32 |",
        "+---------+",
        "| 1       |",
        "+---------+",
    ];
    check_output_stream(output, expected).await;

    // Making a particular database the default by means of the USE statement does not preclude
    // accessing tables in other databases.
    let output = execute_sql(&instance, "select number from public.numbers limit 1").await;
    let expected = vec![
        "+--------+",
        "| number |",
        "+--------+",
        "| 0      |",
        "+--------+",
    ];
    check_output_stream(output, expected).await;
}

async fn execute_sql(instance: &Instance, sql: &str) -> Output {
    execute_sql_in_db(instance, sql, DEFAULT_SCHEMA_NAME).await
}

async fn execute_sql_in_db(instance: &Instance, sql: &str, db: &str) -> Output {
    let query_ctx = Arc::new(QueryContext::with_current_schema(db.to_string()));
    instance.execute_sql(sql, query_ctx).await.unwrap()
}
