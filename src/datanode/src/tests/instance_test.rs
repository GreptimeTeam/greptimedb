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

use std::env;
use std::sync::Arc;

use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_query::Output;
use common_recordbatch::util;
use common_telemetry::logging;
use datatypes::data_type::ConcreteDataType;
use datatypes::vectors::{Int64Vector, StringVector, UInt64Vector, VectorRef};
use session::context::QueryContext;

use crate::error::Error;
use crate::tests::test_util::{self, check_output_stream, setup_test_instance, MockInstance};

#[tokio::test(flavor = "multi_thread")]
async fn test_create_database_and_insert_query() {
    let instance = MockInstance::new("create_database_and_insert_query").await;

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

    let query_output = execute_sql(&instance, "select ts from test.demo order by ts").await;
    match query_output {
        Output::Stream(s) => {
            let batches = util::collect(s).await.unwrap();
            assert_eq!(1, batches[0].num_columns());
            assert_eq!(
                Arc::new(Int64Vector::from_vec(vec![
                    1655276557000_i64,
                    1655276558000_i64
                ])) as VectorRef,
                *batches[0].column(0)
            );
        }
        _ => unreachable!(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_issue477_same_table_name_in_different_databases() {
    let instance = MockInstance::new("test_issue477_same_table_name_in_different_databases").await;

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
    assert!(matches!(output, Output::AffectedRows(0)));

    let output = execute_sql(
        &instance,
        r#"create table b.demo(
             host STRING,
             ts bigint,
             TIME INDEX(ts)
)"#,
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(0)));

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

async fn assert_query_result(instance: &MockInstance, sql: &str, ts: i64, host: &str) {
    let query_output = execute_sql(instance, sql).await;
    match query_output {
        Output::Stream(s) => {
            let batches = util::collect(s).await.unwrap();
            // let columns = batches[0].df_recordbatch.columns();
            assert_eq!(2, batches[0].num_columns());
            assert_eq!(
                Arc::new(StringVector::from(vec![host])) as VectorRef,
                *batches[0].column(0)
            );
            assert_eq!(
                Arc::new(Int64Vector::from_vec(vec![ts])) as VectorRef,
                *batches[0].column(1)
            );
        }
        _ => unreachable!(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_execute_insert() {
    let instance = setup_test_instance("test_execute_insert").await;

    // create table
    execute_sql(
        &instance,
        "create table demo(host string, cpu double, memory double, ts timestamp time index);",
    )
    .await;

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
async fn test_execute_insert_by_select() {
    let instance = setup_test_instance("test_execute_insert_by_select").await;

    // create table
    execute_sql(
        &instance,
        "create table demo1(host string, cpu double, memory double, ts timestamp time index);",
    )
    .await;
    execute_sql(
        &instance,
        "create table demo2(host string, cpu double, memory double, ts timestamp time index);",
    )
    .await;

    let output = execute_sql(
        &instance,
        r#"insert into demo1(host, cpu, memory, ts) values
                           ('host1', 66.6, 1024, 1655276557000),
                           ('host2', 88.8,  333.3, 1655276558000)
                           "#,
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(2)));

    assert!(matches!(
        try_execute_sql(&instance, "insert into demo2(host) select * from demo1")
            .await
            .unwrap_err(),
        Error::ColumnValuesNumberMismatch { .. }
    ));
    assert!(matches!(
        try_execute_sql(&instance, "insert into demo2 select cpu,memory from demo1")
            .await
            .unwrap_err(),
        Error::ColumnValuesNumberMismatch { .. }
    ));

    assert!(matches!(
        try_execute_sql(&instance, "insert into demo2(ts) select memory from demo1")
            .await
            .unwrap_err(),
        Error::ColumnTypeMismatch { .. }
    ));

    let output = execute_sql(&instance, "insert into demo2 select * from demo1").await;
    assert!(matches!(output, Output::AffectedRows(2)));

    let output = execute_sql(&instance, "select * from demo2 order by ts").await;
    let expected = "\
+-------+------+--------+---------------------+
| host  | cpu  | memory | ts                  |
+-------+------+--------+---------------------+
| host1 | 66.6 | 1024.0 | 2022-06-15T07:02:37 |
| host2 | 88.8 | 333.3  | 2022-06-15T07:02:38 |
+-------+------+--------+---------------------+"
        .to_string();
    check_output_stream(output, expected).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_execute_insert_query_with_i64_timestamp() {
    let instance = MockInstance::new("insert_query_i64_timestamp").await;

    test_util::create_test_table(instance.inner(), ConcreteDataType::int64_datatype())
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
            assert_eq!(1, batches[0].num_columns());
            assert_eq!(
                Arc::new(Int64Vector::from_vec(vec![
                    1655276557000_i64,
                    1655276558000_i64
                ])) as VectorRef,
                *batches[0].column(0)
            );
        }
        _ => unreachable!(),
    }

    let query_output = execute_sql(&instance, "select ts as time from demo order by ts").await;
    match query_output {
        Output::Stream(s) => {
            let batches = util::collect(s).await.unwrap();
            assert_eq!(1, batches[0].num_columns());
            assert_eq!(
                Arc::new(Int64Vector::from_vec(vec![
                    1655276557000_i64,
                    1655276558000_i64
                ])) as VectorRef,
                *batches[0].column(0)
            );
        }
        _ => unreachable!(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_execute_query() {
    let instance = MockInstance::new("execute_query").await;

    let output = execute_sql(&instance, "select sum(number) from numbers limit 20").await;
    match output {
        Output::Stream(recordbatch) => {
            let numbers = util::collect(recordbatch).await.unwrap();
            assert_eq!(1, numbers[0].num_columns());
            assert_eq!(numbers[0].column(0).len(), 1);

            assert_eq!(
                Arc::new(UInt64Vector::from_vec(vec![4950_u64])) as VectorRef,
                *numbers[0].column(0),
            );
        }
        _ => unreachable!(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_execute_show_databases_tables() {
    let instance = MockInstance::new("execute_show_databases_tables").await;

    let output = execute_sql(&instance, "show databases").await;
    match output {
        Output::RecordBatches(databases) => {
            let databases = databases.take();
            assert_eq!(1, databases[0].num_columns());
            assert_eq!(databases[0].column(0).len(), 1);

            assert_eq!(
                *databases[0].column(0),
                Arc::new(StringVector::from(vec![Some("public")])) as VectorRef
            );
        }
        _ => unreachable!(),
    }

    let output = execute_sql(&instance, "show databases like '%bl%'").await;
    match output {
        Output::RecordBatches(databases) => {
            let databases = databases.take();
            assert_eq!(1, databases[0].num_columns());
            assert_eq!(databases[0].column(0).len(), 1);

            assert_eq!(
                *databases[0].column(0),
                Arc::new(StringVector::from(vec![Some("public")])) as VectorRef
            );
        }
        _ => unreachable!(),
    }

    let output = execute_sql(&instance, "show tables").await;
    match output {
        Output::RecordBatches(databases) => {
            let databases = databases.take();
            assert_eq!(1, databases[0].num_columns());
            assert_eq!(databases[0].column(0).len(), 2);
        }
        _ => unreachable!(),
    }

    // creat a table
    test_util::create_test_table(
        instance.inner(),
        ConcreteDataType::timestamp_millisecond_datatype(),
    )
    .await
    .unwrap();

    let output = execute_sql(&instance, "show tables").await;
    match output {
        Output::RecordBatches(databases) => {
            let databases = databases.take();
            assert_eq!(1, databases[0].num_columns());
            assert_eq!(databases[0].column(0).len(), 3);
        }
        _ => unreachable!(),
    }

    // show tables like [string]
    let output = execute_sql(&instance, "show tables like 'de%'").await;
    match output {
        Output::RecordBatches(databases) => {
            let databases = databases.take();
            assert_eq!(1, databases[0].num_columns());
            assert_eq!(databases[0].column(0).len(), 1);

            assert_eq!(
                *databases[0].column(0),
                Arc::new(StringVector::from(vec![Some("demo")])) as VectorRef
            );
        }
        _ => unreachable!(),
    }
}

#[tokio::test(flavor = "multi_thread")]
pub async fn test_execute_create() {
    let instance = MockInstance::new("execute_create").await;

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
    assert!(matches!(output, Output::AffectedRows(0)));
}

#[tokio::test]
async fn test_rename_table() {
    common_telemetry::init_default_ut_logging();
    let instance = MockInstance::new("test_rename_table_local").await;

    let output = execute_sql(&instance, "create database db").await;
    assert!(matches!(output, Output::AffectedRows(1)));

    let output = execute_sql_in_db(
        &instance,
        "create table demo(host string, cpu double, memory double, ts timestamp, time index(ts))",
        "db",
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(0)));

    // make sure table insertion is ok before altering table name
    let output = execute_sql_in_db(
        &instance,
        "insert into demo(host, cpu, memory, ts) values ('host1', 1.1, 100, 1000), ('host2', 2.2, 200, 2000)",
        "db",
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(2)));

    // rename table
    let output = execute_sql_in_db(&instance, "alter table demo rename test_table", "db").await;
    assert!(matches!(output, Output::AffectedRows(0)));

    let output = execute_sql_in_db(&instance, "show tables", "db").await;
    let expect = "\
+------------+
| Tables     |
+------------+
| test_table |
+------------+\
"
    .to_string();
    check_output_stream(output, expect).await;

    let output = execute_sql_in_db(&instance, "select * from test_table order by ts", "db").await;
    let expected = "\
+-------+-----+--------+---------------------+
| host  | cpu | memory | ts                  |
+-------+-----+--------+---------------------+
| host1 | 1.1 | 100.0  | 1970-01-01T00:00:01 |
| host2 | 2.2 | 200.0  | 1970-01-01T00:00:02 |
+-------+-----+--------+---------------------+\
"
    .to_string();
    check_output_stream(output, expected).await;

    try_execute_sql_in_db(&instance, "select * from demo", "db")
        .await
        .expect_err("no table found in expect");
}

#[tokio::test]
async fn test_create_table_after_rename_table() {
    let instance = MockInstance::new("test_rename_table_local").await;

    let output = execute_sql(&instance, "create database db").await;
    assert!(matches!(output, Output::AffectedRows(1)));

    // create test table
    let table_name = "demo";
    let output = execute_sql_in_db(
        &instance,
        &format!("create table {table_name}(host string, cpu double, memory double, ts timestamp, time index(ts))"),
        "db",
    )
        .await;
    assert!(matches!(output, Output::AffectedRows(0)));

    // rename table
    let new_table_name = "test_table";
    let output = execute_sql_in_db(
        &instance,
        &format!("alter table {table_name} rename {new_table_name}"),
        "db",
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(0)));

    // create table with same name
    // create test table
    let output = execute_sql_in_db(
        &instance,
        &format!("create table {table_name}(host string, cpu double, memory double, ts timestamp, time index(ts))"),
        "db",
    )
        .await;
    assert!(matches!(output, Output::AffectedRows(0)));

    let expect = "\
+------------+
| Tables     |
+------------+
| demo       |
| test_table |
+------------+\
"
    .to_string();
    let output = execute_sql_in_db(&instance, "show tables", "db").await;
    check_output_stream(output, expect).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_alter_table() {
    let instance = setup_test_instance("test_alter_table").await;

    // create table
    execute_sql(
        &instance,
        "create table demo(host string, cpu double, memory double, ts timestamp time index);",
    )
    .await;

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
    let expected = "\
+-------+-----+--------+---------------------+--------+
| host  | cpu | memory | ts                  | my_tag |
+-------+-----+--------+---------------------+--------+
| host1 | 1.1 | 100.0  | 1970-01-01T00:00:01 |        |
| host2 | 2.2 | 200.0  | 1970-01-01T00:00:02 | hello  |
| host3 | 3.3 | 300.0  | 1970-01-01T00:00:03 |        |
+-------+-----+--------+---------------------+--------+\
    "
    .to_string();
    check_output_stream(output, expected).await;

    // Drop a column
    let output = execute_sql(&instance, "alter table demo drop column memory").await;
    assert!(matches!(output, Output::AffectedRows(0)));

    let output = execute_sql(&instance, "select * from demo order by ts").await;
    let expected = "\
+-------+-----+---------------------+--------+
| host  | cpu | ts                  | my_tag |
+-------+-----+---------------------+--------+
| host1 | 1.1 | 1970-01-01T00:00:01 |        |
| host2 | 2.2 | 1970-01-01T00:00:02 | hello  |
| host3 | 3.3 | 1970-01-01T00:00:03 |        |
+-------+-----+---------------------+--------+\
    "
    .to_string();
    check_output_stream(output, expected).await;

    // insert a new row
    let output = execute_sql(
        &instance,
        "insert into demo(host, cpu, ts, my_tag) values ('host4', 400, 4000, 'world')",
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(1)));

    let output = execute_sql(&instance, "select * from demo order by ts").await;
    let expected = "\
+-------+-------+---------------------+--------+
| host  | cpu   | ts                  | my_tag |
+-------+-------+---------------------+--------+
| host1 | 1.1   | 1970-01-01T00:00:01 |        |
| host2 | 2.2   | 1970-01-01T00:00:02 | hello  |
| host3 | 3.3   | 1970-01-01T00:00:03 |        |
| host4 | 400.0 | 1970-01-01T00:00:04 | world  |
+-------+-------+---------------------+--------+\
    "
    .to_string();
    check_output_stream(output, expected).await;
}

async fn test_insert_with_default_value_for_type(type_name: &str) {
    let instance = MockInstance::new("execute_create").await;

    let create_sql = format!(
        r#"create table test_table(
        host string,
        ts {type_name} DEFAULT CURRENT_TIMESTAMP,
        cpu double default 0,
        TIME INDEX (ts),
        PRIMARY KEY(host)
    ) engine=mito with(regions=1);"#,
    );
    let output = execute_sql(&instance, &create_sql).await;
    assert!(matches!(output, Output::AffectedRows(0)));

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
    let expected = "\
+-------+-----+
| host  | cpu |
+-------+-----+
| host1 | 1.1 |
| host2 | 2.2 |
+-------+-----+\
    "
    .to_string();
    check_output_stream(output, expected).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_insert_with_default_value() {
    test_insert_with_default_value_for_type("timestamp").await;
    test_insert_with_default_value_for_type("bigint").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_use_database() {
    let instance = MockInstance::new("test_use_database").await;

    let output = execute_sql(&instance, "create database db1").await;
    assert!(matches!(output, Output::AffectedRows(1)));

    let output = execute_sql_in_db(
        &instance,
        "create table tb1(col_i32 int, ts bigint, TIME INDEX(ts))",
        "db1",
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(0)));

    let output = execute_sql_in_db(&instance, "show tables", "db1").await;
    let expected = "\
+--------+
| Tables |
+--------+
| tb1    |
+--------+\
    "
    .to_string();
    check_output_stream(output, expected).await;

    let output = execute_sql_in_db(
        &instance,
        r#"insert into tb1(col_i32, ts) values (1, 1655276557000)"#,
        "db1",
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(1)));

    let output = execute_sql_in_db(&instance, "select col_i32 from tb1", "db1").await;
    let expected = "\
+---------+
| col_i32 |
+---------+
| 1       |
+---------+\
    "
    .to_string();
    check_output_stream(output, expected).await;

    // Making a particular database the default by means of the USE statement does not preclude
    // accessing tables in other databases.
    let output = execute_sql(&instance, "select number from public.numbers limit 1").await;
    let expected = "\
+--------+
| number |
+--------+
| 0      |
+--------+\
    "
    .to_string();
    check_output_stream(output, expected).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delete() {
    let instance = MockInstance::new("test_delete").await;

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
    assert!(matches!(output, Output::AffectedRows(0)));

    let output = execute_sql(
        &instance,
        r#"insert into test_table(host, cpu, memory, ts) values
                           ('host1', 66.6, 1024, 1655276557000),
                           ('host2', 77.7,  2048, 1655276558000),
                           ('host3', 88.8,  3072, 1655276559000)
                           "#,
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(3)));

    let output = execute_sql(
        &instance,
        "delete from test_table where host = host1 and ts = 1655276557000 ",
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(1)));

    let output = execute_sql(&instance, "select * from test_table").await;
    let expect = "\
+-------+---------------------+------+--------+
| host  | ts                  | cpu  | memory |
+-------+---------------------+------+--------+
| host2 | 2022-06-15T07:02:38 | 77.7 | 2048.0 |
| host3 | 2022-06-15T07:02:39 | 88.8 | 3072.0 |
+-------+---------------------+------+--------+\
"
    .to_string();
    check_output_stream(output, expect).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_execute_copy_to() {
    let instance = setup_test_instance("test_execute_copy_to").await;

    // setups
    execute_sql(
        &instance,
        "create table demo(host string, cpu double, memory double, ts timestamp time index);",
    )
    .await;

    let output = execute_sql(
        &instance,
        r#"insert into demo(host, cpu, memory, ts) values
                            ('host1', 66.6, 1024, 1655276557000),
                            ('host2', 88.8,  333.3, 1655276558000)
                            "#,
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(2)));

    // exports
    let data_dir = instance.data_tmp_dir().path();

    let copy_to_stmt = format!("Copy demo TO '{}/export/demo.parquet'", data_dir.display());

    let output = execute_sql(&instance, &copy_to_stmt).await;
    assert!(matches!(output, Output::AffectedRows(2)));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_execute_copy_to_s3() {
    logging::init_default_ut_logging();
    if let Ok(bucket) = env::var("GT_S3_BUCKET") {
        if !bucket.is_empty() {
            let instance = setup_test_instance("test_execute_copy_to_s3").await;

            // setups
            execute_sql(
            &instance,
            "create table demo(host string, cpu double, memory double, ts timestamp time index);",
        )
        .await;

            let output = execute_sql(
                &instance,
                r#"insert into demo(host, cpu, memory, ts) values
                            ('host1', 66.6, 1024, 1655276557000),
                            ('host2', 88.8,  333.3, 1655276558000)
                            "#,
            )
            .await;
            assert!(matches!(output, Output::AffectedRows(2)));
            let key_id = env::var("GT_S3_ACCESS_KEY_ID").unwrap();
            let key = env::var("GT_S3_ACCESS_KEY").unwrap();
            let url =
                env::var("GT_S3_ENDPOINT_URL").unwrap_or("https://s3.amazonaws.com".to_string());

            let root = uuid::Uuid::new_v4().to_string();

            // exports
            let copy_to_stmt = format!("Copy demo TO 's3://{}/{}/export/demo.parquet' CONNECTION (ACCESS_KEY_ID='{}',SECRET_ACCESS_KEY='{}',ENDPOINT_URL='{}')", bucket,root ,key_id, key, url);

            let output = execute_sql(&instance, &copy_to_stmt).await;
            assert!(matches!(output, Output::AffectedRows(2)));
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_execute_copy_from() {
    let instance = setup_test_instance("test_execute_copy_from").await;

    // setups
    execute_sql(
        &instance,
        "create table demo(host string, cpu double, memory double, ts timestamp time index);",
    )
    .await;

    let output = execute_sql(
        &instance,
        r#"insert into demo(host, cpu, memory, ts) values
                            ('host1', 66.6, 1024, 1655276557000),
                            ('host2', 88.8,  333.3, 1655276558000)
                            "#,
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(2)));

    // export
    let data_dir = instance.data_tmp_dir().path();

    let copy_to_stmt = format!("Copy demo TO '{}/export/demo.parquet'", data_dir.display());

    let output = execute_sql(&instance, &copy_to_stmt).await;
    assert!(matches!(output, Output::AffectedRows(2)));

    struct Test<'a> {
        sql: &'a str,
        table_name: &'a str,
    }
    let tests = [
        Test {
            sql: &format!(
                "Copy with_filename FROM '{}/export/demo.parquet_1_2'",
                data_dir.display()
            ),
            table_name: "with_filename",
        },
        Test {
            sql: &format!("Copy with_path FROM '{}/export/'", data_dir.display()),
            table_name: "with_path",
        },
        Test {
            sql: &format!(
                "Copy with_pattern FROM '{}/export/' WITH (PATTERN = 'demo.*')",
                data_dir.display()
            ),
            table_name: "with_pattern",
        },
    ];

    for test in tests {
        // import
        execute_sql(
            &instance,
            &format!(
                "create table {}(host string, cpu double, memory double, ts timestamp time index);",
                test.table_name
            ),
        )
        .await;

        let output = execute_sql(&instance, test.sql).await;
        assert!(matches!(output, Output::AffectedRows(2)));

        let output = execute_sql(
            &instance,
            &format!("select * from {} order by ts", test.table_name),
        )
        .await;
        let expected = "\
+-------+------+--------+---------------------+
| host  | cpu  | memory | ts                  |
+-------+------+--------+---------------------+
| host1 | 66.6 | 1024.0 | 2022-06-15T07:02:37 |
| host2 | 88.8 | 333.3  | 2022-06-15T07:02:38 |
+-------+------+--------+---------------------+"
            .to_string();
        check_output_stream(output, expected).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_execute_copy_from_s3() {
    logging::init_default_ut_logging();
    if let Ok(bucket) = env::var("GT_S3_BUCKET") {
        if !bucket.is_empty() {
            let instance = setup_test_instance("test_execute_copy_from_s3").await;

            // setups
            execute_sql(
            &instance,
            "create table demo(host string, cpu double, memory double, ts timestamp time index);",
        )
        .await;

            let output = execute_sql(
                &instance,
                r#"insert into demo(host, cpu, memory, ts) values
                            ('host1', 66.6, 1024, 1655276557000),
                            ('host2', 88.8,  333.3, 1655276558000)
                            "#,
            )
            .await;
            assert!(matches!(output, Output::AffectedRows(2)));

            // export
            let root = uuid::Uuid::new_v4().to_string();
            let key_id = env::var("GT_S3_ACCESS_KEY_ID").unwrap();
            let key = env::var("GT_S3_ACCESS_KEY").unwrap();
            let url =
                env::var("GT_S3_ENDPOINT_URL").unwrap_or("https://s3.amazonaws.com".to_string());

            let copy_to_stmt = format!("Copy demo TO 's3://{}/{}/export/demo.parquet' CONNECTION (ACCESS_KEY_ID='{}',SECRET_ACCESS_KEY='{}',ENDPOINT_URL='{}')", bucket,root ,key_id, key, url);
            logging::info!("Copy table to s3: {}", copy_to_stmt);

            let output = execute_sql(&instance, &copy_to_stmt).await;
            assert!(matches!(output, Output::AffectedRows(2)));

            struct Test<'a> {
                sql: &'a str,
                table_name: &'a str,
            }
            let tests = [
                Test {
                    sql: &format!(
                        "Copy with_filename FROM 's3://{}/{}/export/demo.parquet_1_2'",
                        bucket, root
                    ),
                    table_name: "with_filename",
                },
                Test {
                    sql: &format!("Copy with_path FROM 's3://{}/{}/export/'", bucket, root),
                    table_name: "with_path",
                },
                Test {
                    sql: &format!(
                        "Copy with_pattern FROM 's3://{}/{}/export/' WITH (PATTERN = 'demo.*')",
                        bucket, root
                    ),
                    table_name: "with_pattern",
                },
            ];

            for test in tests {
                // import
                execute_sql(
                    &instance,
                    &format!(
                "create table {}(host string, cpu double, memory double, ts timestamp time index);",
                test.table_name
            ),
                )
                .await;
                let sql = format!(
                    "{} CONNECTION (ACCESS_KEY_ID='{}',SECRET_ACCESS_KEY='{}',ENDPOINT_URL='{}')",
                    test.sql, key_id, key, url
                );
                logging::info!("Running sql: {}", sql);

                let output = execute_sql(&instance, &sql).await;
                assert!(matches!(output, Output::AffectedRows(2)));

                let output = execute_sql(
                    &instance,
                    &format!("select * from {} order by ts", test.table_name),
                )
                .await;
                let expected = "\
+-------+------+--------+---------------------+
| host  | cpu  | memory | ts                  |
+-------+------+--------+---------------------+
| host1 | 66.6 | 1024.0 | 2022-06-15T07:02:37 |
| host2 | 88.8 | 333.3  | 2022-06-15T07:02:38 |
+-------+------+--------+---------------------+"
                    .to_string();
                check_output_stream(output, expected).await;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_by_procedure() {
    common_telemetry::init_default_ut_logging();

    let instance = MockInstance::with_procedure_enabled("create_by_procedure").await;

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
    assert!(matches!(output, Output::AffectedRows(0)));

    // Create if not exists
    let output = execute_sql(
        &instance,
        r#"create table if not exists test_table(
                            host string,
                            ts timestamp,
                            cpu double default 0,
                            memory double,
                            TIME INDEX (ts),
                            PRIMARY KEY(host)
                        ) engine=mito with(regions=1);"#,
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(0)));
}

async fn execute_sql(instance: &MockInstance, sql: &str) -> Output {
    execute_sql_in_db(instance, sql, DEFAULT_SCHEMA_NAME).await
}

async fn try_execute_sql(
    instance: &MockInstance,
    sql: &str,
) -> Result<Output, crate::error::Error> {
    try_execute_sql_in_db(instance, sql, DEFAULT_SCHEMA_NAME).await
}

async fn try_execute_sql_in_db(
    instance: &MockInstance,
    sql: &str,
    db: &str,
) -> Result<Output, crate::error::Error> {
    let query_ctx = Arc::new(QueryContext::with(DEFAULT_CATALOG_NAME, db));
    instance.inner().execute_sql(sql, query_ctx).await
}

async fn execute_sql_in_db(instance: &MockInstance, sql: &str, db: &str) -> Output {
    try_execute_sql_in_db(instance, sql, db).await.unwrap()
}
