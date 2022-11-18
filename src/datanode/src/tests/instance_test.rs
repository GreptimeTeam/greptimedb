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

use arrow::array::{Int64Array, UInt64Array, Utf8Array};
use common_query::Output;
use common_recordbatch::util;
use datafusion::arrow_print;
use datafusion_common::record_batch::RecordBatch as DfRecordBatch;
use datatypes::arrow_array::StringArray;
use datatypes::prelude::ConcreteDataType;

use crate::instance::Instance;
use crate::tests::test_util;

#[tokio::test(flavor = "multi_thread")]
async fn test_create_database_and_insert_query() {
    common_telemetry::init_default_ut_logging();

    let (opts, _guard) =
        test_util::create_tmp_dir_and_datanode_opts("create_database_and_insert_query");
    let instance = Instance::with_mock_meta_client(&opts).await.unwrap();
    instance.start().await.unwrap();

    let output = instance.execute_sql("create database test").await.unwrap();
    assert!(matches!(output, Output::AffectedRows(1)));

    let output = instance
        .execute_sql(
            r#"create table greptime.test.demo(
             host STRING,
             cpu DOUBLE,
             memory DOUBLE,
             ts bigint,
             TIME INDEX(ts)
)"#,
        )
        .await
        .unwrap();
    assert!(matches!(output, Output::AffectedRows(1)));

    let output = instance
        .execute_sql(
            r#"insert into test.demo(host, cpu, memory, ts) values
                           ('host1', 66.6, 1024, 1655276557000),
                           ('host2', 88.8,  333.3, 1655276558000)
                           "#,
        )
        .await
        .unwrap();
    assert!(matches!(output, Output::AffectedRows(2)));

    let query_output = instance
        .execute_sql("select ts from test.demo order by ts")
        .await
        .unwrap();

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
    let output = instance.execute_sql("create database a").await.unwrap();
    assert!(matches!(output, Output::AffectedRows(1)));
    let output = instance.execute_sql("create database b").await.unwrap();
    assert!(matches!(output, Output::AffectedRows(1)));

    // Create table a.demo and b.demo
    let output = instance
        .execute_sql(
            r#"create table a.demo(
             host STRING,
             ts bigint,
             TIME INDEX(ts)
)"#,
        )
        .await
        .unwrap();
    assert!(matches!(output, Output::AffectedRows(1)));

    let output = instance
        .execute_sql(
            r#"create table b.demo(
             host STRING,
             ts bigint,
             TIME INDEX(ts)
)"#,
        )
        .await
        .unwrap();
    assert!(matches!(output, Output::AffectedRows(1)));

    // Insert different data into a.demo and b.demo
    let output = instance
        .execute_sql(
            r#"insert into a.demo(host, ts) values
                           ('host1', 1655276557000)
                           "#,
        )
        .await
        .unwrap();
    assert!(matches!(output, Output::AffectedRows(1)));
    let output = instance
        .execute_sql(
            r#"insert into b.demo(host, ts) values
                           ('host2',1655276558000)
                           "#,
        )
        .await
        .unwrap();
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
    let query_output = instance.execute_sql(sql).await.unwrap();
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
        ConcreteDataType::timestamp_millis_datatype(),
    )
    .await
    .unwrap();

    instance
}

#[tokio::test(flavor = "multi_thread")]
async fn test_execute_insert() {
    let instance = setup_test_instance().await;
    let output = instance
        .execute_sql(
            r#"insert into demo(host, cpu, memory, ts) values
                           ('host1', 66.6, 1024, 1655276557000),
                           ('host2', 88.8,  333.3, 1655276558000)
                           "#,
        )
        .await
        .unwrap();
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

    let output = instance
        .execute_sql(
            r#"insert into demo(host, cpu, memory, ts) values
                           ('host1', 66.6, 1024, 1655276557000),
                           ('host2', 88.8,  333.3, 1655276558000)
                           "#,
        )
        .await
        .unwrap();
    assert!(matches!(output, Output::AffectedRows(2)));

    let query_output = instance
        .execute_sql("select ts from demo order by ts")
        .await
        .unwrap();

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

    let query_output = instance
        .execute_sql("select ts as time from demo order by ts")
        .await
        .unwrap();

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

    let output = instance
        .execute_sql("select sum(number) from numbers limit 20")
        .await
        .unwrap();
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

    let output = instance.execute_sql("show databases").await.unwrap();
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

    let output = instance
        .execute_sql("show databases like '%bl%'")
        .await
        .unwrap();
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

    let output = instance.execute_sql("show tables").await.unwrap();
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
        ConcreteDataType::timestamp_millis_datatype(),
    )
    .await
    .unwrap();

    let output = instance.execute_sql("show tables").await.unwrap();
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
    let output = instance
        .execute_sql("show tables like 'de%'")
        .await
        .unwrap();
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

    let output = instance
        .execute_sql(
            r#"create table test_table(
                            host string,
                            ts timestamp,
                            cpu double default 0,
                            memory double,
                            TIME INDEX (ts),
                            PRIMARY KEY(host)
                        ) engine=mito with(regions=1);"#,
        )
        .await
        .unwrap();
    assert!(matches!(output, Output::AffectedRows(1)));
}

#[tokio::test(flavor = "multi_thread")]
pub async fn test_create_table_illegal_timestamp_type() {
    common_telemetry::init_default_ut_logging();

    let (opts, _guard) =
        test_util::create_tmp_dir_and_datanode_opts("create_table_illegal_timestamp_type");
    let instance = Instance::with_mock_meta_client(&opts).await.unwrap();
    instance.start().await.unwrap();

    let output = instance
        .execute_sql(
            r#"create table test_table(
                            host string,
                            ts bigint,
                            cpu double default 0,
                            memory double,
                            TIME INDEX (ts),
                            PRIMARY KEY(host)
                        ) engine=mito with(regions=1);"#,
        )
        .await
        .unwrap();
    match output {
        Output::AffectedRows(rows) => {
            assert_eq!(1, rows);
        }
        _ => unreachable!(),
    }
}

async fn check_output_stream(output: Output, expected: Vec<&str>) {
    match output {
        Output::Stream(stream) => {
            let recordbatches = util::collect(stream).await.unwrap();
            let recordbatch = recordbatches
                .into_iter()
                .map(|r| r.df_recordbatch)
                .collect::<Vec<DfRecordBatch>>();
            let pretty_print = arrow_print::write(&recordbatch);
            let pretty_print = pretty_print.lines().collect::<Vec<&str>>();
            assert_eq!(pretty_print, expected);
        }
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn test_alter_table() {
    let instance = Instance::new_mock().await.unwrap();
    instance.start().await.unwrap();

    test_util::create_test_table(
        instance.catalog_manager(),
        instance.sql_handler(),
        ConcreteDataType::timestamp_millis_datatype(),
    )
    .await
    .unwrap();
    // make sure table insertion is ok before altering table
    instance
        .execute_sql("insert into demo(host, cpu, memory, ts) values ('host1', 1.1, 100, 1000)")
        .await
        .unwrap();

    // Add column
    let output = instance
        .execute_sql("alter table demo add my_tag string null")
        .await
        .unwrap();
    assert!(matches!(output, Output::AffectedRows(0)));

    let output = instance
        .execute_sql(
            "insert into demo(host, cpu, memory, ts, my_tag) values ('host2', 2.2, 200, 2000, 'hello')",
        )
        .await
        .unwrap();
    assert!(matches!(output, Output::AffectedRows(1)));
    let output = instance
        .execute_sql("insert into demo(host, cpu, memory, ts) values ('host3', 3.3, 300, 3000)")
        .await
        .unwrap();
    assert!(matches!(output, Output::AffectedRows(1)));

    let output = instance
        .execute_sql("select * from demo order by ts")
        .await
        .unwrap();
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
    let output = instance
        .execute_sql("alter table demo drop column memory")
        .await
        .unwrap();
    assert!(matches!(output, Output::AffectedRows(0)));

    let output = instance
        .execute_sql("select * from demo order by ts")
        .await
        .unwrap();
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
    let output = instance
        .execute_sql("insert into demo(host, cpu, ts, my_tag) values ('host4', 400, 4000, 'world')")
        .await
        .unwrap();
    assert!(matches!(output, Output::AffectedRows(1)));

    let output = instance
        .execute_sql("select * from demo order by ts")
        .await
        .unwrap();
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
    let output = instance.execute_sql(&create_sql).await.unwrap();
    assert!(matches!(output, Output::AffectedRows(1)));

    // Insert with ts.
    instance
        .execute_sql("insert into test_table(host, cpu, ts) values ('host1', 1.1, 1000)")
        .await
        .unwrap();
    assert!(matches!(output, Output::AffectedRows(1)));

    // Insert without ts, so it should be filled by default value.
    let output = instance
        .execute_sql("insert into test_table(host, cpu) values ('host2', 2.2)")
        .await
        .unwrap();
    assert!(matches!(output, Output::AffectedRows(1)));

    let output = instance
        .execute_sql("select host, cpu from test_table")
        .await
        .unwrap();
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
