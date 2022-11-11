use arrow::array::{Int64Array, UInt64Array};
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
async fn test_execute_insert() {
    common_telemetry::init_default_ut_logging();

    let (opts, _guard) = test_util::create_tmp_dir_and_datanode_opts("execute_insert");
    let instance = Instance::with_mock_meta_client(&opts).await.unwrap();
    instance.start().await.unwrap();

    test_util::create_test_table(&instance, ConcreteDataType::timestamp_millis_datatype())
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
}

#[tokio::test(flavor = "multi_thread")]
async fn test_execute_insert_query_with_i64_timestamp() {
    common_telemetry::init_default_ut_logging();

    let (opts, _guard) = test_util::create_tmp_dir_and_datanode_opts("insert_query_i64_timestamp");
    let instance = Instance::with_mock_meta_client(&opts).await.unwrap();
    instance.start().await.unwrap();

    test_util::create_test_table(&instance, ConcreteDataType::int64_datatype())
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
    test_util::create_test_table(&instance, ConcreteDataType::timestamp_millis_datatype())
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

    test_util::create_test_table(&instance, ConcreteDataType::timestamp_millis_datatype())
        .await
        .unwrap();
    // make sure table insertion is ok before altering table
    instance
        .execute_sql("insert into demo(host, cpu, memory, ts) values ('host1', 1.1, 100, 1000)")
        .await
        .unwrap();

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

    let output = instance.execute_sql("select * from demo").await.unwrap();
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
