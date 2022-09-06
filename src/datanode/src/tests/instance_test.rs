use arrow::array::UInt64Array;
use common_recordbatch::util;
use datafusion::arrow_print;
use datafusion_common::record_batch::RecordBatch as DfRecordBatch;
use query::Output;

use crate::instance::Instance;
use crate::tests::test_util;

#[tokio::test]
async fn test_execute_insert() {
    common_telemetry::init_default_ut_logging();

    let (opts, _guard) = test_util::create_tmp_dir_and_datanode_opts();
    let instance = Instance::new(&opts).await.unwrap();
    instance.start().await.unwrap();

    test_util::create_test_table(&instance).await.unwrap();

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

#[tokio::test]
async fn test_execute_query() {
    let (opts, _guard) = test_util::create_tmp_dir_and_datanode_opts();
    let instance = Instance::new(&opts).await.unwrap();
    instance.start().await.unwrap();

    let output = instance
        .execute_sql("select sum(number) from numbers limit 20")
        .await
        .unwrap();
    match output {
        Output::RecordBatch(recordbatch) => {
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

#[tokio::test]
pub async fn test_execute_create() {
    common_telemetry::init_default_ut_logging();

    let (opts, _guard) = test_util::create_tmp_dir_and_datanode_opts();
    let instance = Instance::new(&opts).await.unwrap();
    instance.start().await.unwrap();

    let output = instance
        .execute_sql(
            r#"create table test_table(
                            host string,
                            ts bigint,
                            cpu double default 0,
                            memory double,
                            TIME INDEX (ts),
                            PRIMARY KEY(ts, host)
                        ) engine=mito with(regions=1);"#,
        )
        .await
        .unwrap();
    assert!(matches!(output, Output::AffectedRows(1)));
}

#[tokio::test]
async fn test_alter_table() {
    common_telemetry::init_default_ut_logging();

    // TODO(LFC) Use real Mito engine when we can alter its region schema,
    //   and delete the `new_mock` method.
    let instance = Instance::new_mock().await.unwrap();
    instance.start().await.unwrap();

    test_util::create_test_table(&instance).await.unwrap();
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
    match output {
        Output::RecordBatch(stream) => {
            let recordbatches = util::collect(stream).await.unwrap();
            let recordbatch = recordbatches
                .into_iter()
                .map(|r| r.df_recordbatch)
                .collect::<Vec<DfRecordBatch>>();
            let pretty_print = arrow_print::write(&recordbatch);
            let pretty_print = pretty_print.lines().collect::<Vec<&str>>();
            let expected = vec![
                "+-------+------+-----+--------+--------+",
                "| host  | ts   | cpu | memory | my_tag |",
                "+-------+------+-----+--------+--------+",
                "| host1 | 1000 | 1.1 | 100    |        |",
                "| host2 | 2000 | 2.2 | 200    | hello  |",
                "| host3 | 3000 | 3.3 | 300    |        |",
                "+-------+------+-----+--------+--------+",
            ];
            assert_eq!(pretty_print, expected);
        }
        _ => unreachable!(),
    }
}
