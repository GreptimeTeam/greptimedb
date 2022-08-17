use arrow::array::UInt64Array;
use common_recordbatch::util;
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

    test_util::create_test_table(&instance).await.unwrap();

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
