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

use common_catalog::consts::DEFAULT_CATALOG_NAME;
use common_query::Output;
use common_recordbatch::util;
use common_telemetry::logging;
use common_test_util::temp_dir;
use datatypes::vectors::{StringVector, TimestampMillisecondVector, UInt64Vector, VectorRef};
use frontend::error::{Error, Result};
use frontend::instance::Instance;
use operator::error::Error as OperatorError;
use rstest::rstest;
use rstest_reuse::apply;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::{QueryContext, QueryContextRef};

use crate::test_util::check_output_stream;
use crate::tests::test_util::{
    both_instances_cases, both_instances_cases_with_custom_storages, check_unordered_output_stream,
    distributed, distributed_with_multiple_object_stores, find_testing_resource, prepare_path,
    standalone, standalone_instance_case, standalone_with_multiple_object_stores, MockInstance,
};

#[apply(both_instances_cases)]
async fn test_create_database_and_insert_query(instance: Arc<dyn MockInstance>) {
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

#[apply(both_instances_cases)]
async fn test_show_create_table(instance: Arc<dyn MockInstance>) {
    let frontend = instance.frontend();
    let sql = if instance.is_distributed_mode() {
        r#"create table demo(
    n INT PRIMARY KEY,
    ts timestamp,
    TIME INDEX(ts)
)
PARTITION BY RANGE COLUMNS (n) (
    PARTITION r0 VALUES LESS THAN (1),
    PARTITION r1 VALUES LESS THAN (10),
    PARTITION r2 VALUES LESS THAN (100),
    PARTITION r3 VALUES LESS THAN (MAXVALUE),
)"#
    } else {
        r#"create table demo(
    host STRING,
    cpu DOUBLE,
    memory DOUBLE,
    ts timestamp,
    TIME INDEX(ts)
)"#
    };
    let output = execute_sql(&frontend, sql).await;
    assert!(matches!(output, Output::AffectedRows(0)));

    let output = execute_sql(&frontend, "show create table demo").await;

    let expected = if instance.is_distributed_mode() {
        r#"+-------+--------------------------------------------+
| Table | Create Table                               |
+-------+--------------------------------------------+
| demo  | CREATE TABLE IF NOT EXISTS "demo" (        |
|       |   "n" INT NULL,                            |
|       |   "ts" TIMESTAMP(3) NOT NULL,              |
|       |   TIME INDEX ("ts"),                       |
|       |   PRIMARY KEY ("n")                        |
|       | )                                          |
|       | PARTITION BY RANGE COLUMNS ("n") (         |
|       |   PARTITION r0 VALUES LESS THAN (1),       |
|       |   PARTITION r1 VALUES LESS THAN (10),      |
|       |   PARTITION r2 VALUES LESS THAN (100),     |
|       |   PARTITION r3 VALUES LESS THAN (MAXVALUE) |
|       | )                                          |
|       | ENGINE=mito                                |
|       | WITH(                                      |
|       |   regions = 4                              |
|       | )                                          |
+-------+--------------------------------------------+"#
    } else {
        r#"+-------+-------------------------------------+
| Table | Create Table                        |
+-------+-------------------------------------+
| demo  | CREATE TABLE IF NOT EXISTS "demo" ( |
|       |   "host" STRING NULL,               |
|       |   "cpu" DOUBLE NULL,                |
|       |   "memory" DOUBLE NULL,             |
|       |   "ts" TIMESTAMP(3) NOT NULL,       |
|       |   TIME INDEX ("ts")                 |
|       | )                                   |
|       |                                     |
|       | ENGINE=mito                         |
|       | WITH(                               |
|       |   regions = 1                       |
|       | )                                   |
+-------+-------------------------------------+"#
    };

    check_output_stream(output, expected).await;
}

#[apply(both_instances_cases)]
async fn test_validate_external_table_options(instance: Arc<dyn MockInstance>) {
    let frontend = instance.frontend();
    let format = "json";
    let location = find_testing_resource("/tests/data/json/various_type.json");
    let table_name = "various_type_json_with_schema";
    let sql = &format!(
        r#"CREATE EXTERNAL TABLE {table_name} (
            ts TIMESTAMP TIME INDEX DEFAULT 0,
            a BIGINT NULL,
            b DOUBLE NULL,
            c BOOLEAN NULL,
            d STRING NULL,
            e TIMESTAMP(0) NULL,
            f DOUBLE NULL,
            g TIMESTAMP(0) NULL,
          ) WITH (foo='bar', location='{location}', format='{format}');"#,
    );

    let result = try_execute_sql(&frontend, sql).await;
    assert!(matches!(result, Err(Error::ParseSql { .. })));
}

#[apply(both_instances_cases)]
async fn test_show_create_external_table(instance: Arc<dyn MockInstance>) {
    std::env::set_var("TZ", "UTC");

    let fe_instance = instance.frontend();
    let format = "csv";
    let location = find_testing_resource("/tests/data/csv/various_type.csv");
    let table_name = "various_type_csv";

    let output = execute_sql(
        &fe_instance,
        &format!(
            r#"create external table {table_name} with (location='{location}', format='{format}');"#,
        ),
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(0)));

    let output = execute_sql(&fe_instance, &format!("show create table {table_name};")).await;

    let Output::RecordBatches(record_batches) = output else {
        unreachable!()
    };

    // We can't directly test `show create table` by check_output_stream because the location name length depends on the current filesystem.
    let record_batches = record_batches.iter().collect::<Vec<_>>();
    let column = record_batches[0].column_by_name("Create Table").unwrap();
    let actual = column.get(0);
    let expect = format!(
        r#"CREATE EXTERNAL TABLE IF NOT EXISTS "various_type_csv" (
  "c_int" BIGINT NULL,
  "c_float" DOUBLE NULL,
  "c_string" DOUBLE NULL,
  "c_bool" BOOLEAN NULL,
  "c_date" DATE NULL,
  "c_datetime" TIMESTAMP(0) NULL,
  "greptime_timestamp" TIMESTAMP(3) NOT NULL DEFAULT '1970-01-01 00:00:00+0000',
  TIME INDEX ("greptime_timestamp")
)

ENGINE=file
WITH(
  format = 'csv',
  location = '{location}',
  regions = 1
)"#
    );
    assert_eq!(actual.to_string(), expect);
}

#[apply(both_instances_cases)]
async fn test_issue477_same_table_name_in_different_databases(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();

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
             ts timestamp,
             TIME INDEX(ts)
)"#,
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(0)));

    let output = execute_sql(
        &instance,
        r#"create table b.demo(
             host STRING,
             ts timestamp,
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
        "select host,ts from a.demo order by ts limit 1",
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

async fn assert_query_result(instance: &Arc<Instance>, sql: &str, ts: i64, host: &str) {
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
                Arc::new(TimestampMillisecondVector::from_vec(vec![ts])) as VectorRef,
                *batches[0].column(1)
            );
        }
        _ => unreachable!(),
    }
}

#[apply(both_instances_cases)]
async fn test_execute_insert(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();

    // create table
    assert!(matches!(
        execute_sql(
            &instance,
            "create table demo(host string, cpu double, memory double, ts timestamp time index);",
        )
        .await,
        Output::AffectedRows(0)
    ));

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

#[apply(both_instances_cases)]
async fn test_execute_insert_by_select(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();

    // create table
    assert!(matches!(
        execute_sql(
            &instance,
            "create table demo1(host string, cpu double, memory double, ts timestamp time index);",
        )
        .await,
        Output::AffectedRows(0)
    ));
    assert!(matches!(
        execute_sql(
            &instance,
            "create table demo2(host string, cpu double, memory double, ts timestamp time index);",
        )
        .await,
        Output::AffectedRows(0)
    ));

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
        Error::TableOperation {
            source: OperatorError::PlanStatement { .. },
            ..
        }
    ));
    assert!(matches!(
        try_execute_sql(&instance, "insert into demo2 select cpu,memory from demo1")
            .await
            .unwrap_err(),
        Error::TableOperation {
            source: OperatorError::PlanStatement { .. },
            ..
        }
    ));
    assert!(matches!(
        try_execute_sql(&instance, "insert into demo2(ts) select memory from demo1")
            .await
            .unwrap_err(),
        Error::TableOperation {
            source: OperatorError::PlanStatement { .. },
            ..
        }
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
+-------+------+--------+---------------------+";
    check_output_stream(output, expected).await;
}

#[apply(both_instances_cases)]
async fn test_execute_query(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();

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

#[apply(both_instances_cases)]
async fn test_execute_show_databases_tables(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();

    let output = execute_sql(&instance, "show databases").await;
    match output {
        Output::RecordBatches(databases) => {
            let databases = databases.take();
            assert_eq!(1, databases[0].num_columns());
            assert_eq!(databases[0].column(0).len(), 3);

            assert_eq!(
                *databases[0].column(0),
                Arc::new(StringVector::from(vec![
                    Some("greptime_private"),
                    Some("information_schema"),
                    Some("public")
                ])) as VectorRef
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

    let expected = "\
+---------+
| Tables  |
+---------+
| numbers |
+---------+\
";
    let output = execute_sql(&instance, "show tables").await;
    check_unordered_output_stream(output, expected).await;

    assert!(matches!(execute_sql(
        &instance,
        "create table demo(host string, cpu double, memory double, ts timestamp time index, primary key (host));",
    ).await, Output::AffectedRows(0)));

    let output = execute_sql(&instance, "show tables").await;
    let expected = "\
+---------+
| Tables  |
+---------+
| demo    |
| numbers |
+---------+\
";
    check_unordered_output_stream(output, expected).await;

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

#[apply(both_instances_cases)]
async fn test_execute_create(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();

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

#[apply(both_instances_cases)]
async fn test_execute_external_create(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();

    let tmp_dir = temp_dir::create_temp_dir("test_execute_external_create");
    let location = prepare_path(tmp_dir.path().to_str().unwrap());

    let output = execute_sql(
        &instance,
        &format!(
            r#"create external table test_table_0(
                            ts timestamp time index default 0,
                            host string,
                            cpu double default 0,
                            memory double
                        ) with (location='{location}', format='csv');"#
        ),
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(0)));

    let output = execute_sql(
        &instance,
        &format!(
            r#"create external table test_table_1(
                            ts timestamp default 0,
                            host string,
                            cpu double default 0,
                            memory double,
                            time index (ts)
                        ) with (location='{location}', format='csv');"#
        ),
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(0)));
}

#[apply(both_instances_cases)]
async fn test_execute_external_create_infer_format(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();

    let tmp_dir = temp_dir::create_temp_dir("test_execute_external_create_infer_format");
    let location = prepare_path(tmp_dir.path().to_str().unwrap());

    let output = execute_sql(
        &instance,
        &format!(r#"create external table test_table with (location='{location}', format='csv');"#),
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(0)));
}

#[apply(both_instances_cases)]
async fn test_execute_external_create_without_ts(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();

    let tmp_dir = temp_dir::create_temp_dir("test_execute_external_create_without_ts");
    let location = prepare_path(tmp_dir.path().to_str().unwrap());

    let result = try_execute_sql(
        &instance,
        &format!(
            r#"create external table test_table(
                            host string,
                            cpu double default 0,
                            memory double
                        ) with (location='{location}', format='csv');"#
        ),
    )
    .await;
    assert!(matches!(result, Err(Error::TableOperation { .. })));
}

#[apply(both_instances_cases)]
async fn test_execute_external_create_with_invalid_ts(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();

    let tmp_dir = temp_dir::create_temp_dir("test_execute_external_create_with_invalid_ts");
    let location = prepare_path(tmp_dir.path().to_str().unwrap());

    let result = try_execute_sql(
        &instance,
        &format!(
            r#"create external table test_table(
                            ts timestamp time index null,
                            host string,
                            cpu double default 0,
                            memory double
                        ) with (location='{location}', format='csv');"#
        ),
    )
    .await;
    assert!(matches!(result, Err(Error::ParseSql { .. })));

    let result = try_execute_sql(
        &instance,
        &format!(
            r#"create external table test_table(
                            ts bigint time index,
                            host string,
                            cpu double default 0,
                            memory double
                        ) with (location='{location}', format='csv');"#
        ),
    )
    .await;
    assert!(matches!(result, Err(Error::ParseSql { .. })));
}

#[apply(both_instances_cases)]
async fn test_execute_query_external_table_parquet(instance: Arc<dyn MockInstance>) {
    std::env::set_var("TZ", "UTC");

    let instance = instance.frontend();
    let format = "parquet";
    let location = find_testing_resource("/tests/data/parquet/various_type.parquet");
    let table_name = "various_type_parquet";

    let output = execute_sql(
        &instance,
        &format!(
            r#"create external table {table_name} with (location='{location}', format='{format}');"#,
        ),
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(0)));

    let output = execute_sql(&instance, &format!("desc table {table_name};")).await;
    let expect = "\
+--------------------+----------------------+-----+------+--------------------------+---------------+
| Column             | Type                 | Key | Null | Default                  | Semantic Type |
+--------------------+----------------------+-----+------+--------------------------+---------------+
| c_int              | Int64                |     | YES  |                          | FIELD         |
| c_float            | Float64              |     | YES  |                          | FIELD         |
| c_string           | Float64              |     | YES  |                          | FIELD         |
| c_bool             | Boolean              |     | YES  |                          | FIELD         |
| c_date             | Date                 |     | YES  |                          | FIELD         |
| c_datetime         | TimestampSecond      |     | YES  |                          | FIELD         |
| greptime_timestamp | TimestampMillisecond | PRI | NO   | 1970-01-01 00:00:00+0000 | TIMESTAMP     |
+--------------------+----------------------+-----+------+--------------------------+---------------+";
    check_output_stream(output, expect).await;

    let output = execute_sql(&instance, &format!("select * from {table_name};")).await;
    let expect = "\
+-------+-----------+----------+--------+------------+---------------------+---------------------+
| c_int | c_float   | c_string | c_bool | c_date     | c_datetime          | greptime_timestamp  |
+-------+-----------+----------+--------+------------+---------------------+---------------------+
| 1     | 1.1       | 1.11     | true   | 1970-01-01 | 1970-01-01T00:00:00 | 1970-01-01T00:00:00 |
| 2     | 2.2       | 2.22     | true   | 2020-11-08 | 2020-11-08T01:00:00 | 1970-01-01T00:00:00 |
| 3     |           | 3.33     | true   | 1969-12-31 | 1969-11-08T02:00:00 | 1970-01-01T00:00:00 |
| 4     | 4.4       |          | false  |            |                     | 1970-01-01T00:00:00 |
| 5     | 6.6       |          | false  | 1990-01-01 | 1990-01-01T03:00:00 | 1970-01-01T00:00:00 |
| 4     | 4000000.0 |          | false  |            |                     | 1970-01-01T00:00:00 |
| 4     | 4.0e-6    |          | false  |            |                     | 1970-01-01T00:00:00 |
+-------+-----------+----------+--------+------------+---------------------+---------------------+";
    check_output_stream(output, expect).await;

    let output = execute_sql(
        &instance,
        &format!("select c_bool,c_int,c_bool as b from {table_name};"),
    )
    .await;
    let expect = "\
+--------+-------+-------+
| c_bool | c_int | b     |
+--------+-------+-------+
| true   | 1     | true  |
| true   | 2     | true  |
| true   | 3     | true  |
| false  | 4     | false |
| false  | 5     | false |
| false  | 4     | false |
| false  | 4     | false |
+--------+-------+-------+";
    check_output_stream(output, expect).await;
}

#[apply(both_instances_cases)]
async fn test_execute_query_external_table_orc(instance: Arc<dyn MockInstance>) {
    std::env::set_var("TZ", "UTC");

    let instance = instance.frontend();
    let format = "orc";
    let location = find_testing_resource("/src/common/datasource/tests/orc/test.orc");
    let table_name = "various_type_orc";

    let output = execute_sql(
        &instance,
        &format!(
            r#"create external table {table_name} with (location='{location}', format='{format}');"#,
        ),
    )
        .await;
    assert!(matches!(output, Output::AffectedRows(0)));

    let output = execute_sql(&instance, &format!("desc table {table_name};")).await;
    let expect = "\
+------------------------+----------------------+-----+------+--------------------------+---------------+
| Column                 | Type                 | Key | Null | Default                  | Semantic Type |
+------------------------+----------------------+-----+------+--------------------------+---------------+
| double_a               | Float64              |     | YES  |                          | FIELD         |
| a                      | Float32              |     | YES  |                          | FIELD         |
| b                      | Boolean              |     | YES  |                          | FIELD         |
| str_direct             | String               |     | YES  |                          | FIELD         |
| d                      | String               |     | YES  |                          | FIELD         |
| e                      | String               |     | YES  |                          | FIELD         |
| f                      | String               |     | YES  |                          | FIELD         |
| int_short_repeated     | Int32                |     | YES  |                          | FIELD         |
| int_neg_short_repeated | Int32                |     | YES  |                          | FIELD         |
| int_delta              | Int32                |     | YES  |                          | FIELD         |
| int_neg_delta          | Int32                |     | YES  |                          | FIELD         |
| int_direct             | Int32                |     | YES  |                          | FIELD         |
| int_neg_direct         | Int32                |     | YES  |                          | FIELD         |
| bigint_direct          | Int64                |     | YES  |                          | FIELD         |
| bigint_neg_direct      | Int64                |     | YES  |                          | FIELD         |
| bigint_other           | Int64                |     | YES  |                          | FIELD         |
| utf8_increase          | String               |     | YES  |                          | FIELD         |
| utf8_decrease          | String               |     | YES  |                          | FIELD         |
| timestamp_simple       | TimestampNanosecond  |     | YES  |                          | FIELD         |
| date_simple            | Date                 |     | YES  |                          | FIELD         |
| greptime_timestamp     | TimestampMillisecond | PRI | NO   | 1970-01-01 00:00:00+0000 | TIMESTAMP     |
+------------------------+----------------------+-----+------+--------------------------+---------------+";
    check_output_stream(output, expect).await;

    let output = execute_sql(&instance, &format!("select * from {table_name};")).await;
    let expect = "\
+----------+-----+-------+------------+-----+-----+-------+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+----------------------------+-------------+---------------------+
| double_a | a   | b     | str_direct | d   | e   | f     | int_short_repeated | int_neg_short_repeated | int_delta | int_neg_delta | int_direct | int_neg_direct | bigint_direct | bigint_neg_direct | bigint_other | utf8_increase | utf8_decrease | timestamp_simple           | date_simple | greptime_timestamp  |
+----------+-----+-------+------------+-----+-----+-------+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+----------------------------+-------------+---------------------+
| 1.0      | 1.0 | true  | a          | a   | ddd | aaaaa | 5                  | -5                     | 1         | 5             | 1          | -1             | 1             | -1                | 5            | a             | eeeee         | 2023-04-01T20:15:30.002    | 2023-04-01  | 1970-01-01T00:00:00 |
| 2.0      | 2.0 | false | cccccc     | bb  | cc  | bbbbb | 5                  | -5                     | 2         | 4             | 6          | -6             | 6             | -6                | -5           | bb            | dddd          | 2021-08-22T07:26:44.525777 | 2023-03-01  | 1970-01-01T00:00:00 |
| 3.0      |     |       |            |     |     |       |                    |                        |           |               |            |                |               |                   | 1            | ccc           | ccc           | 2023-01-01T00:00:00        | 2023-01-01  | 1970-01-01T00:00:00 |
| 4.0      | 4.0 | true  | ddd        | ccc | bb  | ccccc | 5                  | -5                     | 4         | 2             | 3          | -3             | 3             | -3                | 5            | dddd          | bb            | 2023-02-01T00:00:00        | 2023-02-01  | 1970-01-01T00:00:00 |
| 5.0      | 5.0 | false | ee         | ddd | a   | ddddd | 5                  | -5                     | 5         | 1             | 2          | -2             | 2             | -2                | 5            | eeeee         | a             | 2023-03-01T00:00:00        | 2023-03-01  | 1970-01-01T00:00:00 |
+----------+-----+-------+------------+-----+-----+-------+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+----------------------------+-------------+---------------------+";
    check_output_stream(output, expect).await;

    let output = execute_sql(
        &instance,
        &format!("select double_a,a, str_direct as c, a as another_a from {table_name};"),
    )
    .await;
    let expect = "\
+----------+-----+--------+-----------+
| double_a | a   | c      | another_a |
+----------+-----+--------+-----------+
| 1.0      | 1.0 | a      | 1.0       |
| 2.0      | 2.0 | cccccc | 2.0       |
| 3.0      |     |        |           |
| 4.0      | 4.0 | ddd    | 4.0       |
| 5.0      | 5.0 | ee     | 5.0       |
+----------+-----+--------+-----------+";
    check_output_stream(output, expect).await;
}

#[apply(both_instances_cases)]
async fn test_execute_query_external_table_orc_with_schema(instance: Arc<dyn MockInstance>) {
    std::env::set_var("TZ", "UTC");

    let instance = instance.frontend();
    let format = "orc";
    let location = find_testing_resource("/src/common/datasource/tests/orc/test.orc");
    let table_name = "various_type_orc";

    let output = execute_sql(
        &instance,
        &format!(
            r#"create external table {table_name} (
                a float,
                b boolean,
                d string,
                missing string,
                timestamp_simple timestamp time index,
            ) with (location='{location}', format='{format}');"#,
        ),
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(0)));

    let output = execute_sql(&instance, &format!("desc table {table_name};")).await;
    let expect = "\
+------------------+----------------------+-----+------+---------+---------------+
| Column           | Type                 | Key | Null | Default | Semantic Type |
+------------------+----------------------+-----+------+---------+---------------+
| a                | Float32              |     | YES  |         | FIELD         |
| b                | Boolean              |     | YES  |         | FIELD         |
| d                | String               |     | YES  |         | FIELD         |
| missing          | String               |     | YES  |         | FIELD         |
| timestamp_simple | TimestampMillisecond | PRI | NO   |         | TIMESTAMP     |
+------------------+----------------------+-----+------+---------+---------------+";
    check_output_stream(output, expect).await;

    let output = execute_sql(&instance, &format!("select * from {table_name};")).await;
    let expect = "\
+-----+-------+-----+---------+-------------------------+
| a   | b     | d   | missing | timestamp_simple        |
+-----+-------+-----+---------+-------------------------+
| 1.0 | true  | a   |         | 2023-04-01T20:15:30.002 |
| 2.0 | false | bb  |         | 2021-08-22T07:26:44.525 |
|     |       |     |         | 2023-01-01T00:00:00     |
| 4.0 | true  | ccc |         | 2023-02-01T00:00:00     |
| 5.0 | false | ddd |         | 2023-03-01T00:00:00     |
+-----+-------+-----+---------+-------------------------+";
    check_output_stream(output, expect).await;
}

#[apply(both_instances_cases)]
async fn test_execute_query_external_table_csv(instance: Arc<dyn MockInstance>) {
    std::env::set_var("TZ", "UTC");

    let instance = instance.frontend();
    let format = "csv";
    let location = find_testing_resource("/tests/data/csv/various_type.csv");
    let table_name = "various_type_csv";

    let output = execute_sql(
        &instance,
        &format!(
            r#"create external table {table_name} with (location='{location}', format='{format}');"#,
        ),
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(0)));

    let output = execute_sql(&instance, &format!("desc table {table_name};")).await;
    let expect = "\
+--------------------+----------------------+-----+------+--------------------------+---------------+
| Column             | Type                 | Key | Null | Default                  | Semantic Type |
+--------------------+----------------------+-----+------+--------------------------+---------------+
| c_int              | Int64                |     | YES  |                          | FIELD         |
| c_float            | Float64              |     | YES  |                          | FIELD         |
| c_string           | Float64              |     | YES  |                          | FIELD         |
| c_bool             | Boolean              |     | YES  |                          | FIELD         |
| c_date             | Date                 |     | YES  |                          | FIELD         |
| c_datetime         | TimestampSecond      |     | YES  |                          | FIELD         |
| greptime_timestamp | TimestampMillisecond | PRI | NO   | 1970-01-01 00:00:00+0000 | TIMESTAMP     |
+--------------------+----------------------+-----+------+--------------------------+---------------+";
    check_output_stream(output, expect).await;

    let output = execute_sql(&instance, &format!("select * from {table_name};")).await;
    let expect = "\
+-------+-----------+----------+--------+------------+---------------------+---------------------+
| c_int | c_float   | c_string | c_bool | c_date     | c_datetime          | greptime_timestamp  |
+-------+-----------+----------+--------+------------+---------------------+---------------------+
| 1     | 1.1       | 1.11     | true   | 1970-01-01 | 1970-01-01T00:00:00 | 1970-01-01T00:00:00 |
| 2     | 2.2       | 2.22     | true   | 2020-11-08 | 2020-11-08T01:00:00 | 1970-01-01T00:00:00 |
| 3     |           | 3.33     | true   | 1969-12-31 | 1969-11-08T02:00:00 | 1970-01-01T00:00:00 |
| 4     | 4.4       |          | false  |            |                     | 1970-01-01T00:00:00 |
| 5     | 6.6       |          | false  | 1990-01-01 | 1990-01-01T03:00:00 | 1970-01-01T00:00:00 |
| 4     | 4000000.0 |          | false  |            |                     | 1970-01-01T00:00:00 |
| 4     | 4.0e-6    |          | false  |            |                     | 1970-01-01T00:00:00 |
+-------+-----------+----------+--------+------------+---------------------+---------------------+";
    check_output_stream(output, expect).await;
}

#[apply(both_instances_cases)]
async fn test_execute_query_external_table_json(instance: Arc<dyn MockInstance>) {
    std::env::set_var("TZ", "UTC");

    let instance = instance.frontend();
    let format = "json";
    let location = find_testing_resource("/tests/data/json/various_type.json");
    let table_name = "various_type_json";

    let output = execute_sql(
        &instance,
        &format!(
            r#"create external table {table_name} with (location='{location}', format='{format}');"#,
        ),
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(0)));

    let output = execute_sql(&instance, &format!("desc table {table_name};")).await;
    let expect = "\
+--------------------+----------------------+-----+------+--------------------------+---------------+
| Column             | Type                 | Key | Null | Default                  | Semantic Type |
+--------------------+----------------------+-----+------+--------------------------+---------------+
| a                  | Int64                |     | YES  |                          | FIELD         |
| b                  | Float64              |     | YES  |                          | FIELD         |
| c                  | Boolean              |     | YES  |                          | FIELD         |
| d                  | String               |     | YES  |                          | FIELD         |
| e                  | Int64                |     | YES  |                          | FIELD         |
| f                  | String               |     | YES  |                          | FIELD         |
| g                  | String               |     | YES  |                          | FIELD         |
| greptime_timestamp | TimestampMillisecond | PRI | NO   | 1970-01-01 00:00:00+0000 | TIMESTAMP     |
+--------------------+----------------------+-----+------+--------------------------+---------------+";

    check_output_stream(output, expect).await;

    let output = execute_sql(&instance, &format!("select * from {table_name};")).await;
    let expect = "\
+-----------------+------+-------+------+------------+----------------+-------------------------+---------------------+
| a               | b    | c     | d    | e          | f              | g                       | greptime_timestamp  |
+-----------------+------+-------+------+------------+----------------+-------------------------+---------------------+
| 1               | 2.0  | false | 4    | 1681319393 | 1.02           | 2012-04-23T18:25:43.511 | 1970-01-01T00:00:00 |
| -10             | -3.5 | true  | 4    | 1681356393 | -0.3           | 2016-04-23T18:25:43.511 | 1970-01-01T00:00:00 |
| 2               | 0.6  | false | text | 1681329393 | 1377.223       |                         | 1970-01-01T00:00:00 |
| 1               | 2.0  | false | 4    |            | 1337.009       |                         | 1970-01-01T00:00:00 |
| 7               | -3.5 | true  | 4    |            | 1              |                         | 1970-01-01T00:00:00 |
| 1               | 0.6  | false | text |            | 1338           | 2018-10-23T18:33:16.481 | 1970-01-01T00:00:00 |
| 1               | 2.0  | false | 4    |            | 12345829100000 |                         | 1970-01-01T00:00:00 |
| 5               | -3.5 | true  | 4    |            | 99999999.99    |                         | 1970-01-01T00:00:00 |
| 1               | 0.6  | false | text |            | 1              |                         | 1970-01-01T00:00:00 |
| 1               | 2.0  | false | 4    |            | 1              |                         | 1970-01-01T00:00:00 |
| 1               | -3.5 | true  | 4    |            | 1              |                         | 1970-01-01T00:00:00 |
| 100000000000000 | 0.6  | false | text |            | 1              |                         | 1970-01-01T00:00:00 |
+-----------------+------+-------+------+------------+----------------+-------------------------+---------------------+";
    check_output_stream(output, expect).await;
}

#[apply(both_instances_cases)]
async fn test_execute_query_external_table_json_with_schema(instance: Arc<dyn MockInstance>) {
    std::env::set_var("TZ", "UTC");

    let instance = instance.frontend();
    let format = "json";
    let location = find_testing_resource("/tests/data/json/various_type.json");
    let table_name = "various_type_json_with_schema";

    let output = execute_sql(
        &instance,
        &format!(
            r#"CREATE EXTERNAL TABLE {table_name} (
                a BIGINT NULL,
                b DOUBLE NULL,
                c BOOLEAN NULL,
                d STRING NULL,
                e TIMESTAMP(0) NULL,
                f DOUBLE NULL,
                g TIMESTAMP(0),
                ts TIMESTAMP TIME INDEX DEFAULT 0,
              ) WITH (location='{location}', format='{format}');"#,
        ),
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(0)));

    let output = execute_sql(&instance, &format!("desc table {table_name};")).await;
    let expect = "\
+--------+----------------------+-----+------+--------------------------+---------------+
| Column | Type                 | Key | Null | Default                  | Semantic Type |
+--------+----------------------+-----+------+--------------------------+---------------+
| a      | Int64                |     | YES  |                          | FIELD         |
| b      | Float64              |     | YES  |                          | FIELD         |
| c      | Boolean              |     | YES  |                          | FIELD         |
| d      | String               |     | YES  |                          | FIELD         |
| e      | TimestampSecond      |     | YES  |                          | FIELD         |
| f      | Float64              |     | YES  |                          | FIELD         |
| g      | TimestampSecond      |     | YES  |                          | FIELD         |
| ts     | TimestampMillisecond | PRI | NO   | 1970-01-01 00:00:00+0000 | TIMESTAMP     |
+--------+----------------------+-----+------+--------------------------+---------------+";
    check_output_stream(output, expect).await;

    let output = execute_sql(&instance, &format!("select * from {table_name};")).await;
    let expect = "\
+-----------------+------+-------+------+---------------------+---------------+---------------------+---------------------+
| a               | b    | c     | d    | e                   | f             | g                   | ts                  |
+-----------------+------+-------+------+---------------------+---------------+---------------------+---------------------+
| 1               | 2.0  | false | 4    | 2023-04-12T17:09:53 | 1.02          | 2012-04-23T18:25:43 | 1970-01-01T00:00:00 |
| -10             | -3.5 | true  | 4    | 2023-04-13T03:26:33 | -0.3          | 2016-04-23T18:25:43 | 1970-01-01T00:00:00 |
| 2               | 0.6  | false | text | 2023-04-12T19:56:33 | 1377.223      |                     | 1970-01-01T00:00:00 |
| 1               | 2.0  | false | 4    |                     | 1337.009      |                     | 1970-01-01T00:00:00 |
| 7               | -3.5 | true  | 4    |                     | 1.0           |                     | 1970-01-01T00:00:00 |
| 1               | 0.6  | false | text |                     | 1338.0        | 2018-10-23T18:33:16 | 1970-01-01T00:00:00 |
| 1               | 2.0  | false | 4    |                     | 1.23458291e13 |                     | 1970-01-01T00:00:00 |
| 5               | -3.5 | true  | 4    |                     | 99999999.99   |                     | 1970-01-01T00:00:00 |
| 1               | 0.6  | false | text |                     | 1.0           |                     | 1970-01-01T00:00:00 |
| 1               | 2.0  | false | 4    |                     | 1.0           |                     | 1970-01-01T00:00:00 |
| 1               | -3.5 | true  | 4    |                     | 1.0           |                     | 1970-01-01T00:00:00 |
| 100000000000000 | 0.6  | false | text |                     | 1.0           |                     | 1970-01-01T00:00:00 |
+-----------------+------+-------+------+---------------------+---------------+---------------------+---------------------+";
    check_output_stream(output, expect).await;
}

#[apply(both_instances_cases)]
async fn test_execute_query_external_table_json_type_cast(instance: Arc<dyn MockInstance>) {
    std::env::set_var("TZ", "UTC");

    let instance = instance.frontend();
    let format = "json";
    let location = find_testing_resource("/tests/data/json/type_cast.json");
    let table_name = "type_cast";

    let output = execute_sql(
        &instance,
        &format!(
            r#"create external table {table_name} (
                hostname STRING,
                environment STRING,
                usage_user DOUBLE,
                usage_system DOUBLE,
                usage_idle DOUBLE,
                usage_nice DOUBLE,
                usage_iowait DOUBLE,
                usage_irq DOUBLE,
                usage_softirq DOUBLE,
                usage_steal DOUBLE,
                usage_guest DOUBLE,
                usage_guest_nice DOUBLE,
                ts TIMESTAMP TIME INDEX,
                PRIMARY KEY(hostname)
            ) with (location='{location}', format='{format}');"#,
        ),
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(0)));

    let output = execute_sql(&instance, &format!("desc table {table_name};")).await;
    let expect = "\
+------------------+----------------------+-----+------+---------+---------------+
| Column           | Type                 | Key | Null | Default | Semantic Type |
+------------------+----------------------+-----+------+---------+---------------+
| hostname         | String               | PRI | YES  |         | TAG           |
| environment      | String               |     | YES  |         | FIELD         |
| usage_user       | Float64              |     | YES  |         | FIELD         |
| usage_system     | Float64              |     | YES  |         | FIELD         |
| usage_idle       | Float64              |     | YES  |         | FIELD         |
| usage_nice       | Float64              |     | YES  |         | FIELD         |
| usage_iowait     | Float64              |     | YES  |         | FIELD         |
| usage_irq        | Float64              |     | YES  |         | FIELD         |
| usage_softirq    | Float64              |     | YES  |         | FIELD         |
| usage_steal      | Float64              |     | YES  |         | FIELD         |
| usage_guest      | Float64              |     | YES  |         | FIELD         |
| usage_guest_nice | Float64              |     | YES  |         | FIELD         |
| ts               | TimestampMillisecond | PRI | NO   |         | TIMESTAMP     |
+------------------+----------------------+-----+------+---------+---------------+";
    check_output_stream(output, expect).await;

    let output = execute_sql(&instance, &format!("select * from {table_name};")).await;
    let expect = "\
+----------+-------------+------------+--------------+------------+------------+--------------+-----------+---------------+-------------+-------------+------------------+---------------------+
| hostname | environment | usage_user | usage_system | usage_idle | usage_nice | usage_iowait | usage_irq | usage_softirq | usage_steal | usage_guest | usage_guest_nice | ts                  |
+----------+-------------+------------+--------------+------------+------------+--------------+-----------+---------------+-------------+-------------+------------------+---------------------+
| host_0   | test        | 32.0       | 58.0         | 36.0       | 72.0       | 61.0         | 21.0      | 53.0          | 12.0        | 59.0        | 72.0             | 2023-04-01T00:00:00 |
| host_1   | staging     | 12.0       | 32.0         | 50.0       | 84.0       | 19.0         | 73.0      | 38.0          | 37.0        | 72.0        | 2.0              | 2023-04-01T00:00:00 |
| host_2   | test        | 98.0       | 5.0          | 40.0       | 95.0       | 64.0         | 39.0      | 21.0          | 63.0        | 53.0        | 94.0             | 2023-04-01T00:00:00 |
| host_3   | test        | 98.0       | 95.0         | 7.0        | 48.0       | 99.0         | 67.0      | 14.0          | 86.0        | 36.0        | 23.0             | 2023-04-01T00:00:00 |
| host_4   | test        | 32.0       | 44.0         | 11.0       | 53.0       | 64.0         | 9.0       | 17.0          | 39.0        | 20.0        | 7.0              | 2023-04-01T00:00:00 |
+----------+-------------+------------+--------------+------------+------------+--------------+-----------+---------------+-------------+-------------+------------------+---------------------+";
    check_output_stream(output, expect).await;
}

#[apply(both_instances_cases)]
async fn test_execute_query_external_table_json_default_ts_column(instance: Arc<dyn MockInstance>) {
    std::env::set_var("TZ", "UTC");

    let instance = instance.frontend();
    let format = "json";
    let location = find_testing_resource("/tests/data/json/default_ts_column.json");
    let table_name = "default_ts_column";

    let output = execute_sql(
        &instance,
        &format!(
            r#"create external table {table_name} with (location='{location}', format='{format}');"#,
        ),
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(0)));

    let output = execute_sql(&instance, &format!("desc table {table_name};")).await;
    let expect = "\
+--------------------+----------------------+-----+------+--------------------------+---------------+
| Column             | Type                 | Key | Null | Default                  | Semantic Type |
+--------------------+----------------------+-----+------+--------------------------+---------------+
| environment        | String               |     | YES  |                          | FIELD         |
| greptime_timestamp | TimestampMillisecond | PRI | NO   | 1970-01-01 00:00:00+0000 | TIMESTAMP     |
| hostname           | String               |     | YES  |                          | FIELD         |
| usage_guest        | Int64                |     | YES  |                          | FIELD         |
| usage_guest_nice   | Int64                |     | YES  |                          | FIELD         |
| usage_idle         | Int64                |     | YES  |                          | FIELD         |
| usage_iowait       | Int64                |     | YES  |                          | FIELD         |
| usage_irq          | Int64                |     | YES  |                          | FIELD         |
| usage_nice         | Int64                |     | YES  |                          | FIELD         |
| usage_softirq      | Int64                |     | YES  |                          | FIELD         |
| usage_steal        | Int64                |     | YES  |                          | FIELD         |
| usage_system       | Int64                |     | YES  |                          | FIELD         |
| usage_user         | Int64                |     | YES  |                          | FIELD         |
+--------------------+----------------------+-----+------+--------------------------+---------------+";
    check_output_stream(output, expect).await;

    let output = execute_sql(&instance, &format!("select * from {table_name};")).await;
    let expect = "\
+-------------+---------------------+----------+-------------+------------------+------------+--------------+-----------+------------+---------------+-------------+--------------+------------+
| environment | greptime_timestamp  | hostname | usage_guest | usage_guest_nice | usage_idle | usage_iowait | usage_irq | usage_nice | usage_softirq | usage_steal | usage_system | usage_user |
+-------------+---------------------+----------+-------------+------------------+------------+--------------+-----------+------------+---------------+-------------+--------------+------------+
| test        | 2023-04-01T00:00:00 | host_0   | 59          | 72               | 36         | 61           | 21        | 72         | 53            | 12          | 58           | 32         |
| staging     | 2023-04-01T00:00:00 | host_1   | 72          | 2                | 50         | 19           | 73        | 84         | 38            | 37          | 32           | 12         |
| test        | 2023-04-01T00:00:00 | host_2   | 53          | 94               | 40         | 64           | 39        | 95         | 21            | 63          | 5            | 98         |
| test        | 2023-04-01T00:00:00 | host_3   | 36          | 23               | 7          | 99           | 67        | 48         | 14            | 86          | 95           | 98         |
| test        | 2023-04-01T00:00:00 | host_4   | 20          | 7                | 11         | 64           | 9         | 53         | 17            | 39          | 44           | 32         |
+-------------+---------------------+----------+-------------+------------------+------------+--------------+-----------+------------+---------------+-------------+--------------+------------+";
    check_output_stream(output, expect).await;
}

#[apply(standalone_instance_case)]
async fn test_rename_table(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();

    let output = execute_sql(&instance, "create database db").await;
    assert!(matches!(output, Output::AffectedRows(1)));

    let query_ctx = QueryContext::with(DEFAULT_CATALOG_NAME, "db");
    let output = execute_sql_with(
        &instance,
        "create table demo(host string, cpu double, memory double, ts timestamp, time index(ts))",
        query_ctx.clone(),
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(0)));

    // make sure table insertion is ok before altering table name
    let output = execute_sql_with(
        &instance,
        "insert into demo(host, cpu, memory, ts) values ('host1', 1.1, 100, 1000), ('host2', 2.2, 200, 2000)",
        query_ctx.clone(),
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(2)));

    // rename table
    let output = execute_sql_with(
        &instance,
        "alter table demo rename test_table",
        query_ctx.clone(),
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(0)));

    let output = execute_sql_with(&instance, "show tables", query_ctx.clone()).await;
    let expect = "\
+------------+
| Tables     |
+------------+
| test_table |
+------------+";
    check_output_stream(output, expect).await;

    let output = execute_sql_with(
        &instance,
        "select * from test_table order by ts",
        query_ctx.clone(),
    )
    .await;
    let expected = "\
+-------+-----+--------+---------------------+
| host  | cpu | memory | ts                  |
+-------+-----+--------+---------------------+
| host1 | 1.1 | 100.0  | 1970-01-01T00:00:01 |
| host2 | 2.2 | 200.0  | 1970-01-01T00:00:02 |
+-------+-----+--------+---------------------+";
    check_output_stream(output, expected).await;

    assert!(
        try_execute_sql_with(&instance, "select * from demo", query_ctx)
            .await
            .is_err()
    );
}

// should apply to both instances. tracked in #723
#[apply(standalone_instance_case)]
async fn test_create_table_after_rename_table(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();

    let output = execute_sql(&instance, "create database db").await;
    assert!(matches!(output, Output::AffectedRows(1)));

    // create test table
    let table_name = "demo";
    let query_ctx = QueryContext::with(DEFAULT_CATALOG_NAME, "db");
    let output = execute_sql_with(
        &instance,
        &format!("create table {table_name}(host string, cpu double, memory double, ts timestamp, time index(ts))"),
        query_ctx.clone(),
    )
        .await;
    assert!(matches!(output, Output::AffectedRows(0)));

    // rename table
    let new_table_name = "test_table";
    let output = execute_sql_with(
        &instance,
        &format!("alter table {table_name} rename {new_table_name}"),
        query_ctx.clone(),
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(0)));

    // create table with same name
    // create test table
    let output = execute_sql_with(
        &instance,
        &format!("create table {table_name}(host string, cpu double, memory double, ts timestamp, time index(ts))"),
        query_ctx.clone(),
    )
        .await;
    assert!(matches!(output, Output::AffectedRows(0)));

    let expect = "\
+------------+
| Tables     |
+------------+
| demo       |
| test_table |
+------------+";
    let output = execute_sql_with(&instance, "show tables", query_ctx).await;
    check_output_stream(output, expect).await;
}

#[apply(both_instances_cases)]
async fn test_alter_table(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();

    // create table
    assert!(matches!(
        execute_sql(
            &instance,
            "create table demo(host string, cpu double, memory double, ts timestamp time index);",
        )
        .await,
        Output::AffectedRows(0)
    ));

    // make sure table insertion is ok before altering table
    assert!(matches!(
        execute_sql(
            &instance,
            "insert into demo(host, cpu, memory, ts) values ('host1', 1.1, 100, 1000)",
        )
        .await,
        Output::AffectedRows(1)
    ));

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
+-------+-----+--------+---------------------+--------+";
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
+-------+-----+---------------------+--------+";
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
+-------+-------+---------------------+--------+";
    check_output_stream(output, expected).await;
}

async fn test_insert_with_default_value_for_type(instance: Arc<Instance>, type_name: &str) {
    let table_name = format!("test_table_with_{type_name}");
    let create_sql = format!(
        r#"create table {table_name}(
        host string,
        ts {type_name} DEFAULT CURRENT_TIMESTAMP(),
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
        &format!("insert into {table_name}(host, cpu, ts) values ('host1', 1.1, 1000)"),
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(1)));

    // Insert without ts, so it should be filled by default value.
    let output = execute_sql(
        &instance,
        &format!("insert into {table_name}(host, cpu) values ('host2', 2.2)"),
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(1)));

    let output = execute_sql(&instance, &format!("select host, cpu from {table_name}")).await;
    let expected = "\
+-------+-----+
| host  | cpu |
+-------+-----+
| host1 | 1.1 |
| host2 | 2.2 |
+-------+-----+";
    check_output_stream(output, expected).await;
}

// should apply to both instances. tracked in #1293
#[apply(standalone_instance_case)]
async fn test_insert_with_default_value(instance: Arc<dyn MockInstance>) {
    test_insert_with_default_value_for_type(instance.frontend(), "timestamp").await;
}

#[apply(both_instances_cases)]
async fn test_use_database(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();

    let output = execute_sql(&instance, "create database db1").await;
    assert!(matches!(output, Output::AffectedRows(1)));

    let query_ctx = QueryContext::with(DEFAULT_CATALOG_NAME, "db1");
    let output = execute_sql_with(
        &instance,
        "create table tb1(col_i32 int, ts timestamp, TIME INDEX(ts))",
        query_ctx.clone(),
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(0)));

    let output = execute_sql_with(&instance, "show tables", query_ctx.clone()).await;
    let expected = "\
+--------+
| Tables |
+--------+
| tb1    |
+--------+";
    check_output_stream(output, expected).await;

    let output = execute_sql_with(
        &instance,
        r#"insert into tb1(col_i32, ts) values (1, 1655276557000)"#,
        query_ctx.clone(),
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(1)));

    let output = execute_sql_with(&instance, "select col_i32 from tb1", query_ctx.clone()).await;
    let expected = "\
+---------+
| col_i32 |
+---------+
| 1       |
+---------+";
    check_output_stream(output, expected).await;

    // Making a particular database the default by means of the USE statement does not preclude
    // accessing tables in other databases.
    let output = execute_sql(&instance, "select number from public.numbers limit 1").await;
    let expected = "\
+--------+
| number |
+--------+
| 0      |
+--------+";
    check_output_stream(output, expected).await;
}

#[apply(both_instances_cases)]
async fn test_delete(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();

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
        "delete from test_table where host = 'host1' and ts = 1655276557000 ",
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
+-------+---------------------+------+--------+";
    check_output_stream(output, expect).await;
}

#[apply(both_instances_cases)]
async fn test_execute_copy_to_s3(instance: Arc<dyn MockInstance>) {
    if let Ok(bucket) = env::var("GT_S3_BUCKET") {
        if !bucket.is_empty() {
            let instance = instance.frontend();

            // setups
            assert!(matches!(execute_sql(
                &instance,
                "create table demo(host string, cpu double, memory double, ts timestamp time index);",
            )
            .await, Output::AffectedRows(0)));

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
            let region = env::var("GT_S3_REGION").unwrap();

            let root = uuid::Uuid::new_v4().to_string();

            // exports
            let copy_to_stmt = format!("Copy demo TO 's3://{}/{}/export/demo.parquet' CONNECTION (ACCESS_KEY_ID='{}',SECRET_ACCESS_KEY='{}',REGION='{}')", bucket, root, key_id, key, region);

            let output = execute_sql(&instance, &copy_to_stmt).await;
            assert!(matches!(output, Output::AffectedRows(2)));
        }
    }
}

#[apply(both_instances_cases)]
async fn test_execute_copy_from_s3(instance: Arc<dyn MockInstance>) {
    logging::init_default_ut_logging();
    if let Ok(bucket) = env::var("GT_S3_BUCKET") {
        if !bucket.is_empty() {
            let instance = instance.frontend();

            // setups
            assert!(matches!(execute_sql(
                &instance,
                "create table demo(host string, cpu double, memory double, ts timestamp time index);",
            )
            .await, Output::AffectedRows(0)));

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
            let region = env::var("GT_S3_REGION").unwrap();

            let copy_to_stmt = format!("Copy demo TO 's3://{}/{}/export/demo.parquet' CONNECTION (ACCESS_KEY_ID='{}',SECRET_ACCESS_KEY='{}',REGION='{}')", bucket, root, key_id, key, region);

            let output = execute_sql(&instance, &copy_to_stmt).await;
            assert!(matches!(output, Output::AffectedRows(2)));

            struct Test<'a> {
                sql: &'a str,
                table_name: &'a str,
            }
            let tests = [
                Test {
                    sql: &format!(
                        "Copy with_filename FROM 's3://{}/{}/export/demo.parquet'",
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
                assert!(matches!(
                    execute_sql(
                        &instance,
                        &format!(
                "create table {}(host string, cpu double, memory double, ts timestamp time index);",
                test.table_name
            ),
                    )
                    .await,
                    Output::AffectedRows(0)
                ));
                let sql = format!(
                    "{} CONNECTION (ACCESS_KEY_ID='{}',SECRET_ACCESS_KEY='{}',REGION='{}')",
                    test.sql, key_id, key, region,
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
+-------+------+--------+---------------------+";
                check_output_stream(output, expected).await;
            }
        }
    }
}

#[apply(both_instances_cases)]
async fn test_execute_copy_from_orc_with_cast(instance: Arc<dyn MockInstance>) {
    logging::init_default_ut_logging();
    let instance = instance.frontend();

    // setups
    assert!(matches!(execute_sql(
        &instance,
        "create table demo(bigint_direct timestamp(9), bigint_neg_direct timestamp(6), bigint_other timestamp(3), timestamp_simple timestamp(9), time index (bigint_other));",
    )
    .await, Output::AffectedRows(0)));

    let filepath = find_testing_resource("/src/common/datasource/tests/orc/test.orc");

    let output = execute_sql(
        &instance,
        &format!("copy demo from '{}' WITH(FORMAT='orc');", &filepath),
    )
    .await;

    assert!(matches!(output, Output::AffectedRows(5)));

    let output = execute_sql(&instance, "select * from demo;").await;
    let expected = r#"+-------------------------------+----------------------------+-------------------------+----------------------------+
| bigint_direct                 | bigint_neg_direct          | bigint_other            | timestamp_simple           |
+-------------------------------+----------------------------+-------------------------+----------------------------+
| 1970-01-01T00:00:00.000000006 | 1969-12-31T23:59:59.999994 | 1969-12-31T23:59:59.995 | 2021-08-22T07:26:44.525777 |
|                               |                            | 1970-01-01T00:00:00.001 | 2023-01-01T00:00:00        |
| 1970-01-01T00:00:00.000000002 | 1969-12-31T23:59:59.999998 | 1970-01-01T00:00:00.005 | 2023-03-01T00:00:00        |
+-------------------------------+----------------------------+-------------------------+----------------------------+"#;
    check_output_stream(output, expected).await;
}

#[apply(both_instances_cases)]
async fn test_execute_copy_from_orc(instance: Arc<dyn MockInstance>) {
    logging::init_default_ut_logging();
    let instance = instance.frontend();

    // setups
    assert!(matches!(execute_sql(
        &instance,
        "create table demo(double_a double, a float, b boolean, str_direct string, d string, e string, f string, int_short_repeated int, int_neg_short_repeated int, int_delta int, int_neg_delta int, int_direct int, int_neg_direct int, bigint_direct bigint, bigint_neg_direct bigint, bigint_other bigint, utf8_increase string, utf8_decrease string, timestamp_simple timestamp(9) time index, date_simple date);",
    )
    .await, Output::AffectedRows(0)));

    let filepath = find_testing_resource("/src/common/datasource/tests/orc/test.orc");

    let output = execute_sql(
        &instance,
        &format!("copy demo from '{}' WITH(FORMAT='orc');", &filepath),
    )
    .await;

    assert!(matches!(output, Output::AffectedRows(5)));

    let output = execute_sql(&instance, "select * from demo order by double_a;").await;
    let expected = r#"+----------+-----+-------+------------+-----+-----+-------+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+----------------------------+-------------+
| double_a | a   | b     | str_direct | d   | e   | f     | int_short_repeated | int_neg_short_repeated | int_delta | int_neg_delta | int_direct | int_neg_direct | bigint_direct | bigint_neg_direct | bigint_other | utf8_increase | utf8_decrease | timestamp_simple           | date_simple |
+----------+-----+-------+------------+-----+-----+-------+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+----------------------------+-------------+
| 1.0      | 1.0 | true  | a          | a   | ddd | aaaaa | 5                  | -5                     | 1         | 5             | 1          | -1             | 1             | -1                | 5            | a             | eeeee         | 2023-04-01T20:15:30.002    | 2023-04-01  |
| 2.0      | 2.0 | false | cccccc     | bb  | cc  | bbbbb | 5                  | -5                     | 2         | 4             | 6          | -6             | 6             | -6                | -5           | bb            | dddd          | 2021-08-22T07:26:44.525777 | 2023-03-01  |
| 3.0      |     |       |            |     |     |       |                    |                        |           |               |            |                |               |                   | 1            | ccc           | ccc           | 2023-01-01T00:00:00        | 2023-01-01  |
| 4.0      | 4.0 | true  | ddd        | ccc | bb  | ccccc | 5                  | -5                     | 4         | 2             | 3          | -3             | 3             | -3                | 5            | dddd          | bb            | 2023-02-01T00:00:00        | 2023-02-01  |
| 5.0      | 5.0 | false | ee         | ddd | a   | ddddd | 5                  | -5                     | 5         | 1             | 2          | -2             | 2             | -2                | 5            | eeeee         | a             | 2023-03-01T00:00:00        | 2023-03-01  |
+----------+-----+-------+------------+-----+-----+-------+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+----------------------------+-------------+"#;
    check_output_stream(output, expected).await;
}

#[apply(both_instances_cases)]
async fn test_cast_type_issue_1594(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();

    // setups
    assert!(matches!(execute_sql(
        &instance,
        "create table tsbs_cpu(hostname STRING, environment STRING, usage_user DOUBLE, usage_system DOUBLE, usage_idle DOUBLE, usage_nice DOUBLE, usage_iowait DOUBLE, usage_irq DOUBLE, usage_softirq DOUBLE, usage_steal DOUBLE, usage_guest DOUBLE, usage_guest_nice DOUBLE, ts TIMESTAMP TIME INDEX, PRIMARY KEY(hostname));",
    )
    .await, Output::AffectedRows(0)));

    let filepath = find_testing_resource("/src/common/datasource/tests/csv/type_cast.csv");

    let output = execute_sql(
        &instance,
        &format!("copy tsbs_cpu from '{}' WITH(FORMAT='csv');", &filepath),
    )
    .await;

    assert!(matches!(output, Output::AffectedRows(5)));

    let output = execute_sql(&instance, "select * from tsbs_cpu order by hostname;").await;
    let expected = "\
+----------+-------------+------------+--------------+------------+------------+--------------+-----------+---------------+-------------+-------------+------------------+---------------------+
| hostname | environment | usage_user | usage_system | usage_idle | usage_nice | usage_iowait | usage_irq | usage_softirq | usage_steal | usage_guest | usage_guest_nice | ts                  |
+----------+-------------+------------+--------------+------------+------------+--------------+-----------+---------------+-------------+-------------+------------------+---------------------+
| host_0   | test        | 32.0       | 58.0         | 36.0       | 72.0       | 61.0         | 21.0      | 53.0          | 12.0        | 59.0        | 72.0             | 2023-04-01T00:00:00 |
| host_1   | staging     | 12.0       | 32.0         | 50.0       | 84.0       | 19.0         | 73.0      | 38.0          | 37.0        | 72.0        | 2.0              | 2023-04-01T00:00:00 |
| host_2   | test        | 98.0       | 5.0          | 40.0       | 95.0       | 64.0         | 39.0      | 21.0          | 63.0        | 53.0        | 94.0             | 2023-04-01T00:00:00 |
| host_3   | test        | 98.0       | 95.0         | 7.0        | 48.0       | 99.0         | 67.0      | 14.0          | 86.0        | 36.0        | 23.0             | 2023-04-01T00:00:00 |
| host_4   | test        | 32.0       | 44.0         | 11.0       | 53.0       | 64.0         | 9.0       | 17.0          | 39.0        | 20.0        | 7.0              | 2023-04-01T00:00:00 |
+----------+-------------+------------+--------------+------------+------------+--------------+-----------+---------------+-------------+-------------+------------------+---------------------+";
    check_output_stream(output, expected).await;
}

#[apply(both_instances_cases)]
async fn test_information_schema_dot_tables(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();

    let sql = "create table another_table(i timestamp time index)";
    let query_ctx = QueryContext::with("another_catalog", "another_schema");
    let output = execute_sql_with(&instance, sql, query_ctx.clone()).await;
    assert!(matches!(output, Output::AffectedRows(0)));

    // User can only see information schema under current catalog.
    // A necessary requirement to GreptimeCloud.
    let sql = "select table_catalog, table_schema, table_name, table_type, table_id, engine from information_schema.tables where table_type != 'SYSTEM VIEW' and table_name in ('columns', 'numbers', 'tables', 'another_table') order by table_name";

    let output = execute_sql(&instance, sql).await;
    let expected = "\
+---------------+--------------------+------------+-----------------+----------+-------------+
| table_catalog | table_schema       | table_name | table_type      | table_id | engine      |
+---------------+--------------------+------------+-----------------+----------+-------------+
| greptime      | information_schema | columns    | LOCAL TEMPORARY | 4        |             |
| greptime      | public             | numbers    | LOCAL TEMPORARY | 2        | test_engine |
| greptime      | information_schema | tables     | LOCAL TEMPORARY | 3        |             |
+---------------+--------------------+------------+-----------------+----------+-------------+";

    check_output_stream(output, expected).await;

    let output = execute_sql_with(&instance, sql, query_ctx).await;
    let expected = "\
+-----------------+--------------------+---------------+-----------------+----------+--------+
| table_catalog   | table_schema       | table_name    | table_type      | table_id | engine |
+-----------------+--------------------+---------------+-----------------+----------+--------+
| another_catalog | another_schema     | another_table | BASE TABLE      | 1024     | mito   |
| another_catalog | information_schema | columns       | LOCAL TEMPORARY | 4        |        |
| another_catalog | information_schema | tables        | LOCAL TEMPORARY | 3        |        |
+-----------------+--------------------+---------------+-----------------+----------+--------+";
    check_output_stream(output, expected).await;
}

#[apply(both_instances_cases)]
async fn test_information_schema_dot_columns(instance: Arc<dyn MockInstance>) {
    logging::init_default_ut_logging();
    let instance = instance.frontend();

    let sql = "create table another_table(i timestamp time index)";
    let query_ctx = QueryContext::with("another_catalog", "another_schema");
    let output = execute_sql_with(&instance, sql, query_ctx.clone()).await;
    assert!(matches!(output, Output::AffectedRows(0)));

    // User can only see information schema under current catalog.
    // A necessary requirement to GreptimeCloud.
    let sql = "select table_catalog, table_schema, table_name, column_name, data_type, semantic_type from information_schema.columns where table_name in ('columns', 'numbers', 'tables', 'another_table') order by table_name";

    let output = execute_sql(&instance, sql).await;
    let expected = "\
+---------------+--------------------+------------+----------------+-----------+---------------+
| table_catalog | table_schema       | table_name | column_name    | data_type | semantic_type |
+---------------+--------------------+------------+----------------+-----------+---------------+
| greptime      | information_schema | columns    | table_catalog  | String    | FIELD         |
| greptime      | information_schema | columns    | table_schema   | String    | FIELD         |
| greptime      | information_schema | columns    | table_name     | String    | FIELD         |
| greptime      | information_schema | columns    | column_name    | String    | FIELD         |
| greptime      | information_schema | columns    | data_type      | String    | FIELD         |
| greptime      | information_schema | columns    | semantic_type  | String    | FIELD         |
| greptime      | information_schema | columns    | column_default | String    | FIELD         |
| greptime      | information_schema | columns    | is_nullable    | String    | FIELD         |
| greptime      | information_schema | columns    | column_type    | String    | FIELD         |
| greptime      | information_schema | columns    | column_comment | String    | FIELD         |
| greptime      | public             | numbers    | number         | UInt32    | TAG           |
| greptime      | information_schema | tables     | table_catalog  | String    | FIELD         |
| greptime      | information_schema | tables     | table_schema   | String    | FIELD         |
| greptime      | information_schema | tables     | table_name     | String    | FIELD         |
| greptime      | information_schema | tables     | table_type     | String    | FIELD         |
| greptime      | information_schema | tables     | table_id       | UInt32    | FIELD         |
| greptime      | information_schema | tables     | engine         | String    | FIELD         |
+---------------+--------------------+------------+----------------+-----------+---------------+";

    check_output_stream(output, expected).await;

    let output = execute_sql_with(&instance, sql, query_ctx).await;
    let expected = "\
+-----------------+--------------------+---------------+----------------+----------------------+---------------+
| table_catalog   | table_schema       | table_name    | column_name    | data_type            | semantic_type |
+-----------------+--------------------+---------------+----------------+----------------------+---------------+
| another_catalog | another_schema     | another_table | i              | TimestampMillisecond | TIMESTAMP     |
| another_catalog | information_schema | columns       | table_catalog  | String               | FIELD         |
| another_catalog | information_schema | columns       | table_schema   | String               | FIELD         |
| another_catalog | information_schema | columns       | table_name     | String               | FIELD         |
| another_catalog | information_schema | columns       | column_name    | String               | FIELD         |
| another_catalog | information_schema | columns       | data_type      | String               | FIELD         |
| another_catalog | information_schema | columns       | semantic_type  | String               | FIELD         |
| another_catalog | information_schema | columns       | column_default | String               | FIELD         |
| another_catalog | information_schema | columns       | is_nullable    | String               | FIELD         |
| another_catalog | information_schema | columns       | column_type    | String               | FIELD         |
| another_catalog | information_schema | columns       | column_comment | String               | FIELD         |
| another_catalog | information_schema | tables        | table_catalog  | String               | FIELD         |
| another_catalog | information_schema | tables        | table_schema   | String               | FIELD         |
| another_catalog | information_schema | tables        | table_name     | String               | FIELD         |
| another_catalog | information_schema | tables        | table_type     | String               | FIELD         |
| another_catalog | information_schema | tables        | table_id       | UInt32               | FIELD         |
| another_catalog | information_schema | tables        | engine         | String               | FIELD         |
+-----------------+--------------------+---------------+----------------+----------------------+---------------+";

    check_output_stream(output, expected).await;
}

async fn execute_sql(instance: &Arc<Instance>, sql: &str) -> Output {
    execute_sql_with(instance, sql, QueryContext::arc()).await
}

async fn try_execute_sql(instance: &Arc<Instance>, sql: &str) -> Result<Output> {
    try_execute_sql_with(instance, sql, QueryContext::arc()).await
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

#[apply(both_instances_cases_with_custom_storages)]
async fn test_custom_storage(instance: Arc<dyn MockInstance>) {
    let frontend = instance.frontend();
    let custom_storages = [
        ("S3", "GT_S3_BUCKET"),
        ("Oss", "GT_OSS_BUCKET"),
        ("Azblob", "GT_AZBLOB_CONTAINER"),
        ("Gcs", "GT_GCS_BUCKET"),
    ];
    for (storage_name, custom_storage_env) in custom_storages {
        if let Ok(env_value) = env::var(custom_storage_env) {
            if env_value.is_empty() {
                continue;
            }
            let sql = if instance.is_distributed_mode() {
                format!(
                    r#"create table test_table(
                    a int null primary key,
                    ts timestamp time index,
                )
                PARTITION BY RANGE COLUMNS (a) (
                    PARTITION r0 VALUES LESS THAN (1),
                    PARTITION r1 VALUES LESS THAN (10),
                    PARTITION r2 VALUES LESS THAN (100),
                    PARTITION r3 VALUES LESS THAN (MAXVALUE),
                )
                with(storage='{storage_name}')
                "#
                )
            } else {
                format!(
                    r#"create table test_table(a int primary key, ts timestamp time index)with(storage='{storage_name}');"#
                )
            };

            let output = execute_sql(&instance.frontend(), &sql).await;
            assert!(matches!(output, Output::AffectedRows(0)));
            let output = execute_sql(
                &frontend,
                r#"insert into test_table(a, ts) values
                            (1, 1655276557000),
                            (1000, 1655276558000)
                            "#,
            )
            .await;
            assert!(matches!(output, Output::AffectedRows(2)));

            let output = execute_sql(&frontend, "select * from test_table").await;
            let expected = "\
+------+---------------------+
| a    | ts                  |
+------+---------------------+
| 1    | 2022-06-15T07:02:37 |
| 1000 | 2022-06-15T07:02:38 |
+------+---------------------+";

            check_output_stream(output, expected).await;
            let output = execute_sql(&frontend, "show create table test_table").await;
            let Output::RecordBatches(record_batches) = output else {
                unreachable!()
            };

            let record_batches = record_batches.iter().collect::<Vec<_>>();
            let column = record_batches[0].column_by_name("Create Table").unwrap();
            let actual = column.get(0);

            let expect = if instance.is_distributed_mode() {
                format!(
                    r#"CREATE TABLE IF NOT EXISTS "test_table" (
  "a" INT NULL,
  "ts" TIMESTAMP(3) NOT NULL,
  TIME INDEX ("ts"),
  PRIMARY KEY ("a")
)
PARTITION BY RANGE COLUMNS ("a") (
  PARTITION r0 VALUES LESS THAN (1),
  PARTITION r1 VALUES LESS THAN (10),
  PARTITION r2 VALUES LESS THAN (100),
  PARTITION r3 VALUES LESS THAN (MAXVALUE)
)
ENGINE=mito
WITH(
  regions = 4,
  storage = '{storage_name}'
)"#
                )
            } else {
                format!(
                    r#"CREATE TABLE IF NOT EXISTS "test_table" (
  "a" INT NULL,
  "ts" TIMESTAMP(3) NOT NULL,
  TIME INDEX ("ts"),
  PRIMARY KEY ("a")
)

ENGINE=mito
WITH(
  regions = 1,
  storage = '{storage_name}'
)"#
                )
            };
            assert_eq!(actual.to_string(), expect);
            let output = execute_sql(&frontend, "truncate test_table").await;
            assert!(matches!(output, Output::AffectedRows(0)));
            let output = execute_sql(&frontend, "select * from test_table").await;
            let expected = "\
++
++";

            check_output_stream(output, expected).await;
            let output = execute_sql(&frontend, "drop table test_table").await;
            assert!(matches!(output, Output::AffectedRows(0)));
        }
    }
}
