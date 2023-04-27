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
use datatypes::vectors::{Int64Vector, StringVector, UInt64Vector, VectorRef};
use rstest::rstest;
use rstest_reuse::apply;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::{QueryContext, QueryContextRef};

use crate::error::{Error, Result};
use crate::instance::Instance;
use crate::tests::test_util::{
    both_instances_cases, check_output_stream, check_unordered_output_stream, distributed,
    get_data_dir, standalone, standalone_instance_case, MockInstance,
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

#[apply(both_instances_cases)]
async fn test_show_create_table(instance: Arc<dyn MockInstance>) {
    let frontend = instance.frontend();

    let output = execute_sql(
        &frontend,
        r#"create table demo(
             host STRING,
             cpu DOUBLE,
             memory DOUBLE,
             ts bigint,
             TIME INDEX(ts)
)"#,
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(0)));

    let output = execute_sql(&frontend, "show create table demo").await;

    let expected = if instance.is_distributed_mode() {
        "\
+-------+--------------------------------------------+
| Table | Create Table                               |
+-------+--------------------------------------------+
| demo  | CREATE TABLE IF NOT EXISTS demo (          |
|       |   host STRING NULL,                        |
|       |   cpu DOUBLE NULL,                         |
|       |   memory DOUBLE NULL,                      |
|       |   ts BIGINT NOT NULL,                      |
|       |   TIME INDEX (ts)                          |
|       | )                                          |
|       | PARTITION BY RANGE COLUMNS (ts) (          |
|       |   PARTITION r0 VALUES LESS THAN (MAXVALUE) |
|       | )                                          |
|       | ENGINE=mito                                |
|       |                                            |
+-------+--------------------------------------------+"
    } else {
        "\
+-------+-----------------------------------+
| Table | Create Table                      |
+-------+-----------------------------------+
| demo  | CREATE TABLE IF NOT EXISTS demo ( |
|       |   host STRING NULL,               |
|       |   cpu DOUBLE NULL,                |
|       |   memory DOUBLE NULL,             |
|       |   ts BIGINT NOT NULL,             |
|       |   TIME INDEX (ts)                 |
|       | )                                 |
|       | ENGINE=mito                       |
|       | WITH(                             |
|       |   regions = 1                     |
|       | )                                 |
+-------+-----------------------------------+"
    };

    check_output_stream(output, expected).await;
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
                Arc::new(Int64Vector::from_vec(vec![ts])) as VectorRef,
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

#[apply(both_instances_cases)]
async fn test_execute_insert_by_select(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();

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
        Error::PlanStatement { .. }
    ));
    assert!(matches!(
        try_execute_sql(&instance, "insert into demo2 select cpu,memory from demo1")
            .await
            .unwrap_err(),
        Error::PlanStatement { .. }
    ));

    assert!(matches!(
        try_execute_sql(&instance, "insert into demo2(ts) select memory from demo1")
            .await
            .unwrap_err(),
        Error::PlanStatement { .. }
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
async fn test_execute_insert_query_with_i64_timestamp(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();

    execute_sql(
        &instance,
        "create table demo(host string, cpu double, memory double, ts bigint time index, primary key (host));",
    ).await;

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

    let expected = "\
+---------+
| Tables  |
+---------+
| numbers |
| scripts |
+---------+\
";
    let output = execute_sql(&instance, "show tables").await;
    check_unordered_output_stream(output, expected).await;

    execute_sql(
        &instance,
        "create table demo(host string, cpu double, memory double, ts timestamp time index, primary key (host));",
    ).await;

    let output = execute_sql(&instance, "show tables").await;
    let expected = "\
+---------+
| Tables  |
+---------+
| demo    |
| numbers |
| scripts |
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

    let output = execute_sql(
        &instance,
        r#"create external table test_table(
                            host string,
                            ts timestamp,
                            cpu double default 0,
                            memory double
                        ) with (location='/tmp/', format='csv');"#,
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(0)));
}

#[apply(both_instances_cases)]
async fn test_execute_external_create_without_ts_type(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();

    let output = execute_sql(
        &instance,
        r#"create external table test_table(
                            host string,
                            cpu double default 0,
                            memory double
                        ) with (location='/tmp/', format='csv');"#,
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(0)));
}

#[apply(both_instances_cases)]
async fn test_execute_query_external_table_parquet(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();
    let format = "parquet";
    let location = get_data_dir("../../tests/data/parquet/various_type.parquet")
        .canonicalize()
        .unwrap()
        .display()
        .to_string();

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
+------------+-----------------+------+---------+---------------+
| Field      | Type            | Null | Default | Semantic Type |
+------------+-----------------+------+---------+---------------+
| c_int      | Int64           | YES  |         | FIELD         |
| c_float    | Float64         | YES  |         | FIELD         |
| c_string   | Float64         | YES  |         | FIELD         |
| c_bool     | Boolean         | YES  |         | FIELD         |
| c_date     | Date            | YES  |         | FIELD         |
| c_datetime | TimestampSecond | YES  |         | FIELD         |
+------------+-----------------+------+---------+---------------+";
    check_output_stream(output, expect).await;

    let output = execute_sql(&instance, &format!("select * from {table_name};")).await;
    let expect = "\
+-------+-----------+----------+--------+------------+---------------------+
| c_int | c_float   | c_string | c_bool | c_date     | c_datetime          |
+-------+-----------+----------+--------+------------+---------------------+
| 1     | 1.1       | 1.11     | true   | 1970-01-01 | 1970-01-01T00:00:00 |
| 2     | 2.2       | 2.22     | true   | 2020-11-08 | 2020-11-08T01:00:00 |
| 3     |           | 3.33     | true   | 1969-12-31 | 1969-11-08T02:00:00 |
| 4     | 4.4       |          | false  |            |                     |
| 5     | 6.6       |          | false  | 1990-01-01 | 1990-01-01T03:00:00 |
| 4     | 4000000.0 |          | false  |            |                     |
| 4     | 4.0e-6    |          | false  |            |                     |
+-------+-----------+----------+--------+------------+---------------------+";
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
async fn test_execute_query_external_table_csv(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();
    let format = "csv";
    let location = get_data_dir("../../tests/data/csv/various_type.csv")
        .canonicalize()
        .unwrap()
        .display()
        .to_string();

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
+------------+-----------------+------+---------+---------------+
| Field      | Type            | Null | Default | Semantic Type |
+------------+-----------------+------+---------+---------------+
| c_int      | Int64           | YES  |         | FIELD         |
| c_float    | Float64         | YES  |         | FIELD         |
| c_string   | Float64         | YES  |         | FIELD         |
| c_bool     | Boolean         | YES  |         | FIELD         |
| c_date     | Date            | YES  |         | FIELD         |
| c_datetime | TimestampSecond | YES  |         | FIELD         |
+------------+-----------------+------+---------+---------------+";
    check_output_stream(output, expect).await;

    let output = execute_sql(&instance, &format!("select * from {table_name};")).await;
    let expect = "\
+-------+-----------+----------+--------+------------+---------------------+
| c_int | c_float   | c_string | c_bool | c_date     | c_datetime          |
+-------+-----------+----------+--------+------------+---------------------+
| 1     | 1.1       | 1.11     | true   | 1970-01-01 | 1970-01-01T00:00:00 |
| 2     | 2.2       | 2.22     | true   | 2020-11-08 | 2020-11-08T01:00:00 |
| 3     |           | 3.33     | true   | 1969-12-31 | 1969-11-08T02:00:00 |
| 4     | 4.4       |          | false  |            |                     |
| 5     | 6.6       |          | false  | 1990-01-01 | 1990-01-01T03:00:00 |
| 4     | 4000000.0 |          | false  |            |                     |
| 4     | 4.0e-6    |          | false  |            |                     |
+-------+-----------+----------+--------+------------+---------------------+";
    check_output_stream(output, expect).await;
}

#[apply(both_instances_cases)]
async fn test_execute_query_external_table_json(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();
    let format = "json";
    let location = get_data_dir("../../tests/data/json/various_type.json")
        .canonicalize()
        .unwrap()
        .display()
        .to_string();

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
+-------+---------+------+---------+---------------+
| Field | Type    | Null | Default | Semantic Type |
+-------+---------+------+---------+---------------+
| a     | Int64   | YES  |         | FIELD         |
| b     | Float64 | YES  |         | FIELD         |
| c     | Boolean | YES  |         | FIELD         |
| d     | String  | YES  |         | FIELD         |
| e     | Int64   | YES  |         | FIELD         |
| f     | String  | YES  |         | FIELD         |
| g     | String  | YES  |         | FIELD         |
+-------+---------+------+---------+---------------+";
    check_output_stream(output, expect).await;

    let output = execute_sql(&instance, &format!("select * from {table_name};")).await;
    let expect = "\
+-----------------+------+-------+------+------------+----------------+-------------------------+
| a               | b    | c     | d    | e          | f              | g                       |
+-----------------+------+-------+------+------------+----------------+-------------------------+
| 1               | 2.0  | false | 4    | 1681319393 | 1.02           | 2012-04-23T18:25:43.511 |
| -10             | -3.5 | true  | 4    | 1681356393 | -0.3           | 2016-04-23T18:25:43.511 |
| 2               | 0.6  | false | text | 1681329393 | 1377.223       |                         |
| 1               | 2.0  | false | 4    |            | 1337.009       |                         |
| 7               | -3.5 | true  | 4    |            | 1              |                         |
| 1               | 0.6  | false | text |            | 1338           | 2018-10-23T18:33:16.481 |
| 1               | 2.0  | false | 4    |            | 12345829100000 |                         |
| 5               | -3.5 | true  | 4    |            | 99999999.99    |                         |
| 1               | 0.6  | false | text |            | 1              |                         |
| 1               | 2.0  | false | 4    |            | 1              |                         |
| 1               | -3.5 | true  | 4    |            | 1              |                         |
| 100000000000000 | 0.6  | false | text |            | 1              |                         |
+-----------------+------+-------+------+------------+----------------+-------------------------+";
    check_output_stream(output, expect).await;
}

#[apply(both_instances_cases)]
async fn test_execute_query_external_table_json_with_schame(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();
    let format = "json";
    let location = get_data_dir("../../tests/data/json/various_type.json")
        .canonicalize()
        .unwrap()
        .display()
        .to_string();

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
                g TIMESTAMP(0) NULL,
              ) WITH (location='{location}', format='{format}');"#,
        ),
    )
    .await;
    assert!(matches!(output, Output::AffectedRows(0)));

    let output = execute_sql(&instance, &format!("desc table {table_name};")).await;
    let expect = "\
+-------+-----------------+------+---------+---------------+
| Field | Type            | Null | Default | Semantic Type |
+-------+-----------------+------+---------+---------------+
| a     | Int64           | YES  |         | FIELD         |
| b     | Float64         | YES  |         | FIELD         |
| c     | Boolean         | YES  |         | FIELD         |
| d     | String          | YES  |         | FIELD         |
| e     | TimestampSecond | YES  |         | FIELD         |
| f     | Float64         | YES  |         | FIELD         |
| g     | TimestampSecond | YES  |         | FIELD         |
+-------+-----------------+------+---------+---------------+";
    check_output_stream(output, expect).await;

    let output = execute_sql(&instance, &format!("select * from {table_name};")).await;
    let expect = "\
+-----------------+------+-------+------+---------------------+---------------+---------------------+
| a               | b    | c     | d    | e                   | f             | g                   |
+-----------------+------+-------+------+---------------------+---------------+---------------------+
| 1               | 2.0  | false | 4    | 2023-04-12T17:09:53 | 1.02          | 2012-04-23T18:25:43 |
| -10             | -3.5 | true  | 4    | 2023-04-13T03:26:33 | -0.3          | 2016-04-23T18:25:43 |
| 2               | 0.6  | false | text | 2023-04-12T19:56:33 | 1377.223      |                     |
| 1               | 2.0  | false | 4    |                     | 1337.009      |                     |
| 7               | -3.5 | true  | 4    |                     | 1.0           |                     |
| 1               | 0.6  | false | text |                     | 1338.0        | 2018-10-23T18:33:16 |
| 1               | 2.0  | false | 4    |                     | 1.23458291e13 |                     |
| 5               | -3.5 | true  | 4    |                     | 99999999.99   |                     |
| 1               | 0.6  | false | text |                     | 1.0           |                     |
| 1               | 2.0  | false | 4    |                     | 1.0           |                     |
| 1               | -3.5 | true  | 4    |                     | 1.0           |                     |
| 100000000000000 | 0.6  | false | text |                     | 1.0           |                     |
+-----------------+------+-------+------+---------------------+---------------+---------------------+";
    check_output_stream(output, expect).await;
}

#[apply(standalone_instance_case)]
async fn test_rename_table(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();

    let output = execute_sql(&instance, "create database db").await;
    assert!(matches!(output, Output::AffectedRows(1)));

    let query_ctx = Arc::new(QueryContext::with(DEFAULT_CATALOG_NAME, "db"));
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

    try_execute_sql_with(&instance, "select * from demo", query_ctx)
        .await
        .expect_err("no table found in expect");
}

// should apply to both instances. tracked in #723
#[apply(standalone_instance_case)]
async fn test_create_table_after_rename_table(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();

    let output = execute_sql(&instance, "create database db").await;
    assert!(matches!(output, Output::AffectedRows(1)));

    // create test table
    let table_name = "demo";
    let query_ctx = Arc::new(QueryContext::with(DEFAULT_CATALOG_NAME, "db"));
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
    test_insert_with_default_value_for_type(instance.frontend(), "bigint").await;
}

#[apply(both_instances_cases)]
async fn test_use_database(instance: Arc<dyn MockInstance>) {
    let instance = instance.frontend();

    let output = execute_sql(&instance, "create database db1").await;
    assert!(matches!(output, Output::AffectedRows(1)));

    let query_ctx = Arc::new(QueryContext::with(DEFAULT_CATALOG_NAME, "db1"));
    let output = execute_sql_with(
        &instance,
        "create table tb1(col_i32 int, ts bigint, TIME INDEX(ts))",
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
                execute_sql(
                    &instance,
                    &format!(
                "create table {}(host string, cpu double, memory double, ts timestamp time index);",
                test.table_name
            ),
                )
                .await;
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
async fn test_information_schema(instance: Arc<dyn MockInstance>) {
    let is_distributed_mode = instance.is_distributed_mode();
    let instance = instance.frontend();

    let sql = "create table another_table(i bigint time index)";
    let query_ctx = Arc::new(QueryContext::with("another_catalog", "another_schema"));
    let output = execute_sql_with(&instance, sql, query_ctx.clone()).await;
    assert!(matches!(output, Output::AffectedRows(0)));

    // User can only see information schema under current catalog.
    // A necessary requirement to GreptimeCloud.
    let sql = "select table_catalog, table_schema, table_name, table_type, table_id, engine from information_schema.tables where table_type != 'SYSTEM VIEW' order by table_name";

    let output = execute_sql(&instance, sql).await;
    let expected = match is_distributed_mode {
        true => {
            "\
+---------------+--------------------+------------+------------+----------+-------------+
| table_catalog | table_schema       | table_name | table_type | table_id | engine      |
+---------------+--------------------+------------+------------+----------+-------------+
| greptime      | public             | numbers    | BASE TABLE | 1        | test_engine |
| greptime      | public             | scripts    | BASE TABLE | 1024     | mito        |
| greptime      | information_schema | tables     | VIEW       |          |             |
+---------------+--------------------+------------+------------+----------+-------------+"
        }
        false => {
            "\
+---------------+--------------------+------------+------------+----------+-------------+
| table_catalog | table_schema       | table_name | table_type | table_id | engine      |
+---------------+--------------------+------------+------------+----------+-------------+
| greptime      | public             | numbers    | BASE TABLE | 1        | test_engine |
| greptime      | public             | scripts    | BASE TABLE | 1        | mito        |
| greptime      | information_schema | tables     | VIEW       |          |             |
+---------------+--------------------+------------+------------+----------+-------------+"
        }
    };

    check_output_stream(output, expected).await;

    let output = execute_sql_with(&instance, sql, query_ctx).await;
    let expected = match is_distributed_mode {
        true => {
            "\
+-----------------+--------------------+---------------+------------+----------+--------+
| table_catalog   | table_schema       | table_name    | table_type | table_id | engine |
+-----------------+--------------------+---------------+------------+----------+--------+
| another_catalog | another_schema     | another_table | BASE TABLE | 1025     | mito   |
| another_catalog | information_schema | tables        | VIEW       |          |        |
+-----------------+--------------------+---------------+------------+----------+--------+"
        }
        false => {
            "\
+-----------------+--------------------+---------------+------------+----------+--------+
| table_catalog   | table_schema       | table_name    | table_type | table_id | engine |
+-----------------+--------------------+---------------+------------+----------+--------+
| another_catalog | another_schema     | another_table | BASE TABLE | 1024     | mito   |
| another_catalog | information_schema | tables        | VIEW       |          |        |
+-----------------+--------------------+---------------+------------+----------+--------+"
        }
    };
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
