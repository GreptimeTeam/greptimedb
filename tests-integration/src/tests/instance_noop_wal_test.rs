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

use client::OutputData;
use common_recordbatch::util::collect_batches;
use common_test_util::recordbatch::check_output_stream;
use common_wal::config::{DatanodeWalConfig, MetasrvWalConfig};

use crate::cluster::GreptimeDbClusterBuilder;
use crate::test_util::execute_sql;
use crate::tests::test_util::{MockInstanceBuilder, RebuildableMockInstance, TestContext};

pub(crate) async fn distributed_with_noop_wal() -> TestContext {
    common_telemetry::init_default_ut_logging();
    let test_name = uuid::Uuid::new_v4().to_string();
    let builder = GreptimeDbClusterBuilder::new(&test_name)
        .await
        .with_datanode_wal_config(DatanodeWalConfig::Noop)
        .with_metasrv_wal_config(MetasrvWalConfig::RaftEngine);
    TestContext::new(MockInstanceBuilder::Distributed(builder)).await
}

#[tokio::test]
async fn test_mito_engine() {
    let mut test_context = distributed_with_noop_wal().await;
    let frontend = test_context.frontend();
    let sql = r#"create table demo(
            host STRING,
            cpu DOUBLE,
            memory DOUBLE,
            ts timestamp,
            TIME INDEX(ts)
        )"#;

    let output = execute_sql(&frontend, sql).await.data;
    assert!(matches!(output, OutputData::AffectedRows(0)));

    let output = execute_sql(
        &frontend,
        "insert into demo(host, cpu, memory, ts) values ('host1', 1.1, 1024, 1655276557000)",
    )
    .await
    .data;
    assert!(matches!(output, OutputData::AffectedRows(1)));

    let output = execute_sql(&frontend, "select * from demo order by ts")
        .await
        .data;
    let expected = r#"+-------+-----+--------+---------------------+
| host  | cpu | memory | ts                  |
+-------+-----+--------+---------------------+
| host1 | 1.1 | 1024.0 | 2022-06-15T07:02:37 |
+-------+-----+--------+---------------------+"#;
    check_output_stream(output, expected).await;

    test_context.rebuild().await;
    let frontend = test_context.frontend();
    let output = execute_sql(&frontend, "select * from demo order by ts")
        .await
        .data;
    // Unflushed data should be lost.
    let expected = r#"++
++"#;
    check_output_stream(output, expected).await;

    let output = execute_sql(
        &frontend,
        "insert into demo(host, cpu, memory, ts) values ('host1', 1.1, 1024, 1655276557000)",
    )
    .await
    .data;
    assert!(matches!(output, OutputData::AffectedRows(1)));
    execute_sql(&frontend, "admin flush_table('demo')").await;

    test_context.rebuild().await;
    let frontend = test_context.frontend();
    let output = execute_sql(&frontend, "select * from demo order by ts")
        .await
        .data;
    let expected = r#"+-------+-----+--------+---------------------+
| host  | cpu | memory | ts                  |
+-------+-----+--------+---------------------+
| host1 | 1.1 | 1024.0 | 2022-06-15T07:02:37 |
+-------+-----+--------+---------------------+"#;
    check_output_stream(output, expected).await;
}

#[tokio::test]
async fn test_metric_engine() {
    let mut test_context = distributed_with_noop_wal().await;
    let frontend = test_context.frontend();

    let sql = r#"CREATE TABLE phy (ts timestamp time index, val double) engine=metric with ("physical_metric_table" = "");"#;
    let output = execute_sql(&frontend, sql).await.data;
    assert!(matches!(output, OutputData::AffectedRows(0)));

    let sql = r#"CREATE TABLE t1 (ts timestamp time index, val double, host string primary key) engine = metric with ("on_physical_table" = "phy");"#;
    let output = execute_sql(&frontend, sql).await.data;
    assert!(matches!(output, OutputData::AffectedRows(0)));

    // The logical table should be lost.
    test_context.rebuild().await;
    let frontend = test_context.frontend();
    let output = execute_sql(&frontend, "select * from t1").await;
    let err = unwrap_err(output.data).await;
    // Should returns region not found error.
    assert!(err.contains("not found"));

    let sql = r#"CREATE TABLE t2 (ts timestamp time index, job string primary key, val double) engine = metric with ("on_physical_table" = "phy");"#;
    let output = execute_sql(&frontend, sql).await.data;
    assert!(matches!(output, OutputData::AffectedRows(0)));

    execute_sql(
        &frontend,
        "INSERT INTO t2 VALUES ('job1', 0, 0), ('job2', 1, 1);",
    )
    .await;
    execute_sql(&frontend, "admin flush_table('phy')").await;

    test_context.rebuild().await;
    let frontend = test_context.frontend();
    let output = execute_sql(&frontend, "select * from t2 order by job").await;
    let expected = r#"+------+-------------------------+-----+
| job  | ts                      | val |
+------+-------------------------+-----+
| job1 | 1970-01-01T00:00:00     | 0.0 |
| job2 | 1970-01-01T00:00:00.001 | 1.0 |
+------+-------------------------+-----+"#;
    check_output_stream(output.data, expected).await;
}

async fn unwrap_err(output: OutputData) -> String {
    let error = match output {
        OutputData::Stream(stream) => collect_batches(stream).await.unwrap_err(),
        _ => unreachable!(),
    };
    format!("{error:?}")
}
