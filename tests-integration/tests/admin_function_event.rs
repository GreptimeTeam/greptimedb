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

use common_test_util::temp_dir::create_temp_dir;
use frontend::instance::Instance;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::QueryContext;
use sql::ast::FunctionArguments;
use sql::dialect::GreptimeDbDialect;
use sql::parser::{ParseOptions, ParserContext};
use sql::statements::admin::Admin;
use sql::statements::statement::Statement;
use tests_integration::cluster::GreptimeDbClusterBuilder;
use tests_integration::standalone::GreptimeDbStandaloneBuilder;
use tests_integration::test_util::{StorageType, get_test_store_config};

use crate::event_recorder_test_util::assert_eventually_eq;

const TABLE_NAME: &str = "admin_function_event_test";

#[tokio::test(flavor = "multi_thread")]
async fn test_standalone_admin_function_events() {
    common_telemetry::init_default_ut_logging();
    let standalone = GreptimeDbStandaloneBuilder::new("test_standalone_admin_function_events")
        .build()
        .await;
    assert_admin_function_events(standalone.fe_instance()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_distributed_admin_function_events() {
    let store_type = StorageType::File;
    if !store_type.test_on() {
        return;
    }

    common_telemetry::init_default_ut_logging();
    let (store_config, _guard) = get_test_store_config(&store_type);
    let home_dir = create_temp_dir("test_distributed_admin_function_events_data_home");
    let cluster = GreptimeDbClusterBuilder::new("test_distributed_admin_function_events")
        .await
        .with_datanodes(1)
        .with_store_config(store_config)
        .with_shared_home_dir(Arc::new(home_dir))
        .build(true)
        .await;
    assert_admin_function_events(cluster.fe_instance()).await;
}

async fn assert_admin_function_events(instance: &Arc<Instance>) {
    execute(
        instance,
        &format!("CREATE TABLE {TABLE_NAME} (ts TIMESTAMP TIME INDEX)"),
    )
    .await
    .unwrap();
    execute(instance, &format!("ADMIN FLUSH_TABLE('{TABLE_NAME}')"))
        .await
        .unwrap();

    assert_admin_event(
        instance,
        "flush_table",
        "\
+----------------+----------+---------------------+-----------+---------------------------------------------------------------------------------+--------------+
| type           | actor    | admin_function_name | status    | payload                                                                         | output       |
+----------------+----------+---------------------+-----------+---------------------------------------------------------------------------------+--------------+
| admin_function | greptime | flush_table         | Succeeded | {\"arguments\":{\"type\":\"list\",\"value\":[\"admin_function_event_test\"]},\"version\":1} | {\"result\":0} |
+----------------+----------+---------------------+-----------+---------------------------------------------------------------------------------+--------------+",
    )
    .await;

    let failure = execute(instance, "ADMIN missing_admin_function('missing-input')").await;
    assert!(failure.is_err());

    assert_admin_event(
        instance,
        "missing_admin_function",
        "\
+----------------+----------+------------------------+--------+---------------------------------------------------------------------+-----------------------------------------------------------------+
| type           | actor    | admin_function_name    | status | payload                                                             | output                                                          |
+----------------+----------+------------------------+--------+---------------------------------------------------------------------+-----------------------------------------------------------------+
| admin_function | greptime | missing_admin_function | Failed | {\"arguments\":{\"type\":\"list\",\"value\":[\"missing-input\"]},\"version\":1} | {\"error\":\"0: Admin function not found: missing_admin_function\"} |
+----------------+----------+------------------------+--------+---------------------------------------------------------------------+-----------------------------------------------------------------+",
    )
    .await;

    let failure =
        execute_with_arguments(instance, "missing_no_arguments", FunctionArguments::None).await;
    assert!(failure.is_err());

    assert_admin_event(
        instance,
        "missing_no_arguments",
        "\
+----------------+----------+----------------------+--------+---------------+---------------------------------------------------------------+
| type           | actor    | admin_function_name  | status | payload       | output                                                        |
+----------------+----------+----------------------+--------+---------------+---------------------------------------------------------------+
| admin_function | greptime | missing_no_arguments | Failed | {\"version\":1} | {\"error\":\"0: Admin function not found: missing_no_arguments\"} |
+----------------+----------+----------------------+--------+---------------+---------------------------------------------------------------+",
    )
    .await;

    let Statement::Query(query) = ParserContext::create_with_dialect(
        "SELECT 1",
        &GreptimeDbDialect {},
        ParseOptions::default(),
    )
    .unwrap()
    .remove(0) else {
        panic!("expected query statement")
    };
    let failure = execute_with_arguments(
        instance,
        "missing_subquery",
        FunctionArguments::Subquery(Box::new(query.inner)),
    )
    .await;
    assert!(failure.is_err());

    assert_admin_event(
        instance,
        "missing_subquery",
        "\
+----------------+----------+---------------------+--------+------------------------------------------------------------------+-----------------------------------------------------------+
| type           | actor    | admin_function_name | status | payload                                                          | output                                                    |
+----------------+----------+---------------------+--------+------------------------------------------------------------------+-----------------------------------------------------------+
| admin_function | greptime | missing_subquery    | Failed | {\"arguments\":{\"type\":\"subquery\",\"value\":\"SELECT 1\"},\"version\":1} | {\"error\":\"0: Admin function not found: missing_subquery\"} |
+----------------+----------+---------------------+--------+------------------------------------------------------------------+-----------------------------------------------------------+",
    )
    .await;
}

async fn assert_admin_event(instance: &Arc<Instance>, function: &str, expected: &str) {
    let query = format!(
        r#"SELECT
  type,
  actor,
  admin_function_name,
  admin_function_status AS status,
  json_to_string(payload) AS payload,
  json_to_string(admin_function_output) AS output
FROM greptime_private.events
WHERE type = 'admin_function'
  AND admin_function_name = '{function}'
ORDER BY timestamp DESC
LIMIT 1"#
    );
    assert_eventually_eq(instance, &query, expected).await;
}

async fn execute(
    instance: &Arc<Instance>,
    sql: &str,
) -> servers::error::Result<common_query::Output> {
    instance.do_query(sql, QueryContext::arc()).await.remove(0)
}

async fn execute_with_arguments(
    instance: &Arc<Instance>,
    function: &str,
    arguments: FunctionArguments,
) -> operator::error::Result<common_query::Output> {
    let Statement::Admin(mut statement) = ParserContext::create_with_dialect(
        &format!("ADMIN {function}()"),
        &GreptimeDbDialect {},
        ParseOptions::default(),
    )
    .unwrap()
    .remove(0) else {
        panic!("expected ADMIN statement")
    };
    let Admin::Func(admin_function) = &mut statement;
    admin_function.args = arguments;

    instance
        .statement_executor()
        .execute_sql(Statement::Admin(statement), QueryContext::arc())
        .await
}
