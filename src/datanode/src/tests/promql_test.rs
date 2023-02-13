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

use common_query::Output;
use query::parser::PromQuery;
use session::context::QueryContext;

use crate::tests::test_util::{check_output_stream, setup_test_instance};

#[tokio::test(flavor = "multi_thread")]
async fn sql_insert_promql_query_ceil() {
    let instance = setup_test_instance("test_execute_insert").await;
    let query_ctx = QueryContext::arc();
    let put_output = instance
        .inner()
        .execute_sql(
            r#"insert into demo(host, cpu, memory, ts) values
            ('host1', 66.6, 1024, 0),
            ('host1', 66.6, 2048, 2000),
            ('host1', 66.6, 4096, 5000),
            ('host1', 43.1, 8192, 7000),
            ('host1', 19.1, 10240, 9000),
            ('host1', 99.1, 20480, 10000),
            ('host1', 999.9, 40960, 21000),
            ('host1', 31.9,  8192, 22000),
            ('host1', 95.4,  333.3, 32000),
            ('host1', 12423.1,  1333.3, 49000),
            ('host1', 0,  2333.3, 80000),
            ('host1', 49,  3333.3, 99000);
            "#,
            query_ctx.clone(),
        )
        .await
        .unwrap();
    assert!(matches!(put_output, Output::AffectedRows(12)));

    let promql = PromQuery {
        query: String::from("ceil(demo{host=\"host1\"})"),
        start: String::from("0"),
        end: String::from("100.000"),
        step: String::from("5s"),
    };
    let query_output = instance
        .inner()
        .execute_promql(promql, query_ctx)
        .await
        .unwrap();
    let expected = String::from(
        "\
+---------------------+----------------+-------------------+
| ts                  | ceil(demo.cpu) | ceil(demo.memory) |
+---------------------+----------------+-------------------+
| 1970-01-01T00:00:00 | 67             | 1024              |
| 1970-01-01T00:00:05 | 67             | 4096              |
| 1970-01-01T00:00:10 | 100            | 20480             |
| 1970-01-01T00:00:50 | 12424          | 1334              |
| 1970-01-01T00:01:20 | 0              | 2334              |
| 1970-01-01T00:01:40 | 49             | 3334              |
+---------------------+----------------+-------------------+",
    );
    check_output_stream(query_output, expected).await;
}
