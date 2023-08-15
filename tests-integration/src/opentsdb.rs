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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_query::Output;
    use common_recordbatch::RecordBatches;
    use frontend::instance::Instance;
    use itertools::Itertools;
    use servers::opentsdb::codec::DataPoint;
    use servers::query_handler::sql::SqlQueryHandler;
    use servers::query_handler::OpentsdbProtocolHandler;
    use session::context::QueryContext;

    use crate::tests;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_standalone_exec() {
        let standalone = tests::create_standalone_instance("test_standalone_exec").await;
        let instance = &standalone.instance;

        test_exec(instance).await;
    }

    #[ignore = "https://github.com/GreptimeTeam/greptimedb/issues/1681"]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_distributed_exec() {
        let distributed = tests::create_distributed_instance("test_distributed_exec").await;
        test_exec(&distributed.frontend()).await;
    }

    async fn test_exec(instance: &Arc<Instance>) {
        let ctx = QueryContext::arc();
        let data_point1 = DataPoint::new(
            "my_metric_1".to_string(),
            1000,
            1.0,
            vec![
                ("tagk1".to_string(), "tagv1".to_string()),
                ("tagk2".to_string(), "tagv2".to_string()),
            ],
        );
        // should create new table "my_metric_1" directly
        instance.exec(&data_point1, ctx.clone()).await.unwrap();

        let data_point2 = DataPoint::new(
            "my_metric_1".to_string(),
            2000,
            2.0,
            vec![
                ("tagk2".to_string(), "tagv2".to_string()),
                ("tagk3".to_string(), "tagv3".to_string()),
            ],
        );
        // should create new column "tagk3" directly
        instance.exec(&data_point2, ctx.clone()).await.unwrap();

        let data_point3 = DataPoint::new("my_metric_1".to_string(), 3000, 3.0, vec![]);
        // should handle null tags properly
        instance.exec(&data_point3, ctx.clone()).await.unwrap();

        let output = instance
            .do_query(
                "select * from my_metric_1 order by greptime_timestamp",
                QueryContext::arc(),
            )
            .await
            .remove(0)
            .unwrap();
        match output {
            Output::Stream(stream) => {
                let recordbatches = RecordBatches::try_collect(stream).await.unwrap();
                let pretty_print = recordbatches.pretty_print().unwrap();
                let expected = vec![
                    "+---------------------+----------------+-------+-------+-------+",
                    "| greptime_timestamp  | greptime_value | tagk1 | tagk2 | tagk3 |",
                    "+---------------------+----------------+-------+-------+-------+",
                    "| 1970-01-01T00:00:01 | 1.0            | tagv1 | tagv2 |       |",
                    "| 1970-01-01T00:00:02 | 2.0            |       | tagv2 | tagv3 |",
                    "| 1970-01-01T00:00:03 | 3.0            |       |       |       |",
                    "+---------------------+----------------+-------+-------+-------+",
                ]
                .into_iter()
                .join("\n");
                assert_eq!(pretty_print, expected);
            }
            _ => unreachable!(),
        };
    }
}
