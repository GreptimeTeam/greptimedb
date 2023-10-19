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

    use crate::standalone::GreptimeDbStandaloneBuilder;
    use crate::tests;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_standalone_exec() {
        let standalone = GreptimeDbStandaloneBuilder::new("test_standalone_exec")
            .build()
            .await;
        let instance = &standalone.instance;

        test_exec(instance).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_distributed_exec() {
        let distributed = tests::create_distributed_instance("test_distributed_exec").await;
        test_exec(&distributed.frontend()).await;
    }

    async fn test_exec(instance: &Arc<Instance>) {
        let ctx = QueryContext::arc();

        // should create new table "my_metric_1" directly
        let data_point1 = DataPoint::new(
            "my_metric_1".to_string(),
            1000,
            1.0,
            vec![
                ("tagk1".to_string(), "tagv1".to_string()),
                ("tagk2".to_string(), "tagv2".to_string()),
            ],
        );

        // should create new column "tagk3" directly
        let data_point2 = DataPoint::new(
            "my_metric_1".to_string(),
            2000,
            2.0,
            vec![
                ("tagk2".to_string(), "tagv2".to_string()),
                ("tagk3".to_string(), "tagv3".to_string()),
            ],
        );

        // should handle null tags properly
        let data_point3 = DataPoint::new("my_metric_1".to_string(), 3000, 3.0, vec![]);

        let data_points = vec![data_point1, data_point2, data_point3];
        instance.exec(data_points, ctx.clone()).await.unwrap();

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
                    "+-------+-------+----------------+---------------------+-------+",
                    "| tagk1 | tagk2 | greptime_value | greptime_timestamp  | tagk3 |",
                    "+-------+-------+----------------+---------------------+-------+",
                    "| tagv1 | tagv2 | 1.0            | 1970-01-01T00:00:01 |       |",
                    "|       | tagv2 | 2.0            | 1970-01-01T00:00:02 | tagv3 |",
                    "|       |       | 3.0            | 1970-01-01T00:00:03 |       |",
                    "+-------+-------+----------------+---------------------+-------+",
                ]
                .into_iter()
                .join("\n");
                assert_eq!(pretty_print, expected);
            }
            _ => unreachable!(),
        };
    }
}
