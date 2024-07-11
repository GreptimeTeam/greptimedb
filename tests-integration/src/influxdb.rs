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
mod test {
    use std::sync::Arc;

    use client::OutputData;
    use common_recordbatch::RecordBatches;
    use rstest::rstest;
    use rstest_reuse::apply;
    use servers::influxdb::InfluxdbRequest;
    use servers::query_handler::sql::SqlQueryHandler;
    use servers::query_handler::InfluxdbLineProtocolHandler;
    use session::context::QueryContext;

    use crate::tests::test_util::{both_instances_cases, distributed, standalone, MockInstance};

    #[apply(both_instances_cases)]
    async fn test_put_influxdb_lines_without_time_column(instance: Arc<dyn MockInstance>) {
        let instance = instance.frontend();

        let lines = r"
monitor1,host=host1 cpu=66.6,memory=1024
monitor1,host=host2 memory=1027";
        let request = InfluxdbRequest {
            precision: None,
            lines: lines.to_string(),
        };
        assert!(instance.exec(request, QueryContext::arc()).await.is_ok());

        let mut output = instance
            .do_query(
                "SELECT ts, host, cpu, memory FROM monitor1 ORDER BY ts",
                QueryContext::arc(),
            )
            .await;
        let output = output.remove(0).unwrap();
        let OutputData::Stream(stream) = output.data else {
            unreachable!()
        };

        let recordbatches = RecordBatches::try_collect(stream).await.unwrap();
        let recordbatches: Vec<_> = recordbatches.iter().collect();
        let total = recordbatches
            .into_iter()
            .fold(0, |total, recordbatch| total + recordbatch.num_rows());
        assert_eq!(total, 2);
    }

    #[apply(both_instances_cases)]
    async fn test_put_influxdb_lines(instance: Arc<dyn MockInstance>) {
        let instance = instance.frontend();

        let lines = r"
monitor1,host=host1 cpu=66.6,memory=1024 1663840496100023100
monitor1,host=host2 memory=1027 1663840496400340001";
        let request = InfluxdbRequest {
            precision: None,
            lines: lines.to_string(),
        };
        instance.exec(request, QueryContext::arc()).await.unwrap();

        let mut output = instance
            .do_query(
                "SELECT ts, host, cpu, memory FROM monitor1 ORDER BY ts",
                QueryContext::arc(),
            )
            .await;
        let output = output.remove(0).unwrap();
        let OutputData::Stream(stream) = output.data else {
            unreachable!()
        };
        let recordbatches = RecordBatches::try_collect(stream).await.unwrap();
        assert_eq!(
            recordbatches.pretty_print().unwrap(),
            "\
+-------------------------------+-------+------+--------+
| ts                            | host  | cpu  | memory |
+-------------------------------+-------+------+--------+
| 2022-09-22T09:54:56.100023100 | host1 | 66.6 | 1024.0 |
| 2022-09-22T09:54:56.400340001 | host2 |      | 1027.0 |
+-------------------------------+-------+------+--------+"
        );

        // Put the cpu column for host2.
        let lines = r"
monitor1,host=host2 cpu=32 1663840496400340001";
        let request = InfluxdbRequest {
            precision: None,
            lines: lines.to_string(),
        };
        instance.exec(request, QueryContext::arc()).await.unwrap();

        let mut output = instance
            .do_query(
                "SELECT ts, host, cpu, memory FROM monitor1 ORDER BY ts",
                QueryContext::arc(),
            )
            .await;
        let output = output.remove(0).unwrap();
        let OutputData::Stream(stream) = output.data else {
            unreachable!()
        };
        let recordbatches = RecordBatches::try_collect(stream).await.unwrap();
        assert_eq!(
            recordbatches.pretty_print().unwrap(),
            "\
+-------------------------------+-------+------+--------+
| ts                            | host  | cpu  | memory |
+-------------------------------+-------+------+--------+
| 2022-09-22T09:54:56.100023100 | host1 | 66.6 | 1024.0 |
| 2022-09-22T09:54:56.400340001 | host2 | 32.0 | 1027.0 |
+-------------------------------+-------+------+--------+"
        );
    }
}
