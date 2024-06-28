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
    use common_grpc::precision::Precision;
    use common_recordbatch::RecordBatches;
    use common_test_util::recordbatch::check_output_stream;
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
    }

    #[apply(both_instances_cases)]
    async fn test_put_influxdb_lines_with_already_created_table(instance: Arc<dyn MockInstance>) {
        // First create a table with millisecond time index.
        let sql = "create table monitor (
                            ts timestamp time index,
                            host string primary key,
                            cpu double,
                            memory double,
                        )";
        instance.exec_sql(sql).await;

        // Insert some influxdb lines with millisecond precision.
        let lines = r"
monitor,host=127.0.0.1 cpu=0.1,memory=1.0 1719460800001
monitor,host=127.0.0.2 cpu=0.2,memory=2.0 1719460800002
monitor,host=127.0.0.1 cpu=0.3,memory=3.0 1719460800003";
        let request = InfluxdbRequest {
            precision: Some(Precision::Millisecond),
            lines: lines.to_string(),
        };
        instance
            .frontend()
            .exec(request, QueryContext::arc())
            .await
            .unwrap();

        // Insert other influxdb lines with nanosecond precision.
        let lines = r"
monitor,host=127.0.0.1 cpu=0.4,memory=4.0 1719460800004000000
monitor,host=127.0.0.2 cpu=0.5,memory=5.0 1719460800005000000
monitor,host=127.0.0.1 cpu=0.6,memory=6.0 1719460800006000000";
        let request = InfluxdbRequest {
            precision: Some(Precision::Nanosecond),
            lines: lines.to_string(),
        };
        instance
            .frontend()
            .exec(request, QueryContext::arc())
            .await
            .unwrap();

        // Check the data.
        let output = instance
            .exec_sql("SELECT ts, host, cpu, memory FROM monitor ORDER BY ts")
            .await
            .data;
        let expected = "\
+-------------------------+-----------+-----+--------+
| ts                      | host      | cpu | memory |
+-------------------------+-----------+-----+--------+
| 2024-06-27T04:00:00.001 | 127.0.0.1 | 0.1 | 1.0    |
| 2024-06-27T04:00:00.002 | 127.0.0.2 | 0.2 | 2.0    |
| 2024-06-27T04:00:00.003 | 127.0.0.1 | 0.3 | 3.0    |
| 2024-06-27T04:00:00.004 | 127.0.0.1 | 0.4 | 4.0    |
| 2024-06-27T04:00:00.005 | 127.0.0.2 | 0.5 | 5.0    |
| 2024-06-27T04:00:00.006 | 127.0.0.1 | 0.6 | 6.0    |
+-------------------------+-----------+-----+--------+";
        check_output_stream(output, expected).await;
    }
}
