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

use async_trait::async_trait;
use common_error::prelude::BoxedError;
use servers::influxdb::InfluxdbRequest;
use servers::query_handler::InfluxdbLineProtocolHandler;
use snafu::ResultExt;

use crate::instance::Instance;

#[async_trait]
impl InfluxdbLineProtocolHandler for Instance {
    async fn exec(&self, request: &InfluxdbRequest) -> servers::error::Result<()> {
        let requests = request.try_into()?;
        self.handle_inserts(requests)
            .await
            .map_err(BoxedError::new)
            .context(servers::error::ExecuteGrpcQuerySnafu)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use common_query::Output;
    use common_recordbatch::RecordBatches;
    use servers::query_handler::sql::SqlQueryHandler;
    use session::context::QueryContext;

    use super::*;
    use crate::tests;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_standalone_put_influxdb_lines() {
        let standalone =
            tests::create_standalone_instance("test_standalone_put_influxdb_lines").await;
        let instance = &standalone.instance;

        test_put_influxdb_lines(instance).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_distributed_put_influxdb_lines() {
        let instance =
            tests::create_distributed_instance("test_distributed_put_influxdb_lines").await;
        let instance = &instance.frontend;

        test_put_influxdb_lines(instance).await;
    }

    async fn test_put_influxdb_lines(instance: &Arc<Instance>) {
        let lines = r"
monitor1,host=host1 cpu=66.6,memory=1024 1663840496100023100
monitor1,host=host2 memory=1027 1663840496400340001";
        let request = InfluxdbRequest {
            precision: None,
            tenant: "greptime".to_string(),
            db: "public".to_string(),
            lines: lines.to_string(),
        };
        instance.exec(&request).await.unwrap();

        let mut output = instance
            .do_query(
                "SELECT ts, host, cpu, memory FROM monitor1 ORDER BY ts",
                QueryContext::arc(),
            )
            .await;
        let output = output.remove(0).unwrap();
        let Output::Stream(stream) = output else { unreachable!() };
        let recordbatches = RecordBatches::try_collect(stream).await.unwrap();
        assert_eq!(
            recordbatches.pretty_print().unwrap(),
            "\
+-------------------------+-------+------+--------+
| ts                      | host  | cpu  | memory |
+-------------------------+-------+------+--------+
| 2022-09-22T09:54:56.100 | host1 | 66.6 | 1024   |
| 2022-09-22T09:54:56.400 | host2 |      | 1027   |
+-------------------------+-------+------+--------+"
        );
    }
}
