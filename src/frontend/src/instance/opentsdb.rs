// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use async_trait::async_trait;
use common_error::prelude::BoxedError;
use servers::opentsdb::codec::DataPoint;
use servers::query_handler::OpentsdbProtocolHandler;
use servers::{error as server_error, Mode};
use snafu::prelude::*;

use crate::instance::Instance;

#[async_trait]
impl OpentsdbProtocolHandler for Instance {
    async fn exec(&self, data_point: &DataPoint) -> server_error::Result<()> {
        // TODO(LFC): Insert metrics in batch, then make OpentsdbLineProtocolHandler::exec received multiple data points, when
        // metric table and tags can be created upon insertion.
        match self.mode {
            Mode::Standalone => {
                self.insert_opentsdb_metric(data_point).await?;
            }
            Mode::Distributed => {
                self.dist_insert(vec![data_point.as_grpc_insert()])
                    .await
                    .map_err(BoxedError::new)
                    .context(server_error::ExecuteInsertSnafu {
                        msg: "execute insert failed",
                    })?;
            }
        }

        Ok(())
    }
}

impl Instance {
    async fn insert_opentsdb_metric(&self, data_point: &DataPoint) -> server_error::Result<()> {
        let insert_expr = data_point.as_grpc_insert();
        self.handle_insert(insert_expr)
            .await
            .map_err(BoxedError::new)
            .with_context(|_| server_error::ExecuteQuerySnafu {
                query: format!("{:?}", data_point),
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_query::Output;
    use common_recordbatch::RecordBatches;
    use servers::query_handler::SqlQueryHandler;
    use session::context::QueryContext;

    use super::*;
    use crate::tests;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_exec() {
        let (instance, _guard) = tests::create_frontend_instance("test_exec").await;
        instance
            .exec(
                &DataPoint::try_create(
                    "put sys.if.bytes.out 1479496100 1.3E3 host=web01 interface=eth0",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        instance
            .exec(&DataPoint::try_create("put sys.procs.running 1479496100 42 host=web01").unwrap())
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_insert_opentsdb_metric() {
        let (instance, _guard) =
            tests::create_frontend_instance("test_insert_opentsdb_metric").await;

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
        let result = instance.insert_opentsdb_metric(&data_point1).await;
        assert!(result.is_ok());

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
        let result = instance.insert_opentsdb_metric(&data_point2).await;
        assert!(result.is_ok());

        let data_point3 = DataPoint::new("my_metric_1".to_string(), 3000, 3.0, vec![]);
        // should handle null tags properly
        let result = instance.insert_opentsdb_metric(&data_point3).await;
        assert!(result.is_ok());

        let output = instance
            .do_query(
                "select * from my_metric_1 order by greptime_timestamp",
                Arc::new(QueryContext::new()),
            )
            .await
            .unwrap();
        match output {
            Output::Stream(stream) => {
                let recordbatches = RecordBatches::try_collect(stream).await.unwrap();
                let recordbatches = recordbatches
                    .take()
                    .into_iter()
                    .map(|r| r.df_recordbatch)
                    .collect::<Vec<_>>();
                let pretty_print = arrow_print::write(&recordbatches);
                let pretty_print = pretty_print.lines().collect::<Vec<&str>>();
                let expected = vec![
                    "+---------------------+----------------+-------+-------+-------+",
                    "| greptime_timestamp  | greptime_value | tagk1 | tagk2 | tagk3 |",
                    "+---------------------+----------------+-------+-------+-------+",
                    "| 1970-01-01 00:00:01 | 1              | tagv1 | tagv2 |       |",
                    "| 1970-01-01 00:00:02 | 2              |       | tagv2 | tagv3 |",
                    "| 1970-01-01 00:00:03 | 3              |       |       |       |",
                    "+---------------------+----------------+-------+-------+-------+",
                ];
                assert_eq!(pretty_print, expected);
            }
            _ => unreachable!(),
        };
    }
}
