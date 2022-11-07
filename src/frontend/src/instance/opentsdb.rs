use async_trait::async_trait;
use client::ObjectResult;
use common_error::prelude::BoxedError;
use servers::context::Context;
use servers::error as server_error;
use servers::opentsdb::codec::DataPoint;
use servers::query_handler::OpentsdbProtocolHandler;
use snafu::prelude::*;

use crate::error::{self, Result};
use crate::instance::Instance;

#[async_trait]
impl OpentsdbProtocolHandler for Instance {
    async fn exec(&self, data_point: &DataPoint, _ctx: &Context) -> server_error::Result<()> {
        // TODO(LFC): Insert metrics in batch, then make OpentsdbLineProtocolHandler::exec received multiple data points, when
        // metric table and tags can be created upon insertion.
        self.insert_opentsdb_metric(data_point)
            .await
            .map_err(BoxedError::new)
            .with_context(|_| server_error::PutOpentsdbDataPointSnafu {
                data_point: format!("{:?}", data_point),
            })?;
        Ok(())
    }
}

impl Instance {
    async fn insert_opentsdb_metric(&self, data_point: &DataPoint) -> Result<()> {
        let expr = data_point.as_grpc_insert();

        let result = self.database().insert(expr.clone()).await;

        let object_result = match result {
            Ok(result) => result,
            Err(_) => {
                return Err(result.context(error::RequestDatanodeSnafu).unwrap_err());
            }
        };

        match object_result {
            ObjectResult::Mutate(mutate) => {
                if mutate.success != 1 || mutate.failure != 0 {
                    return error::ExecOpentsdbPutSnafu {
                        reason: format!("illegal result: {:?}", mutate),
                    }
                    .fail();
                }
            }
            ObjectResult::Select(_) => unreachable!(),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use common_query::Output;
    use datafusion::arrow_print;
    use servers::query_handler::SqlQueryHandler;

    use super::*;
    use crate::tests;

    #[tokio::test]
    async fn test_exec() {
        let instance = tests::create_frontend_instance().await;
        instance
            .exec(
                &DataPoint::try_create(
                    "put sys.if.bytes.out 1479496100 1.3E3 host=web01 interface=eth0",
                )
                .unwrap(),
                &Context::new(),
            )
            .await
            .unwrap();
        instance
            .exec(
                &DataPoint::try_create("put sys.procs.running 1479496100 42 host=web01").unwrap(),
                &Context::new(),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_insert_opentsdb_metric() {
        let instance = tests::create_frontend_instance().await;

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
            .do_query("select * from my_metric_1", &Context::new())
            .await
            .unwrap();
        match output {
            Output::RecordBatches(recordbatches) => {
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
