use std::collections::HashMap;

use api::v1::{alter_expr, AddColumn, AlterExpr, ColumnDataType, ColumnDef, CreateExpr};
use async_trait::async_trait;
use client::{Error as ClientError, ObjectResult};
use common_error::prelude::{BoxedError, StatusCode};
use servers::error as server_error;
use servers::opentsdb::codec::{
    OpentsdbDataPoint, OPENTSDB_TIMESTAMP_COLUMN_NAME, OPENTSDB_VALUE_COLUMN_NAME,
};
use servers::query_handler::OpentsdbLineProtocolHandler;
use snafu::prelude::*;

use crate::error::{self, Result};
use crate::instance::Instance;

/// [Implementation Notes](https://greptime.feishu.cn/docx/doxcnbXN6f3C5mMog27Qb0BrcOe)

#[async_trait]
impl OpentsdbLineProtocolHandler for Instance {
    async fn exec(&self, data_point: &OpentsdbDataPoint) -> server_error::Result<()> {
        self.insert_opentsdb_metric(data_point)
            .await
            .map_err(BoxedError::new)
            .with_context(|_| server_error::ExecuteQuerySnafu {
                query: format!("{:?}", data_point),
            })?;
        Ok(())
    }
}

impl Instance {
    async fn insert_opentsdb_metric(&self, data_point: &OpentsdbDataPoint) -> Result<()> {
        let expr = data_point.as_grpc_insert();

        let result = self.db.insert(expr.clone()).await;

        let object_result = match result {
            Ok(result) => result,
            Err(ClientError::Datanode { code, .. }) => {
                let retry = if code == StatusCode::TableNotFound as u32 {
                    self.create_opentsdb_metric(&data_point).await?;
                    true
                } else if code == StatusCode::TableColumnNotFound as u32 {
                    self.create_opentsdb_tags(&data_point).await?;
                    true
                } else {
                    false
                };
                if retry {
                    self.db
                        .insert(expr.clone())
                        .await
                        .context(error::RequestDatanodeSnafu)?
                } else {
                    // `unwrap_err` is safe because we are matching "result" in "Err" arm
                    return Err(result.context(error::RequestDatanodeSnafu).unwrap_err());
                }
            }
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

    async fn create_opentsdb_metric(&self, data_point: &OpentsdbDataPoint) -> Result<()> {
        let mut column_defs = Vec::with_capacity(2 + data_point.tags().len());

        let ts_column = ColumnDef {
            name: OPENTSDB_TIMESTAMP_COLUMN_NAME.to_string(),
            datatype: ColumnDataType::Timestamp as i32,
            is_nullable: false,
        };
        column_defs.push(ts_column);

        let value_column = ColumnDef {
            name: OPENTSDB_VALUE_COLUMN_NAME.to_string(),
            datatype: ColumnDataType::Float64 as i32,
            is_nullable: false,
        };
        column_defs.push(value_column);

        for (tagk, _) in data_point.tags().iter() {
            column_defs.push(ColumnDef {
                name: tagk.to_string(),
                datatype: ColumnDataType::String as i32,
                is_nullable: true,
            })
        }

        let expr = CreateExpr {
            catalog_name: None,
            schema_name: None,
            table_name: data_point.metric().to_string(),
            desc: Some(format!(
                "Table for Opentsdb metric: {}",
                &data_point.metric()
            )),
            column_defs,
            time_index: OPENTSDB_TIMESTAMP_COLUMN_NAME.to_string(),
            primary_keys: vec![],
            create_if_not_exists: true,
            table_options: HashMap::new(),
        };

        let result = self
            .admin
            .create(expr)
            .await
            .context(error::RequestDatanodeSnafu)?;
        let header = result.header.context(error::IncompleteGrpcResultSnafu {
            err_msg: "'header' is missing",
        })?;
        if header.code == (StatusCode::Success as u32)
            || header.code == (StatusCode::TableAlreadyExists as u32)
        {
            Ok(())
        } else {
            error::ExecOpentsdbPutSnafu {
                reason: format!("error code: {}", header.code),
            }
            .fail()
        }
    }

    async fn create_opentsdb_tags(&self, data_point: &OpentsdbDataPoint) -> Result<()> {
        // TODO(LFC): support adding columns in one request
        for (tagk, _) in data_point.tags().iter() {
            let tag_column = ColumnDef {
                name: tagk.to_string(),
                datatype: ColumnDataType::String as i32,
                is_nullable: true,
            };
            let expr = AlterExpr {
                catalog_name: None,
                schema_name: None,
                table_name: data_point.metric().to_string(),
                kind: Some(alter_expr::Kind::AddColumn(AddColumn {
                    column_def: Some(tag_column),
                })),
            };

            let result = self
                .admin
                .alter(expr)
                .await
                .context(error::RequestDatanodeSnafu)?;
            let header = result.header.context(error::IncompleteGrpcResultSnafu {
                err_msg: "'header' is missing",
            })?;
            if header.code != (StatusCode::Success as u32)
                && header.code != (StatusCode::TableColumnExists as u32)
            {
                return error::ExecOpentsdbPutSnafu {
                    reason: format!("error code: {}", header.code),
                }
                .fail();
            }
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
                &OpentsdbDataPoint::try_create(
                    "put sys.if.bytes.out 1479496100 1.3E3 host=web01 interface=eth0",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        instance
            .exec(
                &OpentsdbDataPoint::try_create("put sys.procs.running 1479496100 42 host=web01")
                    .unwrap(),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_insert_opentsdb_metric() {
        let instance = tests::create_frontend_instance().await;

        let data_point1 = OpentsdbDataPoint::new(
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

        let data_point2 = OpentsdbDataPoint::new(
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

        let data_point3 = OpentsdbDataPoint::new("my_metric_1".to_string(), 3000, 3.0, vec![]);
        // should handle null tags properly
        let result = instance.insert_opentsdb_metric(&data_point3).await;
        assert!(result.is_ok());

        let output = instance
            .do_query("select * from my_metric_1")
            .await
            .unwrap();
        match output {
            Output::RecordBatches(recordbatches) => {
                let recordbatches = recordbatches
                    .to_vec()
                    .into_iter()
                    .map(|r| r.df_recordbatch)
                    .collect::<Vec<_>>();
                let pretty_print = arrow_print::write(&recordbatches);
                let pretty_print = pretty_print.lines().collect::<Vec<&str>>();
                let expected = vec![
                    "+---------------------+-------+-------+-------+-------+",
                    "| timestamp           | value | tagk1 | tagk2 | tagk3 |",
                    "+---------------------+-------+-------+-------+-------+",
                    "| 1970-01-01 00:00:01 | 1     | tagv1 | tagv2 |       |",
                    "| 1970-01-01 00:00:02 | 2     |       | tagv2 | tagv3 |",
                    "| 1970-01-01 00:00:03 | 3     |       |       |       |",
                    "+---------------------+-------+-------+-------+-------+",
                ];
                assert_eq!(pretty_print, expected);
            }
            _ => unreachable!(),
        };
    }
}
