use std::collections::HashMap;

use api::v1::codec::InsertBatch;
use api::v1::column::SemanticType;
use api::v1::{column, insert_expr, Column, ColumnDataType, InsertExpr};
use common_catalog::consts::DEFAULT_SCHEMA_NAME;
use common_grpc::writer::Precision;
use table::requests::InsertRequest;

use crate::error::{self, Result};
use crate::line_writer::LineWriter;

pub const OPENTSDB_TIMESTAMP_COLUMN_NAME: &str = "greptime_timestamp";
pub const OPENTSDB_VALUE_COLUMN_NAME: &str = "greptime_value";

#[derive(Debug)]
pub struct DataPoint {
    metric: String,
    ts_millis: i64,
    value: f64,
    tags: Vec<(String, String)>,
}

impl DataPoint {
    pub fn new(metric: String, ts_millis: i64, value: f64, tags: Vec<(String, String)>) -> Self {
        Self {
            metric,
            ts_millis,
            value,
            tags,
        }
    }

    pub fn try_create(line: &str) -> Result<Self> {
        let tokens = line.split_whitespace().collect::<Vec<&str>>();
        let cmd = if tokens.is_empty() { "" } else { tokens[0] };
        // OpenTSDB command is case sensitive, verified in real OpenTSDB.
        if cmd != "put" {
            return error::InvalidQuerySnafu {
                reason: format!("unknown command {}.", cmd),
            }
            .fail();
        }
        if tokens.len() < 4 {
            return error::InvalidQuerySnafu {
                reason: format!(
                    "put: illegal argument: not enough arguments (need least 4, got {})",
                    tokens.len()
                ),
            }
            .fail();
        }

        let metric = tokens[1];

        let ts_millis = match tokens[2].parse::<i64>() {
            Ok(t) => Self::timestamp_to_millis(t),
            Err(_) => {
                return error::InvalidQuerySnafu {
                    reason: format!("put: invalid timestamp: {}", tokens[2]),
                }
                .fail()
            }
        };

        let value = match tokens[3].parse::<f64>() {
            Ok(v) => v,
            Err(_) => {
                return error::InvalidQuerySnafu {
                    reason: format!("put: invalid value: {}", tokens[3]),
                }
                .fail()
            }
        };

        let mut tags = Vec::with_capacity(tokens.len() - 4);
        for token in tokens.iter().skip(4) {
            let tag = token.split('=').collect::<Vec<&str>>();
            if tag.len() != 2 || tag[0].is_empty() || tag[1].is_empty() {
                return error::InvalidQuerySnafu {
                    reason: format!("put: invalid tag: {}", token),
                }
                .fail();
            }
            let tagk = tag[0].to_string();
            let tagv = tag[1].to_string();
            if tags.iter().any(|(t, _)| t == &tagk) {
                return error::InvalidQuerySnafu {
                    reason: format!("put: illegal argument: duplicate tag: {}", tagk),
                }
                .fail();
            }
            tags.push((tagk, tagv));
        }

        Ok(DataPoint {
            metric: metric.to_string(),
            ts_millis,
            value,
            tags,
        })
    }

    pub fn metric(&self) -> &str {
        &self.metric
    }

    pub fn tags(&self) -> &Vec<(String, String)> {
        &self.tags
    }

    pub fn ts_millis(&self) -> i64 {
        self.ts_millis
    }

    pub fn value(&self) -> f64 {
        self.value
    }

    pub fn as_insert_request(&self) -> InsertRequest {
        let mut line_writer = LineWriter::with_lines(DEFAULT_SCHEMA_NAME, self.metric.clone(), 1);
        line_writer.write_ts(
            OPENTSDB_TIMESTAMP_COLUMN_NAME,
            (self.ts_millis(), Precision::MILLISECOND),
        );

        line_writer.write_f64(OPENTSDB_VALUE_COLUMN_NAME, self.value);

        for (tagk, tagv) in self.tags.iter() {
            line_writer.write_tag(tagk, tagv);
        }
        line_writer.commit();
        line_writer.finish()
    }

    // TODO(fys): will remove in the future.
    pub fn as_grpc_insert(&self) -> InsertExpr {
        let schema_name = DEFAULT_SCHEMA_NAME.to_string();
        let mut columns = Vec::with_capacity(2 + self.tags.len());

        let ts_column = Column {
            column_name: OPENTSDB_TIMESTAMP_COLUMN_NAME.to_string(),
            values: Some(column::Values {
                ts_millis_values: vec![self.ts_millis],
                ..Default::default()
            }),
            semantic_type: SemanticType::Timestamp as i32,
            datatype: ColumnDataType::Timestamp as i32,
            ..Default::default()
        };
        columns.push(ts_column);

        let value_column = Column {
            column_name: OPENTSDB_VALUE_COLUMN_NAME.to_string(),
            values: Some(column::Values {
                f64_values: vec![self.value],
                ..Default::default()
            }),
            semantic_type: SemanticType::Field as i32,
            datatype: ColumnDataType::Float64 as i32,
            ..Default::default()
        };
        columns.push(value_column);

        for (tagk, tagv) in self.tags.iter() {
            columns.push(Column {
                column_name: tagk.to_string(),
                values: Some(column::Values {
                    string_values: vec![tagv.to_string()],
                    ..Default::default()
                }),
                semantic_type: SemanticType::Tag as i32,
                datatype: ColumnDataType::String as i32,
                ..Default::default()
            });
        }

        let batch = InsertBatch {
            columns,
            row_count: 1,
        };
        InsertExpr {
            schema_name,
            table_name: self.metric.clone(),
            expr: Some(insert_expr::Expr::Values(insert_expr::Values {
                values: vec![batch.into()],
            })),
            options: HashMap::default(),
            region_number: 0,
        }
    }

    pub fn timestamp_to_millis(t: i64) -> i64 {
        // 9999999999999 (13 digits) is of date "Sat Nov 20 2286 17:46:39 UTC",
        // 999999999999 (12 digits) is "Sun Sep 09 2001 01:46:39 UTC",
        // so timestamp digits less than 13 means we got seconds here.
        // (We are not expecting to store data that is 21 years ago, are we?)
        if t.abs().to_string().len() < 13 {
            t * 1000
        } else {
            t
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use common_time::timestamp::TimeUnit;
    use common_time::Timestamp;
    use datatypes::value::Value;
    use datatypes::vectors::Vector;

    use super::*;

    #[test]
    fn test_try_create() {
        fn test_illegal_line(line: &str, expected_err: &str) {
            let result = DataPoint::try_create(line);
            match result.unwrap_err() {
                error::Error::InvalidQuery { reason, .. } => {
                    assert_eq!(reason, expected_err)
                }
                _ => unreachable!(),
            }
        }

        test_illegal_line("no_put", "unknown command no_put.");
        test_illegal_line(
            "put",
            "put: illegal argument: not enough arguments (need least 4, got 1)",
        );
        test_illegal_line(
            "put metric.foo notatime 42 host=web01",
            "put: invalid timestamp: notatime",
        );
        test_illegal_line(
            "put metric.foo 1000 notavalue host=web01",
            "put: invalid value: notavalue",
        );
        test_illegal_line("put metric.foo 1000 42 host=", "put: invalid tag: host=");
        test_illegal_line(
            "put metric.foo 1000 42 host=web01 host=web02",
            "put: illegal argument: duplicate tag: host",
        );

        let data_point = DataPoint::try_create(
            "put sys.if.bytes.out 1479496100 1.3E3 host=web01 interface=eth0",
        )
        .unwrap();
        assert_eq!(data_point.metric, "sys.if.bytes.out");
        assert_eq!(data_point.ts_millis, 1479496100000);
        assert_eq!(data_point.value, 1.3e3);
        assert_eq!(
            data_point.tags,
            vec![
                ("host".to_string(), "web01".to_string()),
                ("interface".to_string(), "eth0".to_string())
            ]
        );

        let data_point =
            DataPoint::try_create("put sys.procs.running 1479496100 42 host=web01").unwrap();
        assert_eq!(data_point.metric, "sys.procs.running");
        assert_eq!(data_point.ts_millis, 1479496100000);
        assert_eq!(data_point.value, 42f64);
        assert_eq!(
            data_point.tags,
            vec![("host".to_string(), "web01".to_string())]
        );
    }

    #[test]
    fn test_as_insert_request() {
        let data_point = DataPoint {
            metric: "my_metric_1".to_string(),
            ts_millis: 1000,
            value: 1.0,
            tags: vec![
                ("tagk1".to_string(), "tagv1".to_string()),
                ("tagk2".to_string(), "tagv2".to_string()),
            ],
        };
        let insert_request = data_point.as_insert_request();
        assert_eq!("my_metric_1", insert_request.table_name);
        let columns = insert_request.columns_values;
        assert_eq!(4, columns.len());
        let ts = columns.get(OPENTSDB_TIMESTAMP_COLUMN_NAME).unwrap();
        let expected = vec![datatypes::prelude::Value::Timestamp(Timestamp::new(
            1000,
            TimeUnit::Millisecond,
        ))];
        assert_vector(&expected, ts);
        let val = columns.get(OPENTSDB_VALUE_COLUMN_NAME).unwrap();
        assert_vector(&[1.0.into()], val);
        let tagk1 = columns.get("tagk1").unwrap();
        assert_vector(&["tagv1".into()], tagk1);
        let tagk2 = columns.get("tagk2").unwrap();
        assert_vector(&["tagv2".into()], tagk2);
    }

    fn assert_vector(expected: &[Value], vector: &Arc<dyn Vector>) {
        for (idx, expected) in expected.iter().enumerate() {
            let val = vector.get(idx);
            assert_eq!(*expected, val);
        }
    }

    // TODO(fys): will remove in the future.
    #[test]
    fn test_as_grpc_insert() {
        let data_point = DataPoint {
            metric: "my_metric_1".to_string(),
            ts_millis: 1000,
            value: 1.0,
            tags: vec![
                ("tagk1".to_string(), "tagv1".to_string()),
                ("tagk2".to_string(), "tagv2".to_string()),
            ],
        };

        let grpc_insert = data_point.as_grpc_insert();
        assert_eq!(grpc_insert.table_name, "my_metric_1");

        match grpc_insert.expr {
            Some(insert_expr::Expr::Values(insert_expr::Values { values })) => {
                assert_eq!(values.len(), 1);
                let insert_batch = InsertBatch::try_from(values[0].as_slice()).unwrap();
                assert_eq!(insert_batch.row_count, 1);
                let columns = insert_batch.columns;
                assert_eq!(columns.len(), 4);

                assert_eq!(columns[0].column_name, OPENTSDB_TIMESTAMP_COLUMN_NAME);
                assert_eq!(
                    columns[0].values.as_ref().unwrap().ts_millis_values,
                    vec![1000]
                );

                assert_eq!(columns[1].column_name, OPENTSDB_VALUE_COLUMN_NAME);
                assert_eq!(columns[1].values.as_ref().unwrap().f64_values, vec![1.0]);

                assert_eq!(columns[2].column_name, "tagk1");
                assert_eq!(
                    columns[2].values.as_ref().unwrap().string_values,
                    vec!["tagv1"]
                );

                assert_eq!(columns[3].column_name, "tagk2");
                assert_eq!(
                    columns[3].values.as_ref().unwrap().string_values,
                    vec!["tagv2"]
                );
            }
            _ => unreachable!(),
        }
    }
}
