use std::collections::HashMap;

use api::v1::{
    insert_expr::{self, Expr},
    InsertExpr,
};
use common_grpc::writer::{LinesWriter, Precision};
use influxdb_line_protocol::{parse_lines, FieldValue};
use snafu::ResultExt;

use crate::error::{Error, InfluxdbLineProtocolSnafu, InfluxdbLinesWriteSnafu};

pub const INFLUXDB_TIMESTAMP_COLUMN_NAME: &str = "ts";
pub const DEFAULT_TIME_PRECISION: Precision = Precision::NANOSECOND;

pub struct InfluxdbRequest {
    pub precision: Option<Precision>,
    pub lines: String,
}

type TableName = String;

impl TryFrom<&InfluxdbRequest> for Vec<InsertExpr> {
    type Error = Error;

    fn try_from(value: &InfluxdbRequest) -> Result<Self, Self::Error> {
        let mut writers: HashMap<TableName, LinesWriter> = HashMap::new();
        let lines = parse_lines(&value.lines)
            .collect::<influxdb_line_protocol::Result<Vec<_>>>()
            .context(InfluxdbLineProtocolSnafu)?;
        let line_len = lines.len();

        for line in lines {
            let table_name = line.series.measurement;
            let writer = writers
                .entry(table_name.to_string())
                .or_insert_with(|| LinesWriter::with_lines(line_len));

            let tags = line.series.tag_set;
            if let Some(tags) = tags {
                for (k, v) in tags {
                    writer
                        .write_tag(k.as_str(), v.as_str())
                        .context(InfluxdbLinesWriteSnafu)?;
                }
            }

            let fields = line.field_set;
            for (k, v) in fields {
                let column_name = k.as_str();
                match v {
                    FieldValue::I64(value) => {
                        writer
                            .write_i64(column_name, value)
                            .context(InfluxdbLinesWriteSnafu)?;
                    }
                    FieldValue::U64(value) => {
                        writer
                            .write_u64(column_name, value)
                            .context(InfluxdbLinesWriteSnafu)?;
                    }
                    FieldValue::F64(value) => {
                        writer
                            .write_f64(column_name, value)
                            .context(InfluxdbLinesWriteSnafu)?;
                    }
                    FieldValue::String(value) => {
                        writer
                            .write_string(column_name, value.as_str())
                            .context(InfluxdbLinesWriteSnafu)?;
                    }
                    FieldValue::Boolean(value) => {
                        writer
                            .write_bool(column_name, value)
                            .context(InfluxdbLinesWriteSnafu)?;
                    }
                }
            }

            if let Some(timestamp) = line.timestamp {
                let precision = if let Some(val) = &value.precision {
                    *val
                } else {
                    DEFAULT_TIME_PRECISION
                };
                writer
                    .write_ts(INFLUXDB_TIMESTAMP_COLUMN_NAME, (timestamp, precision))
                    .context(InfluxdbLinesWriteSnafu)?;
            }

            writer.commit();
        }

        Ok(writers
            .into_iter()
            .map(|(table_name, writer)| InsertExpr {
                table_name,
                expr: Some(Expr::Values(insert_expr::Values {
                    values: vec![writer.finish().into()],
                })),
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;

    use api::v1::{
        codec::InsertBatch,
        column::{SemanticType, Values},
        insert_expr::Expr,
        Column, ColumnDataType, InsertExpr,
    };
    use common_base::BitVec;

    use crate::influxdb::InfluxdbRequest;

    #[test]
    fn test_convert_influxdb_lines() {
        let lines = r"
monitor1,host=host1 cpu=66.6,memory=1024 1663840496100023100
monitor1,host=host2 memory=1027 1663840496400340001
monitor2,host=host3 cpu=66.5 1663840496100023102
monitor2,host=host4 cpu=66.3,memory=1029 1663840496400340003";

        let influxdb_req = &InfluxdbRequest {
            precision: None,
            lines: lines.to_string(),
        };

        let insert_exprs: Vec<InsertExpr> = influxdb_req.try_into().unwrap();

        assert_eq!(2, insert_exprs.len());

        for expr in insert_exprs {
            let values = match expr.expr.unwrap() {
                Expr::Values(vals) => vals,
                Expr::Sql(_) => panic!(),
            };
            let raw_batch = values.values.get(0).unwrap();
            let batch: InsertBatch = raw_batch.deref().try_into().unwrap();
            match &expr.table_name[..] {
                "monitor1" => assert_monitor_1(&batch),
                "monitor2" => assert_monitor_2(&batch),
                _ => panic!(),
            }
        }
    }

    fn assert_monitor_1(insert_batch: &InsertBatch) {
        let columns = &insert_batch.columns;
        assert_eq!(4, columns.len());
        verify_column(
            &columns[0],
            "host",
            ColumnDataType::String,
            SemanticType::Tag,
            Vec::new(),
            Values {
                string_values: vec!["host1".to_string(), "host2".to_string()],
                ..Default::default()
            },
        );

        verify_column(
            &columns[1],
            "cpu",
            ColumnDataType::Float64,
            SemanticType::Field,
            vec![false, true],
            Values {
                f64_values: vec![66.6],
                ..Default::default()
            },
        );

        verify_column(
            &columns[2],
            "memory",
            ColumnDataType::Float64,
            SemanticType::Field,
            Vec::new(),
            Values {
                f64_values: vec![1024.0, 1027.0],
                ..Default::default()
            },
        );

        verify_column(
            &columns[3],
            "ts",
            ColumnDataType::Timestamp,
            SemanticType::Timestamp,
            Vec::new(),
            Values {
                ts_millis_values: vec![1663840496100, 1663840496400],
                ..Default::default()
            },
        );
    }

    fn assert_monitor_2(insert_batch: &InsertBatch) {
        let columns = &insert_batch.columns;
        assert_eq!(4, columns.len());
        verify_column(
            &columns[0],
            "host",
            ColumnDataType::String,
            SemanticType::Tag,
            Vec::new(),
            Values {
                string_values: vec!["host3".to_string(), "host4".to_string()],
                ..Default::default()
            },
        );

        verify_column(
            &columns[1],
            "cpu",
            ColumnDataType::Float64,
            SemanticType::Field,
            Vec::new(),
            Values {
                f64_values: vec![66.5, 66.3],
                ..Default::default()
            },
        );

        verify_column(
            &columns[2],
            "ts",
            ColumnDataType::Timestamp,
            SemanticType::Timestamp,
            Vec::new(),
            Values {
                ts_millis_values: vec![1663840496100, 1663840496400],
                ..Default::default()
            },
        );

        verify_column(
            &columns[3],
            "memory",
            ColumnDataType::Float64,
            SemanticType::Field,
            vec![true, false],
            Values {
                f64_values: vec![1029.0],
                ..Default::default()
            },
        );
    }

    fn verify_column(
        column: &Column,
        name: &str,
        datatype: ColumnDataType,
        semantic_type: SemanticType,
        null_mask: Vec<bool>,
        vals: Values,
    ) {
        assert_eq!(name, column.column_name);
        assert_eq!(datatype as i32, column.datatype);
        assert_eq!(semantic_type as i32, column.semantic_type);
        verify_null_mask(&column.null_mask, null_mask);
        assert_eq!(Some(vals), column.values);
    }

    fn verify_null_mask(data: &[u8], expected: Vec<bool>) {
        let bitvec = BitVec::from_slice(data);
        for (idx, b) in expected.iter().enumerate() {
            assert_eq!(b, bitvec.get(idx).unwrap())
        }
    }
}
