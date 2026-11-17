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

use std::collections::HashMap;
use std::sync::Arc;

use api::helper;
use api::v1::{ColumnSchema, Row, Rows};
use datatypes::arrow::datatypes::{Field, Schema};
use datatypes::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datatypes::data_type::{ConcreteDataType, DataType};
use datatypes::value::Value;
use snafu::{OptionExt, ResultExt};
use store_api::metric_engine_consts::DATA_SCHEMA_TABLE_ID_COLUMN_NAME;
use store_api::storage::RegionNumber;

use crate::PartitionRuleRef;
use crate::error::{self, Result};
use crate::partition::PartitionEvalContext;

pub struct RowSplitter {
    partition_rule: PartitionRuleRef,
}

impl RowSplitter {
    pub fn new(partition_rule: PartitionRuleRef) -> Self {
        Self { partition_rule }
    }

    pub fn split(&self, rows: Rows) -> Result<HashMap<RegionNumber, Rows>> {
        // No data
        if rows.rows.is_empty() {
            return Ok(HashMap::new());
        }

        // No partition
        let partition_columns = self.partition_rule.partition_columns();
        if partition_columns.is_empty() {
            return Ok(HashMap::from([(0, rows)]));
        }

        let splitter = SplitReadRowHelper::new(rows, &self.partition_rule);
        splitter.split_rows()
    }
}

struct SplitReadRowHelper<'a> {
    schema: Vec<ColumnSchema>,
    rows: Vec<Row>,
    partition_rule: &'a PartitionRuleRef,
    // Map from partition column name to index in the schema/row.
    partition_cols_indexes: Vec<Option<usize>>,
}

impl<'a> SplitReadRowHelper<'a> {
    fn new(rows: Rows, partition_rule: &'a PartitionRuleRef) -> Self {
        let col_name_to_idx = rows
            .schema
            .iter()
            .enumerate()
            .map(|(idx, col)| (&col.column_name, idx))
            .collect::<HashMap<_, _>>();
        let partition_cols = partition_rule.partition_columns();
        let partition_cols_indexes = partition_cols
            .iter()
            .map(|col_name| col_name_to_idx.get(&col_name).cloned())
            .collect::<Vec<_>>();

        Self {
            schema: rows.schema,
            rows: rows.rows,
            partition_rule,
            partition_cols_indexes,
        }
    }

    fn split_rows(mut self) -> Result<HashMap<RegionNumber, Rows>> {
        let regions = self.split_to_regions()?;
        if regions.len() == 1
            && let Some((&region, indices)) = regions.iter().next()
            && indices.len() == self.rows.len()
        {
            return Ok(HashMap::from([(
                region,
                Rows {
                    schema: self.schema,
                    rows: self.rows,
                },
            )]));
        }
        let request_splits = regions
            .into_iter()
            .map(|(region_number, row_indexes)| {
                let rows = row_indexes
                    .into_iter()
                    .map(|row_idx| std::mem::take(&mut self.rows[row_idx]))
                    .collect();
                let rows = Rows {
                    schema: self.schema.clone(),
                    rows,
                };
                (region_number, rows)
            })
            .collect::<HashMap<_, _>>();

        Ok(request_splits)
    }

    fn split_to_regions(&self) -> Result<HashMap<RegionNumber, Vec<usize>>> {
        if matches!(
            self.partition_rule.eval_context(),
            PartitionEvalContext::LogicalTable { .. }
        ) {
            let batch = self.routing_batch()?;
            return Ok(self
                .partition_rule
                .split_record_batch(&batch)?
                .into_iter()
                .filter(|(_, mask)| !mask.select_none())
                .map(|(region, mask)| (region, mask.array().values().set_indices().collect()))
                .collect());
        }
        let mut regions_row_indexes: HashMap<RegionNumber, Vec<usize>> = HashMap::new();
        for (row_idx, values) in self.iter_partition_values().enumerate() {
            let region_number = self.partition_rule.find_region(&values)?;
            regions_row_indexes
                .entry(region_number)
                .or_default()
                .push(row_idx);
        }

        Ok(regions_row_indexes)
    }

    /// Converts present payload partition columns; the evaluator supplies missing inputs.
    fn routing_batch(&self) -> Result<RecordBatch> {
        let mut fields = Vec::new();
        let mut arrays = Vec::new();
        for (name, index) in self
            .partition_rule
            .partition_columns()
            .iter()
            .zip(&self.partition_cols_indexes)
        {
            if name == DATA_SCHEMA_TABLE_ID_COLUMN_NAME {
                continue;
            }
            let Some(index) = *index else {
                continue;
            };
            let column = &self.schema[index];
            let data_type = helper::ColumnDataTypeWrapper::try_new(
                column.datatype,
                column.datatype_extension.clone(),
            )
            .map_err(|err| {
                error::InvalidPartitionInputSnafu {
                    reason: err.to_string(),
                }
                .build()
            })?;
            let mut builder =
                ConcreteDataType::from(data_type).create_mutable_vector(self.rows.len());
            for row in &self.rows {
                let value =
                    row.values
                        .get(index)
                        .with_context(|| error::InvalidPartitionInputSnafu {
                            reason: format!("Missing value for partition column {name}"),
                        })?;
                builder
                    .try_push_value_ref(&helper::pb_value_to_value_ref(
                        value,
                        column.datatype_extension.as_ref(),
                    ))
                    .context(error::ConvertToVectorSnafu)?;
            }
            let array = builder.to_vector().to_arrow_array();
            fields.push(Field::new(name, array.data_type().clone(), true));
            arrays.push(array);
        }
        RecordBatch::try_new_with_options(
            Arc::new(Schema::new(fields)),
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(self.rows.len())),
        )
        .context(error::ComputeArrowKernelSnafu)
    }

    fn iter_partition_values(&'a self) -> impl Iterator<Item = Vec<Value>> + 'a {
        self.rows.iter().map(|row| {
            self.partition_cols_indexes
                .iter()
                .map(|idx| {
                    idx.as_ref().map_or(Value::Null, |idx| {
                        helper::pb_value_to_value_ref(
                            &row.values[*idx],
                            self.schema[*idx].datatype_extension.as_ref(),
                        )
                        .into()
                    })
                })
                .collect()
        })
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::Arc;

    use api::v1::ColumnDataType;
    use api::v1::helper::{field_column_schema, tag_column_schema};
    use api::v1::value::ValueData;
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::PartitionRule;
    use crate::partition::RegionMask;

    fn mock_rows() -> Rows {
        let schema = vec![
            tag_column_schema("id", ColumnDataType::String),
            tag_column_schema("name", ColumnDataType::String),
            field_column_schema("age", ColumnDataType::Uint32),
        ];
        let rows = vec![
            Row {
                values: vec![
                    ValueData::StringValue("1".to_string()).into(),
                    ValueData::StringValue("Smith".to_string()).into(),
                    ValueData::U32Value(20).into(),
                ],
            },
            Row {
                values: vec![
                    ValueData::StringValue("2".to_string()).into(),
                    ValueData::StringValue("Johnson".to_string()).into(),
                    ValueData::U32Value(21).into(),
                ],
            },
            Row {
                values: vec![
                    ValueData::StringValue("3".to_string()).into(),
                    ValueData::StringValue("Williams".to_string()).into(),
                    ValueData::U32Value(22).into(),
                ],
            },
        ];
        Rows { schema, rows }
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct MockPartitionRule {
        partition_columns: Vec<String>,
    }

    impl Default for MockPartitionRule {
        fn default() -> Self {
            Self {
                partition_columns: vec!["id".to_string()],
            }
        }
    }

    impl PartitionRule for MockPartitionRule {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn partition_columns(&self) -> &[String] {
            &self.partition_columns
        }

        fn find_region(&self, values: &[Value]) -> Result<RegionNumber> {
            let val = values.first().unwrap().clone();
            let val = match val {
                Value::String(v) => v.as_utf8().to_string(),
                _ => unreachable!(),
            };

            Ok(val.parse::<u32>().unwrap() % 2)
        }

        fn split_record_batch(
            &self,
            _record_batch: &datatypes::arrow::array::RecordBatch,
        ) -> Result<HashMap<RegionNumber, RegionMask>> {
            unimplemented!()
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct MockMissedColPartitionRule {
        partition_columns: Vec<String>,
    }

    impl Default for MockMissedColPartitionRule {
        fn default() -> Self {
            Self {
                partition_columns: vec!["missed_col".to_string()],
            }
        }
    }

    impl PartitionRule for MockMissedColPartitionRule {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn partition_columns(&self) -> &[String] {
            &self.partition_columns
        }

        fn find_region(&self, values: &[Value]) -> Result<RegionNumber> {
            let val = values.first().unwrap().clone();
            let val = match val {
                Value::Null => 1,
                _ => 0,
            };

            Ok(val)
        }

        fn split_record_batch(
            &self,
            _record_batch: &datatypes::arrow::array::RecordBatch,
        ) -> Result<HashMap<RegionNumber, RegionMask>> {
            unimplemented!()
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct EmptyPartitionRule;

    impl PartitionRule for EmptyPartitionRule {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn partition_columns(&self) -> &[String] {
            &[]
        }

        fn find_region(&self, _values: &[Value]) -> Result<RegionNumber> {
            Ok(0)
        }

        fn split_record_batch(
            &self,
            _record_batch: &datatypes::arrow::array::RecordBatch,
        ) -> Result<HashMap<RegionNumber, RegionMask>> {
            unimplemented!()
        }
    }
    #[test]
    fn test_invalid_logical_partition_input() {
        use common_error::ext::ErrorExt;
        use common_error::status_code::StatusCode;

        let rule = crate::multi_dim::MultiDimPartitionRule::try_new(
            vec!["a".to_string()],
            vec![0],
            vec![crate::expr::col("a").gt_eq(Value::Int32(0))],
            false,
        )
        .unwrap()
        .with_eval_context(PartitionEvalContext::LogicalTable { table_id: 1025 });
        let splitter = RowSplitter::new(Arc::new(rule));
        for (datatype, values) in [
            (-1, vec![ValueData::I32Value(1).into()]),
            (ColumnDataType::Int32 as i32, vec![]),
        ] {
            let mut column = tag_column_schema("a", ColumnDataType::Int32);
            column.datatype = datatype;
            let error = splitter
                .split(Rows {
                    schema: vec![column],
                    rows: vec![api::v1::Row { values }],
                })
                .unwrap_err();
            assert!(matches!(error, error::Error::InvalidPartitionInput { .. }));
            assert_eq!(error.status_code(), StatusCode::InvalidArguments);
        }
    }

    #[test]
    fn test_writer_splitter() {
        let rows = mock_rows();
        let rule = Arc::new(MockPartitionRule::default()) as PartitionRuleRef;
        let splitter = RowSplitter::new(rule);

        let mut splits = splitter.split(rows).unwrap();
        assert_eq!(splits.len(), 2);

        let rows0 = splits.remove(&0).unwrap().rows;
        let rows1 = splits.remove(&1).unwrap().rows;
        assert_eq!(rows0.len(), 1);
        assert_eq!(rows1.len(), 2);
    }

    #[test]
    fn test_missed_col_writer_splitter() {
        let rows = mock_rows();
        let rule = Arc::new(MockMissedColPartitionRule::default()) as PartitionRuleRef;

        let splitter = RowSplitter::new(rule);
        let mut splits = splitter.split(rows).unwrap();
        assert_eq!(splits.len(), 1);

        let rows = splits.remove(&1).unwrap().rows;
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn test_empty_partition_rule_writer_splitter() {
        let rows = mock_rows();
        let rule = Arc::new(EmptyPartitionRule) as PartitionRuleRef;
        let splitter = RowSplitter::new(rule);

        let mut splits = splitter.split(rows).unwrap();
        assert_eq!(splits.len(), 1);

        let rows = splits.remove(&0).unwrap().rows;
        assert_eq!(rows.len(), 3);
    }
}
