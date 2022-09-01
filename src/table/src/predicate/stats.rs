use std::process::id;

use datafusion::parquet::metadata::RowGroupMetaData;
use datafusion::physical_optimizer::pruning::PruningStatistics;
use datafusion_common::field_util::SchemaExt;
use datafusion_common::Column;
use datatypes::arrow;
use datatypes::arrow::array::{ArrayRef, Int64Vec};
use datatypes::arrow::io::parquet::read::statistics::BooleanStatistics;
use datatypes::arrow::io::parquet::read::PhysicalType;
use datatypes::prelude::Vector;
use datatypes::vectors::{BooleanVector, Int64Vector};

pub struct RowGroupPruningStatistics<'a> {
    pub meta_data: &'a [RowGroupMetaData],
    pub schema: &'a arrow::datatypes::Schema,
}

impl<'a> PruningStatistics for RowGroupPruningStatistics<'a> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        todo!()
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        todo!()
    }

    fn num_containers(&self) -> usize {
        self.meta_data.len()
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let (idx, field) = if let Some(v) = self.schema.column_with_name(&column.name) {
            v
        } else {
            return None;
        };

        let mut values: Vec<Option<i64>> = Vec::with_capacity(self.meta_data.len());

        for m in self.meta_data {
            let col = m.column(idx);
            let stat = col.statistics().unwrap().unwrap();
            let bs = stat.null_count();
            values.push(bs);
        }

        Some(Int64Vector::from(values).to_arrow_array())
    }
}
