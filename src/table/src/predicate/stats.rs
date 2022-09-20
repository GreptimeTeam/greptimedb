use std::collections::HashMap;

use datafusion::parquet::metadata::RowGroupMetaData;
use datafusion::parquet::statistics::{
    BinaryStatistics, BooleanStatistics, FixedLenStatistics, PrimitiveStatistics,
};
use datafusion::physical_optimizer::pruning::PruningStatistics;
use datafusion_common::{Column, ScalarValue};
use datatypes::arrow;
use datatypes::arrow::array::ArrayRef;
use datatypes::arrow::datatypes::DataType;
use datatypes::arrow::io::parquet::read::PhysicalType;
use datatypes::prelude::Vector;
use datatypes::vectors::Int64Vector;
use paste::paste;

pub struct RowGroupPruningStatistics<'a> {
    pub meta_data: &'a [RowGroupMetaData],
    pub schema: &'a arrow::datatypes::Schema,
    name_to_index: HashMap<String, (usize, DataType)>,
}

impl<'a> RowGroupPruningStatistics<'a> {
    pub fn new(meta_data: &'a [RowGroupMetaData], schema: &'a arrow::datatypes::Schema) -> Self {
        let name_to_index = schema
            .fields
            .iter()
            .enumerate()
            .map(|(idx, f)| (f.name.clone(), (idx, f.data_type.clone())))
            .collect::<HashMap<_, _>>();
        Self {
            meta_data,
            schema,
            name_to_index,
        }
    }
}

macro_rules! impl_min_max_values {
    ($self:ident, $col:ident, $min_max: ident) => {
        paste! {
            {
                let (column_index, data_type) = $self.name_to_index.get(&$col.name)?;
                let null_scalar: ScalarValue = data_type.try_into().ok()?;
                let scalar_values: Vec<ScalarValue> = $self
                    .meta_data
                    .iter()
                    .flat_map(|meta| meta.column(*column_index).statistics())
                    .map(|stats| {
                        let stats = stats.ok()?;
                        let res = match stats.physical_type() {
                            PhysicalType::Boolean => {
                                let $min_max = stats.as_any().downcast_ref::<BooleanStatistics>().unwrap().[<$min_max _value>];
                                Some(ScalarValue::Boolean($min_max))
                            }
                            PhysicalType::Int32 => {
                                let $min_max = stats
                                    .as_any()
                                    .downcast_ref::<PrimitiveStatistics<i32>>()
                                    .unwrap()
                                    .[<$min_max _value>];
                                Some(ScalarValue::Int32($min_max))
                            }
                            PhysicalType::Int64 => {
                                let $min_max = stats
                                    .as_any()
                                    .downcast_ref::<PrimitiveStatistics<i64>>()
                                    .unwrap()
                                    .[<$min_max _value>];
                                Some(ScalarValue::Int64($min_max))
                            }
                            PhysicalType::Int96 => {
                                // INT96 currently not supported
                                None
                            }
                            PhysicalType::Float => {
                                let $min_max = stats
                                    .as_any()
                                    .downcast_ref::<PrimitiveStatistics<f32>>()
                                    .unwrap()
                                    .[<$min_max _value>];
                                Some(ScalarValue::Float32($min_max))
                            }
                            PhysicalType::Double => {
                                let $min_max = stats
                                    .as_any()
                                    .downcast_ref::<PrimitiveStatistics<f64>>()
                                    .unwrap()
                                    .[<$min_max _value>];
                                Some(ScalarValue::Float64($min_max))
                            }
                            PhysicalType::ByteArray => {
                                let $min_max = stats
                                    .as_any()
                                    .downcast_ref::<BinaryStatistics>()
                                    .unwrap()
                                    .[<$min_max _value>]
                                    .clone();
                                Some(ScalarValue::Binary($min_max))
                            }
                            PhysicalType::FixedLenByteArray(_) => {
                                let $min_max = stats
                                    .as_any()
                                    .downcast_ref::<FixedLenStatistics>()
                                    .unwrap()
                                    .[<$min_max _value>]
                                    .clone();
                                Some(ScalarValue::Binary($min_max))
                            }
                        };

                        res
                    })
                    .map(|maybe_scalar| maybe_scalar.unwrap_or_else(|| null_scalar.clone()))
                    .collect::<Vec<_>>();
                ScalarValue::iter_to_array(scalar_values).ok()
        }
    }
    };
}

impl<'a> PruningStatistics for RowGroupPruningStatistics<'a> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        impl_min_max_values!(self, column, min)
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        impl_min_max_values!(self, column, max)
    }

    fn num_containers(&self) -> usize {
        self.meta_data.len()
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let (idx, _) = self.name_to_index.get(&column.name)?;
        let mut values: Vec<Option<i64>> = Vec::with_capacity(self.meta_data.len());
        for m in self.meta_data {
            let col = m.column(*idx);
            let stat = col.statistics()?.ok()?;
            let bs = stat.null_count();
            values.push(bs);
        }

        Some(Int64Vector::from(values).to_arrow_array())
    }
}
