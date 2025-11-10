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

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, BinaryArray, LargeStringArray, StringArray};
use arrow_schema::{DataType, Field};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    Accumulator, AggregateUDF, Signature, SimpleAggregateUDF, TypeSignature, Volatility,
};
use datafusion_functions_aggregate_common::accumulator::AccumulatorArgs;
use nalgebra::{Const, DVectorView, Dyn, OVector};

use crate::scalars::vector::impl_conv::{
    binlit_as_veclit, parse_veclit_from_strlit, veclit_to_binlit,
};

/// The accumulator for the `vec_sum` aggregate function.
#[derive(Debug, Default)]
pub struct VectorSum {
    sum: Option<OVector<f32, Dyn>>,
    has_null: bool,
}

impl VectorSum {
    /// Create a new `AggregateUDF` for the `vec_sum` aggregate function.
    pub fn uadf_impl() -> AggregateUDF {
        let signature = Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Binary]),
            ],
            Volatility::Immutable,
        );
        let udaf = SimpleAggregateUDF::new_with_signature(
            "vec_sum",
            signature,
            DataType::Binary,
            Arc::new(Self::accumulator),
            vec![Arc::new(Field::new("x", DataType::Binary, true))],
        );
        AggregateUDF::from(udaf)
    }

    fn accumulator(args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if args.schema.fields().len() != 1 {
            return Err(datafusion_common::DataFusionError::Internal(format!(
                "expect creating `VEC_SUM` with only one input field, actual {}",
                args.schema.fields().len()
            )));
        }

        let t = args.schema.field(0).data_type();
        if !matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary) {
            return Err(datafusion_common::DataFusionError::Internal(format!(
                "unexpected input datatype {t} when creating `VEC_SUM`"
            )));
        }

        Ok(Box::new(VectorSum::default()))
    }

    fn inner(&mut self, len: usize) -> &mut OVector<f32, Dyn> {
        self.sum
            .get_or_insert_with(|| OVector::zeros_generic(Dyn(len), Const::<1>))
    }

    fn update(&mut self, values: &[ArrayRef], is_update: bool) -> Result<()> {
        if values.is_empty() || self.has_null {
            return Ok(());
        };

        match values[0].data_type() {
            DataType::Utf8 => {
                let arr: &StringArray = values[0].as_string();
                for s in arr.iter() {
                    let Some(s) = s else {
                        if is_update {
                            self.has_null = true;
                            self.sum = None;
                        }
                        return Ok(());
                    };
                    let values = parse_veclit_from_strlit(s)?;
                    let vec_column = DVectorView::from_slice(&values, values.len());
                    *self.inner(vec_column.len()) += vec_column;
                }
            }
            DataType::LargeUtf8 => {
                let arr: &LargeStringArray = values[0].as_string();
                for s in arr.iter() {
                    let Some(s) = s else {
                        if is_update {
                            self.has_null = true;
                            self.sum = None;
                        }
                        return Ok(());
                    };
                    let values = parse_veclit_from_strlit(s)?;
                    let vec_column = DVectorView::from_slice(&values, values.len());
                    *self.inner(vec_column.len()) += vec_column;
                }
            }
            DataType::Binary => {
                let arr: &BinaryArray = values[0].as_binary();
                for b in arr.iter() {
                    let Some(b) = b else {
                        if is_update {
                            self.has_null = true;
                            self.sum = None;
                        }
                        return Ok(());
                    };
                    let values = binlit_as_veclit(b)?;
                    let vec_column = DVectorView::from_slice(&values, values.len());
                    *self.inner(vec_column.len()) += vec_column;
                }
            }
            _ => {
                return Err(datafusion_common::DataFusionError::NotImplemented(format!(
                    "unsupported data type {} for `VEC_SUM`",
                    values[0].data_type()
                )));
            }
        }
        Ok(())
    }
}

impl Accumulator for VectorSum {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.evaluate().map(|v| vec![v])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.update(values, true)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update(states, false)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        match &self.sum {
            None => Ok(ScalarValue::Binary(None)),
            Some(vector) => Ok(ScalarValue::Binary(Some(veclit_to_binlit(
                vector.as_slice(),
            )))),
        }
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::StringArray;
    use datatypes::scalars::ScalarVector;
    use datatypes::vectors::{ConstantVector, StringVector, Vector};

    use super::*;

    #[test]
    fn test_update_batch() {
        // test update empty batch, expect not updating anything
        let mut vec_sum = VectorSum::default();
        vec_sum.update_batch(&[]).unwrap();
        assert!(vec_sum.sum.is_none());
        assert!(!vec_sum.has_null);
        assert_eq!(ScalarValue::Binary(None), vec_sum.evaluate().unwrap());

        // test update one not-null value
        let mut vec_sum = VectorSum::default();
        let v: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec![Some(
            "[1.0,2.0,3.0]".to_string(),
        )]))];
        vec_sum.update_batch(&v).unwrap();
        assert_eq!(
            ScalarValue::Binary(Some(veclit_to_binlit(&[1.0, 2.0, 3.0]))),
            vec_sum.evaluate().unwrap()
        );

        // test update one null value
        let mut vec_sum = VectorSum::default();
        let v: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec![Option::<String>::None]))];
        vec_sum.update_batch(&v).unwrap();
        assert_eq!(ScalarValue::Binary(None), vec_sum.evaluate().unwrap());

        // test update no null-value batch
        let mut vec_sum = VectorSum::default();
        let v: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec![
            Some("[1.0,2.0,3.0]".to_string()),
            Some("[4.0,5.0,6.0]".to_string()),
            Some("[7.0,8.0,9.0]".to_string()),
        ]))];
        vec_sum.update_batch(&v).unwrap();
        assert_eq!(
            ScalarValue::Binary(Some(veclit_to_binlit(&[12.0, 15.0, 18.0]))),
            vec_sum.evaluate().unwrap()
        );

        // test update null-value batch
        let mut vec_sum = VectorSum::default();
        let v: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec![
            Some("[1.0,2.0,3.0]".to_string()),
            None,
            Some("[7.0,8.0,9.0]".to_string()),
        ]))];
        vec_sum.update_batch(&v).unwrap();
        assert_eq!(ScalarValue::Binary(None), vec_sum.evaluate().unwrap());

        // test update with constant vector
        let mut vec_sum = VectorSum::default();
        let v: Vec<ArrayRef> = vec![
            Arc::new(ConstantVector::new(
                Arc::new(StringVector::from_vec(vec!["[1.0,2.0,3.0]".to_string()])),
                4,
            ))
            .to_arrow_array(),
        ];
        vec_sum.update_batch(&v).unwrap();
        assert_eq!(
            ScalarValue::Binary(Some(veclit_to_binlit(&[4.0, 8.0, 12.0]))),
            vec_sum.evaluate().unwrap()
        );
    }
}
