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

use std::borrow::Cow;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, BinaryArray, LargeStringArray, StringArray};
use arrow::compute::sum;
use arrow::datatypes::UInt64Type;
use arrow_schema::{DataType, Field};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    Accumulator, AggregateUDF, Signature, SimpleAggregateUDF, TypeSignature, Volatility,
};
use datafusion_functions_aggregate_common::accumulator::AccumulatorArgs;
use nalgebra::{Const, DVector, DVectorView, Dyn, OVector};

use crate::scalars::vector::impl_conv::{
    binlit_as_veclit, parse_veclit_from_strlit, veclit_to_binlit,
};

/// The accumulator for the `vec_avg` aggregate function.
#[derive(Debug, Default)]
pub struct VectorAvg {
    sum: Option<OVector<f32, Dyn>>,
    count: u64,
}

impl VectorAvg {
    /// Create a new `AggregateUDF` for the `vec_avg` aggregate function.
    pub fn uadf_impl() -> AggregateUDF {
        let signature = Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::LargeUtf8]),
                TypeSignature::Exact(vec![DataType::Binary]),
            ],
            Volatility::Immutable,
        );
        let udaf = SimpleAggregateUDF::new_with_signature(
            "vec_avg",
            signature,
            DataType::Binary,
            Arc::new(Self::accumulator),
            vec![
                Arc::new(Field::new("sum", DataType::Binary, true)),
                Arc::new(Field::new("count", DataType::UInt64, true)),
            ],
        );
        AggregateUDF::from(udaf)
    }

    fn accumulator(args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if args.schema.fields().len() != 1 {
            return Err(datafusion_common::DataFusionError::Internal(format!(
                "expect creating `VEC_AVG` with only one input field, actual {}",
                args.schema.fields().len()
            )));
        }

        let t = args.schema.field(0).data_type();
        if !matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary) {
            return Err(datafusion_common::DataFusionError::Internal(format!(
                "unexpected input datatype {t} when creating `VEC_AVG`"
            )));
        }

        Ok(Box::new(VectorAvg::default()))
    }

    fn inner(&mut self, len: usize) -> &mut OVector<f32, Dyn> {
        self.sum
            .get_or_insert_with(|| OVector::zeros_generic(Dyn(len), Const::<1>))
    }

    fn update(&mut self, values: &[ArrayRef], is_update: bool) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        };

        let vectors = match values[0].data_type() {
            DataType::Utf8 => {
                let arr: &StringArray = values[0].as_string();
                arr.iter()
                    .filter_map(|x| x.map(|s| parse_veclit_from_strlit(s).map_err(Into::into)))
                    .map(|x| x.map(Cow::Owned))
                    .collect::<Result<Vec<_>>>()?
            }
            DataType::LargeUtf8 => {
                let arr: &LargeStringArray = values[0].as_string();
                arr.iter()
                    .filter_map(|x| x.map(|s| parse_veclit_from_strlit(s).map_err(Into::into)))
                    .map(|x: Result<Vec<f32>>| x.map(Cow::Owned))
                    .collect::<Result<Vec<_>>>()?
            }
            DataType::Binary => {
                let arr: &BinaryArray = values[0].as_binary();
                arr.iter()
                    .filter_map(|x| x.map(|b| binlit_as_veclit(b).map_err(Into::into)))
                    .collect::<Result<Vec<_>>>()?
            }
            _ => {
                return Err(datafusion_common::DataFusionError::NotImplemented(format!(
                    "unsupported data type {} for `VEC_AVG`",
                    values[0].data_type()
                )));
            }
        };

        if vectors.is_empty() {
            return Ok(());
        }

        let len = if is_update {
            vectors.len() as u64
        } else {
            sum(values[1].as_primitive::<UInt64Type>()).unwrap_or_default()
        };

        let dims = vectors[0].len();
        let mut sum = DVector::zeros(dims);
        for v in vectors {
            if v.len() != dims {
                return Err(datafusion_common::DataFusionError::Execution(
                    "vectors length not match: VEC_AVG".to_string(),
                ));
            }
            let v_view = DVectorView::from_slice(&v, dims);
            sum += &v_view;
        }

        *self.inner(dims) += sum;
        self.count += len;

        Ok(())
    }
}

impl Accumulator for VectorAvg {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let vector = match &self.sum {
            None => ScalarValue::Binary(None),
            Some(sum) => ScalarValue::Binary(Some(veclit_to_binlit(sum.as_slice()))),
        };
        Ok(vec![vector, ScalarValue::from(self.count)])
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
            Some(sum) => Ok(ScalarValue::Binary(Some(veclit_to_binlit(
                (sum / self.count as f32).as_slice(),
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
        let mut vec_avg = VectorAvg::default();
        vec_avg.update_batch(&[]).unwrap();
        assert!(vec_avg.sum.is_none());
        assert_eq!(ScalarValue::Binary(None), vec_avg.evaluate().unwrap());

        // test update one not-null value
        let mut vec_avg = VectorAvg::default();
        let v: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec![
            Some("[1.0,2.0,3.0]".to_string()),
            Some("[4.0,5.0,6.0]".to_string()),
        ]))];
        vec_avg.update_batch(&v).unwrap();
        assert_eq!(
            ScalarValue::Binary(Some(veclit_to_binlit(&[2.5, 3.5, 4.5]))),
            vec_avg.evaluate().unwrap()
        );

        // test update one null value
        let mut vec_avg = VectorAvg::default();
        let v: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec![Option::<String>::None]))];
        vec_avg.update_batch(&v).unwrap();
        assert_eq!(ScalarValue::Binary(None), vec_avg.evaluate().unwrap());

        // test update no null-value batch
        let mut vec_avg = VectorAvg::default();
        let v: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec![
            Some("[1.0,2.0,3.0]".to_string()),
            Some("[4.0,5.0,6.0]".to_string()),
            Some("[7.0,8.0,9.0]".to_string()),
        ]))];
        vec_avg.update_batch(&v).unwrap();
        assert_eq!(
            ScalarValue::Binary(Some(veclit_to_binlit(&[4.0, 5.0, 6.0]))),
            vec_avg.evaluate().unwrap()
        );

        // test update null-value batch
        let mut vec_avg = VectorAvg::default();
        let v: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec![
            Some("[1.0,2.0,3.0]".to_string()),
            None,
            Some("[7.0,8.0,9.0]".to_string()),
        ]))];
        vec_avg.update_batch(&v).unwrap();
        assert_eq!(
            ScalarValue::Binary(Some(veclit_to_binlit(&[4.0, 5.0, 6.0]))),
            vec_avg.evaluate().unwrap()
        );

        let mut vec_avg = VectorAvg::default();
        let v: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec![
            None,
            Some("[4.0,5.0,6.0]".to_string()),
            Some("[7.0,8.0,9.0]".to_string()),
        ]))];
        vec_avg.update_batch(&v).unwrap();
        assert_eq!(
            ScalarValue::Binary(Some(veclit_to_binlit(&[5.5, 6.5, 7.5]))),
            vec_avg.evaluate().unwrap()
        );

        // test update with constant vector
        let mut vec_avg = VectorAvg::default();
        let v: Vec<ArrayRef> = vec![
            Arc::new(ConstantVector::new(
                Arc::new(StringVector::from_vec(vec!["[1.0,2.0,3.0]".to_string()])),
                4,
            ))
            .to_arrow_array(),
        ];
        vec_avg.update_batch(&v).unwrap();
        assert_eq!(
            ScalarValue::Binary(Some(veclit_to_binlit(&[1.0, 2.0, 3.0]))),
            vec_avg.evaluate().unwrap()
        );
    }
}
