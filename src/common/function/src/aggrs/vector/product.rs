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
use arrow_schema::{DataType, Field};
use datafusion::logical_expr::{Signature, TypeSignature, Volatility};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{Accumulator, AggregateUDF, SimpleAggregateUDF};
use datafusion_functions_aggregate_common::accumulator::AccumulatorArgs;
use nalgebra::{Const, DVectorView, Dyn, OVector};

use crate::scalars::vector::impl_conv::{
    binlit_as_veclit, parse_veclit_from_strlit, veclit_to_binlit,
};

/// Aggregates by multiplying elements across the same dimension, returns a vector.
#[derive(Debug, Default)]
pub struct VectorProduct {
    product: Option<OVector<f32, Dyn>>,
    has_null: bool,
}

impl VectorProduct {
    /// Create a new `AggregateUDF` for the `vec_product` aggregate function.
    pub fn uadf_impl() -> AggregateUDF {
        let signature = Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Binary]),
            ],
            Volatility::Immutable,
        );
        let udaf = SimpleAggregateUDF::new_with_signature(
            "vec_product",
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
                "expect creating `VEC_PRODUCT` with only one input field, actual {}",
                args.schema.fields().len()
            )));
        }

        let t = args.schema.field(0).data_type();
        if !matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary) {
            return Err(datafusion_common::DataFusionError::Internal(format!(
                "unexpected input datatype {t} when creating `VEC_PRODUCT`"
            )));
        }

        Ok(Box::new(VectorProduct::default()))
    }

    fn inner(&mut self, len: usize) -> &mut OVector<f32, Dyn> {
        self.product.get_or_insert_with(|| {
            OVector::from_iterator_generic(Dyn(len), Const::<1>, (0..len).map(|_| 1.0))
        })
    }

    fn update(&mut self, values: &[ArrayRef], is_update: bool) -> Result<()> {
        if values.is_empty() || self.has_null {
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
                    "unsupported data type {} for `VEC_PRODUCT`",
                    values[0].data_type()
                )));
            }
        };
        if vectors.len() != values[0].len() {
            if is_update {
                self.has_null = true;
                self.product = None;
            }
            return Ok(());
        }

        vectors.iter().for_each(|v| {
            let v = DVectorView::from_slice(v, v.len());
            let inner = self.inner(v.len());
            *inner = inner.component_mul(&v);
        });
        Ok(())
    }
}

impl Accumulator for VectorProduct {
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
        match &self.product {
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

    use datatypes::scalars::ScalarVector;
    use datatypes::vectors::{ConstantVector, StringVector, Vector};

    use super::*;

    #[test]
    fn test_update_batch() {
        // test update empty batch, expect not updating anything
        let mut vec_product = VectorProduct::default();
        vec_product.update_batch(&[]).unwrap();
        assert!(vec_product.product.is_none());
        assert!(!vec_product.has_null);
        assert_eq!(ScalarValue::Binary(None), vec_product.evaluate().unwrap());

        // test update one not-null value
        let mut vec_product = VectorProduct::default();
        let v: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec![Some(
            "[1.0,2.0,3.0]".to_string(),
        )]))];
        vec_product.update_batch(&v).unwrap();
        assert_eq!(
            ScalarValue::Binary(Some(veclit_to_binlit(&[1.0, 2.0, 3.0]))),
            vec_product.evaluate().unwrap()
        );

        // test update one null value
        let mut vec_product = VectorProduct::default();
        let v: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec![Option::<String>::None]))];
        vec_product.update_batch(&v).unwrap();
        assert_eq!(ScalarValue::Binary(None), vec_product.evaluate().unwrap());

        // test update no null-value batch
        let mut vec_product = VectorProduct::default();
        let v: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec![
            Some("[1.0,2.0,3.0]".to_string()),
            Some("[4.0,5.0,6.0]".to_string()),
            Some("[7.0,8.0,9.0]".to_string()),
        ]))];
        vec_product.update_batch(&v).unwrap();
        assert_eq!(
            ScalarValue::Binary(Some(veclit_to_binlit(&[28.0, 80.0, 162.0]))),
            vec_product.evaluate().unwrap()
        );

        // test update null-value batch
        let mut vec_product = VectorProduct::default();
        let v: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec![
            Some("[1.0,2.0,3.0]".to_string()),
            None,
            Some("[7.0,8.0,9.0]".to_string()),
        ]))];
        vec_product.update_batch(&v).unwrap();
        assert_eq!(ScalarValue::Binary(None), vec_product.evaluate().unwrap());

        // test update with constant vector
        let mut vec_product = VectorProduct::default();
        let v: Vec<ArrayRef> = vec![
            Arc::new(ConstantVector::new(
                Arc::new(StringVector::from_vec(vec!["[1.0,2.0,3.0]".to_string()])),
                4,
            ))
            .to_arrow_array(),
        ];

        vec_product.update_batch(&v).unwrap();

        assert_eq!(
            ScalarValue::Binary(Some(veclit_to_binlit(&[1.0, 16.0, 81.0]))),
            vec_product.evaluate().unwrap()
        );
    }
}
