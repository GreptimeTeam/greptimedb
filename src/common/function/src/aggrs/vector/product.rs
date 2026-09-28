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
///
/// The result is NULL if any input vector is NULL, so the partial state carries a
/// `has_null` flag: a NULL `product` alone can't tell a NULL input from an empty partition.
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
            vec![
                Arc::new(Field::new("product", DataType::Binary, true)),
                Arc::new(Field::new("has_null", DataType::Boolean, true)),
            ],
        );
        AggregateUDF::from(udaf)
    }

    fn accumulator(args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if args.exprs.len() != 1 {
            return Err(datafusion_common::DataFusionError::Internal(format!(
                "expect creating `VEC_PRODUCT` with only one input field, actual {}",
                args.exprs.len()
            )));
        }

        let t = args.expr_fields[0].data_type();
        if !matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary) {
            return Err(datafusion_common::DataFusionError::Internal(format!(
                "unexpected input datatype {t} when creating `VEC_PRODUCT`"
            )));
        }

        Ok(Box::new(VectorProduct::default()))
    }

    fn mul(&mut self, vector: &[f32]) {
        let vector = DVectorView::from_slice(vector, vector.len());
        let product = self.product.get_or_insert_with(|| {
            OVector::from_iterator_generic(
                Dyn(vector.len()),
                Const::<1>,
                (0..vector.len()).map(|_| 1.0),
            )
        });
        *product = product.component_mul(&vector);
    }

    fn set_null(&mut self) {
        self.has_null = true;
        self.product = None;
    }
}

impl Accumulator for VectorProduct {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            self.evaluate()?,
            ScalarValue::Boolean(Some(self.has_null)),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
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
            self.set_null();
            return Ok(());
        }

        vectors.iter().for_each(|v| self.mul(v));
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let [products, has_nulls] = states else {
            return Err(datafusion_common::DataFusionError::Internal(format!(
                "expect 2 states for `VEC_PRODUCT`, actual {}",
                states.len()
            )));
        };
        if self.has_null {
            return Ok(());
        }
        if has_nulls.as_boolean().true_count() > 0 {
            self.set_null();
            return Ok(());
        }

        // A NULL product without `has_null` comes from a partition without input rows.
        for b in products.as_binary::<i32>().iter().flatten() {
            self.mul(&binlit_as_veclit(b)?);
        }
        Ok(())
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

        // test update with repeated values
        let mut vec_product = VectorProduct::default();
        let v = vec![
            ScalarValue::Utf8(Some("[1.0,2.0,3.0]".to_string()))
                .to_array_of_size(4)
                .unwrap(),
        ];

        vec_product.update_batch(&v).unwrap();

        assert_eq!(
            ScalarValue::Binary(Some(veclit_to_binlit(&[1.0, 16.0, 81.0]))),
            vec_product.evaluate().unwrap()
        );
    }

    #[test]
    fn test_merge_batch() {
        let partial = |v: Option<&str>| {
            let mut acc = VectorProduct::default();
            let v: ArrayRef = Arc::new(StringArray::from(vec![v]));
            acc.update_batch(&[v]).unwrap();
            acc.state().unwrap()
        };
        let states = |states: Vec<Vec<ScalarValue>>| -> Vec<ArrayRef> {
            (0..2)
                .map(|i| ScalarValue::iter_to_array(states.iter().map(|s| s[i].clone())).unwrap())
                .collect()
        };

        // An empty partition in the middle of the batch must not stop the merge.
        let mut merged = VectorProduct::default();
        merged
            .merge_batch(&states(vec![
                partial(Some("[1.0,2.0]")),
                VectorProduct::default().state().unwrap(),
                partial(Some("[3.0,4.0]")),
            ]))
            .unwrap();
        assert_eq!(
            ScalarValue::Binary(Some(veclit_to_binlit(&[3.0, 8.0]))),
            merged.evaluate().unwrap()
        );

        // A NULL input in any partition makes the result NULL.
        merged
            .merge_batch(&states(vec![partial(Some("[1.0,2.0]")), partial(None)]))
            .unwrap();
        merged
            .merge_batch(&states(vec![partial(Some("[3.0,4.0]"))]))
            .unwrap();
        assert_eq!(ScalarValue::Binary(None), merged.evaluate().unwrap());
    }
}
