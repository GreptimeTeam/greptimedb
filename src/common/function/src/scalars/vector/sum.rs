use std::sync::Arc;

use common_macro::{as_aggr_func_creator, AggrFuncTypeStore};
use common_query::error::{CreateAccumulatorSnafu, Error, InvalidFuncArgsSnafu};
use common_query::logical_plan::{Accumulator, AggregateFunctionCreator};
use common_query::prelude::AccumulatorCreatorFunction;
use datatypes::prelude::{ConcreteDataType, Value, *};
use datatypes::vectors::VectorRef;
use nalgebra::{Const, DVectorView, Dyn, OVector};
use snafu::ensure;

use crate::scalars::vector::impl_conv::{as_veclit, as_veclit_if_const, veclit_to_binlit};

#[derive(Debug, Default)]
pub struct VectorSum {
    sum: Option<OVector<f32, Dyn>>,
    has_null: bool,
}

#[as_aggr_func_creator]
#[derive(Debug, Default, AggrFuncTypeStore)]
pub struct VectorSumCreator {}

impl AggregateFunctionCreator for VectorSumCreator {
    fn creator(&self) -> AccumulatorCreatorFunction {
        let creator: AccumulatorCreatorFunction = Arc::new(move |types: &[ConcreteDataType]| {
            ensure!(
                types.len() == 1,
                InvalidFuncArgsSnafu {
                    err_msg: format!(
                        "The length of the args is not correct, expect exactly one, have: {}",
                        types.len()
                    )
                }
            );
            let input_type = &types[0];
            match input_type {
                ConcreteDataType::String(_) | ConcreteDataType::Binary(_) => {
                    Ok(Box::new(VectorSum::default()))
                }
                _ => {
                    let err_msg = format!(
                        "\"VEC_SUM\" aggregate function not support data type {:?}",
                        input_type.logical_type_id(),
                    );
                    CreateAccumulatorSnafu { err_msg }.fail()?
                }
            }
        });
        creator
    }

    fn output_type(&self) -> common_query::error::Result<ConcreteDataType> {
        Ok(ConcreteDataType::binary_datatype())
    }

    fn state_types(&self) -> common_query::error::Result<Vec<ConcreteDataType>> {
        Ok(vec![self.output_type()?])
    }
}

impl VectorSum {
    fn inner(&mut self, len: usize) -> &mut OVector<f32, Dyn> {
        self.sum
            .get_or_insert_with(|| OVector::zeros_generic(Dyn(len), Const::<1>))
    }

    fn update(&mut self, values: &[VectorRef], is_update: bool) -> Result<(), Error> {
        if values.is_empty() || self.has_null {
            return Ok(());
        };
        let column = &values[0];
        let len = column.len();

        match as_veclit_if_const(column)? {
            Some(column) => {
                let vec_column = DVectorView::from_slice(&column, column.len()).scale(len as f32);
                *self.inner(vec_column.len()) += vec_column;
            }
            None => {
                for i in 0..len {
                    let Some(arg0) = as_veclit(column.get_ref(i))? else {
                        if is_update {
                            self.has_null = true;
                            self.sum = None;
                        }
                        return Ok(());
                    };
                    let vec_column = DVectorView::from_slice(&arg0, arg0.len());
                    *self.inner(vec_column.len()) += vec_column;
                }
            }
        }
        Ok(())
    }
}

impl Accumulator for VectorSum {
    fn state(&self) -> common_query::error::Result<Vec<Value>> {
        self.evaluate().map(|v| vec![v])
    }

    fn update_batch(&mut self, values: &[VectorRef]) -> common_query::error::Result<()> {
        self.update(values, true)
    }

    fn merge_batch(&mut self, states: &[VectorRef]) -> common_query::error::Result<()> {
        self.update(states, false)
    }

    fn evaluate(&self) -> common_query::error::Result<Value> {
        match &self.sum {
            None => Ok(Value::Null),
            Some(vector) => Ok(Value::from(veclit_to_binlit(vector.as_slice()))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::vectors::{ConstantVector, StringVector};

    use super::*;

    #[test]
    fn test_update_batch() {
        // test update empty batch, expect not updating anything
        let mut vec_sum = VectorSum::default();
        vec_sum.update_batch(&[]).unwrap();
        assert!(vec_sum.sum.is_none());
        assert!(!vec_sum.has_null);
        assert_eq!(Value::Null, vec_sum.evaluate().unwrap());

        // test update one not-null value
        let mut vec_sum = VectorSum::default();
        let v: Vec<VectorRef> = vec![Arc::new(StringVector::from(vec![Some(
            "[1.0,2.0,3.0]".to_string(),
        )]))];
        vec_sum.update_batch(&v).unwrap();
        assert_eq!(
            Value::from(veclit_to_binlit(&[1.0, 2.0, 3.0])),
            vec_sum.evaluate().unwrap()
        );

        // test update one null value
        let mut vec_sum = VectorSum::default();
        let v: Vec<VectorRef> = vec![Arc::new(StringVector::from(vec![Option::<String>::None]))];
        vec_sum.update_batch(&v).unwrap();
        assert_eq!(Value::Null, vec_sum.evaluate().unwrap());

        // test update no null-value batch
        let mut vec_sum = VectorSum::default();
        let v: Vec<VectorRef> = vec![Arc::new(StringVector::from(vec![
            Some("[1.0,2.0,3.0]".to_string()),
            Some("[4.0,5.0,6.0]".to_string()),
            Some("[7.0,8.0,9.0]".to_string()),
        ]))];
        vec_sum.update_batch(&v).unwrap();
        assert_eq!(
            Value::from(veclit_to_binlit(&[12.0, 15.0, 18.0])),
            vec_sum.evaluate().unwrap()
        );

        // test update null-value batch
        let mut vec_sum = VectorSum::default();
        let v: Vec<VectorRef> = vec![Arc::new(StringVector::from(vec![
            Some("[1.0,2.0,3.0]".to_string()),
            None,
            Some("[7.0,8.0,9.0]".to_string()),
        ]))];
        vec_sum.update_batch(&v).unwrap();
        assert_eq!(Value::Null, vec_sum.evaluate().unwrap());

        // test update with constant vector
        let mut vec_sum = VectorSum::default();
        let v: Vec<VectorRef> = vec![Arc::new(ConstantVector::new(
            Arc::new(StringVector::from_vec(vec!["[1.0,2.0,3.0]".to_string()])),
            4,
        ))];
        vec_sum.update_batch(&v).unwrap();
        assert_eq!(
            Value::from(veclit_to_binlit(&[4.0, 8.0, 12.0])),
            vec_sum.evaluate().unwrap()
        );
    }
}
