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

use std::marker::PhantomData;
use std::sync::Arc;

use common_macro::{as_aggr_func_creator, AggrFuncTypeStore};
use common_query::error::{
    CreateAccumulatorSnafu, DowncastVectorSnafu, FromScalarValueSnafu, Result,
};
use common_query::logical_plan::{Accumulator, AggregateFunctionCreator};
use common_query::prelude::*;
use datatypes::prelude::*;
use datatypes::value::ListValue;
use datatypes::vectors::{ConstantVector, Helper, ListVector};
use datatypes::with_match_primitive_type_id;
use num_traits::AsPrimitive;
use snafu::{ensure, OptionExt, ResultExt};

// https://numpy.org/doc/stable/reference/generated/numpy.diff.html
// I is the input type, O is the output type.
#[derive(Debug, Default)]
pub struct Diff<I, O> {
    values: Vec<I>,
    _phantom: PhantomData<O>,
}

impl<I, O> Diff<I, O> {
    fn push(&mut self, value: I) {
        self.values.push(value);
    }
}

impl<I, O> Accumulator for Diff<I, O>
where
    I: WrapperType,
    O: WrapperType,
    I::Native: AsPrimitive<O::Native>,
    O::Native: std::ops::Sub<Output = O::Native>,
{
    fn state(&self) -> Result<Vec<Value>> {
        let nums = self
            .values
            .iter()
            .map(|&n| n.into())
            .collect::<Vec<Value>>();
        Ok(vec![Value::List(ListValue::new(
            Some(Box::new(nums)),
            I::LogicalType::build_data_type(),
        ))])
    }

    fn update_batch(&mut self, values: &[VectorRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        ensure!(values.len() == 1, InvalidInputStateSnafu);

        let column = &values[0];
        let mut len = 1;
        let column: &<I as Scalar>::VectorType = if column.is_const() {
            len = column.len();
            let column: &ConstantVector = unsafe { Helper::static_cast(column) };
            unsafe { Helper::static_cast(column.inner()) }
        } else {
            unsafe { Helper::static_cast(column) }
        };
        (0..len).for_each(|_| {
            for v in column.iter_data().flatten() {
                self.push(v);
            }
        });
        Ok(())
    }

    fn merge_batch(&mut self, states: &[VectorRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let states = &states[0];
        let states = states
            .as_any()
            .downcast_ref::<ListVector>()
            .with_context(|| DowncastVectorSnafu {
                err_msg: format!(
                    "expect ListVector, got vector type {}",
                    states.vector_type_name()
                ),
            })?;
        for state in states.values_iter() {
            if let Some(state) = state.context(FromScalarValueSnafu)? {
                self.update_batch(&[state])?;
            }
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<Value> {
        if self.values.is_empty() || self.values.len() == 1 {
            return Ok(Value::Null);
        }
        let diff = self
            .values
            .windows(2)
            .map(|x| {
                let native = x[1].into_native().as_() - x[0].into_native().as_();
                O::from_native(native).into()
            })
            .collect::<Vec<Value>>();
        let diff = Value::List(ListValue::new(
            Some(Box::new(diff)),
            O::LogicalType::build_data_type(),
        ));
        Ok(diff)
    }
}

#[as_aggr_func_creator]
#[derive(Debug, Default, AggrFuncTypeStore)]
pub struct DiffAccumulatorCreator {}

impl AggregateFunctionCreator for DiffAccumulatorCreator {
    fn creator(&self) -> AccumulatorCreatorFunction {
        let creator: AccumulatorCreatorFunction = Arc::new(move |types: &[ConcreteDataType]| {
            let input_type = &types[0];
            with_match_primitive_type_id!(
                input_type.logical_type_id(),
                |$S| {
                    Ok(Box::new(Diff::<<$S as LogicalPrimitiveType>::Wrapper, <<$S as LogicalPrimitiveType>::LargestType as LogicalPrimitiveType>::Wrapper>::default()))
                },
                {
                    let err_msg = format!(
                        "\"DIFF\" aggregate function not support data type {:?}",
                        input_type.logical_type_id(),
                    );
                    CreateAccumulatorSnafu { err_msg }.fail()?
                }
            )
        });
        creator
    }

    fn output_type(&self) -> Result<ConcreteDataType> {
        let input_types = self.input_types()?;
        ensure!(input_types.len() == 1, InvalidInputStateSnafu);
        with_match_primitive_type_id!(
            input_types[0].logical_type_id(),
            |$S| {
                Ok(ConcreteDataType::list_datatype($S::default().into()))
            },
            {
                unreachable!()
            }
        )
    }

    fn state_types(&self) -> Result<Vec<ConcreteDataType>> {
        let input_types = self.input_types()?;
        ensure!(input_types.len() == 1, InvalidInputStateSnafu);
        with_match_primitive_type_id!(
            input_types[0].logical_type_id(),
            |$S| {
                Ok(vec![ConcreteDataType::list_datatype($S::default().into())])
            },
            {
                unreachable!()
            }
        )
    }
}

#[cfg(test)]
mod test {
    use datatypes::vectors::Int32Vector;

    use super::*;

    #[test]
    fn test_update_batch() {
        // test update empty batch, expect not updating anything
        let mut diff = Diff::<i32, i64>::default();
        diff.update_batch(&[]).unwrap();
        assert!(diff.values.is_empty());
        assert_eq!(Value::Null, diff.evaluate().unwrap());

        // test update one not-null value
        let mut diff = Diff::<i32, i64>::default();
        let v: Vec<VectorRef> = vec![Arc::new(Int32Vector::from(vec![Some(42)]))];
        diff.update_batch(&v).unwrap();
        assert_eq!(Value::Null, diff.evaluate().unwrap());

        // test update one null value
        let mut diff = Diff::<i32, i64>::default();
        let v: Vec<VectorRef> = vec![Arc::new(Int32Vector::from(vec![Option::<i32>::None]))];
        diff.update_batch(&v).unwrap();
        assert_eq!(Value::Null, diff.evaluate().unwrap());

        // test update no null-value batch
        let mut diff = Diff::<i32, i64>::default();
        let v: Vec<VectorRef> = vec![Arc::new(Int32Vector::from(vec![
            Some(-1i32),
            Some(1),
            Some(2),
        ]))];
        let values = vec![Value::from(2_i64), Value::from(1_i64)];
        diff.update_batch(&v).unwrap();
        assert_eq!(
            Value::List(ListValue::new(
                Some(Box::new(values)),
                ConcreteDataType::int64_datatype()
            )),
            diff.evaluate().unwrap()
        );

        // test update null-value batch
        let mut diff = Diff::<i32, i64>::default();
        let v: Vec<VectorRef> = vec![Arc::new(Int32Vector::from(vec![
            Some(-2i32),
            None,
            Some(3),
            Some(4),
        ]))];
        let values = vec![Value::from(5_i64), Value::from(1_i64)];
        diff.update_batch(&v).unwrap();
        assert_eq!(
            Value::List(ListValue::new(
                Some(Box::new(values)),
                ConcreteDataType::int64_datatype()
            )),
            diff.evaluate().unwrap()
        );

        // test update with constant vector
        let mut diff = Diff::<i32, i64>::default();
        let v: Vec<VectorRef> = vec![Arc::new(ConstantVector::new(
            Arc::new(Int32Vector::from_vec(vec![4])),
            4,
        ))];
        let values = vec![Value::from(0_i64), Value::from(0_i64), Value::from(0_i64)];
        diff.update_batch(&v).unwrap();
        assert_eq!(
            Value::List(ListValue::new(
                Some(Box::new(values)),
                ConcreteDataType::int64_datatype()
            )),
            diff.evaluate().unwrap()
        );
    }
}
