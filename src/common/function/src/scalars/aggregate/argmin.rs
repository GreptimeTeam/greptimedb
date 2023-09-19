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

use std::cmp::Ordering;
use std::sync::Arc;

use common_macro::{as_aggr_func_creator, AggrFuncTypeStore};
use common_query::error::{BadAccumulatorImplSnafu, CreateAccumulatorSnafu, Result};
use common_query::logical_plan::{Accumulator, AggregateFunctionCreator};
use common_query::prelude::*;
use datatypes::prelude::*;
use datatypes::vectors::{ConstantVector, Helper};
use datatypes::with_match_primitive_type_id;
use snafu::ensure;

// // https://numpy.org/doc/stable/reference/generated/numpy.argmin.html
#[derive(Debug, Default)]
pub struct Argmin<T> {
    min: Option<T>,
    n: u32,
}

impl<T> Argmin<T>
where
    T: Copy + PartialOrd,
{
    fn update(&mut self, value: T, index: u32) {
        match self.min {
            Some(min) => {
                if let Some(Ordering::Greater) = min.partial_cmp(&value) {
                    self.min = Some(value);
                    self.n = index;
                }
            }
            None => {
                self.min = Some(value);
                self.n = index;
            }
        }
    }
}

impl<T> Accumulator for Argmin<T>
where
    T: WrapperType + PartialOrd,
{
    fn state(&self) -> Result<Vec<Value>> {
        match self.min {
            Some(min) => Ok(vec![min.into(), self.n.into()]),
            _ => Ok(vec![Value::Null, self.n.into()]),
        }
    }

    fn update_batch(&mut self, values: &[VectorRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        ensure!(values.len() == 1, InvalidInputStateSnafu);

        let column = &values[0];
        let column: &<T as Scalar>::VectorType = if column.is_const() {
            let column: &ConstantVector = unsafe { Helper::static_cast(column) };
            unsafe { Helper::static_cast(column.inner()) }
        } else {
            unsafe { Helper::static_cast(column) }
        };
        for (i, v) in column.iter_data().enumerate() {
            if let Some(value) = v {
                self.update(value, i as u32);
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[VectorRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        ensure!(
            states.len() == 2,
            BadAccumulatorImplSnafu {
                err_msg: "expect 2 states in `merge_batch`",
            }
        );

        let min = &states[0];
        let index = &states[1];
        let min: &<T as Scalar>::VectorType = unsafe { Helper::static_cast(min) };
        let index: &<u32 as Scalar>::VectorType = unsafe { Helper::static_cast(index) };
        index
            .iter_data()
            .flatten()
            .zip(min.iter_data().flatten())
            .for_each(|(i, min)| self.update(min, i));
        Ok(())
    }

    fn evaluate(&self) -> Result<Value> {
        match self.min {
            Some(_) => Ok(self.n.into()),
            _ => Ok(Value::Null),
        }
    }
}

#[as_aggr_func_creator]
#[derive(Debug, Default, AggrFuncTypeStore)]
pub struct ArgminAccumulatorCreator {}

impl AggregateFunctionCreator for ArgminAccumulatorCreator {
    fn creator(&self) -> AccumulatorCreatorFunction {
        let creator: AccumulatorCreatorFunction = Arc::new(move |types: &[ConcreteDataType]| {
            let input_type = &types[0];
            with_match_primitive_type_id!(
                input_type.logical_type_id(),
                |$S| {
                    Ok(Box::new(Argmin::<<$S as LogicalPrimitiveType>::Wrapper>::default()))
                },
                {
                    let err_msg = format!(
                        "\"ARGMIN\" aggregate function not support data type {:?}",
                        input_type.logical_type_id(),
                    );
                    CreateAccumulatorSnafu { err_msg }.fail()?
                }
            )
        });
        creator
    }

    fn output_type(&self) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::uint32_datatype())
    }

    fn state_types(&self) -> Result<Vec<ConcreteDataType>> {
        let input_types = self.input_types()?;

        ensure!(input_types.len() == 1, InvalidInputStateSnafu);

        Ok(vec![
            input_types.into_iter().next().unwrap(),
            ConcreteDataType::uint32_datatype(),
        ])
    }
}

#[cfg(test)]
mod test {
    use datatypes::vectors::Int32Vector;

    use super::*;
    #[test]
    fn test_update_batch() {
        // test update empty batch, expect not updating anything
        let mut argmin = Argmin::<i32>::default();
        argmin.update_batch(&[]).unwrap();
        assert_eq!(Value::Null, argmin.evaluate().unwrap());

        // test update one not-null value
        let mut argmin = Argmin::<i32>::default();
        let v: Vec<VectorRef> = vec![Arc::new(Int32Vector::from(vec![Some(42)]))];
        argmin.update_batch(&v).unwrap();
        assert_eq!(Value::from(0_u32), argmin.evaluate().unwrap());

        // test update one null value
        let mut argmin = Argmin::<i32>::default();
        let v: Vec<VectorRef> = vec![Arc::new(Int32Vector::from(vec![Option::<i32>::None]))];
        argmin.update_batch(&v).unwrap();
        assert_eq!(Value::Null, argmin.evaluate().unwrap());

        // test update no null-value batch
        let mut argmin = Argmin::<i32>::default();
        let v: Vec<VectorRef> = vec![Arc::new(Int32Vector::from(vec![
            Some(-1i32),
            Some(1),
            Some(3),
        ]))];
        argmin.update_batch(&v).unwrap();
        assert_eq!(Value::from(0_u32), argmin.evaluate().unwrap());

        // test update null-value batch
        let mut argmin = Argmin::<i32>::default();
        let v: Vec<VectorRef> = vec![Arc::new(Int32Vector::from(vec![
            Some(-2i32),
            None,
            Some(4),
        ]))];
        argmin.update_batch(&v).unwrap();
        assert_eq!(Value::from(0_u32), argmin.evaluate().unwrap());

        // test update with constant vector
        let mut argmin = Argmin::<i32>::default();
        let v: Vec<VectorRef> = vec![Arc::new(ConstantVector::new(
            Arc::new(Int32Vector::from_vec(vec![4])),
            10,
        ))];
        argmin.update_batch(&v).unwrap();
        assert_eq!(Value::from(0_u32), argmin.evaluate().unwrap());
    }
}
