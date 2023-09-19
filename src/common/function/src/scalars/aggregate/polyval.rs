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
    self, BadAccumulatorImplSnafu, CreateAccumulatorSnafu, DowncastVectorSnafu,
    FromScalarValueSnafu, InvalidInputColSnafu, Result,
};
use common_query::logical_plan::{Accumulator, AggregateFunctionCreator};
use common_query::prelude::*;
use datatypes::prelude::*;
use datatypes::types::{LogicalPrimitiveType, WrapperType};
use datatypes::value::ListValue;
use datatypes::vectors::{ConstantVector, Helper, Int64Vector, ListVector};
use datatypes::with_match_primitive_type_id;
use num_traits::AsPrimitive;
use snafu::{ensure, OptionExt, ResultExt};

// https://numpy.org/doc/stable/reference/generated/numpy.polyval.html
#[derive(Debug, Default)]
pub struct Polyval<T, PolyT>
where
    T: WrapperType,
    T::Native: AsPrimitive<PolyT::Native>,
    PolyT: WrapperType,
    PolyT::Native: std::ops::Mul<Output = PolyT::Native>,
{
    values: Vec<T>,
    // DataFusion casts constant in into i64 type.
    x: Option<i64>,
    _phantom: PhantomData<PolyT>,
}

impl<T, PolyT> Polyval<T, PolyT>
where
    T: WrapperType,
    T::Native: AsPrimitive<PolyT::Native>,
    PolyT: WrapperType,
    PolyT::Native: std::ops::Mul<Output = PolyT::Native>,
{
    fn push(&mut self, value: T) {
        self.values.push(value);
    }
}

impl<T, PolyT> Accumulator for Polyval<T, PolyT>
where
    T: WrapperType,
    T::Native: AsPrimitive<PolyT::Native>,
    PolyT: WrapperType + std::iter::Sum<<PolyT as WrapperType>::Native>,
    PolyT::Native: std::ops::Mul<Output = PolyT::Native> + std::iter::Sum<PolyT::Native>,
    i64: AsPrimitive<<PolyT as WrapperType>::Native>,
{
    fn state(&self) -> Result<Vec<Value>> {
        let nums = self
            .values
            .iter()
            .map(|&n| n.into())
            .collect::<Vec<Value>>();
        Ok(vec![
            Value::List(ListValue::new(
                Some(Box::new(nums)),
                T::LogicalType::build_data_type(),
            )),
            self.x.into(),
        ])
    }

    fn update_batch(&mut self, values: &[VectorRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        ensure!(values.len() == 2, InvalidInputStateSnafu);
        ensure!(values[0].len() == values[1].len(), InvalidInputStateSnafu);
        if values[0].len() == 0 {
            return Ok(());
        }
        // This is a unary accumulator, so only one column is provided.
        let column = &values[0];
        let mut len = 1;
        let column: &<T as Scalar>::VectorType = if column.is_const() {
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

        let x = &values[1];
        let x = Helper::check_get_scalar::<i64>(x).context(error::InvalidInputTypeSnafu {
            err_msg: "expecting \"POLYVAL\" function's second argument to be a positive integer",
        })?;
        // `get(0)` is safe because we have checked `values[1].len() == values[0].len() != 0`
        let first = x.get(0);
        ensure!(!first.is_null(), InvalidInputColSnafu);

        for i in 1..x.len() {
            ensure!(first == x.get(i), InvalidInputColSnafu);
        }

        let first = match first {
            Value::Int64(v) => v,
            // unreachable because we have checked `first` is not null and is i64 above
            _ => unreachable!(),
        };
        if let Some(x) = self.x {
            ensure!(x == first, InvalidInputColSnafu);
        } else {
            self.x = Some(first);
        };
        Ok(())
    }

    // DataFusion executes accumulators in partitions. In some execution stage, DataFusion will
    // merge states from other accumulators (returned by `state()` method).
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

        let x = &states[1];
        let x = x
            .as_any()
            .downcast_ref::<Int64Vector>()
            .with_context(|| DowncastVectorSnafu {
                err_msg: format!(
                    "expect Int64Vector, got vector type {}",
                    x.vector_type_name()
                ),
            })?;
        let x = x.get(0);
        if x.is_null() {
            return Ok(());
        }
        let x = match x {
            Value::Int64(x) => x,
            _ => unreachable!(),
        };
        self.x = Some(x);

        let values = &states[0];
        let values = values
            .as_any()
            .downcast_ref::<ListVector>()
            .with_context(|| DowncastVectorSnafu {
                err_msg: format!(
                    "expect ListVector, got vector type {}",
                    values.vector_type_name()
                ),
            })?;
        for value in values.values_iter() {
            if let Some(value) = value.context(FromScalarValueSnafu)? {
                let column: &<T as Scalar>::VectorType = unsafe { Helper::static_cast(&value) };
                for v in column.iter_data().flatten() {
                    self.push(v);
                }
            }
        }

        Ok(())
    }

    // DataFusion expects this function to return the final value of this aggregator.
    fn evaluate(&self) -> Result<Value> {
        if self.values.is_empty() {
            return Ok(Value::Null);
        }
        let x = if let Some(x) = self.x {
            x
        } else {
            return Ok(Value::Null);
        };
        let len = self.values.len();
        let polyval: PolyT = self
            .values
            .iter()
            .enumerate()
            .map(|(i, &value)| value.into_native().as_() * x.pow((len - 1 - i) as u32).as_())
            .sum();
        Ok(polyval.into())
    }
}

#[as_aggr_func_creator]
#[derive(Debug, Default, AggrFuncTypeStore)]
pub struct PolyvalAccumulatorCreator {}

impl AggregateFunctionCreator for PolyvalAccumulatorCreator {
    fn creator(&self) -> AccumulatorCreatorFunction {
        let creator: AccumulatorCreatorFunction = Arc::new(move |types: &[ConcreteDataType]| {
            let input_type = &types[0];
            with_match_primitive_type_id!(
                input_type.logical_type_id(),
                |$S| {
                    Ok(Box::new(Polyval::<<$S as LogicalPrimitiveType>::Wrapper, <<$S as LogicalPrimitiveType>::LargestType as LogicalPrimitiveType>::Wrapper>::default()))
                },
                {
                    let err_msg = format!(
                        "\"POLYVAL\" aggregate function not support data type {:?}",
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
        ensure!(input_types.len() == 2, InvalidInputStateSnafu);
        let input_type = self.input_types()?[0].logical_type_id();
        with_match_primitive_type_id!(
            input_type,
            |$S| {
                Ok(<<$S as LogicalPrimitiveType>::LargestType as LogicalPrimitiveType>::build_data_type())
            },
            {
                unreachable!()
            }
        )
    }

    fn state_types(&self) -> Result<Vec<ConcreteDataType>> {
        let input_types = self.input_types()?;
        ensure!(input_types.len() == 2, InvalidInputStateSnafu);
        Ok(vec![
            ConcreteDataType::list_datatype(input_types.into_iter().next().unwrap()),
            ConcreteDataType::int64_datatype(),
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
        let mut polyval = Polyval::<i32, i64>::default();
        polyval.update_batch(&[]).unwrap();
        assert!(polyval.values.is_empty());
        assert_eq!(Value::Null, polyval.evaluate().unwrap());

        // test update one not-null value
        let mut polyval = Polyval::<i32, i64>::default();
        let v: Vec<VectorRef> = vec![
            Arc::new(Int32Vector::from(vec![Some(3)])),
            Arc::new(Int64Vector::from(vec![Some(2_i64)])),
        ];
        polyval.update_batch(&v).unwrap();
        assert_eq!(Value::Int64(3), polyval.evaluate().unwrap());

        // test update one null value
        let mut polyval = Polyval::<i32, i64>::default();
        let v: Vec<VectorRef> = vec![
            Arc::new(Int32Vector::from(vec![Option::<i32>::None])),
            Arc::new(Int64Vector::from(vec![Some(2_i64)])),
        ];
        polyval.update_batch(&v).unwrap();
        assert_eq!(Value::Null, polyval.evaluate().unwrap());

        // test update no null-value batch
        let mut polyval = Polyval::<i32, i64>::default();
        let v: Vec<VectorRef> = vec![
            Arc::new(Int32Vector::from(vec![Some(3), Some(0), Some(1)])),
            Arc::new(Int64Vector::from(vec![
                Some(2_i64),
                Some(2_i64),
                Some(2_i64),
            ])),
        ];
        polyval.update_batch(&v).unwrap();
        assert_eq!(Value::Int64(13), polyval.evaluate().unwrap());

        // test update null-value batch
        let mut polyval = Polyval::<i32, i64>::default();
        let v: Vec<VectorRef> = vec![
            Arc::new(Int32Vector::from(vec![Some(3), Some(0), None, Some(1)])),
            Arc::new(Int64Vector::from(vec![
                Some(2_i64),
                Some(2_i64),
                Some(2_i64),
                Some(2_i64),
            ])),
        ];
        polyval.update_batch(&v).unwrap();
        assert_eq!(Value::Int64(13), polyval.evaluate().unwrap());

        // test update with constant vector
        let mut polyval = Polyval::<i32, i64>::default();
        let v: Vec<VectorRef> = vec![
            Arc::new(ConstantVector::new(
                Arc::new(Int32Vector::from_vec(vec![4])),
                2,
            )),
            Arc::new(Int64Vector::from(vec![Some(5_i64), Some(5_i64)])),
        ];
        polyval.update_batch(&v).unwrap();
        assert_eq!(Value::Int64(24), polyval.evaluate().unwrap());
    }
}
