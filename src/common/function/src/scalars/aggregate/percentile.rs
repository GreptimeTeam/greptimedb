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

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;

use common_macro::{as_aggr_func_creator, AggrFuncTypeStore};
use common_query::error::{
    self, BadAccumulatorImplSnafu, CreateAccumulatorSnafu, DowncastVectorSnafu,
    FromScalarValueSnafu, InvalidInputColSnafu, Result,
};
use common_query::logical_plan::{Accumulator, AggregateFunctionCreator};
use common_query::prelude::*;
use datatypes::prelude::*;
use datatypes::types::OrdPrimitive;
use datatypes::value::{ListValue, OrderedFloat};
use datatypes::vectors::{ConstantVector, Float64Vector, Helper, ListVector};
use datatypes::with_match_primitive_type_id;
use num::NumCast;
use snafu::{ensure, OptionExt, ResultExt};

// https://numpy.org/doc/stable/reference/generated/numpy.percentile.html?highlight=percentile#numpy.percentile
// if the p is 50,then the Percentile become median
// we use two heap great and not_greater
// the not_greater push the value that smaller than P-value
// the greater push the value that bigger than P-value
// just like the percentile in numpy:
// Given a vector V of length N, the q-th percentile of V is the value q/100 of the way from the minimum to the maximum in a sorted copy of V.
// The values and distances of the two nearest neighbors as well as the method parameter will determine the percentile
// if the normalized ranking does not match the location of q exactly.
// This function is the same as the median if q=50, the same as the minimum if q=0 and the same as the maximum if q=100.
// This optional method parameter specifies the method to use when the desired quantile lies between two data points i < j.
// If g is the fractional part of the index surrounded by i and alpha and beta are correction constants modifying i and j.
//              i+g = (q-alpha)/(n-alpha-beta+1)
// Below, 'q' is the quantile value, 'n' is the sample size and alpha and beta are constants. The following formula gives an interpolation "i + g" of where the quantile would be in the sorted sample.
// With 'i' being the floor and 'g' the fractional part of the result.
// the default method is linear where
// alpha = 1
// beta = 1
#[derive(Debug, Default)]
pub struct Percentile<T>
where
    T: WrapperType,
{
    greater: BinaryHeap<Reverse<OrdPrimitive<T>>>,
    not_greater: BinaryHeap<OrdPrimitive<T>>,
    n: u64,
    p: Option<f64>,
}

impl<T> Percentile<T>
where
    T: WrapperType,
{
    fn push(&mut self, value: T) {
        let value = OrdPrimitive::<T>(value);

        self.n += 1;
        if self.not_greater.is_empty() {
            self.not_greater.push(value);
            return;
        }
        // to keep the not_greater length == floor+1
        // so to ensure the peek of the not_greater is array[floor]
        // and the peek of the greater is array[floor+1]
        let p = if let Some(p) = self.p { p } else { 0.0_f64 };
        let floor = (((self.n - 1) as f64) * p / (100_f64)).floor();
        if value <= *self.not_greater.peek().unwrap() {
            self.not_greater.push(value);
            if self.not_greater.len() > (floor + 1.0) as usize {
                self.greater.push(Reverse(self.not_greater.pop().unwrap()));
            }
        } else {
            self.greater.push(Reverse(value));
            if self.not_greater.len() < (floor + 1.0) as usize {
                self.not_greater.push(self.greater.pop().unwrap().0);
            }
        }
    }
}

impl<T> Accumulator for Percentile<T>
where
    T: WrapperType,
{
    fn state(&self) -> Result<Vec<Value>> {
        let nums = self
            .greater
            .iter()
            .map(|x| &x.0)
            .chain(self.not_greater.iter())
            .map(|&n| n.into())
            .collect::<Vec<Value>>();
        Ok(vec![
            Value::List(ListValue::new(
                Some(Box::new(nums)),
                T::LogicalType::build_data_type(),
            )),
            self.p.into(),
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

        let x = &values[1];
        let x = Helper::check_get_scalar::<f64>(x).context(error::InvalidInputTypeSnafu {
            err_msg: "expecting \"POLYVAL\" function's second argument to be float64",
        })?;
        // `get(0)` is safe because we have checked `values[1].len() == values[0].len() != 0`
        let first = x.get(0);
        ensure!(!first.is_null(), InvalidInputColSnafu);

        for i in 1..x.len() {
            ensure!(first == x.get(i), InvalidInputColSnafu);
        }

        let first = match first {
            Value::Float64(OrderedFloat(v)) => v,
            // unreachable because we have checked `first` is not null and is i64 above
            _ => unreachable!(),
        };
        if let Some(p) = self.p {
            ensure!(p == first, InvalidInputColSnafu);
        } else {
            self.p = Some(first);
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

        ensure!(
            states.len() == 2,
            BadAccumulatorImplSnafu {
                err_msg: "expect 2 states in `merge_batch`"
            }
        );

        let p = &states[1];
        let p = p
            .as_any()
            .downcast_ref::<Float64Vector>()
            .with_context(|| DowncastVectorSnafu {
                err_msg: format!(
                    "expect float64vector, got vector type {}",
                    p.vector_type_name()
                ),
            })?;
        let p = p.get(0);
        if p.is_null() {
            return Ok(());
        }
        let p = match p {
            Value::Float64(OrderedFloat(p)) => p,
            _ => unreachable!(),
        };
        self.p = Some(p);

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

    fn evaluate(&self) -> Result<Value> {
        if self.not_greater.is_empty() {
            assert!(
                self.greater.is_empty(),
                "not expected in two-heap percentile algorithm, there must be a bug when implementing it"
            );
        }
        let not_greater = self.not_greater.peek();
        if not_greater.is_none() {
            return Ok(Value::Null);
        }
        let not_greater = (*self.not_greater.peek().unwrap()).as_primitive();
        let percentile = if self.greater.is_empty() {
            NumCast::from(not_greater).unwrap()
        } else {
            let greater = self.greater.peek().unwrap();
            let p = if let Some(p) = self.p {
                p
            } else {
                return Ok(Value::Null);
            };
            let fract = (((self.n - 1) as f64) * p / 100_f64).fract();
            let not_greater_v: f64 = NumCast::from(not_greater).unwrap();
            let greater_v: f64 = NumCast::from(greater.0.as_primitive()).unwrap();
            not_greater_v * (1.0 - fract) + greater_v * fract
        };
        Ok(Value::from(percentile))
    }
}

#[as_aggr_func_creator]
#[derive(Debug, Default, AggrFuncTypeStore)]
pub struct PercentileAccumulatorCreator {}

impl AggregateFunctionCreator for PercentileAccumulatorCreator {
    fn creator(&self) -> AccumulatorCreatorFunction {
        let creator: AccumulatorCreatorFunction = Arc::new(move |types: &[ConcreteDataType]| {
            let input_type = &types[0];
            with_match_primitive_type_id!(
                input_type.logical_type_id(),
                |$S| {
                    Ok(Box::new(Percentile::<<$S as LogicalPrimitiveType>::Wrapper>::default()))
                },
                {
                    let err_msg = format!(
                        "\"PERCENTILE\" aggregate function not support data type {:?}",
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
        // unwrap is safe because we have checked input_types len must equals 1
        Ok(ConcreteDataType::float64_datatype())
    }

    fn state_types(&self) -> Result<Vec<ConcreteDataType>> {
        let input_types = self.input_types()?;
        ensure!(input_types.len() == 2, InvalidInputStateSnafu);
        Ok(vec![
            ConcreteDataType::list_datatype(input_types.into_iter().next().unwrap()),
            ConcreteDataType::float64_datatype(),
        ])
    }
}

#[cfg(test)]
mod test {
    use datatypes::vectors::{Float64Vector, Int32Vector};

    use super::*;
    #[test]
    fn test_update_batch() {
        // test update empty batch, expect not updating anything
        let mut percentile = Percentile::<i32>::default();
        percentile.update_batch(&[]).unwrap();
        assert!(percentile.not_greater.is_empty());
        assert!(percentile.greater.is_empty());
        assert_eq!(Value::Null, percentile.evaluate().unwrap());

        // test update one not-null value
        let mut percentile = Percentile::<i32>::default();
        let v: Vec<VectorRef> = vec![
            Arc::new(Int32Vector::from(vec![Some(42)])),
            Arc::new(Float64Vector::from(vec![Some(100.0_f64)])),
        ];
        percentile.update_batch(&v).unwrap();
        assert_eq!(Value::from(42.0_f64), percentile.evaluate().unwrap());

        // test update one null value
        let mut percentile = Percentile::<i32>::default();
        let v: Vec<VectorRef> = vec![
            Arc::new(Int32Vector::from(vec![Option::<i32>::None])),
            Arc::new(Float64Vector::from(vec![Some(100.0_f64)])),
        ];
        percentile.update_batch(&v).unwrap();
        assert_eq!(Value::Null, percentile.evaluate().unwrap());

        // test update no null-value batch
        let mut percentile = Percentile::<i32>::default();
        let v: Vec<VectorRef> = vec![
            Arc::new(Int32Vector::from(vec![Some(-1i32), Some(1), Some(2)])),
            Arc::new(Float64Vector::from(vec![
                Some(100.0_f64),
                Some(100.0_f64),
                Some(100.0_f64),
            ])),
        ];
        percentile.update_batch(&v).unwrap();
        assert_eq!(Value::from(2_f64), percentile.evaluate().unwrap());

        // test update null-value batch
        let mut percentile = Percentile::<i32>::default();
        let v: Vec<VectorRef> = vec![
            Arc::new(Int32Vector::from(vec![Some(-2i32), None, Some(3), Some(4)])),
            Arc::new(Float64Vector::from(vec![
                Some(100.0_f64),
                Some(100.0_f64),
                Some(100.0_f64),
                Some(100.0_f64),
            ])),
        ];
        percentile.update_batch(&v).unwrap();
        assert_eq!(Value::from(4_f64), percentile.evaluate().unwrap());

        // test update with constant vector
        let mut percentile = Percentile::<i32>::default();
        let v: Vec<VectorRef> = vec![
            Arc::new(ConstantVector::new(
                Arc::new(Int32Vector::from_vec(vec![4])),
                2,
            )),
            Arc::new(Float64Vector::from(vec![Some(100.0_f64), Some(100.0_f64)])),
        ];
        percentile.update_batch(&v).unwrap();
        assert_eq!(Value::from(4_f64), percentile.evaluate().unwrap());

        // test left border
        let mut percentile = Percentile::<i32>::default();
        let v: Vec<VectorRef> = vec![
            Arc::new(Int32Vector::from(vec![Some(-1i32), Some(1), Some(2)])),
            Arc::new(Float64Vector::from(vec![
                Some(0.0_f64),
                Some(0.0_f64),
                Some(0.0_f64),
            ])),
        ];
        percentile.update_batch(&v).unwrap();
        assert_eq!(Value::from(-1.0_f64), percentile.evaluate().unwrap());

        // test medium
        let mut percentile = Percentile::<i32>::default();
        let v: Vec<VectorRef> = vec![
            Arc::new(Int32Vector::from(vec![Some(-1i32), Some(1), Some(2)])),
            Arc::new(Float64Vector::from(vec![
                Some(50.0_f64),
                Some(50.0_f64),
                Some(50.0_f64),
            ])),
        ];
        percentile.update_batch(&v).unwrap();
        assert_eq!(Value::from(1.0_f64), percentile.evaluate().unwrap());

        // test right border
        let mut percentile = Percentile::<i32>::default();
        let v: Vec<VectorRef> = vec![
            Arc::new(Int32Vector::from(vec![Some(-1i32), Some(1), Some(2)])),
            Arc::new(Float64Vector::from(vec![
                Some(100.0_f64),
                Some(100.0_f64),
                Some(100.0_f64),
            ])),
        ];
        percentile.update_batch(&v).unwrap();
        assert_eq!(Value::from(2.0_f64), percentile.evaluate().unwrap());

        // the following is the result of numpy.percentile
        // numpy.percentile
        // a = np.array([[10,7,4]])
        // np.percentile(a,40)
        // >> 6.400000000000
        let mut percentile = Percentile::<i32>::default();
        let v: Vec<VectorRef> = vec![
            Arc::new(Int32Vector::from(vec![Some(10i32), Some(7), Some(4)])),
            Arc::new(Float64Vector::from(vec![
                Some(40.0_f64),
                Some(40.0_f64),
                Some(40.0_f64),
            ])),
        ];
        percentile.update_batch(&v).unwrap();
        assert_eq!(Value::from(6.400000000_f64), percentile.evaluate().unwrap());

        // the following is the result of numpy.percentile
        // a = np.array([[10,7,4]])
        // np.percentile(a,95)
        // >> 9.7000000000000011
        let mut percentile = Percentile::<i32>::default();
        let v: Vec<VectorRef> = vec![
            Arc::new(Int32Vector::from(vec![Some(10i32), Some(7), Some(4)])),
            Arc::new(Float64Vector::from(vec![
                Some(95.0_f64),
                Some(95.0_f64),
                Some(95.0_f64),
            ])),
        ];
        percentile.update_batch(&v).unwrap();
        assert_eq!(
            Value::from(9.700_000_000_000_001_f64),
            percentile.evaluate().unwrap()
        );
    }
}
