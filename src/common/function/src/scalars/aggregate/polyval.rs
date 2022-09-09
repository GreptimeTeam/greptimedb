use std::marker::PhantomData;
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use common_query::error::{
    self, BadAccumulatorImplSnafu, CreateAccumulatorSnafu, DowncastVectorSnafu,
    FromScalarValueSnafu, InvalidInputColSnafu, InvalidInputStateSnafu, Result,
};
use common_query::logical_plan::{Accumulator, AggregateFunctionCreator};
use common_query::prelude::*;
use datatypes::types::PrimitiveType;
use datatypes::value::ListValue;
use datatypes::vectors::{ConstantVector, Int64Vector, ListVector};
use datatypes::{prelude::*, with_match_primitive_type_id};
use num_traits::AsPrimitive;
use snafu::{ensure, OptionExt, ResultExt};

// https://numpy.org/doc/stable/reference/generated/numpy.polyval.html
#[derive(Debug, Default)]
pub struct Polyval<T, PolyT>
where
    T: Primitive + AsPrimitive<PolyT>,
    PolyT: Primitive + std::ops::Mul<Output = PolyT>,
{
    values: Vec<T>,
    // DataFusion casts constant in into i64 type.
    x: Option<i64>,
    _phantom: PhantomData<PolyT>,
}

impl<T, PolyT> Polyval<T, PolyT>
where
    T: Primitive + AsPrimitive<PolyT>,
    PolyT: Primitive + std::ops::Mul<Output = PolyT>,
{
    fn push(&mut self, value: T) {
        self.values.push(value);
    }
}

impl<T, PolyT> Accumulator for Polyval<T, PolyT>
where
    T: Primitive + AsPrimitive<PolyT>,
    PolyT: Primitive + std::ops::Mul<Output = PolyT> + std::iter::Sum<PolyT>,
    for<'a> T: Scalar<RefType<'a> = T>,
    for<'a> PolyT: Scalar<RefType<'a> = PolyT>,
    i64: AsPrimitive<PolyT>,
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
                T::default().into().data_type(),
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
            let column: &ConstantVector = unsafe { VectorHelper::static_cast(column) };
            unsafe { VectorHelper::static_cast(column.inner()) }
        } else {
            unsafe { VectorHelper::static_cast(column) }
        };
        (0..len).for_each(|_| {
            for v in column.iter_data().flatten() {
                self.push(v);
            }
        });

        let x = &values[1];
        let x = VectorHelper::check_get_scalar::<i64>(x).context(error::InvalidInputsSnafu {
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
            let value = value.context(FromScalarValueSnafu)?;
            let column: &<T as Scalar>::VectorType = unsafe { VectorHelper::static_cast(&value) };
            for v in column.iter_data().flatten() {
                self.push(v);
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
            .map(|(i, &value)| value.as_() * (x.pow((len - 1 - i) as u32)).as_())
            .sum();
        Ok(polyval.into())
    }
}

#[derive(Debug, Default)]
pub struct PolyvalAccumulatorCreator {
    input_types: ArcSwapOption<Vec<ConcreteDataType>>,
}

impl AggregateFunctionCreator for PolyvalAccumulatorCreator {
    fn creator(&self) -> AccumulatorCreatorFunction {
        let creator: AccumulatorCreatorFunction = Arc::new(move |types: &[ConcreteDataType]| {
            let input_type = &types[0];
            with_match_primitive_type_id!(
                input_type.logical_type_id(),
                |$S| {
                    Ok(Box::new(Polyval::<$S,<$S as Primitive>::LargestType>::default()))
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

    fn input_types(&self) -> Result<Vec<ConcreteDataType>> {
        let input_types = self.input_types.load();
        ensure!(input_types.is_some(), InvalidInputStateSnafu);
        Ok(input_types.as_ref().unwrap().as_ref().clone())
    }

    fn set_input_types(&self, input_types: Vec<ConcreteDataType>) -> Result<()> {
        let old = self.input_types.swap(Some(Arc::new(input_types.clone())));
        if let Some(old) = old {
            ensure!(old.len() == input_types.len(), InvalidInputStateSnafu);
            for (x, y) in old.iter().zip(input_types.iter()) {
                ensure!(x == y, InvalidInputStateSnafu);
            }
        }
        Ok(())
    }

    fn output_type(&self) -> Result<ConcreteDataType> {
        let input_types = self.input_types()?;
        ensure!(input_types.len() == 2, InvalidInputStateSnafu);
        let input_type = self.input_types()?[0].logical_type_id();
        with_match_primitive_type_id!(
            input_type,
            |$S| {
                Ok(PrimitiveType::<<$S as Primitive>::LargestType>::default().into())
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
    use datatypes::vectors::PrimitiveVector;

    use super::*;
    #[test]
    fn test_update_batch() {
        // test update empty batch, expect not updating anything
        let mut polyval = Polyval::<i32, i64>::default();
        assert!(polyval.update_batch(&[]).is_ok());
        assert!(polyval.values.is_empty());
        assert_eq!(Value::Null, polyval.evaluate().unwrap());

        // test update one not-null value
        let mut polyval = Polyval::<i32, i64>::default();
        let v: Vec<VectorRef> = vec![
            Arc::new(PrimitiveVector::<i32>::from(vec![Some(3)])),
            Arc::new(PrimitiveVector::<i64>::from(vec![Some(2_i64)])),
        ];
        assert!(polyval.update_batch(&v).is_ok());
        assert_eq!(Value::Int64(3), polyval.evaluate().unwrap());

        // test update one null value
        let mut polyval = Polyval::<i32, i64>::default();
        let v: Vec<VectorRef> = vec![
            Arc::new(PrimitiveVector::<i32>::from(vec![Option::<i32>::None])),
            Arc::new(PrimitiveVector::<i64>::from(vec![Some(2_i64)])),
        ];
        assert!(polyval.update_batch(&v).is_ok());
        assert_eq!(Value::Null, polyval.evaluate().unwrap());

        // test update no null-value batch
        let mut polyval = Polyval::<i32, i64>::default();
        let v: Vec<VectorRef> = vec![
            Arc::new(PrimitiveVector::<i32>::from(vec![
                Some(3),
                Some(0),
                Some(1),
            ])),
            Arc::new(PrimitiveVector::<i64>::from(vec![
                Some(2_i64),
                Some(2_i64),
                Some(2_i64),
            ])),
        ];
        assert!(polyval.update_batch(&v).is_ok());
        assert_eq!(Value::Int64(13), polyval.evaluate().unwrap());

        // test update null-value batch
        let mut polyval = Polyval::<i32, i64>::default();
        let v: Vec<VectorRef> = vec![
            Arc::new(PrimitiveVector::<i32>::from(vec![
                Some(3),
                Some(0),
                None,
                Some(1),
            ])),
            Arc::new(PrimitiveVector::<i64>::from(vec![
                Some(2_i64),
                Some(2_i64),
                Some(2_i64),
                Some(2_i64),
            ])),
        ];
        assert!(polyval.update_batch(&v).is_ok());
        assert_eq!(Value::Int64(13), polyval.evaluate().unwrap());

        // test update with constant vector
        let mut polyval = Polyval::<i32, i64>::default();
        let v: Vec<VectorRef> = vec![
            Arc::new(ConstantVector::new(
                Arc::new(PrimitiveVector::<i32>::from_vec(vec![4])),
                2,
            )),
            Arc::new(PrimitiveVector::<i64>::from(vec![Some(5_i64), Some(5_i64)])),
        ];
        assert!(polyval.update_batch(&v).is_ok());
        assert_eq!(Value::Int64(24), polyval.evaluate().unwrap());
    }
}
