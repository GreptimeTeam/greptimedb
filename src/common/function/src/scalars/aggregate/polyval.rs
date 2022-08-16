use std::marker::PhantomData;
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use common_query::error::{
    CreateAccumulatorSnafu, DowncastVectorSnafu, ExecuteFunctionSnafu, FromScalarValueSnafu, Result,
};
use common_query::logical_plan::{Accumulator, AggregateFunctionCreator};
use common_query::prelude::*;
use datafusion_common::DataFusionError;
use datatypes::prelude::*;
use datatypes::value::ListValue;
use datatypes::vectors::{ConstantVector, ListVector};
use datatypes::with_match_ordered_primitive_type_id;
use num_traits::{AsPrimitive, Pow};
use snafu::{OptionExt, ResultExt};

// https://numpy.org/doc/stable/reference/generated/numpy.polyval.html
#[derive(Debug, Default)]
pub struct Polyval<T, PolyT>
where
    T: Primitive + AsPrimitive<PolyT>,
    PolyT: Primitive + std::ops::Mul<Output = PolyT> + Pow<usize, Output = PolyT>,
{
    values: Vec<T>,
    x: T,
    n: usize,
    _phantom: PhantomData<PolyT>,
}

impl<T, PolyT> Polyval<T, PolyT>
where
    T: Primitive + AsPrimitive<PolyT>,
    PolyT: Primitive + std::ops::Mul<Output = PolyT> + Pow<usize, Output = PolyT>,
{
    fn push(&mut self, value: T) {
        self.values.push(value);
        self.n += 1;
    }
}

impl<T, PolyT> Accumulator for Polyval<T, PolyT>
where
    T: Primitive + AsPrimitive<PolyT>,
    PolyT: Primitive
        + std::ops::Mul<Output = PolyT>
        + Pow<usize, Output = PolyT>
        + std::iter::Sum<PolyT>,
    for<'a> T: Scalar<RefType<'a> = T>,
    for<'a> PolyT: Scalar<RefType<'a> = PolyT>,
{
    fn state(&self) -> Result<Vec<Value>> {
        let nums = self
            .values
            .iter()
            .map(|&n| n.into())
            .collect::<Vec<Value>>();
        Ok(vec![Value::List(ListValue::new(
            Some(Box::new(nums)),
            T::default().into().data_type(),
        ))])
    }

    fn update_batch(&mut self, values: &[VectorRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        };

        // This is a unary accumulator, so only one column is provided.
        let column = &values[0];
        let column: &<T as Scalar>::VectorType = if column.is_const() {
            let column: &ConstantVector = unsafe { VectorHelper::static_cast(column) };
            unsafe { VectorHelper::static_cast(column.inner()) }
        } else {
            unsafe { VectorHelper::static_cast(column) }
        };
        for v in column.iter_data().flatten() {
            self.push(v);
        }
        Ok(())
    }

    // DataFusion executes accumulators in partitions. In some execution stage, DataFusion will
    // merge states from other accumulators (returned by `state()` method).
    fn merge_batch(&mut self, states: &[VectorRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        };
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
            let state = state.context(FromScalarValueSnafu)?;
            // merging state is simply accumulate stored numbers from others', so just call update
            self.update_batch(&[state])?
        }
        Ok(())
    }

    // DataFusion expects this function to return the final value of this aggregator.
    fn evaluate(&self) -> Result<Value> {
        if self.n == 0 {
            return Ok(Value::Null);
        }
        let polyval: PolyT = self
            .values
            .iter()
            .enumerate()
            .map(|(i, &value)| value.as_() * (self.x.as_().pow(self.n - 1 - i)))
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
            with_match_ordered_primitive_type_id!(
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
        if input_types.is_none() {
            return Err(datafusion_internal_error()).context(ExecuteFunctionSnafu)?;
        }
        Ok(input_types.as_ref().unwrap().as_ref().clone())
    }

    fn set_input_types(&self, input_types: Vec<ConcreteDataType>) -> Result<()> {
        let old = self.input_types.swap(Some(Arc::new(input_types.clone())));
        if let Some(old) = old {
            if old.len() != input_types.len() {
                return Err(datafusion_internal_error()).context(ExecuteFunctionSnafu)?;
            }
            for (x, y) in old.iter().zip(input_types.iter()) {
                if x != y {
                    return Err(datafusion_internal_error()).context(ExecuteFunctionSnafu)?;
                }
            }
        }
        Ok(())
    }

    fn output_type(&self) -> Result<ConcreteDataType> {
        let input_types = self.input_types()?;
        if input_types.len() != 1 {
            return Err(datafusion_internal_error()).context(ExecuteFunctionSnafu)?;
        }
        // unwrap is safe because we have checked input_types len must equals 1
        Ok(input_types.into_iter().next().unwrap())
    }

    fn state_types(&self) -> Result<Vec<ConcreteDataType>> {
        Ok(vec![
            ConcreteDataType::list_datatype(self.output_type()?),
            self.output_type()?,
            ConcreteDataType::uint32_datatype(),
        ])
    }
}

fn datafusion_internal_error() -> DataFusionError {
    DataFusionError::Internal(
        "Illegal input_types status, check if DataFusion has changed its UDAF execution logic."
            .to_string(),
    )
}

#[cfg(test)]
mod test {
    use datatypes::vectors::PrimitiveVector;

    use super::*;
    #[test]
    fn test_update_batch() {
        // test update empty batch, expect not updating anything
        let mut polyval = Polyval::<i32, i64> {
            x: 5,
            ..Default::default()
        };
        assert!(polyval.update_batch(&[]).is_ok());
        assert!(polyval.values.is_empty());
        assert_eq!(Value::Null, polyval.evaluate().unwrap());

        // test update one not-null value
        let mut polyval = Polyval::<i32, i64> {
            x: 5,
            ..Default::default()
        };
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![Some(3)]))];
        assert!(polyval.update_batch(&v).is_ok());
        assert_eq!(Value::Int64(3), polyval.evaluate().unwrap());

        // test update one null value
        let mut polyval = Polyval::<i32, i64> {
            x: 5,
            ..Default::default()
        };
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![
            Option::<i32>::None,
        ]))];
        assert!(polyval.update_batch(&v).is_ok());
        assert_eq!(Value::Null, polyval.evaluate().unwrap());

        // test update no null-value batch
        let mut polyval = Polyval::<i32, i64> {
            x: 5,
            ..Default::default()
        };
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![
            Some(3),
            Some(0),
            Some(1),
        ]))];
        assert!(polyval.update_batch(&v).is_ok());
        assert_eq!(Value::Int64(76), polyval.evaluate().unwrap());

        // test update null-value batch
        let mut polyval = Polyval::<i32, i64> {
            x: 5,
            ..Default::default()
        };
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![
            Some(3),
            Some(0),
            None,
            Some(1),
        ]))];
        assert!(polyval.update_batch(&v).is_ok());
        assert_eq!(Value::Int64(76), polyval.evaluate().unwrap());

        // test update with constant vector
        let mut polyval = Polyval::<i32, i64> {
            x: 5,
            ..Default::default()
        };
        let v: Vec<VectorRef> = vec![Arc::new(ConstantVector::new(
            Arc::new(PrimitiveVector::<i32>::from_vec(vec![4])),
            10,
        ))];
        assert!(polyval.update_batch(&v).is_ok());
        assert_eq!(Value::Int64(4), polyval.evaluate().unwrap());
    }
}
