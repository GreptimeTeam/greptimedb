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
use num::NumCast;
use num_traits::AsPrimitive;
use snafu::{OptionExt, ResultExt};

// https://numpy.org/doc/stable/reference/generated/numpy.std.html
#[derive(Debug, Default)]
pub struct Stddev<T, SumT>
where
    T: Primitive + AsPrimitive<SumT>,
    SumT: Primitive + std::iter::Sum<SumT>,
{
    values: Vec<T>,
    _phantom: PhantomData<SumT>,
}

impl<T, SumT> Stddev<T, SumT>
where
    T: Primitive + AsPrimitive<SumT>,
    SumT: Primitive + std::iter::Sum<SumT>,
{
    fn push(&mut self, value: T) {
        self.values.push(value);
    }

    fn mean(&self) -> Option<f64> {
        let sum: SumT = self.values.iter().map(|value| value.as_()).sum();
        let count = self.values.len();

        let sum: f64 = NumCast::from(sum).unwrap();

        match count {
            positive if positive > 0 => Some(sum / count as f64),
            _ => None,
        }
    }

    fn std_deviation(&self) -> Option<f64> {
        match (self.mean(), self.values.len()) {
            (Some(value_mean), count) if count > 0 => {
                let variance = self
                    .values
                    .iter()
                    .map(|value| {
                        let value: f64 = NumCast::from(*value).unwrap();
                        let diff = value_mean - value;
                        diff * diff
                    })
                    .sum::<f64>()
                    / count as f64;

                Some(variance.sqrt())
            }
            _ => None,
        }
    }
}

impl<T, SumT> Accumulator for Stddev<T, SumT>
where
    T: Primitive + AsPrimitive<SumT>,
    SumT: Primitive + std::iter::Sum<SumT>,
    for<'a> T: Scalar<RefType<'a> = T>,
    for<'a> SumT: Scalar<RefType<'a> = SumT>,
{
    fn state(&self) -> Result<Vec<Value>> {
        let nums = self
            .values
            .iter()
            .map(|&x| x.into())
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
            self.update_batch(&[state])?
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<Value> {
        match self.std_deviation() {
            Some(std) => Ok(std.into()),
            None => Ok(Value::Null),
        }
    }
}

#[derive(Debug, Default)]
pub struct StddevAccumulatorCreator {
    input_types: ArcSwapOption<Vec<ConcreteDataType>>,
}

impl AggregateFunctionCreator for StddevAccumulatorCreator {
    fn creator(&self) -> AccumulatorCreatorFunction {
        let creator: AccumulatorCreatorFunction = Arc::new(move |types: &[ConcreteDataType]| {
            let input_type = &types[0];
            with_match_ordered_primitive_type_id!(
                input_type.logical_type_id(),
                |$S| {
                    Ok(Box::new(Stddev::<$S, <$S as Primitive>::LargestType>::default()))
                },
                {
                    let err_msg = format!(
                        "\"STDDEV\" aggregate function not support data type {:?}",
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
        Ok(ConcreteDataType::float64_datatype())
    }

    fn state_types(&self) -> Result<Vec<ConcreteDataType>> {
        Ok(vec![ConcreteDataType::list_datatype(self.output_type()?)])
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
        let mut stddev = Stddev::<i32, i64>::default();
        assert!(stddev.update_batch(&[]).is_ok());
        assert!(stddev.values.is_empty());
        assert_eq!(Value::Null, stddev.evaluate().unwrap());

        // test update no null-value batch
        let mut stddev = Stddev::<i32, i64>::default();
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![
            Some(-1i32),
            Some(1),
            Some(2),
        ]))];
        assert!(stddev.update_batch(&v).is_ok());
        assert_eq!(Value::from(1.247219128924647), stddev.evaluate().unwrap());

        // test update null-value batch
        let mut stddev = Stddev::<i32, i64>::default();
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![
            Some(-2i32),
            None,
            Some(3),
            Some(4),
        ]))];
        assert!(stddev.update_batch(&v).is_ok());
        assert_eq!(Value::from(2.6246692913372702), stddev.evaluate().unwrap());
    }
}
