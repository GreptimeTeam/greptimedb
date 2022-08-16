use std::sync::Arc;

use arc_swap::ArcSwapOption;
use common_query::error::{
    CreateAccumulatorSnafu, DowncastVectorSnafu, ExecuteFunctionSnafu, FromScalarValueSnafu, Result,
};
use common_query::logical_plan::{Accumulator, AggregateFunctionCreator};
use common_query::prelude::*;
use datafusion_common::DataFusionError;
use datatypes::value::ListValue;
use datatypes::vectors::{ConstantVector, ListVector};
use datatypes::{prelude::*, with_match_primitive_type_id};
use snafu::{OptionExt, ResultExt};

// https://numpy.org/doc/stable/reference/generated/numpy.diff.html
#[derive(Debug, Default)]
pub struct Diff<T>
where
    T: Primitive + std::ops::Sub<Output = T>,
{
    values: Vec<T>,
}

impl<T> Diff<T>
where
    T: Primitive + std::ops::Sub<Output = T>,
{
    fn push(&mut self, value: T) {
        self.values.push(value);
    }
}

impl<T> Accumulator for Diff<T>
where
    T: Primitive + std::ops::Sub<Output = T>,
    for<'a> T: Scalar<RefType<'a> = T>,
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

        if values.len() != 1 {
            return Err(datafusion_internal_error()).context(ExecuteFunctionSnafu)?;
        }

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
        if self.values.is_empty() || self.values.len() == 1 {
            return Ok(Value::Null);
        }
        let diff = self
            .values
            .windows(2)
            .map(|x| (x[1] - x[0]).into())
            .collect::<Vec<Value>>();
        let diff = Value::List(ListValue::new(
            Some(Box::new(diff)),
            T::default().into().data_type(),
        ));
        Ok(diff)
    }
}

#[derive(Debug, Default)]
pub struct DiffAccumulatorCreator {
    input_types: ArcSwapOption<Vec<ConcreteDataType>>,
}

impl AggregateFunctionCreator for DiffAccumulatorCreator {
    fn creator(&self) -> AccumulatorCreatorFunction {
        let creator: AccumulatorCreatorFunction = Arc::new(move |types: &[ConcreteDataType]| {
            let input_type = &types[0];
            with_match_primitive_type_id!(
                input_type.logical_type_id(),
                |$S| {
                    Ok(Box::new(Diff::<$S>::default()))
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
        Ok(ConcreteDataType::list_datatype(
            input_types.into_iter().next().unwrap(),
        ))
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
        let mut diff = Diff::<i32>::default();
        assert!(diff.update_batch(&[]).is_ok());
        assert!(diff.values.is_empty());
        assert_eq!(Value::Null, diff.evaluate().unwrap());

        // test update one not-null value
        let mut diff = Diff::<i32>::default();
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![Some(42)]))];
        assert!(diff.update_batch(&v).is_ok());
        assert_eq!(Value::Null, diff.evaluate().unwrap());

        // test update one null value
        let mut diff = Diff::<i32>::default();
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![
            Option::<i32>::None,
        ]))];
        assert!(diff.update_batch(&v).is_ok());
        assert_eq!(Value::Null, diff.evaluate().unwrap());

        // test update no null-value batch
        let mut diff = Diff::<i32>::default();
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![
            Some(-1i32),
            Some(1),
            Some(2),
        ]))];
        let values = vec![Value::from(2_i32), Value::from(1_i32)];
        assert!(diff.update_batch(&v).is_ok());
        assert_eq!(
            Value::List(ListValue::new(
                Some(Box::new(values)),
                ConcreteDataType::int32_datatype()
            )),
            diff.evaluate().unwrap()
        );

        // test update null-value batch
        let mut diff = Diff::<i32>::default();
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![
            Some(-2i32),
            None,
            Some(3),
            Some(4),
        ]))];
        let values = vec![Value::from(5_i32), Value::from(1_i32)];
        assert!(diff.update_batch(&v).is_ok());
        assert_eq!(
            Value::List(ListValue::new(
                Some(Box::new(values)),
                ConcreteDataType::int32_datatype()
            )),
            diff.evaluate().unwrap()
        );

        // test update with constant vector
        let mut diff = Diff::<i32>::default();
        let v: Vec<VectorRef> = vec![Arc::new(ConstantVector::new(
            Arc::new(PrimitiveVector::<i32>::from_vec(vec![4])),
            10,
        ))];
        assert!(diff.update_batch(&v).is_ok());
        assert_eq!(Value::Null, diff.evaluate().unwrap());
    }
}
