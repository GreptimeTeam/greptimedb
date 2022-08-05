use std::sync::Arc;

use arc_swap::ArcSwapOption;
use common_query::error::{
    CreateAccumulatorSnafu, DowncastVectorSnafu, ExecuteFunctionSnafu, FromScalarValueSnafu, Result,
};
use common_query::logical_plan::{Accumulator, AggregateFunctionCreator};
use common_query::prelude::*;
use datafusion_common::DataFusionError;
use datatypes::prelude::*;
use datatypes::vectors::{ConstantVector, ListVector};
use datatypes::with_match_ordered_primitive_type_id;
use snafu::{OptionExt, ResultExt};

// https://numpy.org/doc/stable/reference/generated/numpy.argmin.html
#[derive(Debug, Default)]
pub struct Argmin<T>
where
    T: Primitive + Ord,
{
    min: Option<T>,
}

impl<T> Argmin<T>
where
    T: Primitive + Ord,
{
    fn push(&mut self, value: T) {
        self.min = match self.min {
            Some(min) => {
                if min < value {
                    Some(min)
                } else {
                    Some(value)
                }
            }
            None => Some(value),
        };
    }
}

impl<T> Accumulator for Argmin<T>
where
    T: Primitive + Ord,
    for<'a> T: Scalar<RefType<'a> = T>,
    datatypes::prelude::Value: From<Option<T>>,
{
    fn state(&self) -> Result<Vec<Value>> {
        Ok(vec![self.min.into()])
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
        Ok(self.min.into())
    }
}

#[derive(Debug, Default)]
pub struct ArgminAccumulatorCreator {
    input_types: ArcSwapOption<Vec<ConcreteDataType>>,
}

impl AggregateFunctionCreator for ArgminAccumulatorCreator {
    fn creator(&self) -> AccumulatorCreatorFunction {
        let creator: AccumulatorCreatorFunction = Arc::new(move |types: &[ConcreteDataType]| {
            let input_type = &types[0];
            with_match_ordered_primitive_type_id!(
                input_type.logical_type_id(),
                |$S| {
                    Ok(Box::new(Argmin::<$S>::default()))
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
        Ok(vec![self.output_type()?])
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
        let mut argmin = Argmin::<i32>::default();
        assert!(argmin.update_batch(&[]).is_ok());
        assert_eq!(Value::Null, argmin.evaluate().unwrap());

        // test update one not-null value
        let mut argmin = Argmin::<i32>::default();
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![Some(42)]))];
        assert!(argmin.update_batch(&v).is_ok());
        assert_eq!(Value::from(42_i32), argmin.evaluate().unwrap());

        // test update one null value
        let mut argmin = Argmin::<i32>::default();
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![
            Option::<i32>::None,
        ]))];
        assert!(argmin.update_batch(&v).is_ok());
        assert_eq!(Value::Null, argmin.evaluate().unwrap());

        // test update no null-value batch
        let mut argmin = Argmin::<i32>::default();
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![
            Some(-1i32),
            Some(1),
            Some(3),
        ]))];
        assert!(argmin.update_batch(&v).is_ok());
        assert_eq!(Value::from(-1_i32), argmin.evaluate().unwrap());

        // test update null-value batch
        let mut argmin = Argmin::<i32>::default();
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![
            Some(-2i32),
            None,
            Some(4),
        ]))];
        assert!(argmin.update_batch(&v).is_ok());
        assert_eq!(Value::from(-2_i32), argmin.evaluate().unwrap());

        // test update with constant vector
        let mut argmin = Argmin::<i32>::default();
        let v: Vec<VectorRef> = vec![Arc::new(ConstantVector::new(
            Arc::new(PrimitiveVector::<i32>::from_vec(vec![4])),
            10,
        ))];
        assert!(argmin.update_batch(&v).is_ok());
        assert_eq!(Value::from(4_i32), argmin.evaluate().unwrap());
    }
}
