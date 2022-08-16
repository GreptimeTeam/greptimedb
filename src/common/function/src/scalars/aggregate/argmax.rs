use std::cmp::Ordering;
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use common_query::error::{CreateAccumulatorSnafu, ExecuteFunctionSnafu, Result};
use common_query::logical_plan::{Accumulator, AggregateFunctionCreator};
use common_query::prelude::*;
use datafusion_common::DataFusionError;
use datatypes::vectors::ConstantVector;
use datatypes::{prelude::*, with_match_primitive_type_id};
use snafu::ResultExt;

#[derive(Debug, Default)]
pub struct Argmax<T>
where
    T: Primitive + PartialOrd,
{
    max: Option<T>,
    n: u64,
}

impl<T> Argmax<T>
where
    T: Primitive + PartialOrd,
{
    fn update(&mut self, value: T, index: u64) {
        if let Some(Ordering::Less) = self.max.partial_cmp(&Some(value)) {
            self.max = Some(value);
            self.n = index;
        }
    }
}

// UDAFs are built using the trait `Accumulator`, that offers DataFusion the necessary functions
// to use them.
impl<T> Accumulator for Argmax<T>
where
    T: Primitive + PartialOrd,
    for<'a> T: Scalar<RefType<'a> = T>,
{
    // This function serializes our state to `ScalarValue`, which DataFusion uses to pass this
    // state between execution stages. Note that this can be arbitrary data.
    //
    // The `ScalarValue`s returned here will be passed in as argument `states: &[VectorRef]` to
    // `merge_batch` function.
    fn state(&self) -> Result<Vec<Value>> {
        match self.max {
            Some(max) => Ok(vec![max.into(), self.n.into()]),
            _ => Ok(vec![Value::Null, self.n.into()]),
        }
    }

    // how to deal with the empty
    // DataFusion calls this function to update the accumulator's state for a batch of inputs rows.
    // It is expected this function to update the accumulator's state.
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
        for (i, v) in column.iter_data().flatten().enumerate() {
            self.update(v, i as u64);
        }
        Ok(())
    }

    // DataFusion executes accumulators in partitions. In some execution stage, DataFusion will
    // merge states from other accumulators (returned by `state()` method).
    fn merge_batch(&mut self, states: &[VectorRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        };
        // ensure two
        assert!(states.len() == 2);
        // The states here are returned by the `state` method. Since we only returned a vector
        // with one value in that method, `states[0]` is fine.
        // for argmax we use two vector
        let max = &states[0];
        let index = &states[1];
        let max: &<T as Scalar>::VectorType = unsafe { VectorHelper::static_cast(max) };
        let index: &<u64 as Scalar>::VectorType = unsafe { VectorHelper::static_cast(index) };
        index
            .iter_data()
            .flatten()
            .zip(max.iter_data().flatten())
            .for_each(|(i, max)| self.update(max, i));
        Ok(())
    }

    // DataFusion expects this function to return the final value of this aggregator.
    fn evaluate(&self) -> Result<Value> {
        match self.max {
            Some(max) => Ok(max.into()),
            _ => Ok(Value::Null),
        }
    }
}

#[derive(Debug, Default)]
pub struct ArgmaxAccumulatorCreator {
    input_types: ArcSwapOption<Vec<ConcreteDataType>>,
}

impl AggregateFunctionCreator for ArgmaxAccumulatorCreator {
    fn creator(&self) -> AccumulatorCreatorFunction {
        let creator: AccumulatorCreatorFunction = Arc::new(move |types: &[ConcreteDataType]| {
            let input_type = &types[0];
            with_match_primitive_type_id!(
                input_type.logical_type_id(),
                |$S| {
                    Ok(Box::new(Argmax::<$S>::default()))
                },
                {
                    let err_msg = format!(
                        "\"ARGMAX\" aggregate function not support data type {:?}",
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
        Ok(ConcreteDataType::uint64_datatype())
    }

    fn state_types(&self) -> Result<Vec<ConcreteDataType>> {
        let input_types = self.input_types()?;
        // 确保输入的类型一定是1
        if input_types.len() != 1 {
            return Err(datafusion_internal_error()).context(ExecuteFunctionSnafu)?;
        }
        // unwrap is safe because we have checked input_types len must equals 1

        Ok(vec![
            input_types.into_iter().next().unwrap(),
            ConcreteDataType::uint64_datatype(),
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
        let mut argmax = Argmax::<i32>::default();
        assert!(argmax.update_batch(&[]).is_ok());
        assert_eq!(Value::Null, argmax.evaluate().unwrap());

        // test update one not-null value
        let mut argmax = Argmax::<i32>::default();
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![Some(42)]))];
        assert!(argmax.update_batch(&v).is_ok());
        assert_eq!(Value::from(42_i32), argmax.evaluate().unwrap());

        // test update one null value
        let mut argmax = Argmax::<i32>::default();
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![
            Option::<i32>::None,
        ]))];
        assert!(argmax.update_batch(&v).is_ok());
        assert_eq!(Value::Null, argmax.evaluate().unwrap());

        // test update no null-value batch
        let mut argmax = Argmax::<i32>::default();
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![
            Some(-1i32),
            Some(1),
            Some(3),
        ]))];
        assert!(argmax.update_batch(&v).is_ok());
        assert_eq!(Value::from(3_i32), argmax.evaluate().unwrap());

        // test update null-value batch
        let mut argmax = Argmax::<i32>::default();
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![
            Some(-2i32),
            None,
            Some(4),
        ]))];
        assert!(argmax.update_batch(&v).is_ok());
        assert_eq!(Value::from(4_i32), argmax.evaluate().unwrap());

        // test update with constant vector
        let mut argmax = Argmax::<i32>::default();
        let v: Vec<VectorRef> = vec![Arc::new(ConstantVector::new(
            Arc::new(PrimitiveVector::<i32>::from_vec(vec![4])),
            10,
        ))];
        assert!(argmax.update_batch(&v).is_ok());
        assert_eq!(Value::from(4_i32), argmax.evaluate().unwrap());
    }
}
