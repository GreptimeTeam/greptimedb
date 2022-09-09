use std::marker::PhantomData;
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use common_query::error::{
    CreateAccumulatorSnafu, DowncastVectorSnafu, FromScalarValueSnafu, InvalidInputStateSnafu,
    Result,
};
use common_query::logical_plan::{Accumulator, AggregateFunctionCreator};
use common_query::prelude::*;
use datatypes::types::PrimitiveType;
use datatypes::value::ListValue;
use datatypes::vectors::{ConstantVector, ListVector};
use datatypes::{prelude::*, with_match_primitive_type_id};
use num_traits::AsPrimitive;
use snafu::{ensure, OptionExt, ResultExt};

// https://numpy.org/doc/stable/reference/generated/numpy.diff.html
#[derive(Debug, Default)]
pub struct Diff<T, SubT>
where
    T: Primitive + AsPrimitive<SubT>,
    SubT: Primitive + std::ops::Sub<Output = SubT>,
{
    values: Vec<T>,
    _phantom: PhantomData<SubT>,
}

impl<T, SubT> Diff<T, SubT>
where
    T: Primitive + AsPrimitive<SubT>,
    SubT: Primitive + std::ops::Sub<Output = SubT>,
{
    fn push(&mut self, value: T) {
        self.values.push(value);
    }
}

impl<T, SubT> Accumulator for Diff<T, SubT>
where
    T: Primitive + AsPrimitive<SubT>,
    for<'a> T: Scalar<RefType<'a> = T>,
    SubT: Primitive + std::ops::Sub<Output = SubT>,
    for<'a> SubT: Scalar<RefType<'a> = SubT>,
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
        }

        ensure!(values.len() == 1, InvalidInputStateSnafu);

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
            .map(|x| (x[1].as_() - x[0].as_()).into())
            .collect::<Vec<Value>>();
        let diff = Value::List(ListValue::new(
            Some(Box::new(diff)),
            SubT::default().into().data_type(),
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
                    Ok(Box::new(Diff::<$S,<$S as Primitive>::LargestType>::default()))
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
        ensure!(input_types.is_some(), InvalidInputStateSnafu);

        Ok(input_types.as_ref().unwrap().as_ref().clone())
    }

    fn set_input_types(&self, input_types: Vec<ConcreteDataType>) -> Result<()> {
        let old = self.input_types.swap(Some(Arc::new(input_types.clone())));
        if let Some(old) = old {
            ensure!(old.len() != input_types.len(), InvalidInputStateSnafu);
            for (x, y) in old.iter().zip(input_types.iter()) {
                ensure!(x == y, InvalidInputStateSnafu);
            }
        }
        Ok(())
    }

    fn output_type(&self) -> Result<ConcreteDataType> {
        let input_types = self.input_types()?;
        ensure!(input_types.len() == 1, InvalidInputStateSnafu);
        with_match_primitive_type_id!(
            input_types[0].logical_type_id(),
            |$S| {
                Ok(ConcreteDataType::list_datatype(PrimitiveType::<<$S as Primitive>::LargestType>::default().into()))
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
                Ok(vec![ConcreteDataType::list_datatype(PrimitiveType::<$S>::default().into())])
            },
            {
                unreachable!()
            }
        )
    }
}

#[cfg(test)]
mod test {
    use datatypes::vectors::PrimitiveVector;

    use super::*;
    #[test]
    fn test_update_batch() {
        // test update empty batch, expect not updating anything
        let mut diff = Diff::<i32, i64>::default();
        assert!(diff.update_batch(&[]).is_ok());
        assert!(diff.values.is_empty());
        assert_eq!(Value::Null, diff.evaluate().unwrap());

        // test update one not-null value
        let mut diff = Diff::<i32, i64>::default();
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![Some(42)]))];
        assert!(diff.update_batch(&v).is_ok());
        assert_eq!(Value::Null, diff.evaluate().unwrap());

        // test update one null value
        let mut diff = Diff::<i32, i64>::default();
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![
            Option::<i32>::None,
        ]))];
        assert!(diff.update_batch(&v).is_ok());
        assert_eq!(Value::Null, diff.evaluate().unwrap());

        // test update no null-value batch
        let mut diff = Diff::<i32, i64>::default();
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![
            Some(-1i32),
            Some(1),
            Some(2),
        ]))];
        let values = vec![Value::from(2_i64), Value::from(1_i64)];
        assert!(diff.update_batch(&v).is_ok());
        assert_eq!(
            Value::List(ListValue::new(
                Some(Box::new(values)),
                ConcreteDataType::int64_datatype()
            )),
            diff.evaluate().unwrap()
        );

        // test update null-value batch
        let mut diff = Diff::<i32, i64>::default();
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![
            Some(-2i32),
            None,
            Some(3),
            Some(4),
        ]))];
        let values = vec![Value::from(5_i64), Value::from(1_i64)];
        assert!(diff.update_batch(&v).is_ok());
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
            Arc::new(PrimitiveVector::<i32>::from_vec(vec![4])),
            4,
        ))];
        let values = vec![Value::from(0_i64), Value::from(0_i64), Value::from(0_i64)];
        assert!(diff.update_batch(&v).is_ok());
        assert_eq!(
            Value::List(ListValue::new(
                Some(Box::new(values)),
                ConcreteDataType::int64_datatype()
            )),
            diff.evaluate().unwrap()
        );
    }
}
