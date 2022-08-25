use std::cmp::Ordering;
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use common_query::error::{
    BadAccumulatorImplSnafu, CreateAccumulatorSnafu, InvalidInputStateSnafu, Result,
};
use common_query::logical_plan::{Accumulator, AggregateFunctionCreator};
use common_query::prelude::*;
use datatypes::vectors::ConstantVector;
use datatypes::{prelude::*, with_match_primitive_type_id};
use snafu::ensure;

// // https://numpy.org/doc/stable/reference/generated/numpy.argmin.html
#[derive(Debug, Default)]
pub struct Argmin<T>
where
    T: Primitive + PartialOrd,
{
    min: Option<T>,
    n: u32,
}

impl<T> Argmin<T>
where
    T: Primitive + PartialOrd,
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
    T: Primitive + PartialOrd,
    for<'a> T: Scalar<RefType<'a> = T>,
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
            let column: &ConstantVector = unsafe { VectorHelper::static_cast(column) };
            unsafe { VectorHelper::static_cast(column.inner()) }
        } else {
            unsafe { VectorHelper::static_cast(column) }
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
        let min: &<T as Scalar>::VectorType = unsafe { VectorHelper::static_cast(min) };
        let index: &<u32 as Scalar>::VectorType = unsafe { VectorHelper::static_cast(index) };
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

#[derive(Debug, Default)]
pub struct ArgminAccumulatorCreator {
    input_types: ArcSwapOption<Vec<ConcreteDataType>>,
}

impl AggregateFunctionCreator for ArgminAccumulatorCreator {
    fn creator(&self) -> AccumulatorCreatorFunction {
        let creator: AccumulatorCreatorFunction = Arc::new(move |types: &[ConcreteDataType]| {
            let input_type = &types[0];
            with_match_primitive_type_id!(
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
        assert_eq!(Value::from(0_u32), argmin.evaluate().unwrap());

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
        assert_eq!(Value::from(0_u32), argmin.evaluate().unwrap());

        // test update null-value batch
        let mut argmin = Argmin::<i32>::default();
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![
            Some(-2i32),
            None,
            Some(4),
        ]))];
        assert!(argmin.update_batch(&v).is_ok());
        assert_eq!(Value::from(0_u32), argmin.evaluate().unwrap());

        // test update with constant vector
        let mut argmin = Argmin::<i32>::default();
        let v: Vec<VectorRef> = vec![Arc::new(ConstantVector::new(
            Arc::new(PrimitiveVector::<i32>::from_vec(vec![4])),
            10,
        ))];
        assert!(argmin.update_batch(&v).is_ok());
        assert_eq!(Value::from(0_u32), argmin.evaluate().unwrap());
    }
}
