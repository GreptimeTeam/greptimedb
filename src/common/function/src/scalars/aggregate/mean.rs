use std::marker::PhantomData;
use std::sync::Arc;

use common_function_macro::{as_aggr_func_creator, AggrFuncTypeStore};
use common_query::error::{
    BadAccumulatorImplSnafu, CreateAccumulatorSnafu, DowncastVectorSnafu, Result,
};
use common_query::logical_plan::{Accumulator, AggregateFunctionCreator};
use common_query::prelude::*;
use datatypes::prelude::*;
use datatypes::vectors::{ConstantVector, Float64Vector, UInt64Vector};
use datatypes::with_match_primitive_type_id;
use num_traits::AsPrimitive;
use snafu::{ensure, OptionExt};

#[derive(Debug, Default)]
pub struct Mean<T>
where
    T: Primitive + AsPrimitive<f64>,
{
    sum: f64,
    n: u64,
    _phantom: PhantomData<T>,
}

impl<T> Mean<T>
where
    T: Primitive + AsPrimitive<f64>,
{
    #[inline(always)]
    fn push(&mut self, value: T) {
        self.sum += value.as_();
        self.n += 1;
    }

    #[inline(always)]
    fn update(&mut self, sum: f64, n: u64) {
        self.sum += sum;
        self.n += n;
    }
}

impl<T> Accumulator for Mean<T>
where
    T: Primitive + AsPrimitive<f64>,
    for<'a> T: Scalar<RefType<'a> = T>,
{
    fn state(&self) -> Result<Vec<Value>> {
        Ok(vec![self.sum.into(), self.n.into()])
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

        ensure!(
            states.len() == 2,
            BadAccumulatorImplSnafu {
                err_msg: "expect 2 states in `merge_batch`",
            }
        );

        let sum = &states[0];
        let n = &states[1];

        let sum = sum
            .as_any()
            .downcast_ref::<Float64Vector>()
            .with_context(|| DowncastVectorSnafu {
                err_msg: format!(
                    "expect Float64Vector, got vector type {}",
                    sum.vector_type_name()
                ),
            })?;

        let n = n
            .as_any()
            .downcast_ref::<UInt64Vector>()
            .with_context(|| DowncastVectorSnafu {
                err_msg: format!(
                    "expect UInt64Vector, got vector type {}",
                    sum.vector_type_name()
                ),
            })?;

        sum.iter_data().zip(n.iter_data()).for_each(|(sum, n)| {
            if let (Some(sum), Some(n)) = (sum, n) {
                self.update(sum, n);
            }
        });
        Ok(())
    }

    fn evaluate(&self) -> Result<Value> {
        if self.n == 0 {
            return Ok(Value::Null);
        }
        let values = self.sum / self.n as f64;
        Ok(values.into())
    }
}

#[as_aggr_func_creator]
#[derive(Debug, Default, AggrFuncTypeStore)]
pub struct MeanAccumulatorCreator {}

impl AggregateFunctionCreator for MeanAccumulatorCreator {
    fn creator(&self) -> AccumulatorCreatorFunction {
        let creator: AccumulatorCreatorFunction = Arc::new(move |types: &[ConcreteDataType]| {
            let input_type = &types[0];
            with_match_primitive_type_id!(
                input_type.logical_type_id(),
                |$S| {
                    Ok(Box::new(Mean::<$S>::default()))
                },
                {
                    let err_msg = format!(
                        "\"MEAN\" aggregate function not support data type {:?}",
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
        ensure!(input_types.len() == 1, InvalidInputStateSnafu);
        Ok(ConcreteDataType::float64_datatype())
    }

    fn state_types(&self) -> Result<Vec<ConcreteDataType>> {
        let input_types = self.input_types()?;
        ensure!(input_types.len() == 1, InvalidInputStateSnafu);
        Ok(vec![
            ConcreteDataType::float64_datatype(),
            ConcreteDataType::uint64_datatype(),
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
        let mut mean = Mean::<i32>::default();
        assert!(mean.update_batch(&[]).is_ok());
        assert_eq!(Value::Null, mean.evaluate().unwrap());

        // test update one not-null value
        let mut mean = Mean::<i32>::default();
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![Some(42)]))];
        assert!(mean.update_batch(&v).is_ok());
        assert_eq!(Value::from(42.0_f64), mean.evaluate().unwrap());

        // test update one null value
        let mut mean = Mean::<i32>::default();
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![
            Option::<i32>::None,
        ]))];
        assert!(mean.update_batch(&v).is_ok());
        assert_eq!(Value::Null, mean.evaluate().unwrap());

        // test update no null-value batch
        let mut mean = Mean::<i32>::default();
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![
            Some(-1i32),
            Some(1),
            Some(2),
        ]))];
        assert!(mean.update_batch(&v).is_ok());
        assert_eq!(Value::from(0.6666666666666666), mean.evaluate().unwrap());

        // test update null-value batch
        let mut mean = Mean::<i32>::default();
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![
            Some(-2i32),
            None,
            Some(3),
            Some(4),
        ]))];
        assert!(mean.update_batch(&v).is_ok());
        assert_eq!(Value::from(1.6666666666666667), mean.evaluate().unwrap());

        // test update with constant vector
        let mut mean = Mean::<i32>::default();
        let v: Vec<VectorRef> = vec![Arc::new(ConstantVector::new(
            Arc::new(PrimitiveVector::<i32>::from_vec(vec![4])),
            10,
        ))];
        assert!(mean.update_batch(&v).is_ok());
        assert_eq!(Value::from(4.0), mean.evaluate().unwrap());
    }
}
