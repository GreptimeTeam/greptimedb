use std::marker::PhantomData;
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use common_query::error::{CreateAccumulatorSnafu, ExecuteFunctionSnafu, Result};
use common_query::logical_plan::{Accumulator, AggregateFunctionCreator};
use common_query::prelude::*;
use datafusion_common::DataFusionError;
use datatypes::vectors::ConstantVector;
use datatypes::{prelude::*, with_match_primitive_type_id};
use num::NumCast;
use num_traits::AsPrimitive;
use snafu::ResultExt;

// https://numpy.org/doc/stable/reference/generated/numpy.mean.html
#[derive(Debug, Default)]
pub struct Mean<T, SumT>
where
    T: Primitive + AsPrimitive<SumT>,
    SumT: Primitive + std::ops::AddAssign,
{
    sum: SumT,
    n: u64,
    _phantom: PhantomData<T>,
}

impl<T, SumT> Mean<T, SumT>
where
    T: Primitive + AsPrimitive<SumT>,
    SumT: Primitive + std::ops::AddAssign,
{
    fn update(&mut self, value: T) {
        self.sum += value.as_();
        self.n += 1;
    }

    fn merge(&mut self, sum: Option<SumT>, num: Option<u64>) {
        if let (Some(sum), Some(num)) = (sum, num) {
            self.sum += sum;
            self.n += num;
        }
    }
}

impl<T, SumT> Accumulator for Mean<T, SumT>
where
    T: Primitive + AsPrimitive<SumT>,
    SumT: Primitive + std::ops::AddAssign,
    for<'a> T: Scalar<RefType<'a> = T>,
    for<'a> SumT: Scalar<RefType<'a> = SumT>,
{
    fn state(&self) -> Result<Vec<Value>> {
        Ok(vec![self.sum.into(), self.n.into()])
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
            self.update(v);
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[VectorRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        };

        let sum = &states[0];
        let n = &states[1];

        let sum: &<SumT as Scalar>::VectorType = unsafe { VectorHelper::static_cast(sum) };
        let n: &<u64 as Scalar>::VectorType = unsafe { VectorHelper::static_cast(n) };

        sum.iter_data()
            .zip(n.iter_data())
            .for_each(|(sum, n)| self.merge(sum, n));
        Ok(())
    }

    fn evaluate(&self) -> Result<Value> {
        if self.n == 0 {
            return Ok(Value::Null);
        }
        let sum: f64 = NumCast::from(self.sum).unwrap();
        let n: f64 = NumCast::from(self.n).unwrap();
        Ok(Value::from(sum / n))
    }
}

#[derive(Debug, Default)]
pub struct MeanAccumulatorCreator {
    input_types: ArcSwapOption<Vec<ConcreteDataType>>,
}

impl AggregateFunctionCreator for MeanAccumulatorCreator {
    fn creator(&self) -> AccumulatorCreatorFunction {
        let creator: AccumulatorCreatorFunction = Arc::new(move |types: &[ConcreteDataType]| {
            let input_type = &types[0];
            with_match_primitive_type_id!(
                input_type.logical_type_id(),
                |$S| {
                    Ok(Box::new(Mean::<$S, <$S as Primitive>::LargestType>::default()))
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
        Ok(vec![
            self.input_types()?[0].clone(),
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
        let mut mean = Mean::<i32, i64>::default();
        assert!(mean.update_batch(&[]).is_ok());
        assert!(mean.n == 0);
        assert_eq!(Value::Null, mean.evaluate().unwrap());

        // test update one not-null value
        let mut mean = Mean::<i32, i64>::default();
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![Some(42)]))];
        assert!(mean.update_batch(&v).is_ok());
        assert_eq!(Value::from(42.0_f64), mean.evaluate().unwrap());

        // test update one null value
        let mut mean = Mean::<i32, i64>::default();
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![
            Option::<i32>::None,
        ]))];
        assert!(mean.update_batch(&v).is_ok());
        assert_eq!(Value::Null, mean.evaluate().unwrap());

        // test update no null-value batch
        let mut mean = Mean::<i32, i64>::default();
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![
            Some(-1i32),
            Some(1),
            Some(3),
        ]))];
        assert!(mean.update_batch(&v).is_ok());
        assert_eq!(Value::from(1.00_f64), mean.evaluate().unwrap());

        // test update null-value batch
        let mut mean = Mean::<i32, i64>::default();
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![
            Some(-2i32),
            None,
            Some(4),
        ]))];
        assert!(mean.update_batch(&v).is_ok());
        assert_eq!(Value::from(1.0_f64), mean.evaluate().unwrap());

        // test update with constant vector
        let mut mean = Mean::<i32, i64>::default();
        let v: Vec<VectorRef> = vec![Arc::new(ConstantVector::new(
            Arc::new(PrimitiveVector::<i32>::from_vec(vec![4])),
            10,
        ))];
        assert!(mean.update_batch(&v).is_ok());
        assert_eq!(Value::from(4_f64), mean.evaluate().unwrap());
    }
}
