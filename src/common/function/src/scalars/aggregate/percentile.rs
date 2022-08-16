use std::cmp::Reverse;
use std::collections::BinaryHeap;
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
use snafu::{OptionExt, ResultExt};

// https://numpy.org/doc/stable/reference/generated/numpy.percentile.html?highlight=percentile#numpy.percentile
#[derive(Debug, Default)]
pub struct Percentile<T>
where
    T: Primitive + Ord,
{
    greater: BinaryHeap<Reverse<T>>,
    not_greater: BinaryHeap<T>,
    n: u64,
    p: f64,
}

impl<T> Percentile<T>
where
    T: Primitive + Ord,
{
    fn push(&mut self, value: T) {
        self.n += 1;
        if self.not_greater.is_empty() {
            self.not_greater.push(value);
            return;
        }
        // to keep the not_greater length == floor+1
        // so to ensure the peek of the not_greater is array[floor]
        // and the peek of the greater is array[floor+1]
        let floor = (((self.n - 1) as f64) * self.p / (100_f64)).floor();
        if value <= *self.not_greater.peek().unwrap() {
            self.not_greater.push(value);
            if self.not_greater.len() > (floor + 1.0) as usize {
                self.greater.push(Reverse(self.not_greater.pop().unwrap()));
            }
        } else {
            self.greater.push(Reverse(value));
            if self.not_greater.len() < (floor + 1.0) as usize {
                self.not_greater.push(self.greater.pop().unwrap().0);
            }
        }
    }
}

impl<T> Accumulator for Percentile<T>
where
    T: Primitive + Ord,
    for<'a> T: Scalar<RefType<'a> = T>,
{
    fn state(&self) -> Result<Vec<Value>> {
        let nums = self
            .greater
            .iter()
            .map(|x| &x.0)
            .chain(self.not_greater.iter())
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
        if self.not_greater.is_empty() {
            assert!(
                self.greater.is_empty(),
                "not expected in two-heap percentile algorithm, there must be a bug when implementing it"
            );
            return Ok(Value::Null);
        }

        let not_greater = *self.not_greater.peek().unwrap();
        let percentile = if self.greater.is_empty() {
            NumCast::from(not_greater).unwrap()
        } else {
            let greater = self.greater.peek().unwrap();
            let fract = (((self.n - 1) as f64) * self.p / 100_f64).fract();
            let not_greater_v: f64 = NumCast::from(not_greater).unwrap();
            let greater_v: f64 = NumCast::from(greater.0).unwrap();
            let percentile = not_greater_v * (1.0 - fract) + greater_v * fract;
            percentile
        };
        Ok(Value::from(percentile))
    }
}

#[derive(Debug, Default)]
pub struct PercentileAccumulatorCreator {
    input_types: ArcSwapOption<Vec<ConcreteDataType>>,
}

impl AggregateFunctionCreator for PercentileAccumulatorCreator {
    fn creator(&self) -> AccumulatorCreatorFunction {
        let creator: AccumulatorCreatorFunction = Arc::new(move |types: &[ConcreteDataType]| {
            let input_type = &types[0];
            with_match_ordered_primitive_type_id!(
                input_type.logical_type_id(),
                |$S| {
                    Ok(Box::new(Percentile::<$S>::default()))
                },
                {
                    let err_msg = format!(
                        "\"PERCENTILE\" aggregate function not support data type {:?}",
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
        Ok(ConcreteDataType::float64_datatype())
    }

    fn state_types(&self) -> Result<Vec<ConcreteDataType>> {
        let input_types = self.input_types()?;
        if input_types.len() != 1 {
            return Err(datafusion_internal_error()).context(ExecuteFunctionSnafu)?;
        }
        Ok(vec![ConcreteDataType::list_datatype(
            input_types.into_iter().next().unwrap(),
        )])
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
        let mut percentile = Percentile::<i32> {
            p: 100.0,
            ..Default::default()
        };
        assert!(percentile.update_batch(&[]).is_ok());
        assert!(percentile.not_greater.is_empty());
        assert!(percentile.greater.is_empty());
        assert_eq!(Value::Null, percentile.evaluate().unwrap());

        // test update one not-null value
        let mut percentile = Percentile::<i32> {
            p: 100.0,
            ..Default::default()
        };
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![Some(42)]))];
        assert!(percentile.update_batch(&v).is_ok());
        assert_eq!(Value::from(42.0_f64), percentile.evaluate().unwrap());

        // test update one null value
        let mut percentile = Percentile::<i32> {
            p: 100.0,
            ..Default::default()
        };
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![
            Option::<i32>::None,
        ]))];
        assert!(percentile.update_batch(&v).is_ok());
        assert_eq!(Value::Null, percentile.evaluate().unwrap());

        // test update no null-value batch
        let mut percentile = Percentile::<i32> {
            p: 100.0,
            ..Default::default()
        };
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![
            Some(-1i32),
            Some(1),
            Some(2),
        ]))];
        assert!(percentile.update_batch(&v).is_ok());
        assert_eq!(Value::from(2_f64), percentile.evaluate().unwrap());

        // test update null-value batch
        let mut percentile = Percentile::<i32> {
            p: 100.0,
            ..Default::default()
        };
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![
            Some(-2i32),
            None,
            Some(3),
            Some(4),
        ]))];
        assert!(percentile.update_batch(&v).is_ok());
        assert_eq!(Value::from(4_f64), percentile.evaluate().unwrap());

        // test update with constant vector
        let mut percentile = Percentile::<i32> {
            p: 100.0,
            ..Default::default()
        };
        let v: Vec<VectorRef> = vec![Arc::new(ConstantVector::new(
            Arc::new(PrimitiveVector::<i32>::from_vec(vec![4])),
            10,
        ))];
        assert!(percentile.update_batch(&v).is_ok());
        assert_eq!(Value::from(4_f64), percentile.evaluate().unwrap());

        // test left border
        let mut percentile = Percentile::<i32> {
            p: 0.0,
            ..Default::default()
        };
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![
            Some(-1i32),
            Some(1),
            Some(2),
        ]))];
        assert!(percentile.update_batch(&v).is_ok());
        assert_eq!(Value::from(-1.0_f64), percentile.evaluate().unwrap());

        // test medium
        let mut percentile = Percentile::<i32> {
            p: 50.0,
            ..Default::default()
        };
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![
            Some(-1i32),
            Some(1),
            Some(2),
        ]))];
        assert!(percentile.update_batch(&v).is_ok());
        assert_eq!(Value::from(1.0_f64), percentile.evaluate().unwrap());

        // test right border
        let mut percentile = Percentile::<i32> {
            p: 100.0,
            ..Default::default()
        };
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![
            Some(-1i32),
            Some(1),
            Some(2),
        ]))];
        assert!(percentile.update_batch(&v).is_ok());
        assert_eq!(Value::from(2.0_f64), percentile.evaluate().unwrap());
    }
}
