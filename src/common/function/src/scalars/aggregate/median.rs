use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;

use common_function_macro::{as_aggr_func_creator, AggrFuncTypeStore};
use common_query::error::{
    CreateAccumulatorSnafu, DowncastVectorSnafu, FromScalarValueSnafu, Result,
};
use common_query::logical_plan::{Accumulator, AggregateFunctionCreator};
use common_query::prelude::*;
use datatypes::prelude::*;
use datatypes::types::OrdPrimitive;
use datatypes::value::ListValue;
use datatypes::vectors::{ConstantVector, ListVector};
use datatypes::with_match_primitive_type_id;
use num::NumCast;
use snafu::{ensure, OptionExt, ResultExt};

// This median calculation algorithm's details can be found at
// https://leetcode.cn/problems/find-median-from-data-stream/
//
// Basically, it uses two heaps, a maximum heap and a minimum. The maximum heap stores numbers that
// are not greater than the median, and the minimum heap stores the greater. In a streaming of
// numbers, when a number is arrived, we adjust the heaps' tops, so that either one top is the
// median or both tops can be averaged to get the median.
//
// The time complexity to update the median is O(logn), O(1) to get the median; and the space
// complexity is O(n). (Ignore the costs for heap expansion.)
//
// From the point of algorithm, [quick select](https://en.wikipedia.org/wiki/Quickselect) might be
// better. But to use quick select here, we need a mutable self in the final calculation(`evaluate`)
// to swap stored numbers in the states vector. Though we can make our `evaluate` received
// `&mut self`, DataFusion calls our accumulator with `&self` (see `DfAccumulatorAdaptor`). That
// means we have to introduce some kinds of interior mutability, and the overhead is not neglectable.
//
// TODO(LFC): Use quick select to get median when we can modify DataFusion's code, and benchmark with two-heap algorithm.
#[derive(Debug, Default)]
pub struct Median<T>
where
    T: Primitive,
{
    greater: BinaryHeap<Reverse<OrdPrimitive<T>>>,
    not_greater: BinaryHeap<OrdPrimitive<T>>,
}

impl<T> Median<T>
where
    T: Primitive,
{
    fn push(&mut self, value: T) {
        let value = OrdPrimitive::<T>(value);

        if self.not_greater.is_empty() {
            self.not_greater.push(value);
            return;
        }
        // The `unwrap`s below are safe because there are `push`s before them.
        if value <= *self.not_greater.peek().unwrap() {
            self.not_greater.push(value);
            if self.not_greater.len() > self.greater.len() + 1 {
                self.greater.push(Reverse(self.not_greater.pop().unwrap()));
            }
        } else {
            self.greater.push(Reverse(value));
            if self.greater.len() > self.not_greater.len() {
                self.not_greater.push(self.greater.pop().unwrap().0);
            }
        }
    }
}

// UDAFs are built using the trait `Accumulator`, that offers DataFusion the necessary functions
// to use them.
impl<T> Accumulator for Median<T>
where
    T: Primitive,
    for<'a> T: Scalar<RefType<'a> = T>,
{
    // This function serializes our state to `ScalarValue`, which DataFusion uses to pass this
    // state between execution stages. Note that this can be arbitrary data.
    //
    // The `ScalarValue`s returned here will be passed in as argument `states: &[VectorRef]` to
    // `merge_batch` function.
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

    // DataFusion calls this function to update the accumulator's state for a batch of inputs rows.
    // It is expected this function to update the accumulator's state.
    fn update_batch(&mut self, values: &[VectorRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        ensure!(values.len() == 1, InvalidInputStateSnafu);

        // This is a unary accumulator, so only one column is provided.
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

    // DataFusion executes accumulators in partitions. In some execution stage, DataFusion will
    // merge states from other accumulators (returned by `state()` method).
    fn merge_batch(&mut self, states: &[VectorRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        // The states here are returned by the `state` method. Since we only returned a vector
        // with one value in that method, `states[0]` is fine.
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
        if self.not_greater.is_empty() {
            assert!(
                self.greater.is_empty(),
                "not expected in two-heap median algorithm, there must be a bug when implementing it"
            );
            return Ok(Value::Null);
        }

        // unwrap is safe because we checked not_greater heap's len above
        let not_greater = *self.not_greater.peek().unwrap();
        let median = if self.not_greater.len() > self.greater.len() {
            not_greater.into()
        } else {
            // unwrap is safe because greater heap len >= not_greater heap len, which is > 0
            let greater = self.greater.peek().unwrap();

            // the following three NumCast's `unwrap`s are safe because T is primitive
            let not_greater_v: f64 = NumCast::from(not_greater.as_primitive()).unwrap();
            let greater_v: f64 = NumCast::from(greater.0.as_primitive()).unwrap();
            let median: T = NumCast::from((not_greater_v + greater_v) / 2.0).unwrap();
            median.into()
        };
        Ok(median)
    }
}

#[as_aggr_func_creator]
#[derive(Debug, Default, AggrFuncTypeStore)]
pub struct MedianAccumulatorCreator {}

impl AggregateFunctionCreator for MedianAccumulatorCreator {
    fn creator(&self) -> AccumulatorCreatorFunction {
        let creator: AccumulatorCreatorFunction = Arc::new(move |types: &[ConcreteDataType]| {
            let input_type = &types[0];
            with_match_primitive_type_id!(
                input_type.logical_type_id(),
                |$S| {
                    Ok(Box::new(Median::<$S>::default()))
                },
                {
                    let err_msg = format!(
                        "\"MEDIAN\" aggregate function not support data type {:?}",
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
        // unwrap is safe because we have checked input_types len must equals 1
        Ok(input_types.into_iter().next().unwrap())
    }

    fn state_types(&self) -> Result<Vec<ConcreteDataType>> {
        Ok(vec![ConcreteDataType::list_datatype(self.output_type()?)])
    }
}

#[cfg(test)]
mod test {
    use datatypes::vectors::PrimitiveVector;

    use super::*;
    #[test]
    fn test_update_batch() {
        // test update empty batch, expect not updating anything
        let mut median = Median::<i32>::default();
        assert!(median.update_batch(&[]).is_ok());
        assert!(median.not_greater.is_empty());
        assert!(median.greater.is_empty());
        assert_eq!(Value::Null, median.evaluate().unwrap());

        // test update one not-null value
        let mut median = Median::<i32>::default();
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![Some(42)]))];
        assert!(median.update_batch(&v).is_ok());
        assert_eq!(Value::Int32(42), median.evaluate().unwrap());

        // test update one null value
        let mut median = Median::<i32>::default();
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![
            Option::<i32>::None,
        ]))];
        assert!(median.update_batch(&v).is_ok());
        assert_eq!(Value::Null, median.evaluate().unwrap());

        // test update no null-value batch
        let mut median = Median::<i32>::default();
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![
            Some(-1i32),
            Some(1),
            Some(2),
        ]))];
        assert!(median.update_batch(&v).is_ok());
        assert_eq!(Value::Int32(1), median.evaluate().unwrap());

        // test update null-value batch
        let mut median = Median::<i32>::default();
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![
            Some(-2i32),
            None,
            Some(3),
            Some(4),
        ]))];
        assert!(median.update_batch(&v).is_ok());
        assert_eq!(Value::Int32(3), median.evaluate().unwrap());

        // test update with constant vector
        let mut median = Median::<i32>::default();
        let v: Vec<VectorRef> = vec![Arc::new(ConstantVector::new(
            Arc::new(PrimitiveVector::<i32>::from_vec(vec![4])),
            10,
        ))];
        assert!(median.update_batch(&v).is_ok());
        assert_eq!(Value::Int32(4), median.evaluate().unwrap());
    }
}
