use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use common_query::error::{ExecuteFunctionSnafu, FromScalarValueSnafu, Result};
use common_query::logical_plan::{Accumulator, AccumulatorCreator};
use common_query::prelude::*;
use datafusion_common::DataFusionError;
use datatypes::prelude::*;
use datatypes::value::ListValue;
use datatypes::vectors::ListVector;
use datatypes::with_match_ordered_primitive_type_id;
use num::NumCast;
use snafu::ResultExt;

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
    T: Primitive + Ord,
{
    greater: BinaryHeap<Reverse<T>>,
    not_greater: BinaryHeap<T>,
}

impl<T> Median<T>
where
    T: Primitive + Ord,
{
    fn push(&mut self, value: T) {
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
    T: Primitive + Ord,
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
        };

        // This is a unary accumulator, so only one column is provided.
        let column = &values[0];
        let column: &<T as Scalar>::VectorType = unsafe { VectorHelper::static_cast(column) };
        for v in column.iter_data().flatten() {
            self.push(v);
        }
        Ok(())
    }

    // DataFusion executes accumulators in partitions. In some execution stage, DataFusion will
    // merge states from other accumulators (returned by `state()` method).
    fn merge_batch(&mut self, states: &[VectorRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        };

        // The states here are returned by the `state` method. Since we only returned a vector
        // with one value in that method, `states[0]` is fine.
        let states = &states[0];
        let states = states.as_any().downcast_ref::<ListVector>().unwrap();
        for state in states.values_iter() {
            let state = state.context(FromScalarValueSnafu)?;
            // merging state is simply accumulate stored numbers from others', so just call update
            self.update_batch(&vec![state])?
        }
        Ok(())
    }

    // DataFusion expects this function to return the final value of this aggregator.
    fn evaluate(&self) -> Result<Value> {
        let median = if self.not_greater.len() > self.greater.len() {
            (*self.not_greater.peek().unwrap()).into()
        } else {
            let not_greater_v: f64 = NumCast::from(*self.not_greater.peek().unwrap()).unwrap();
            let greater_v: f64 = NumCast::from(self.greater.peek().unwrap().0).unwrap();
            let median: T = NumCast::from((not_greater_v + greater_v) / 2.0).unwrap();
            median.into()
        };
        Ok(median)
    }
}

#[derive(Debug, Default)]
pub struct MedianAccumulatorCreator {
    input_type: ArcSwapOption<Vec<ConcreteDataType>>,
}

impl AccumulatorCreator for MedianAccumulatorCreator {
    fn creator(&self) -> AccumulatorCreatorFunction {
        let creator: AccumulatorCreatorFunction = Arc::new(move |types: &[ConcreteDataType]| {
            let input_type = &types[0];
            with_match_ordered_primitive_type_id!(
                input_type.logical_type_id(),
                |$S| {
                    Ok(Box::new(Median::<$S>::default()))
                },
                {
                    let err_msg = format!(
                        "\"MEDIAN\" aggregate function not support date type {:?}",
                        input_type.logical_type_id(),
                    );
                    let err: std::result::Result<(), DataFusionError> = Err(DataFusionError::Execution(err_msg));
                    Err(err.context(ExecuteFunctionSnafu).err().unwrap().into())
                }
            )
        });
        creator
    }

    fn input_types(&self) -> Vec<ConcreteDataType> {
        self.input_type
            .load()
            .as_ref()
            .expect("input_type is not present, check if DataFusion has changed its UDAF execution logic")
            .as_ref()
            .clone()
    }

    fn set_input_types(&self, input_types: Vec<ConcreteDataType>) {
        let old = self.input_type.swap(Some(Arc::new(input_types.clone())));
        if let Some(old) = old {
            assert_eq!(old.len(), input_types.len());
            old.iter().zip(input_types.iter()).for_each(|(x, y)|
                assert_eq!(x, y, "input type {:?} != {:?}, check if DataFusion has changed its UDAF execution logic", x, y)
            );
        }
    }

    fn output_type(&self) -> ConcreteDataType {
        self.input_types().into_iter().next().unwrap()
    }

    fn state_types(&self) -> Vec<ConcreteDataType> {
        vec![ConcreteDataType::list_datatype(self.output_type())]
    }
}
