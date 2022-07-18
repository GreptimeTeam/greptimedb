use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use common_query::error::{ExecuteFunctionSnafu, Result};
use common_query::logical_plan::{Accumulator, AccumulatorCreator};
use common_query::prelude::*;
use datafusion_common::DataFusionError;
use datatypes::prelude::*;
use datatypes::vectors::{PrimitiveVector, StringVector, VectorRef};
use num::NumCast;
use snafu::ResultExt;

macro_rules! with_match_ordered_primitive_type_id {
    ($key_type:expr, | $_:tt $T:ident | $body:tt, $nbody:tt) => {{
        macro_rules! __with_ty__ {
            ( $_ $T:ident ) => {
                $body
            };
        }

        match $key_type {
            LogicalTypeId::Int8 => __with_ty__! { i8 },
            LogicalTypeId::Int16 => __with_ty__! { i16 },
            LogicalTypeId::Int32 => __with_ty__! { i32 },
            LogicalTypeId::Int64 => __with_ty__! { i64 },
            LogicalTypeId::UInt8 => __with_ty__! { u8 },
            LogicalTypeId::UInt16 => __with_ty__! { u16 },
            LogicalTypeId::UInt32 => __with_ty__! { u32 },
            LogicalTypeId::UInt64 => __with_ty__! { u64 },

            _ => $nbody,
        }
    }};
}

// This median calculation algorithm's details can be found at
// https://leetcode.cn/problems/find-median-from-data-stream/
//
// Basically, it uses two heaps, a maximum heap and a minimum. The maximum heap stores numbers that
// are not greater than the median, and the minimum heap stores the greater. In a steaming of
// numbers, when a number is arrived, we adjust the heaps' tops, so that either one top is the
// median or both tops can be averaged to get the median.
//
// The time complexity to update the median is O(logn), O(1) to get the median; and the space
// complexity is O(n). (Ignore the costs for heap expansion.)
//
// Why not use the [quick select](https://en.wikipedia.org/wiki/Quickselect) or just simply sort to
// calculate the median? Because both ways need to either modified the stored internal state in the
// final `evaluate` in `Accumulator`, which requires a mutable `self` and is against the method's
// signature; or worse, clone the whole stored numbers.
#[derive(Debug, Default)]
pub struct Median<T>
where
    // Bound `FromStr` because we serialize state to string.
    T: Primitive + Ord + FromStr,
{
    greater: BinaryHeap<Reverse<T>>,
    not_greater: BinaryHeap<T>,
}

impl<T> Median<T>
where
    T: Primitive + Ord + FromStr,
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
    T: Primitive + Ord + FromStr,
{
    // This function serializes our state to `ScalarValue`, which DataFusion uses to pass this
    // state between execution stages. Note that this can be arbitrary data.
    //
    // The `ScalarValue`s returned here will be passed in as argument `states: &[VectorRef]` to
    // `merge_batch` function, so we cannot use "`ScalarValue::List`" here because there are no
    // corresponding vectors.
    fn state(&self) -> Result<Vec<ScalarValue>> {
        let nums = self
            .greater
            .iter()
            .map(|x| x.0)
            .chain(self.not_greater.iter().copied())
            .map(|n| n.to_string())
            .collect::<Vec<String>>()
            .join(",");
        Ok(vec![ScalarValue::LargeUtf8(Some(nums))])
    }

    // DataFusion calls this function to update the accumulator's state for a batch of inputs rows.
    // It is expected this function to update the accumulator's state.
    fn update_batch(&mut self, values: &[VectorRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        };

        // This is a unary accumulator, so only one column is provided.
        let column = &values[0];
        // DataFusion does try to coerce arguments to Accumulator's return type.
        let column = column
            .as_any()
            .downcast_ref::<PrimitiveVector<T>>()
            .unwrap();
        for v in column.iter().flatten() {
            self.push(*v);
        }
        Ok(())
    }

    // DataFusion executes accumulators in partitions. In some execution stage, DataFusion will
    // merge states from other accumulators (returned by `state()` method).
    fn merge_batch(&mut self, states: &[VectorRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        };

        let states = &states[0];
        let states = states.as_any().downcast_ref::<StringVector>().unwrap();
        for s in states.iter().flatten() {
            s.split(',')
                .filter_map(|n| n.parse::<T>().ok())
                .for_each(|n| self.push(n));
        }
        Ok(())
    }

    // DataFusion expects this function to return the final value of this aggregator.
    fn evaluate(&self) -> Result<ScalarValue> {
        let median = if self.not_greater.len() > self.greater.len() {
            (*self.not_greater.peek().unwrap()).into()
        } else {
            let not_greater_v: f64 = NumCast::from(*self.not_greater.peek().unwrap()).unwrap();
            let greater_v: f64 = NumCast::from(self.greater.peek().unwrap().0).unwrap();
            let median: T = NumCast::from(not_greater_v / 2.0 + greater_v / 2.0).unwrap();
            median.into()
        };
        Ok(ScalarValue::from(median))
    }
}

#[derive(Debug, Default)]
pub struct MedianAccumulatorCreator {
    input_type: Arc<Mutex<Option<ConcreteDataType>>>,
}

impl AccumulatorCreator for MedianAccumulatorCreator {
    fn creator(&self) -> AccumulatorCreatorFunc {
        let creator: AccumulatorCreatorFunc = Arc::new(move |input_type: &ConcreteDataType| {
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

    fn get_input_type(&self) -> ConcreteDataType {
        self.input_type
            .lock()
            .unwrap()
            .as_ref()
            .expect("input_type is not present, check if DataFusion has changed its UDAF execution logic")
            .clone()
    }

    fn set_input_type(&self, input_type: ConcreteDataType) {
        let mut input_type_holder = self.input_type.lock().unwrap();
        if let Some(old) = input_type_holder.replace(input_type.clone()) {
            assert_eq!(
                old, input_type,
                "input type {:?} != {:?}, check if DataFusion has changed its UDAF execution logic",
                old, input_type
            );
        }
    }

    fn get_output_type(&self) -> ConcreteDataType {
        self.get_input_type()
    }

    fn get_state_types(&self) -> Vec<ConcreteDataType> {
        vec![ConcreteDataType::string_datatype()]
    }
}
