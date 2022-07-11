use std::cmp::Reverse;
use std::collections::BinaryHeap;

use common_query::error::Result;
use common_query::error::{Error, ExecuteFunctionSnafu};
use common_query::logical_plan::Accumulator;
use datafusion_common::ScalarValue;
use datatypes::vectors::VectorRef;
use snafu::ResultExt;

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
#[derive(Debug)]
pub struct Median {
    greater: BinaryHeap<Reverse<u32>>,
    not_greater: BinaryHeap<u32>,
}

/// The implementation for this Median UDAF is modified from DataFusion's geometric mean UDAF
/// example, the comments are left unchanged for future reference.
impl Median {
    pub fn new() -> Self {
        Median {
            greater: BinaryHeap::new(),
            not_greater: BinaryHeap::new(),
        }
    }

    // this function receives one entry per argument of this accumulator.
    // DataFusion calls this function on every row, and expects this function to update the
    // accumulator's state.
    fn update(&mut self, values: &[ScalarValue]) -> Result<()> {
        assert_eq!(1, values.len());
        // this is a one-argument UDAF, and thus we use `0`.
        let value = &values[0];
        match value {
            // here we map `ScalarValue` to our internal state. `UInt32` indicates that this function
            // only accepts UInt32 as its argument (DataFusion does try to coerce arguments to this type)
            //
            // Note that `.map` here ensures that we ignore Nulls.
            ScalarValue::UInt32(e) => e.map(|value| {
                self.push(value);
            }),
            _ => unreachable!(""),
        };
        Ok(())
    }

    fn push(&mut self, value: u32) {
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

    // this function receives states from other accumulators (Vec<ScalarValue>)
    // and updates the accumulator.
    fn merge(&mut self, states: Vec<ScalarValue>) -> Result<()> {
        states
            .into_iter()
            .filter_map(|x| match x {
                ScalarValue::LargeUtf8(e) => e,
                _ => unreachable!(),
            })
            .for_each(|value| {
                value
                    .split(',')
                    .filter_map(|n| n.parse::<u32>().ok())
                    .for_each(|n| self.push(n))
            });
        Ok(())
    }
}

impl Default for Median {
    fn default() -> Self {
        Self::new()
    }
}

// UDAFs are built using the trait `Accumulator`, that offers DataFusion the necessary functions
// to use them.
impl Accumulator for Median {
    // This function serializes our state to `ScalarValue`, which DataFusion uses to pass this
    // state between execution stages. Note that this can be arbitrary data.
    //
    // TODO(LFC) Use List datatype if there's one.
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
    fn update_batch(&mut self, values: &[VectorRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        };
        (0..values[0].len()).try_for_each(|index| {
            let v = values
                .iter()
                .map(|array| {
                    ScalarValue::try_from_array(&array.to_arrow_array(), index)
                        .context(ExecuteFunctionSnafu)
                        .map_err(Error::from)
                })
                .collect::<Result<Vec<_>>>()?;
            self.update(&v)
        })
    }

    // Optimization hint: this trait also supports `update_batch` and `merge_batch`,
    // that can be used to perform these operations on arrays instead of single values.
    // By default, these methods call `update` and `merge` row by row
    fn merge_batch(&mut self, states: &[VectorRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        };
        (0..states[0].len()).try_for_each(|index| {
            let v = states
                .iter()
                .map(|array| {
                    ScalarValue::try_from_array(&array.to_arrow_array(), index)
                        .context(ExecuteFunctionSnafu)
                        .map_err(Error::from)
                })
                .collect::<Result<Vec<_>>>()?;
            self.merge(v)
        })
    }

    // DataFusion expects this function to return the final value of this aggregator.
    fn evaluate(&self) -> Result<ScalarValue> {
        let median = if self.not_greater.len() > self.greater.len() {
            *self.not_greater.peek().unwrap()
        } else {
            (*self.not_greater.peek().unwrap() >> 1) + (self.greater.peek().unwrap().0 >> 1)
        };
        Ok(ScalarValue::from(median))
    }
}
