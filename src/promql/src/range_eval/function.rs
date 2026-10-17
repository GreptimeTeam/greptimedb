// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Float range-vector functions as window kernels, sharing their arithmetic with the range UDFs.

use super::{FloatWindow, WindowKernel};
use crate::functions::{add_counter_resets, extrapolate, instant_delta};

/// A PromQL range-vector function over float samples.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RangeFunction {
    Rate,
    Increase,
    Delta,
    Irate,
    Idelta,
}

impl RangeFunction {
    /// Returns the kernel computing this function over one window.
    pub(crate) fn kernel(self) -> WindowKernel {
        match self {
            Self::Rate => Box::new(extrapolated::<true, true>),
            Self::Increase => Box::new(extrapolated::<true, false>),
            Self::Delta => Box::new(extrapolated::<false, false>),
            Self::Irate => Box::new(last_two::<true>),
            Self::Idelta => Box::new(last_two::<false>),
        }
    }
}

fn extrapolated<const IS_COUNTER: bool, const IS_RATE: bool>(
    window: FloatWindow<'_>,
) -> Option<f64> {
    let FloatWindow {
        timestamps, values, ..
    } = window;
    if values.len() < 2 {
        return None;
    }
    let first = values[0];
    let last = values[values.len() - 1];
    let mut result = last - first;
    if IS_COUNTER {
        result = add_counter_resets(result, values);
    }
    Some(extrapolate::<IS_COUNTER, IS_RATE>(
        result,
        first,
        timestamps[0],
        timestamps[timestamps.len() - 1],
        values.len(),
        window.eval_ts,
        window.range_ms,
    ))
}

fn last_two<const IS_RATE: bool>(window: FloatWindow<'_>) -> Option<f64> {
    let [.., prev_ts, last_ts] = window.timestamps else {
        return None;
    };
    let [.., prev, last] = window.values else {
        return None;
    };
    instant_delta::<IS_RATE>((*prev_ts, *prev), (*last_ts, *last))
}
