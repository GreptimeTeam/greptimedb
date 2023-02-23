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

use common_function_macro::range_fn;
use datafusion::arrow::array::TimestampMillisecondArray;

/// This is docstring of `IDelta` function.
#[range_fn(name = "IDelta", ret = "Float64Array", display_name = "prom_idelta")]
fn compute(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    let len = values.len();
    let values = values.values();
    if len < 2 {
        None
    } else {
        Some(values[len - 1] - values[len - 2])
    }
}
