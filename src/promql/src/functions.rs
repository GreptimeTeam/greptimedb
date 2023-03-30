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

mod aggr_over_time;
mod changes;
mod deriv;
mod extrapolate_rate;
mod idelta;
mod quantile;
mod resets;
#[cfg(test)]
mod test_util;

pub use aggr_over_time::{
    AbsentOverTime, AvgOverTime, CountOverTime, LastOverTime, MaxOverTime, MinOverTime,
    PresentOverTime, SumOverTime,
};
use datafusion::arrow::array::ArrayRef;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ColumnarValue;
pub use extrapolate_rate::{Delta, Increase, Rate};
pub use idelta::IDelta;
pub use quantile::QuantileOverTime;

pub(crate) fn extract_array(columnar_value: &ColumnarValue) -> Result<ArrayRef, DataFusionError> {
    if let ColumnarValue::Array(array) = columnar_value {
        Ok(array.clone())
    } else {
        Err(DataFusionError::Execution(
            "expect array as input, found scalar value".to_string(),
        ))
    }
}
