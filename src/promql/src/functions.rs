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

mod idelta;
mod increase;

use datafusion::arrow::array::ArrayRef;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ColumnarValue;
pub use idelta::IDelta;
pub use increase::Increase;

pub(crate) fn extract_array(columnar_value: &ColumnarValue) -> Result<ArrayRef, DataFusionError> {
    if let ColumnarValue::Array(array) = columnar_value {
        Ok(array.clone())
    } else {
        Err(DataFusionError::Execution(
            "expect array as input, found scalar value".to_string(),
        ))
    }
}
