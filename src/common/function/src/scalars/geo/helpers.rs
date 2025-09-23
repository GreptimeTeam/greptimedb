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

use datafusion::arrow::array::{ArrayRef, ArrowPrimitiveType};
use datafusion::arrow::compute;

macro_rules! ensure_and_coerce {
    ($compare:expr, $coerce:expr) => {{
        if !$compare {
            return Err(datafusion_common::DataFusionError::Execution(
                "argument out of valid range".to_string(),
            ));
        }
        Ok(Some($coerce))
    }};
}

pub(crate) use ensure_and_coerce;

pub(crate) fn cast<T: ArrowPrimitiveType>(array: &ArrayRef) -> datafusion_common::Result<ArrayRef> {
    let x = compute::cast_with_options(
        array.as_ref(),
        &T::DATA_TYPE,
        &compute::CastOptions {
            safe: false,
            ..Default::default()
        },
    )?;
    Ok(x)
}
