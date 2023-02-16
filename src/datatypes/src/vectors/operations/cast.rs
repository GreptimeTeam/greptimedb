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

macro_rules! cast_non_constant {
    ($vector: expr, $VectorType: ty, $to_type: expr) => {{
        use std::sync::Arc;

        use arrow::compute;
        use snafu::ResultExt;

        use crate::data_type::DataType;

        let arrow_array = $vector.to_arrow_array();
        let casted = compute::cast(&arrow_array, &$to_type.as_arrow_type())
            .context(crate::error::ArrowComputeSnafu)?;
        Ok(Arc::new(<$VectorType>::try_from_arrow_array(casted)?))
    }};
}

pub(crate) use cast_non_constant;
