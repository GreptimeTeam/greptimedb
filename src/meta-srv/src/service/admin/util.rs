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

use std::collections::HashMap;

use snafu::{OptionExt, ResultExt};

use crate::error::{self, Result};

pub fn extract_cluster_id(params: &HashMap<String, String>) -> Result<u64> {
    params
        .get("cluster_id")
        .map(|id| id.parse::<u64>())
        .context(error::MissingRequiredParameterSnafu {
            param: "cluster_id",
        })?
        .context(error::ParseNumSnafu {
            err_msg: "`cluster_id` is not a valid number",
        })
}
