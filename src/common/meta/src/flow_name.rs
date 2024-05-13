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

use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

/// The owned flow name.
#[derive(Debug, Clone, Hash, Eq, PartialEq, Deserialize, Serialize)]
pub struct FlowName {
    pub catalog_name: String,
    pub flow_name: String,
}

impl Display for FlowName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&common_catalog::format_full_flow_name(
            &self.catalog_name,
            &self.flow_name,
        ))
    }
}
