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

use sqlparser::ast::ObjectName;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopyTable {
    To(CopyTableArgument),
    From(CopyTableArgument),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyTableArgument {
    pub table_name: ObjectName,
    pub with: HashMap<String, String>,
    pub connection: HashMap<String, String>,
    /// Copy tbl [To|From] 'location'.
    pub location: String,
}

#[cfg(test)]
impl CopyTableArgument {
    const FORMAT: &str = "format";

    pub fn format(&self) -> Option<String> {
        self.with
            .get(Self::FORMAT)
            .cloned()
            .or_else(|| Some("PARQUET".to_string()))
    }

    pub fn pattern(&self) -> Option<String> {
        self.with.get("PATTERN").cloned()
    }
}
