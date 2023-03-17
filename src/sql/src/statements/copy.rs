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

use crate::error::{self, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopyTable {
    To(CopyTableArgument),
    From(CopyTableArgument),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyTableArgument {
    pub table_name: ObjectName,
    pub format: Format,
    pub connection: HashMap<String, String>,
    pub pattern: Option<String>,
    /// Copy tbl [To|From] 'location'.
    pub location: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Format {
    Parquet,
}

impl TryFrom<String> for Format {
    type Error = error::Error;

    fn try_from(name: String) -> Result<Self> {
        if name.eq_ignore_ascii_case("PARQUET") {
            return Ok(Format::Parquet);
        }
        error::UnsupportedCopyFormatOptionSnafu { name }.fail()
    }
}
