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

use sqlparser::ast::ObjectName;

use crate::error::{self, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyTable {
    table_name: ObjectName,
    file_name: String,
    format: Format,
}

impl CopyTable {
    pub(crate) fn new(table_name: ObjectName, file_name: String, format: Format) -> Self {
        Self {
            table_name,
            file_name,
            format,
        }
    }

    pub fn table_name(&self) -> &ObjectName {
        &self.table_name
    }

    pub fn file_name(&self) -> &str {
        &self.file_name
    }

    pub fn format(&self) -> &Format {
        &self.format
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Format {
    Parquet,
}

impl TryFrom<String> for Format {
    type Error = error::Error;

    fn try_from(name: String) -> Result<Self> {
        match name.to_uppercase().as_str() {
            "PARQUET" => Ok(Format::Parquet),
            _ => error::UnsupportedCopyFormatOptionSnafu { name }.fail(),
        }
    }
}
