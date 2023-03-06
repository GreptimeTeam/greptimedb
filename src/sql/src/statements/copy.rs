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
    To(CopyTableTo),
    From(CopyTableFrom),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyTableTo {
    table_name: ObjectName,
    file_name: String,
    format: Format,
}

impl CopyTableTo {
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

// TODO: To combine struct CopyTableFrom and CopyTableTo
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyTableFrom {
    pub table_name: ObjectName,
    pub format: Format,
    pub connection: HashMap<String, String>,
    pub pattern: Option<String>,
    pub from: String,
}

impl CopyTableFrom {
    pub(crate) fn new(
        table_name: ObjectName,
        from: String,
        format: Format,
        pattern: Option<String>,
        connection: HashMap<String, String>,
    ) -> Self {
        CopyTableFrom {
            table_name,
            format,
            connection,
            pattern,
            from,
        }
    }
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
