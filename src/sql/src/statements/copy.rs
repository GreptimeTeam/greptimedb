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
use sqlparser_derive::{Visit, VisitMut};

use crate::statements::OptionMap;

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut)]
pub enum Copy {
    CopyTable(CopyTable),
    CopyDatabase(CopyDatabaseArgument),
}

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut)]
pub enum CopyTable {
    To(CopyTableArgument),
    From(CopyTableArgument),
}

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut)]
pub struct CopyDatabaseArgument {
    pub database_name: ObjectName,
    pub with: OptionMap,
    pub connection: OptionMap,
    pub location: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut)]
pub struct CopyTableArgument {
    pub table_name: ObjectName,
    pub with: OptionMap,
    pub connection: OptionMap,
    /// Copy tbl [To|From] 'location'.
    pub location: String,
}

#[cfg(test)]
impl CopyTableArgument {
    pub fn format(&self) -> Option<String> {
        self.with
            .get(common_datasource::file_format::FORMAT_TYPE)
            .cloned()
            .or_else(|| Some("PARQUET".to_string()))
    }

    pub fn pattern(&self) -> Option<String> {
        self.with
            .get(common_datasource::file_format::FILE_PATTERN)
            .cloned()
    }
}
