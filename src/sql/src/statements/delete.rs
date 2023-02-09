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

use sqlparser::ast::{Expr, ObjectName, Statement, TableFactor};
use sqlparser::parser::ParserError;

use crate::error::Result;
use crate::statements::table_idents_to_full_name;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delete {
    // Can only be sqlparser::ast::Statement::Delete variant
    pub inner: Statement,
}

impl Delete {
    pub fn full_table_name(&self) -> Result<(String, String, String)> {
        match &self.inner {
            Statement::Delete {
                table_name: TableFactor::Table { name, .. },
                ..
            } => table_idents_to_full_name(name),
            _ => unreachable!(),
        }
    }

    pub fn table_name(&self) -> &ObjectName {
        match &self.inner {
            Statement::Delete {
                table_name: TableFactor::Table { name, .. },
                ..
            } => name,
            _ => unreachable!(),
        }
    }

    pub fn selection(&self) -> &Option<Expr> {
        match &self.inner {
            Statement::Delete { selection, .. } => selection,
            _ => unreachable!(),
        }
    }
}

impl TryFrom<Statement> for Delete {
    type Error = ParserError;

    fn try_from(stmt: Statement) -> std::result::Result<Self, Self::Error> {
        match stmt {
            Statement::Delete { .. } => Ok(Delete { inner: stmt }),
            unexp => Err(ParserError::ParserError(format!(
                "Not expected to be {unexp}"
            ))),
        }
    }
}
