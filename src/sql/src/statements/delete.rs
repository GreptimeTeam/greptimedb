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

use crate::error::{Error, InvalidSqlSnafu, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delete {
    table_name: ObjectName,
    selection: Option<Expr>,
}

impl Delete {
    pub fn table_name(&self) -> &ObjectName {
        &self.table_name
    }

    pub fn selection(&self) -> &Option<Expr> {
        &self.selection
    }
}

impl TryFrom<Statement> for Delete {
    type Error = Error;

    fn try_from(stmt: Statement) -> Result<Self> {
        match stmt {
            Statement::Delete {
                table_name,
                using,
                selection,
                returning,
            } => {
                if using.is_some() || returning.is_some() {
                    return InvalidSqlSnafu {
                        msg: "delete sql isn't support using and returning.".to_string(),
                    }
                    .fail();
                }
                match table_name {
                    TableFactor::Table { name, .. } => Ok(Delete {
                        table_name: name,
                        selection,
                    }),
                    _ => InvalidSqlSnafu {
                        msg: "can't find table name, tableFactor is not Table type".to_string(),
                    }
                    .fail(),
                }
            }
            unexp => InvalidSqlSnafu {
                msg: format!("Not expected to be {unexp}"),
            }
            .fail(),
        }
    }
}
