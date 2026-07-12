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

use std::fmt::{self, Display, Formatter};

use serde::Serialize;
use sqlparser_derive::{Visit, VisitMut};

use crate::ast::{Ident, ObjectName};

/// Represents a SQL COMMENT statement for adding or removing comments on database objects.
///
/// # Examples
///
/// ```sql
/// COMMENT ON TABLE my_table IS 'This is a table comment';
/// COMMENT ON COLUMN my_table.my_column IS 'This is a column comment';
/// COMMENT ON FLOW my_flow IS NULL;
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub struct Comment {
    pub object: CommentObject,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub enum CommentObject {
    Table(ObjectName),
    Column { table: ObjectName, column: Ident },
    Flow(ObjectName),
}

impl Display for Comment {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "COMMENT ON {} IS ", self.object)?;
        match &self.comment {
            Some(comment) => {
                let escaped = comment.replace('\'', "''");
                write!(f, "'{}'", escaped)
            }
            None => f.write_str("NULL"),
        }
    }
}

impl Display for CommentObject {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            CommentObject::Table(name) => write!(f, "TABLE {}", name),
            CommentObject::Column { table, column } => {
                write!(f, "COLUMN {}.{}", table, column)
            }
            CommentObject::Flow(name) => write!(f, "FLOW {}", name),
        }
    }
}
