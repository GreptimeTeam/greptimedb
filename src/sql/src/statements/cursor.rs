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

use std::fmt::Display;

use sqlparser::ast::ObjectName;
use sqlparser_derive::{Visit, VisitMut};

use super::query::Query;

/// Represents a DECLARE CURSOR statement
///
/// This statement will carry a SQL query
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut)]
pub struct DeclareCursor {
    pub cursor_name: ObjectName,
    pub query: Box<Query>,
}

impl Display for DeclareCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DECLARE {} CURSOR FOR {}", self.cursor_name, self.query)
    }
}

/// Represents a FETCH FROM cursor statement
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut)]
pub struct FetchCursor {
    pub cursor_name: ObjectName,
    pub fetch_size: u64,
}

impl Display for FetchCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FETCH {} FROM {}", self.fetch_size, self.cursor_name)
    }
}

/// Represents a CLOSE cursor statement
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut)]
pub struct CloseCursor {
    pub cursor_name: ObjectName,
}

impl Display for CloseCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CLOSE {}", self.cursor_name)
    }
}
