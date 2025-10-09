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

use serde::Serialize;
use sqlparser::ast::{AnalyzeFormat, Statement as SpStatement};
use sqlparser_derive::{Visit, VisitMut};

use crate::error::Error;
use crate::statements::statement::Statement;

/// Explain statement.
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub struct ExplainStatement {
    /// `EXPLAIN ANALYZE ..`
    pub analyze: bool,
    /// `EXPLAIN .. VERBOSE ..`
    pub verbose: bool,
    /// `EXPLAIN .. FORMAT `
    pub format: Option<AnalyzeFormat>,
    /// The statement to analyze. Note this is a Greptime [`Statement`] (not a
    /// [`sqlparser::ast::Statement`] so that we can use
    /// Greptime specific statements
    pub statement: Box<Statement>,
}

impl ExplainStatement {
    pub fn format(&self) -> Option<AnalyzeFormat> {
        self.format
    }
}

impl Display for ExplainStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "EXPLAIN")?;
        if self.analyze {
            write!(f, " ANALYZE")?;
        }
        if self.verbose {
            write!(f, " VERBOSE")?;
        }
        if let Some(format) = &self.format {
            write!(f, " FORMAT {}", format)?;
        }
        write!(f, " {}", self.statement)
    }
}
