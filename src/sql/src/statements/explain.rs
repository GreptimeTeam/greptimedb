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

/// Explain statement.
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub struct Explain {
    pub inner: SpStatement,
}

impl Explain {
    pub fn format(&self) -> Option<AnalyzeFormat> {
        match self.inner {
            SpStatement::Explain { format, .. } => format,
            _ => None,
        }
    }
}

impl TryFrom<SpStatement> for Explain {
    type Error = Error;

    fn try_from(value: SpStatement) -> Result<Self, Self::Error> {
        Ok(Explain { inner: value })
    }
}

impl Display for Explain {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}
