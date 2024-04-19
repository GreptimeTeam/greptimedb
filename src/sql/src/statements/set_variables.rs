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

use sqlparser::ast::{Expr, ObjectName};
use sqlparser_derive::{Visit, VisitMut};

/// SET variables statement.
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut)]
pub struct SetVariables {
    pub variable: ObjectName,
    pub value: Vec<Expr>,
}

impl SetVariables {
    pub fn variable(&self) -> &ObjectName {
        &self.variable
    }

    pub fn format_value(&self) -> String {
        // The number of value is always one.
        self.value
            .iter()
            .map(|expr| format!("{}", expr))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

impl Display for SetVariables {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let variable = self.variable();
        let value = self.format_value();
        write!(f, r#"SET {variable} = {value}"#)
    }
}
