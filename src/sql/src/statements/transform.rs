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

use std::ops::ControlFlow;
use std::sync::Arc;

use lazy_static::lazy_static;
use sqlparser::ast::{visit_expressions_mut, Expr};

use crate::error::Result;
use crate::statements::statement::Statement;
mod type_alias;
pub use type_alias::get_data_type_by_alias_name;
use type_alias::TypeAliasTransformRule;

lazy_static! {
    /// [TransformRule] registry
    static ref RULES: Vec<Arc<dyn TransformRule>> = vec![
        Arc::new(TypeAliasTransformRule{}),
    ];
}

/// Transform rule to transform statement or expr
pub(crate) trait TransformRule: Send + Sync {
    /// Visit a [Statement]
    fn visit_statement(&self, _stmt: &mut Statement) -> Result<()> {
        Ok(())
    }

    /// Visit an [Expr]
    fn visit_expr(&self, _expr: &mut Expr) -> ControlFlow<()> {
        ControlFlow::<()>::Continue(())
    }
}

/// Transform statements by rules
pub fn transform_statements(stmts: &mut Vec<Statement>) -> Result<()> {
    for stmt in &mut *stmts {
        for rule in RULES.iter() {
            rule.visit_statement(stmt)?;
        }
    }

    visit_expressions_mut(stmts, |expr| {
        for rule in RULES.iter() {
            rule.visit_expr(expr)?;
        }

        ControlFlow::<()>::Continue(())
    });

    Ok(())
}
