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

use std::collections::BTreeMap;
use std::sync::Mutex;

use datafusion_common::{DataFusionError, Result};
use promql_parser::parser::{BinaryExpr, Expr};
use promql_parser::util::visitor::{ExprVisitorMut, walk_expr_mut};

/// Records only rewrites approved by the main planner with resolved tag/field contexts.
#[derive(Debug, Default)]
pub(crate) struct RewriteCollector(Mutex<BTreeMap<String, Expr>>);

impl RewriteCollector {
    pub(crate) fn record(&self, original: &BinaryExpr, rewritten: &BinaryExpr) -> Result<()> {
        let key = Expr::Binary(original.clone()).to_string();
        let value = Expr::Binary(rewritten.clone());
        let mut entries = self
            .0
            .lock()
            .map_err(|_| DataFusionError::Internal("Research rewrite lock poisoned".into()))?;
        if let Some(previous) = entries.get(&key) {
            if previous != &value {
                return Err(DataFusionError::NotImplemented(
                    "Context-dependent research rewrite".into(),
                ));
            }
        } else {
            entries.insert(key, value);
        }
        Ok(())
    }

    pub(crate) fn rewrite(&self, expr: &Expr) -> Result<Expr> {
        struct Apply<'a>(&'a BTreeMap<String, Expr>);
        impl ExprVisitorMut for Apply<'_> {
            type Error = DataFusionError;
            fn pre_visit(&mut self, expr: &mut Expr) -> Result<bool> {
                if matches!(expr, Expr::Binary(_)) {
                    if let Some(rewritten) = self.0.get(&expr.to_string()) {
                        *expr = rewritten.clone();
                    }
                }
                Ok(true)
            }
        }
        let entries = self
            .0
            .lock()
            .map_err(|_| DataFusionError::Internal("Research rewrite lock poisoned".into()))?;
        let mut result = expr.clone();
        walk_expr_mut(&mut Apply(&entries), &mut result)?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use promql_parser::parser::parse;

    #[test]
    fn replays_both_branches_and_rejects_conflicting_contexts() {
        let collector = RewriteCollector::default();
        let binary = |value: &str| match parse(value).unwrap() {
            Expr::Binary(binary) => binary,
            _ => panic!("binary"),
        };
        collector
            .record(&binary("a / b"), &binary("a{host=\"x\"} / b{host=\"x\"}"))
            .unwrap();
        collector
            .record(&binary("c / d"), &binary("c{host=\"y\"} / d{host=\"y\"}"))
            .unwrap();
        let actual = collector
            .rewrite(&parse("sum(a / b) * sum(c / d)").unwrap())
            .unwrap();
        let expected =
            parse("sum(a{host=\"x\"} / b{host=\"x\"}) * sum(c{host=\"y\"} / d{host=\"y\"})")
                .unwrap();
        assert_eq!(actual, expected);
        assert!(
            collector
                .record(&binary("a / b"), &binary("a{host=\"z\"} / b"))
                .is_err()
        );
    }
}
