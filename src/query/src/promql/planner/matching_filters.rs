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

//! Propagation of equality matchers between the operands of a PromQL binary expression.

use promql_parser::label::{MatchOp, Matchers};
use promql_parser::parser::{BinaryExpr, Expr, LabelModifier, VectorMatchCardinality, token};

/// Range functions whose result carries exactly the labels of the matrix selector they
/// wrap, and produce a result series only for an input series.
const LABEL_PRESERVING_RANGE_FUNCTIONS: [&str; 9] = [
    "rate",
    "irate",
    "increase",
    "count_over_time",
    "sum_over_time",
    "avg_over_time",
    "min_over_time",
    "max_over_time",
    "last_over_time",
];

/// Copies non-empty equality matchers on matching labels from one operand of `binary` to
/// the other, so both scans discard non-joining series instead of the join.
///
/// One-to-one arithmetic operands are inner-joined on their matching labels, so every row
/// that survives the join already satisfies the other operand's equality matchers on those
/// labels.
///
/// `left_tags` and `right_tags` are the tag columns of the two already planned operands. A
/// selector matcher can also constrain a value field, which the parsed expression does not
/// distinguish from a label, so only names that are tags on both sides are propagated.
///
/// Returns `None` when the expression is outside this subset or nothing was added.
pub(super) fn propagate(
    binary: &BinaryExpr,
    left_tags: &[String],
    right_tags: &[String],
) -> Option<BinaryExpr> {
    if !matches!(
        binary.op.id(),
        token::T_ADD | token::T_SUB | token::T_MUL | token::T_DIV | token::T_MOD | token::T_POW
    ) {
        return None;
    }
    if binary.modifier.as_ref().is_some_and(|modifier| {
        !matches!(modifier.card, VectorMatchCardinality::OneToOne)
            || modifier.fill_values.lhs.is_some()
            || modifier.fill_values.rhs.is_some()
    }) {
        return None;
    }
    let matching = binary
        .modifier
        .as_ref()
        .and_then(|modifier| modifier.matching.as_ref());
    // `ignoring(...)` selects the matching labels by exclusion, so the join keys depend on
    // label sets this function is not given.
    if matches!(matching, Some(LabelModifier::Exclude(_))) {
        return None;
    }

    let mut rewritten = binary.clone();
    let left = selector_matchers(&mut rewritten.lhs)?;
    let right = selector_matchers(&mut rewritten.rhs)?;
    if !left.or_matchers.is_empty() || !right.or_matchers.is_empty() {
        return None;
    }

    let is_matching_label = |name: &String| {
        // `__name__`, `__field__`, `__schema__` and the metric engine's internal columns
        // are not joined on, and PromQL reserves the `__` prefix.
        !name.starts_with("__")
            && left_tags.contains(name)
            && right_tags.contains(name)
            && match matching {
                None => true,
                Some(LabelModifier::Include(on)) => on.labels.contains(name),
                Some(LabelModifier::Exclude(_)) => false,
            }
    };
    let constraints = left
        .matchers
        .iter()
        .chain(&right.matchers)
        .filter(|matcher| {
            // An empty value also matches an absent label, which the join normalizes
            // differently from a scan filter.
            matches!(matcher.op, MatchOp::Equal)
                && !matcher.value.is_empty()
                && is_matching_label(&matcher.name)
        })
        .cloned()
        .collect::<Vec<_>>();

    let mut changed = false;
    for matcher in constraints {
        for target in [&mut left.matchers, &mut right.matchers] {
            if !target.contains(&matcher) {
                target.push(matcher.clone());
                changed = true;
            }
        }
    }
    changed.then_some(rewritten)
}

/// Returns the matchers of the vector selector an operand scans, or `None` when the
/// operand's output labels are not proven to be that selector's labels.
fn selector_matchers(expr: &mut Expr) -> Option<&mut Matchers> {
    match expr {
        Expr::VectorSelector(selector) => Some(&mut selector.matchers),
        Expr::Paren(paren) => selector_matchers(&mut paren.expr),
        Expr::Call(call)
            if call.args.args.len() == 1
                && LABEL_PRESERVING_RANGE_FUNCTIONS.contains(&call.func.name) =>
        {
            match call.args.args[0].as_mut() {
                Expr::MatrixSelector(selector) => Some(&mut selector.vs.matchers),
                // Aggregation, label rewriting and subqueries need separate proofs.
                _ => None,
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use promql_parser::parser::parse;

    use super::*;

    fn rewrite_with(query: &str, left_tags: &[&str], right_tags: &[&str]) -> Option<Expr> {
        let Expr::Binary(binary) = parse(query).unwrap() else {
            panic!("expected binary")
        };
        let owned = |tags: &[&str]| tags.iter().map(|tag| tag.to_string()).collect::<Vec<_>>();
        propagate(&binary, &owned(left_tags), &owned(right_tags)).map(Expr::Binary)
    }

    fn rewrite(query: &str) -> Option<Expr> {
        rewrite_with(query, &["host", "zone"], &["host", "zone"])
    }

    #[test]
    fn propagates_only_explicit_matching_labels() {
        let rewritten = rewrite(r#"a / on(host) b{host="x",zone="y"}"#).unwrap();
        assert_eq!(
            rewritten,
            parse(r#"a{host="x"} / on(host) b{host="x",zone="y"}"#).unwrap()
        );
        assert!(rewrite(&rewritten.to_string()).is_none());
    }

    #[test]
    fn propagates_default_matching_without_metric_name() {
        let rewritten = rewrite(r#"a / {host="x",__name__="b"}"#).unwrap();
        assert_eq!(
            rewritten,
            parse(r#"a{host="x"} / {host="x",__name__="b"}"#).unwrap()
        );
        assert!(rewrite(&rewritten.to_string()).is_none());
        assert!(rewrite(r#"a / {__name__="b"}"#).is_none());
    }

    #[test]
    fn propagates_only_names_that_are_tags_on_both_sides() {
        // `value` is a field column of both metrics, even when named in `on(...)`.
        for query in [r#"a / b{value="2"}"#, r#"a / on(host,value) b{value="2"}"#] {
            assert!(rewrite(query).is_none(), "{query}");
        }
        assert_eq!(
            rewrite(r#"a / b{host="x",value="2"}"#).unwrap(),
            parse(r#"a{host="x"} / b{host="x",value="2"}"#).unwrap()
        );
        // `zone` is a tag of the right metric only.
        assert!(rewrite_with(r#"a / b{zone="y"}"#, &["host"], &["host", "zone"]).is_none());
    }

    #[test]
    fn preserves_windows_offsets_and_conflicting_matchers() {
        let rewritten = rewrite(
            r#"rate(a{host="x"}[5m] offset 1h) / on(host) count_over_time(b{host="y"}[1m])"#,
        )
        .unwrap();
        assert_eq!(
            rewritten,
            parse(
                r#"rate(a{host="x",host="y"}[5m] offset 1h) / on(host) count_over_time(b{host="y",host="x"}[1m])"#
            )
            .unwrap()
        );
    }

    #[test]
    fn leaves_unproven_semantics_unchanged() {
        for query in [
            r#"a or on(host) b{host="x"}"#,
            r#"a / ignoring(zone) b{host="x"}"#,
            r#"a / on(host) group_left b{host="x"}"#,
            r#"a / on(host) b{host=""}"#,
            r#"a / on(host) b{host=~"x.*"}"#,
            r#"sum by(host)(a) / on(host) b{host="x"}"#,
            r#"label_replace(a,"host","x","zone",".*") / on(host) b{host="x"}"#,
            r#"a > on(host) b{host="x"}"#,
            r#"a / on(host) quantile_over_time(0.9, b{host="x"}[5m])"#,
            r#"a / on(host) absent_over_time(b{host="x"}[5m])"#,
        ] {
            assert!(rewrite(query).is_none(), "{query}");
        }
    }
}
