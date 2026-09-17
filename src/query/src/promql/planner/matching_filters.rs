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

//! Propagation of label matchers between the operands of a PromQL binary expression.

use promql_parser::label::{MatchOp, Matcher, Matchers};
use promql_parser::parser::{
    AggregateExpr, BinaryExpr, Expr, LabelModifier, VectorMatchCardinality, token,
};

/// Regular expression that every label value satisfies. [`super::PromPlanner::matchers_to_expr`]
/// emits no filter for it, so propagating it would only trigger a pointless re-plan.
const MATCH_ALL_REGEX: &str = "^(?:.*)$";

/// Rollup functions that emit at most one output series per input series, carrying the labels
/// of the matrix selector they wrap.
///
/// `absent_over_time` is deliberately absent: it synthesizes a series from the matchers when
/// the input has none, so filtering its input changes its output.
const LABEL_PRESERVING_RANGE_FUNCTIONS: [&str; 20] = [
    "avg_over_time",
    "changes",
    "count_over_time",
    "delta",
    "deriv",
    "double_exponential_smoothing",
    "idelta",
    "increase",
    "irate",
    "last_over_time",
    "max_over_time",
    "min_over_time",
    "predict_linear",
    "present_over_time",
    "quantile_over_time",
    "rate",
    "resets",
    "stddev_over_time",
    "stdvar_over_time",
    "sum_over_time",
];

/// Copies label matchers from one operand of `binary` to the other, so both scans discard
/// non-joining series instead of the join.
///
/// One-to-one arithmetic operands are inner-joined on their matching labels with plain column
/// equality. A matcher on a matching label is therefore already enforced on both sides for
/// every surviving pair: it originates on one operand, and the join propagates it to the pairs
/// it forms. Copying it to the other operand can only drop rows that had no surviving partner,
/// which holds for every matcher kind, including regular expressions, negations, and empty
/// values, and for `NULL` label values, which only ever pair with `NULL`.
///
/// `left_tags` and `right_tags` are the tag columns of the two already planned operands. A
/// selector matcher can also constrain a value field, which the parsed expression does not
/// distinguish from a label, so only names that are tags on both sides are propagated. For an
/// aggregated operand those tags are its grouping labels, which is what makes filtering the
/// aggregate's input equivalent to filtering its output.
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

    let mut rewritten = binary.clone();
    let left = selector_matchers(&mut rewritten.lhs)?;
    let right = selector_matchers(&mut rewritten.rhs)?;
    if !left.or_matchers.is_empty() || !right.or_matchers.is_empty() {
        return None;
    }

    let is_matching_label = |name: &String| {
        // `__name__`, `__field__`, `__schema__` and the metric engine's internal columns are
        // not joined on, and PromQL reserves the `__` prefix.
        !name.starts_with("__")
            && left_tags.contains(name)
            && right_tags.contains(name)
            && match matching {
                None => true,
                Some(LabelModifier::Include(on)) => on.labels.contains(name),
                Some(LabelModifier::Exclude(ignoring)) => !ignoring.labels.contains(name),
            }
    };
    let constraints = left
        .matchers
        .iter()
        .chain(&right.matchers)
        .filter(|matcher| is_matching_label(&matcher.name) && !matches_every_value(matcher))
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

fn matches_every_value(matcher: &Matcher) -> bool {
    matches!(&matcher.op, MatchOp::Re(re) if re.as_str() == MATCH_ALL_REGEX)
}

/// Aggregations that partition their input by the grouping labels, so that dropping input
/// series by a grouping label drops exactly the corresponding output series and leaves the
/// remaining aggregated values untouched.
///
/// `topk`, `bottomk` and `limitk` select series across a group and carry the input labels
/// through, so filtering their input changes which series they return. `count_values` adds an
/// output label that does not exist in its input.
fn partitions_by_grouping_labels(aggregate: &AggregateExpr) -> bool {
    matches!(
        aggregate.op.id(),
        token::T_SUM
            | token::T_AVG
            | token::T_COUNT
            | token::T_MIN
            | token::T_MAX
            | token::T_GROUP
            | token::T_STDDEV
            | token::T_STDVAR
            | token::T_QUANTILE
    )
}

/// Returns the matchers of the vector selector an operand scans, or `None` when the operand's
/// output labels are not proven to be that selector's labels.
fn selector_matchers(expr: &mut Expr) -> Option<&mut Matchers> {
    match expr {
        Expr::VectorSelector(selector) => Some(&mut selector.matchers),
        Expr::Paren(paren) => selector_matchers(&mut paren.expr),
        Expr::Aggregate(aggregate) if partitions_by_grouping_labels(aggregate) => {
            selector_matchers(&mut aggregate.expr)
        }
        Expr::Call(call) if LABEL_PRESERVING_RANGE_FUNCTIONS.contains(&call.func.name) => {
            // The remaining arguments are scalars, so the sole matrix argument carries the
            // labels regardless of where the function takes it.
            let matrix = single_matrix_argument(&call.args.args)?;
            match call.args.args[matrix].as_mut() {
                Expr::MatrixSelector(selector) => Some(&mut selector.vs.matchers),
                _ => None,
            }
        }
        // Label rewriting and subqueries need separate proofs.
        _ => None,
    }
}

fn single_matrix_argument(args: &[Box<Expr>]) -> Option<usize> {
    let mut found = None;
    for (index, arg) in args.iter().enumerate() {
        if matches!(arg.as_ref(), Expr::MatrixSelector(_)) {
            if found.is_some() {
                return None;
            }
            found = Some(index);
        }
    }
    found
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

    #[track_caller]
    fn assert_rewrite(query: &str, expected: &str) {
        assert_eq!(rewrite(query).unwrap(), parse(expected).unwrap(), "{query}");
        // The rewrite adds every constraint to both sides, so re-running it is a no-op.
        assert!(rewrite(expected).is_none(), "{expected}");
    }

    #[test]
    fn propagates_only_explicit_matching_labels() {
        assert_rewrite(
            r#"a / on(host) b{host="x",zone="y"}"#,
            r#"a{host="x"} / on(host) b{host="x",zone="y"}"#,
        );
    }

    #[test]
    fn propagates_labels_not_named_in_ignoring() {
        assert_rewrite(
            r#"a{zone="y"} / ignoring(zone) b{host="x"}"#,
            r#"a{zone="y",host="x"} / ignoring(zone) b{host="x"}"#,
        );
    }

    #[test]
    fn propagates_default_matching_without_metric_name() {
        assert_rewrite(
            r#"a / {host="x",__name__="b"}"#,
            r#"a{host="x"} / {host="x",__name__="b"}"#,
        );
        assert!(rewrite(r#"a / {__name__="b"}"#).is_none());
    }

    #[test]
    fn propagates_only_names_that_are_tags_on_both_sides() {
        // `value` is a field column of both metrics, even when named in `on(...)`.
        for query in [r#"a / b{value="2"}"#, r#"a / on(host,value) b{value="2"}"#] {
            assert!(rewrite(query).is_none(), "{query}");
        }
        assert_rewrite(
            r#"a / b{host="x",value="2"}"#,
            r#"a{host="x"} / b{host="x",value="2"}"#,
        );
        // `zone` is a tag of the right metric only.
        assert!(rewrite_with(r#"a / b{zone="y"}"#, &["host"], &["host", "zone"]).is_none());
    }

    #[test]
    fn propagates_matchers_the_join_enforces_anyway() {
        // The join compares label values directly, so any predicate on a matching label is
        // already satisfied by every surviving pair.
        for (query, expected) in [
            (
                r#"a / b{host=~"x.*"}"#,
                r#"a{host=~"x.*"} / b{host=~"x.*"}"#,
            ),
            (r#"a / b{host!="x"}"#, r#"a{host!="x"} / b{host!="x"}"#),
            (
                r#"a / b{host!~"x.*"}"#,
                r#"a{host!~"x.*"} / b{host!~"x.*"}"#,
            ),
            (r#"a / b{host=""}"#, r#"a{host=""} / b{host=""}"#),
        ] {
            assert_rewrite(query, expected);
        }
        // `=~".*"` lowers to no filter at all, so copying it would only force a re-plan.
        assert!(rewrite(r#"a / b{host=~".*"}"#).is_none());
    }

    #[test]
    fn propagates_through_partitioning_aggregations() {
        assert_rewrite(
            r#"sum by(host) (a) / on(host) max by(host) (b{host="x"})"#,
            r#"sum by(host) (a{host="x"}) / on(host) max by(host) (b{host="x"})"#,
        );
        assert_rewrite(
            r#"avg without(zone) (rate(a[5m])) / b{host="x"}"#,
            r#"avg without(zone) (rate(a{host="x"}[5m])) / b{host="x"}"#,
        );
    }

    #[test]
    fn leaves_selecting_aggregations_alone() {
        // These carry input labels through, so filtering their input changes which series
        // they return.
        for query in [
            r#"topk(3, a) / on(host) b{host="x"}"#,
            r#"bottomk(3, a) / on(host) b{host="x"}"#,
            r#"count_values("v", a) / on(host) b{host="x"}"#,
        ] {
            assert!(rewrite(query).is_none(), "{query}");
        }
    }

    #[test]
    fn finds_the_matrix_argument_of_multi_argument_rollups() {
        assert_rewrite(
            r#"quantile_over_time(0.9, a[5m]) / on(host) b{host="x"}"#,
            r#"quantile_over_time(0.9, a{host="x"}[5m]) / on(host) b{host="x"}"#,
        );
        assert_rewrite(
            r#"predict_linear(a[5m], 60) / on(host) b{host="x"}"#,
            r#"predict_linear(a{host="x"}[5m], 60) / on(host) b{host="x"}"#,
        );
        assert_rewrite(
            r#"double_exponential_smoothing(a[5m], 0.5, 0.5) / on(host) b{host="x"}"#,
            r#"double_exponential_smoothing(a{host="x"}[5m], 0.5, 0.5) / on(host) b{host="x"}"#,
        );
    }

    #[test]
    fn preserves_windows_offsets_and_conflicting_matchers() {
        assert_eq!(
            rewrite(
                r#"rate(a{host="x"}[5m] offset 1h) / on(host) count_over_time(b{host="y"}[1m])"#
            )
            .unwrap(),
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
            r#"a / on(host) group_left b{host="x"}"#,
            r#"sum by(host)(a) / on(host) group_right b{host="x"}"#,
            r#"label_replace(a,"host","x","zone",".*") / on(host) b{host="x"}"#,
            r#"a > on(host) b{host="x"}"#,
            r#"a / on(host) absent_over_time(b{host="x"}[5m])"#,
            r#"a / on(host) (b{host="x"} + b)"#,
        ] {
            assert!(rewrite(query).is_none(), "{query}");
        }
    }
}
