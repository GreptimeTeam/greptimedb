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

/// [`super::PromPlanner::matchers_to_expr`] emits no filter for this, so copying it would only
/// force a re-plan.
const MATCH_ALL_REGEX: &str = "^(?:.*)$";

/// Rollup functions emitting at most one output series per input series, carrying the labels of
/// the matrix selector they wrap. `absent_over_time` is excluded: it synthesizes a series from
/// the matchers when its input has none.
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
/// equality, so a matcher on a matching label already holds for every surviving pair. Copying it
/// to the other operand can only drop rows that had no partner, whatever the matcher kind, and
/// `NULL` pairs only with `NULL`. Nor can it split a match group, whose series agree on every
/// matching label, so it does not affect a cardinality check such as #9209.
///
/// `left_tags` and `right_tags` are the tag columns of the planned operands: a matcher may just
/// as well constrain a value field, which the parsed expression does not distinguish from a
/// label. For an aggregated operand they are its grouping labels, which is what makes filtering
/// its input equivalent to filtering its output.
pub(super) fn propagate(
    binary: &BinaryExpr,
    left_tags: &[String],
    right_tags: &[String],
) -> Option<BinaryExpr> {
    match try_propagate(binary, left_tags, right_tags) {
        Ok(rewritten) => Some(rewritten),
        Err(reason) => {
            common_telemetry::debug!("Matching filter not propagated ({reason}): {binary}");
            None
        }
    }
}

/// `Err` carries why the rewrite does not apply, for [`propagate`] to log.
fn try_propagate(
    binary: &BinaryExpr,
    left_tags: &[String],
    right_tags: &[String],
) -> Result<BinaryExpr, &'static str> {
    if !matches!(
        binary.op.id(),
        token::T_ADD | token::T_SUB | token::T_MUL | token::T_DIV | token::T_MOD | token::T_POW
    ) {
        return Err("operator is not arithmetic");
    }
    if let Some(modifier) = &binary.modifier {
        if !matches!(modifier.card, VectorMatchCardinality::OneToOne) {
            return Err("matching is not one-to-one");
        }
        if modifier.fill_values.lhs.is_some() || modifier.fill_values.rhs.is_some() {
            return Err("operand carries a fill modifier");
        }
    }
    let matching = binary
        .modifier
        .as_ref()
        .and_then(|modifier| modifier.matching.as_ref());

    let mut rewritten = binary.clone();
    let left = selector_matchers(&mut rewritten.lhs).ok_or("left operand is not a selector")?;
    let right = selector_matchers(&mut rewritten.rhs).ok_or("right operand is not a selector")?;
    if !left.or_matchers.is_empty() || !right.or_matchers.is_empty() {
        return Err("selector has an or matcher group");
    }

    let is_matching_label = |name: &String| {
        // PromQL reserves the `__` prefix; none of those names is a join key.
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
    if !changed {
        return Err("operands already carry the same matching-label matchers");
    }
    Ok(rewritten)
}

fn matches_every_value(matcher: &Matcher) -> bool {
    matches!(&matcher.op, MatchOp::Re(re) if re.as_str() == MATCH_ALL_REGEX)
}

/// Aggregations partitioning their input by the grouping labels, so that dropping input series
/// by a grouping label drops exactly the matching output series and leaves the remaining
/// aggregated values untouched.
///
/// `topk`, `bottomk` and `limitk` rank across a group and carry the input labels through, so
/// filtering before them changes the candidate set. `count_values` adds an output label that
/// does not exist in its input.
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
            // Every other argument is a scalar, so position does not matter.
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
