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

//! SQL surface of series sampling: `FROM <metric table> SAMPLE SERIES (<fraction>)`.
//!
//! Sampling keeps a deterministic fraction of the *series* of a metric-engine
//! table. It is not row-level `TABLESAMPLE`: a kept series returns every one of
//! its rows, a dropped series returns none.
//!
//! # Why the clause is decoded by hand
//!
//! sqlparser has no node for a series fraction. It parses any `SAMPLE`/`TABLESAMPLE`
//! clause into [`TableSample`] and accepts a sample method word only from the
//! fixed set `ROW`/`BERNOULLI`/`SYSTEM`/`BLOCK`. `SERIES` is not one of them, so
//! the method word falls through to the sample *quantity*, which is parsed as an
//! expression:
//!
//! ```text
//! SELECT * FROM metrics SAMPLE SERIES (1/16)
//!   => TableFactor::Table {
//!        sample: Some(AfterTableAlias(TableSample {
//!          modifier: SAMPLE,
//!          name: None,
//!          quantity: Some(TableSampleQuantity {
//!            parenthesized: false,                       // `SERIES` is not `(`
//!            value: Function { name: SERIES, args: [1/16] }, // so it parses as a call
//!            unit: None,
//!          }),
//!        })) }
//! ```
//!
//! So the existing [`TableSample`] AST does carry our fraction, and this module
//! decodes exactly that shape and strips the clause from the statement.
//! DataFusion's default relation planning drops `sample` on the floor, so a
//! clause we decoded and then left in the AST would be silently ignored; the
//! clause is removed instead and the fraction is applied explicitly.
//!
//! Everything else (`TABLESAMPLE (10 PERCENT)`, `SAMPLE SYSTEM (25 PERCENT)`,
//! `REPEATABLE (42)`, ...) is left untouched: those forms are still planned by
//! DataFusion the same way as before, so this module never silently reinterprets
//! a row-level sampling clause as a series fraction.
//!
//! # Where the fraction goes
//!
//! A metric table is read through a region query: the frontend plans the SQL,
//! ships the region plan to the datanode, and the datanode builds the
//! [`store_api::storage::ScanRequest`] per region. The only channel that rides
//! along is the query context (it is part of the region request header), so the
//! fraction is recorded there as one reserved extension key per table
//! ([`session::hints::series_sample_extension_key`], value `"<num>/<den>"`) and
//! read back where the scan request is built. A locally planned scan
//! (`DfTableProviderAdapter`) gets the same hint on the provider itself.

use std::ops::ControlFlow;
use std::sync::Arc;

use datafusion::datasource::DefaultTableSource;
use datafusion_common::{DataFusionError, TableReference};
use datafusion_expr::TableSource;
use session::context::QueryContext;
use snafu::ResultExt;
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, ObjectName, Statement,
    TableFactor, TableSample, TableSampleKind, TableSampleModifier, Value, VisitMut, VisitorMut,
};
use store_api::storage::SeriesSample;
use table::TableRef;
use table::table::adapter::DfTableProviderAdapter;

use crate::error::{PlanSqlSnafu, Result};

/// The sample method word of `SAMPLE SERIES (<fraction>)`.
pub const SERIES_METHOD: &str = "SERIES";

/// Returns a planning error with a user-visible message.
fn plan_error(message: impl Into<String>) -> crate::error::Error {
    // `Error::DataFusion` is transparent, so this keeps the message intact.
    DataFusionError::Plan(message.into()).into()
}

/// A decoded `SAMPLE SERIES (<fraction>)` clause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeriesSampleClause {
    /// The sampled table, as written in the `FROM` clause.
    pub table: ObjectName,
    /// The fraction of the series to keep.
    pub sample: SeriesSample,
}

/// Removes every `SAMPLE SERIES (<fraction>)` clause of `statement` and returns
/// the sampled tables together with their fractions.
///
/// The returned clauses are removed from the AST: a clause left in place would
/// be dropped by DataFusion's relation planning and the query would silently
/// read every series.
pub fn take_series_samples(statement: &mut Statement) -> Result<Vec<SeriesSampleClause>> {
    let mut collector = SeriesSampleCollector::default();
    // The traversal stops at the first malformed clause.
    let _ = statement.visit(&mut collector);
    if let Some(error) = collector.error {
        return Err(plan_error(error));
    }
    Ok(collector.samples)
}

/// Returns the series fraction recorded for `table_id` in `query_ctx`.
pub fn series_sample_of(query_ctx: &QueryContext, table_id: u32) -> Option<SeriesSample> {
    let key = session::hints::series_sample_extension_key(table_id);
    let (num, den) = session::hints::parse_series_sample_fraction(query_ctx.extension(&key)?)?;
    Some(SeriesSample { num, den })
}

/// Records `sample` as the series fraction of `table_id` in `query_ctx`.
///
/// Two different fractions for one table cannot both be honoured, so the second
/// one is rejected instead of silently overriding the first.
pub fn set_series_sample(
    query_ctx: &mut QueryContext,
    table: &str,
    table_id: u32,
    sample: SeriesSample,
) -> Result<()> {
    if let Some(existing) = series_sample_of(query_ctx, table_id)
        && existing != sample
    {
        return Err(plan_error(format!(
            "conflicting series samples for table `{table}`: {existing} and {sample}; \
             sample a table once per query"
        )));
    }
    query_ctx.set_extension(
        session::hints::series_sample_extension_key(table_id),
        sample.to_string(),
    );
    Ok(())
}

/// Rejects a table the metric-engine series scan cannot sample.
///
/// Series sampling prunes whole series by `tsid`, which only the metric engine
/// keeps for its series index. Rejecting the other engines here means the user
/// gets a planning error instead of a full scan that was asked to be sampled.
pub fn ensure_series_sample_supported(table: &str, engine: &str) -> Result<()> {
    if engine != common_catalog::consts::METRIC_ENGINE {
        return Err(plan_error(format!(
            "SAMPLE SERIES is not supported on table `{table}`: it uses the `{engine}` engine, \
             but series sampling is only supported on metric-engine tables \
             (the sample keeps series, not rows, so it needs the metric series index)"
        )));
    }
    Ok(())
}

/// Returns the table provider of `source`, rejecting relations that cannot be sampled.
fn adapter_of_source(source: &Arc<dyn TableSource>) -> Result<&DfTableProviderAdapter> {
    let unsupported = || {
        plan_error(
            "SAMPLE SERIES is not supported on this relation: only metric-engine base tables can \
             be sampled",
        )
    };
    let Some(source) = source.as_ref().downcast_ref::<DefaultTableSource>() else {
        return Err(unsupported());
    };
    let Some(adapter) = source
        .table_provider
        .downcast_ref::<DfTableProviderAdapter>()
    else {
        return Err(unsupported());
    };
    Ok(adapter)
}

/// Returns the table behind `source`.
pub fn table_of_source(source: &Arc<dyn TableSource>) -> Result<TableRef> {
    Ok(adapter_of_source(source)?.table())
}

/// Records `sample` on the provider of a locally planned scan.
///
/// A region query gets the fraction from the query context, but a scan planned
/// in this process (a datanode scanning its own table) reads its provider
/// directly, so the hint is set there as well.
pub fn apply_series_sample(source: &Arc<dyn TableSource>, sample: SeriesSample) -> Result<()> {
    adapter_of_source(source)?.with_series_sample(sample);
    Ok(())
}

/// The `SeriesSampleCollector` receives every table factor of a statement.
#[derive(Default)]
struct SeriesSampleCollector {
    samples: Vec<SeriesSampleClause>,
    error: Option<String>,
}

impl VisitorMut for SeriesSampleCollector {
    type Break = ();

    fn pre_visit_table_factor(&mut self, table_factor: &mut TableFactor) -> ControlFlow<()> {
        let TableFactor::Table { name, sample, .. } = table_factor else {
            return ControlFlow::Continue(());
        };
        let Some(kind) = sample.as_ref() else {
            return ControlFlow::Continue(());
        };
        let table_sample = match kind {
            TableSampleKind::BeforeTableAlias(sample)
            | TableSampleKind::AfterTableAlias(sample) => sample.as_ref(),
        };
        if !is_series_sample(table_sample) {
            // Another sampling form: DataFusion plans it exactly as it did before.
            return ControlFlow::Continue(());
        }
        match decode_series_sample(table_sample) {
            Ok(decoded) => self.samples.push(SeriesSampleClause {
                table: name.clone(),
                sample: decoded,
            }),
            Err(reason) => {
                self.error = Some(reason);
                return ControlFlow::Break(());
            }
        }
        *sample = None;
        ControlFlow::Continue(())
    }
}

/// Returns whether `table_sample` is the `SAMPLE SERIES (<fraction>)` shape.
pub fn is_series_sample(table_sample: &TableSample) -> bool {
    if table_sample.modifier != TableSampleModifier::Sample
        || table_sample.name.is_some()
        || table_sample.bucket.is_some()
        || table_sample.seed.is_some()
        || table_sample.offset.is_some()
    {
        return false;
    }
    let Some(quantity) = table_sample.quantity.as_ref() else {
        return false;
    };
    // `SERIES (1/16)` is one expression: the method word parses as a function
    // call, so the quantity itself is not parenthesized and carries no unit.
    if quantity.parenthesized || quantity.unit.is_some() {
        return false;
    }
    series_method_args(&quantity.value).is_some()
}

/// Returns the single argument of a `SERIES(<arg>)` call.
fn series_method_args(expr: &Expr) -> Option<&Expr> {
    let Expr::Function(function) = expr else {
        return None;
    };
    let name = function.name.to_string();
    if !name.eq_ignore_ascii_case(SERIES_METHOD) {
        return None;
    }
    let FunctionArguments::List(list) = &function.args else {
        return None;
    };
    if list.duplicate_treatment.is_some() || !list.clauses.is_empty() || list.args.len() != 1 {
        return None;
    }
    let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = &list.args[0] else {
        return None;
    };
    Some(expr)
}

/// Decodes the fraction of a `SAMPLE SERIES (<fraction>)` clause.
fn decode_series_sample(table_sample: &TableSample) -> std::result::Result<SeriesSample, String> {
    let Some(quantity) = table_sample.quantity.as_ref() else {
        return Err(
            "invalid series sample: `SAMPLE SERIES` needs a fraction, e.g. \
                    `SAMPLE SERIES (1/16)`"
                .to_string(),
        );
    };
    let Some(expr) = series_method_args(&quantity.value) else {
        return Err(format!(
            "invalid series sample `{expr}`: `SAMPLE SERIES` takes one fraction, e.g. \
             `SAMPLE SERIES (1/16)` or `SAMPLE SERIES (0.0625)`",
            expr = quantity.value
        ));
    };
    fraction_from_expr(expr)
}

/// Decodes a fraction written as `num/den` or as a decimal.
fn fraction_from_expr(expr: &Expr) -> std::result::Result<SeriesSample, String> {
    match expr {
        Expr::Nested(inner) => fraction_from_expr(inner),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Divide,
            right,
        } => {
            let num = integer_literal(left)?;
            let den = integer_literal(right)?;
            validate_fraction(num, den)
        }
        Expr::Value(value) => {
            let Value::Number(text, _) = &value.value else {
                return Err(format!(
                    "invalid series sample `{value}`: expected a fraction such as `1/16` or a \
                     decimal such as `0.0625`"
                ));
            };
            let Some((num, den)) = decimal_fraction(text) else {
                return Err(format!(
                    "invalid series sample `{text}`: expected a fraction such as `1/16` or a \
                     decimal such as `0.0625`"
                ));
            };
            validate_fraction(num, den)
        }
        other => Err(format!(
            "invalid series sample `{other}`: expected a fraction such as `1/16` or a decimal \
             such as `0.0625`"
        )),
    }
}

/// Reads `n` as an unsigned integer literal.
fn integer_literal(expr: &Expr) -> std::result::Result<u64, String> {
    match expr {
        Expr::Nested(inner) => integer_literal(inner),
        Expr::Value(value) => match &value.value {
            Value::Number(text, _) if text.chars().all(|c| c.is_ascii_digit()) => text
                .parse::<u64>()
                .map_err(|_| format!("invalid series sample `{text}`: the value is too large")),
            _ => Err(format!(
                "invalid series sample `{value}`: the numerator and the denominator must be \
                 unsigned integers"
            )),
        },
        other => Err(format!(
            "invalid series sample `{other}`: the numerator and the denominator must be \
             unsigned integers"
        )),
    }
}

/// Turns the digits of a decimal literal into an exact fraction.
fn decimal_fraction(text: &str) -> Option<(u64, u64)> {
    let (integral, fractional) = match text.split_once('.') {
        Some((integral, fractional)) => (integral, fractional),
        None => (text, ""),
    };
    if integral.is_empty() && fractional.is_empty() {
        return None;
    }
    if !integral.chars().all(|c| c.is_ascii_digit())
        || !fractional.chars().all(|c| c.is_ascii_digit())
    {
        return None;
    }
    // A fraction with more decimal places than this cannot be represented
    // exactly in `u64` (nor reduced into a `u32` denominator).
    if fractional.len() > 9 {
        return None;
    }
    let digits = format!("{integral}{fractional}");
    let num = digits.parse::<u64>().ok()?;
    let den = 10u64.checked_pow(fractional.len() as u32)?;
    Some((num, den))
}

/// Validates and reduces a fraction to the lowest terms.
fn validate_fraction(num: u64, den: u64) -> std::result::Result<SeriesSample, String> {
    if den == 0 {
        return Err(format!(
            "invalid series sample {num}/{den}: the denominator must be greater than 0"
        ));
    }
    if num == 0 {
        return Err(format!(
            "invalid series sample {num}/{den}: the fraction must be greater than 0 and less \
             than 1, e.g. `SAMPLE SERIES (1/16)`"
        ));
    }
    if num >= den {
        return Err(format!(
            "invalid series sample {num}/{den}: the fraction must be less than 1, e.g. \
             `SAMPLE SERIES (1/16)`; 1 or more would keep every series"
        ));
    }
    let divisor = gcd(num, den);
    let (num, den) = (num / divisor, den / divisor);
    if num > u32::MAX.into() || den > u32::MAX.into() {
        return Err(format!(
            "invalid series sample {num}/{den}: the numerator and the denominator must fit in 32 \
             bits"
        ));
    }
    Ok(SeriesSample {
        num: num as u32,
        den: den as u32,
    })
}

fn gcd(a: u64, b: u64) -> u64 {
    if b == 0 { a } else { gcd(b, a % b) }
}

/// Converts a table name of the `FROM` clause to a table reference.
pub fn table_reference(
    name: &ObjectName,
    enable_ident_normalization: bool,
) -> Result<TableReference> {
    datafusion_sql::planner::object_name_to_table_reference(
        name.clone(),
        enable_ident_normalization,
    )
    .context(PlanSqlSnafu)
}

#[cfg(test)]
mod tests {
    use common_catalog::consts::{METRIC_ENGINE, MITO_ENGINE};
    use sql::dialect::GreptimeDbDialect;

    use super::*;

    fn parse(sql: &str) -> Statement {
        sqlparser::parser::Parser::parse_sql(&GreptimeDbDialect {}, sql)
            .unwrap()
            .pop()
            .unwrap()
    }

    fn samples_of(sql: &str) -> Vec<(String, SeriesSample)> {
        let mut statement = parse(sql);
        take_series_samples(&mut statement)
            .unwrap()
            .into_iter()
            .map(|clause| (clause.table.to_string(), clause.sample))
            .collect()
    }

    #[test]
    fn test_take_series_sample_division() {
        assert_eq!(
            vec![("metrics".to_string(), SeriesSample { num: 1, den: 16 })],
            samples_of("SELECT * FROM metrics SAMPLE SERIES (1/16)")
        );
    }

    #[test]
    fn test_take_series_sample_decimal() {
        assert_eq!(
            vec![("metrics".to_string(), SeriesSample { num: 1, den: 16 })],
            samples_of("SELECT ts, v FROM metrics SAMPLE SERIES (0.0625) WHERE v > 0")
        );
    }

    #[test]
    fn test_take_series_sample_after_alias() {
        assert_eq!(
            vec![("metrics".to_string(), SeriesSample { num: 1, den: 4 })],
            samples_of("SELECT m.v FROM metrics AS m SAMPLE SERIES (2/8)")
        );
    }

    #[test]
    fn test_take_series_sample_is_removed_from_the_statement() {
        let mut statement = parse("SELECT * FROM metrics SAMPLE SERIES (1/16)");
        take_series_samples(&mut statement).unwrap();
        let rendered = statement.to_string();
        assert!(!rendered.to_uppercase().contains("SAMPLE"), "{rendered}");
    }

    #[test]
    fn test_tablesample_is_left_alone() {
        for sql in [
            "SELECT COUNT(*) FROM t TABLESAMPLE (10 PERCENT)",
            "SELECT COUNT(*) FROM t TABLESAMPLE SYSTEM (25 PERCENT)",
            "SELECT COUNT(*) FROM t TABLESAMPLE (100 ROWS)",
            "SELECT COUNT(*) FROM t SAMPLE (10)",
        ] {
            let mut statement = parse(sql);
            assert!(
                take_series_samples(&mut statement).unwrap().is_empty(),
                "{sql}"
            );
            assert_eq!(sql, statement.to_string());
        }
    }

    #[test]
    fn test_invalid_fractions_are_rejected() {
        for (sql, expected) in [
            (
                "SELECT * FROM metrics SAMPLE SERIES (0)",
                "the fraction must be greater than 0 and less than 1",
            ),
            (
                "SELECT * FROM metrics SAMPLE SERIES (0/16)",
                "the fraction must be greater than 0 and less than 1",
            ),
            (
                "SELECT * FROM metrics SAMPLE SERIES (1/1)",
                "the fraction must be less than 1",
            ),
            (
                "SELECT * FROM metrics SAMPLE SERIES (3/2)",
                "the fraction must be less than 1",
            ),
            (
                "SELECT * FROM metrics SAMPLE SERIES (1.5)",
                "the fraction must be less than 1",
            ),
            (
                "SELECT * FROM metrics SAMPLE SERIES (1/0)",
                "the denominator must be greater than 0",
            ),
            (
                "SELECT * FROM metrics SAMPLE SERIES (x/16)",
                "the numerator and the denominator must be unsigned integers",
            ),
        ] {
            let mut statement = parse(sql);
            let error = take_series_samples(&mut statement).unwrap_err().to_string();
            assert!(error.contains(expected), "{sql}: {error}");
        }
    }

    #[test]
    fn test_non_metric_table_is_rejected() {
        let error = ensure_series_sample_supported("classic", MITO_ENGINE)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("SAMPLE SERIES is not supported on table `classic`"),
            "{error}"
        );
        assert!(error.contains(MITO_ENGINE), "{error}");
        ensure_series_sample_supported("metrics", METRIC_ENGINE).unwrap();
    }

    #[test]
    fn test_query_context_roundtrip() {
        let mut query_ctx = (*QueryContext::arc()).clone();
        assert_eq!(None, series_sample_of(&query_ctx, 42));
        set_series_sample(
            &mut query_ctx,
            "metrics",
            42,
            SeriesSample { num: 1, den: 16 },
        )
        .unwrap();
        assert_eq!(
            Some(SeriesSample { num: 1, den: 16 }),
            series_sample_of(&query_ctx, 42)
        );
        // Another table of the same query is untouched.
        assert_eq!(None, series_sample_of(&query_ctx, 43));
        // The recorded key survives query-option validation (i.e. is reserved).
        query_ctx.effective_query_options().unwrap();
        // And it rides the wire format of the region request header.
        let wire: api::v1::QueryContext = (&query_ctx).into();
        let decoded = QueryContext::try_from(wire).unwrap();
        assert_eq!(
            Some(SeriesSample { num: 1, den: 16 }),
            series_sample_of(&decoded, 42)
        );
    }

    #[test]
    fn test_conflicting_samples_for_one_table_are_rejected() {
        let mut query_ctx = (*QueryContext::arc()).clone();
        set_series_sample(
            &mut query_ctx,
            "metrics",
            42,
            SeriesSample { num: 1, den: 16 },
        )
        .unwrap();
        let error = set_series_sample(
            &mut query_ctx,
            "metrics",
            42,
            SeriesSample { num: 1, den: 4 },
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("conflicting series samples"), "{error}");
    }
}
