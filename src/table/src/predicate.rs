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

use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arc_swap::ArcSwap;
use common_recordbatch::DfRecordBatch;
use common_recordbatch::filter::SimpleFilterEvaluator;
use common_telemetry::{debug, warn};
use common_time::Timestamp;
use common_time::range::TimestampRange;
use common_time::timestamp::TimeUnit;
use datafusion::common::ScalarValue;
use datafusion::error::Result as DfResult;
use datafusion::physical_optimizer::pruning::PruningPredicateBuilder;
use datafusion_common::pruning::PruningStatistics;
use datafusion_common::tree_node::TreeNode;
use datafusion_common::{DFSchema, DataFusionError, ToDFSchema};
use datafusion_expr::ColumnarValue;
use datafusion_expr::expr::{Expr, InList, ScalarFunction};
use datafusion_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion_expr::{Between, BinaryExpr, Operator};
use datafusion_physical_expr::execution_props::ExecutionProps;
use datafusion_physical_expr::expressions::{
    BinaryExpr as PhysicalBinaryExpr, DynamicFilterPhysicalExpr,
};
use datafusion_physical_expr::{PhysicalExpr, create_physical_expr};
use datatypes::arrow;
use datatypes::arrow::array::{Array, BooleanArray};
use datatypes::arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema};
use datatypes::value::scalar_value_to_timestamp;
use snafu::ResultExt;

use crate::error;

#[cfg(test)]
mod stats;

/// Assert the scalar value is not utf8. Returns `None` if it's utf8.
/// In theory, it should be converted to a timestamp scalar value by `TypeConversionRule`.
macro_rules! return_none_if_utf8 {
    ($lit: ident) => {
        if is_string_timestamp_literal($lit) {
            warn!(
                "Unexpected ScalarValue::Utf8 in time range predicate: {:?}. Maybe it's an implicit bug, please report it to https://github.com/GreptimeTeam/greptimedb/issues",
                $lit
            );

            // Make the predicate ineffective.
            return None;
        }
    };
}

pub fn is_string_timestamp_literal(scalar: &ScalarValue) -> bool {
    matches!(
        scalar,
        ScalarValue::Utf8(_) | ScalarValue::LargeUtf8(_) | ScalarValue::Utf8View(_)
    )
}

/// Reference-counted pointer to a list of logical exprs and a list of dynamic filter physical exprs.
#[derive(Debug, Clone, Default)]
pub struct Predicate {
    /// logical exprs
    exprs: Arc<Vec<Expr>>,
    /// dynamic filter physical exprs, only useful if dynamic filtering is enabled
    ///
    /// They are usually from `TopK` or `Join` operators, and can dynamically filter data during query execution by using current runtime information to further reduce data scanning
    dyn_filters: Arc<ArcSwap<Vec<Arc<DynamicFilterPhysicalExpr>>>>,
}

impl Predicate {
    /// Creates a new `Predicate` by converting logical exprs to physical exprs that can be
    /// evaluated against record batches.
    /// Returns error when failed to convert exprs.
    pub fn new(exprs: Vec<Expr>) -> Self {
        Self {
            exprs: Arc::new(exprs),
            dyn_filters: Arc::new(ArcSwap::new(Arc::new(vec![]))),
        }
    }

    pub fn with_dyn_filters(
        exprs: Vec<Expr>,
        dyn_filters: Vec<Arc<DynamicFilterPhysicalExpr>>,
    ) -> Self {
        Self {
            exprs: Arc::new(exprs),
            dyn_filters: Arc::new(ArcSwap::new(Arc::new(dyn_filters))),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.exprs.is_empty() && self.dyn_filters.load().is_empty()
    }

    /// Adds dynamic filter physical exprs to the existing list.
    pub fn add_dyn_filters(&self, dyn_filters: Vec<Arc<DynamicFilterPhysicalExpr>>) {
        self.dyn_filters.rcu(|existing| {
            let mut new_filters = existing.as_ref().clone();
            new_filters.extend(dyn_filters.clone());
            Arc::new(new_filters)
        });
    }

    /// Removes dynamic filters while preserving the static logical expressions.
    pub fn clear_dyn_filters(&self) {
        self.dyn_filters.store(Arc::new(vec![]));
    }

    /// Returns the logical exprs.
    pub fn exprs(&self) -> &[Expr] {
        &self.exprs
    }

    /// Returns the dynamic filter physical exprs. Notice this return a live dynamic filters which
    /// can change during query execution.
    ///
    /// # Precondition: pruning only, never row filtering
    ///
    /// A consumer may only use these expressions to *prune*: to skip a region, a file or a
    /// row group whose statistics prove that no row can match, as
    /// [`Self::prune_with_stats`] does. A remote dynamic filter is a bounded pruning hint,
    /// not a copy of the producer's predicate — the coordinator ships the join's build-key
    /// list as `IN`/`NOT IN` and drops the top-level conjuncts it cannot ship, so the
    /// predicate can be *wider* than the one the producer evaluates (see
    /// `DynFilterPayload::decode_datafusion_expr` in `common-query`). Over-approximating is
    /// free for pruning but loses rows for filtering, so row filtering must not consume
    /// these expressions unconditionally.
    pub fn dyn_filters(&self) -> Arc<Vec<Arc<DynamicFilterPhysicalExpr>>> {
        self.dyn_filters.load_full()
    }

    /// Returns snapshots of the dynamic filters as physical expressions.
    ///
    /// Pruning only: see [`Self::dyn_filters`] for why these expressions must not filter
    /// rows. Producers must conservatively encode which NULL inputs can contribute.
    pub fn dyn_filter_phy_exprs(&self) -> error::Result<Vec<Arc<dyn PhysicalExpr>>> {
        self.dyn_filters
            .load()
            .iter()
            .map(|e| e.current())
            .collect::<Result<Vec<_>, _>>()
            .context(error::DatafusionSnafu)
    }

    /// Builds a single physical expr for a caller that *evaluates* it against record batches.
    ///
    /// Regex comparisons are rewritten into [`PrecompiledRegexExpr`] because
    /// `create_physical_expr` builds a comparison that recompiles its pattern on every evaluation,
    /// while a scan evaluates one physical expr per batch.
    ///
    /// Pruning must not use this: it never evaluates the expression, only folds it into column
    /// statistics, so [`Self::to_pruning_physical_exprs`] keeps the plain comparison.
    pub fn to_physical_expr(
        expr: &Expr,
        schema: &arrow::datatypes::SchemaRef,
    ) -> error::Result<Arc<dyn PhysicalExpr>> {
        let df_schema = schema
            .clone()
            .to_dfschema_ref()
            .context(error::DatafusionSnafu)?;

        // TODO(hl): `execution_props` provides variables required by evaluation.
        // we may reuse the `execution_props` from `SessionState` once we support
        // registering variables.
        let execution_props = &ExecutionProps::new();
        let planning_context = &PhysicalPlanningContext::default();

        if let Some(physical) = Self::to_regex_physical_expr(
            expr,
            schema,
            df_schema.as_ref(),
            execution_props,
            planning_context,
        ) {
            return Ok(physical);
        }

        Self::to_plain_physical_expr(expr, df_schema.as_ref())
    }

    /// Builds the physical expr `create_physical_expr` produces, without the regex rewrite.
    ///
    /// This is what pruning consumes: it is a shape the pruning predicate builder recognizes.
    fn to_plain_physical_expr(
        expr: &Expr,
        df_schema: &DFSchema,
    ) -> error::Result<Arc<dyn PhysicalExpr>> {
        let execution_props = &ExecutionProps::new();
        let planning_context = &PhysicalPlanningContext::default();
        create_physical_expr(expr, df_schema, execution_props, planning_context)
            .context(error::DatafusionSnafu)
    }

    /// Rewrites the regex comparisons in `expr` into [`PrecompiledRegexExpr`], so each pattern is
    /// compiled once per scan instead of once per batch.
    ///
    /// Returns `None` when nothing in `expr` can be precompiled, which leaves the caller with the
    /// unmodified `create_physical_expr` path. `AND`/`OR` are rebuilt with only the rewritable
    /// branch replaced because a label matcher accepting the empty string is planned as
    /// `is_null(col) OR col =~ 'pattern'`: the rewrite has to reach the regex below the `OR` or a
    /// nullable label column never benefits. Every other node — `NOT`, `CASE`, a cast around the
    /// whole comparison — is left to the planner.
    fn to_regex_physical_expr(
        expr: &Expr,
        schema: &arrow::datatypes::SchemaRef,
        df_schema: &DFSchema,
        execution_props: &ExecutionProps,
        planning_context: &PhysicalPlanningContext,
    ) -> Option<Arc<dyn PhysicalExpr>> {
        let Expr::BinaryExpr(binary) = expr else {
            return None;
        };
        let compile = |expr: &Expr| {
            create_physical_expr(expr, df_schema, execution_props, planning_context).ok()
        };
        let rewrite = |expr: &Expr| {
            Self::to_regex_physical_expr(expr, schema, df_schema, execution_props, planning_context)
        };

        if matches!(binary.op, Operator::And | Operator::Or) {
            let left = rewrite(&binary.left);
            let right = rewrite(&binary.right);
            if left.is_none() && right.is_none() {
                return None;
            }
            let left = left.or_else(|| compile(&binary.left))?;
            let right = right.or_else(|| compile(&binary.right))?;
            return Some(Arc::new(PhysicalBinaryExpr::new(left, binary.op, right)));
        }

        if !matches!(binary.op, Operator::RegexMatch | Operator::RegexNotMatch) {
            return None;
        }
        // Precompile only what the precompiled kernel can evaluate exactly; other string layouts
        // keep the arrow comparison instead of failing at scan time.
        let column_type = regex_column_type(&binary.left, schema)?;
        if !is_precompilable_regex_type(&column_type) {
            return None;
        }
        let filter = SimpleFilterEvaluator::try_new_with_column_type(expr, &|name| {
            schema
                .field_with_name(name)
                .ok()
                .map(|field| field.data_type().clone())
        })?;
        let Expr::Literal(literal, _) = binary.right.as_ref() else {
            return None;
        };
        let pattern = literal.try_as_str()?.map(str::to_string)?;
        let column = compile(&binary.left)?;

        Some(Arc::new(PrecompiledRegexExpr {
            column,
            filter,
            pattern,
            op: binary.op,
        }))
    }

    /// Builds the physical exprs [`Self::prune_with_stats`] folds into column statistics.
    ///
    /// Pruning never evaluates a predicate, and the regex rewrite exists only to make evaluation
    /// cheap, so this deliberately builds what `create_physical_expr` builds. Besides saving the
    /// conversion on a per-file (manifest pruning) and per-batch (bulk memtable pruning) path,
    /// [`PrecompiledRegexExpr`] is a node the pruning predicate builder does not know: it replaces
    /// unknown nodes with `true`, while the plain comparison below is handled (or rejected) the
    /// same way it was before the rewrite existed.
    pub fn to_pruning_physical_exprs(
        &self,
        schema: &arrow::datatypes::SchemaRef,
    ) -> error::Result<Vec<Arc<dyn PhysicalExpr>>> {
        let df_schema = schema
            .clone()
            .to_dfschema_ref()
            .context(error::DatafusionSnafu)?;
        let dyn_filters = self.dyn_filter_phy_exprs()?;

        Ok(self
            .exprs
            .iter()
            .filter_map(|expr| Self::to_plain_physical_expr(expr, df_schema.as_ref()).ok())
            .chain(dyn_filters)
            .collect::<Vec<_>>())
    }

    /// Evaluates the predicate against the `stats`.
    /// Returns a vector of boolean values, among which `false` means the row group can be skipped.
    pub fn prune_with_stats<S: PruningStatistics>(
        &self,
        stats: &S,
        schema: &arrow::datatypes::SchemaRef,
    ) -> Vec<bool> {
        let mut res = vec![true; stats.num_containers()];
        let physical_exprs = match self.to_pruning_physical_exprs(schema) {
            Ok(expr) => expr,
            Err(e) => {
                warn!(e; "Failed to build physical expr from predicates: {:?}", &self.exprs);
                return res;
            }
        };

        for expr in &physical_exprs {
            match PruningPredicateBuilder::new()
                .with_file_schema(schema.clone())
                .try_build(expr.clone())
            {
                Ok(p) => match p.prune(stats) {
                    Ok(r) => {
                        for (curr_val, res) in r.into_iter().zip(res.iter_mut()) {
                            *res &= curr_val
                        }
                    }
                    Err(e) => {
                        warn!(e; "Failed to prune row groups");
                    }
                },
                Err(e) => {
                    // since dynamic filter exprs could be complex, it's possible that the pruning predicate builder fails to prove anything from it. In that case, we just log it and skip pruning with this expr.
                    debug!("Failed to create pruning predicate for expr: {e:?}");
                }
            }
        }
        res
    }
}

/// Returns whether `data_type` is a string layout [`SimpleFilterEvaluator`] can evaluate.
///
/// Its regex kernels accept `Utf8` and `Dictionary(UInt32, Utf8)` only; `LargeUtf8`, `Utf8View`
/// and other dictionary layouts keep the arrow comparison.
fn is_precompilable_regex_type(data_type: &ArrowDataType) -> bool {
    match data_type {
        ArrowDataType::Utf8 => true,
        ArrowDataType::Dictionary(key, value) => {
            **key == ArrowDataType::UInt32 && **value == ArrowDataType::Utf8
        }
        _ => false,
    }
}

/// Returns the arrow type the regex comparison in `expr` evaluates, `None` if `expr` is not a
/// column (or a cast of one) this rewrite can reason about.
fn regex_column_type(expr: &Expr, schema: &arrow::datatypes::SchemaRef) -> Option<ArrowDataType> {
    match expr {
        Expr::Column(column) => schema
            .field_with_name(&column.name)
            .ok()
            .map(|field| field.data_type().clone()),
        // A cast decides the compared type, whatever the column holds underneath.
        Expr::Cast(cast) => match cast.expr.as_ref() {
            Expr::Column(_) => Some(cast.field.data_type().clone()),
            _ => None,
        },
        _ => None,
    }
}

/// A regex comparison (`col =~ 'pattern'` / `col !~ 'pattern'`) compiled on first evaluation.
///
/// `create_physical_expr` turns the comparison into a binary expression that recompiles the pattern
/// on every evaluation, because arrow's `regexp_is_match_scalar` compiles the pattern it is given.
/// A scan evaluates its physical expr once per batch, so that costs one `Regex::new` per batch, and
/// a series scan evaluates one batch per series. [`SimpleFilterEvaluator`] lazily compiles its
/// pattern once on first evaluation, then borrows one cache per batch rather than one per row.
///
/// The result is bit-identical to the arrow expression it replaces: `evaluate_array` folds SQL
/// `NULL` into `false`, so the column's logical nulls are restored afterwards — exactly the validity
/// arrow's kernel produces for a regex, which never fails on `NULL`.
///
/// Only [`Predicate::to_physical_expr`] builds this node, for callers that evaluate the predicate.
/// Pruning uses [`Predicate::to_pruning_physical_exprs`] and keeps the arrow comparison.
#[derive(Debug)]
struct PrecompiledRegexExpr {
    /// The column the pattern is matched against.
    column: Arc<dyn PhysicalExpr>,
    /// The pattern, compiled on first evaluation and reused thereafter.
    filter: SimpleFilterEvaluator,
    /// The pattern text, kept for display and equality.
    pattern: String,
    /// `RegexMatch` or `RegexNotMatch`, kept for display and equality.
    op: Operator,
}

impl fmt::Display for PrecompiledRegexExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let op = if matches!(self.op, Operator::RegexMatch) {
            "=~"
        } else {
            "!~"
        };
        write!(f, "{} {op} '{}'", self.column, self.pattern)
    }
}

impl PartialEq for PrecompiledRegexExpr {
    fn eq(&self, other: &Self) -> bool {
        self.op == other.op && self.pattern == other.pattern && self.column.eq(&other.column)
    }
}

impl Eq for PrecompiledRegexExpr {}

impl Hash for PrecompiledRegexExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.op.hash(state);
        self.pattern.hash(state);
        self.column.hash(state);
    }
}

impl PhysicalExpr for PrecompiledRegexExpr {
    fn data_type(&self, _input_schema: &ArrowSchema) -> DfResult<ArrowDataType> {
        Ok(ArrowDataType::Boolean)
    }

    fn nullable(&self, input_schema: &ArrowSchema) -> DfResult<bool> {
        self.column.nullable(input_schema)
    }

    fn evaluate(&self, batch: &DfRecordBatch) -> DfResult<ColumnarValue> {
        let column = self.column.evaluate(batch)?.into_array(batch.num_rows())?;
        let matched = self
            .filter
            .evaluate_array(&column)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(ColumnarValue::Array(Arc::new(BooleanArray::new(
            matched,
            column.logical_nulls(),
        ))))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.column]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DfResult<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self {
            column: children[0].clone(),
            filter: self.filter.clone(),
            pattern: self.pattern.clone(),
            op: self.op,
        }))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

// tests for `build_time_range_predicate` locates in src/query/tests/time_range_filter_test.rs
// since it requires query engine to convert sql to filters.
/// `build_time_range_predicate` extracts time range from logical exprs to facilitate fast
/// time range pruning.
pub fn build_time_range_predicate(
    ts_col_name: &str,
    ts_col_unit: TimeUnit,
    filters: &[Expr],
) -> TimestampRange {
    let mut res = TimestampRange::min_to_max();
    for expr in filters {
        if let Some(range) = extract_time_range_from_expr(ts_col_name, ts_col_unit, expr) {
            res = res.and(&range);
        }
    }
    res
}

/// The outcome of strictly extracting the time range of `ts_col_name` from a
/// scan's filters. Unlike [`build_time_range_predicate`], which quietly widens
/// to `min_to_max` on anything it cannot parse (fine for pruning), this
/// distinguishes "the column is not filtered at all" from "it is filtered in a
/// way that cannot be safely turned into a range" — for callers whose contract
/// forbids silently falling back to a default window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimeRangeExtraction {
    /// No filter references the column.
    Absent,
    /// Every filter referencing the column was folded into this range, which
    /// over-approximates the filters' satisfying set.
    Extracted(TimestampRange),
    /// At least one filter references the column in a shape that cannot be
    /// safely extracted (e.g. under `OR`/`NOT`, or compared to a non-literal).
    Unsupported,
}

/// Strictly extracts the time range of `ts_col_name` from the (implicitly
/// AND-ed) scan filters. See [`TimeRangeExtraction`].
pub fn extract_time_range_strict(
    ts_col_name: &str,
    ts_col_unit: TimeUnit,
    filters: &[Expr],
) -> TimeRangeExtraction {
    let mut range: Option<TimestampRange> = None;
    for expr in filters {
        if !expr
            .column_refs()
            .iter()
            .any(|column| column.name == ts_col_name)
        {
            continue;
        }
        // Disjunctive shapes (`OR`, `IN`) collapse disjoint ranges into their
        // convex hull, which is not exactly representable as one contiguous
        // range; callers deriving synthetic timestamps from the bounds would
        // emit points inside the gaps.
        if contains_disjunction_over_column(expr, ts_col_name) {
            return TimeRangeExtraction::Unsupported;
        }
        // `Some` from the lenient extractor over-approximates the expression's
        // satisfying set (an `AND` side it cannot parse is dropped, which only
        // widens), so intersecting extracted conjuncts stays an
        // over-approximation.
        // Ranges derived from `date_bin` are skipped: they can be a bucket wider than
        // the filter, so a timestamp synthesized at their bounds may not satisfy it.
        match extract_time_range(ts_col_name, ts_col_unit, expr, DateBinFilters::Skip) {
            Some(extracted) => {
                range = Some(match range {
                    Some(acc) => acc.and(&extracted),
                    None => extracted,
                });
            }
            None => return TimeRangeExtraction::Unsupported,
        }
    }
    match range {
        Some(range) => TimeRangeExtraction::Extracted(range),
        None => TimeRangeExtraction::Absent,
    }
}

fn contains_disjunction_over_column(expr: &Expr, ts_col_name: &str) -> bool {
    let is_disjunction = |expr: &Expr| {
        matches!(
            expr,
            Expr::BinaryExpr(BinaryExpr {
                op: Operator::Or,
                ..
            }) | Expr::InList(_)
        ) && expr
            .column_refs()
            .iter()
            .any(|column| column.name == ts_col_name)
    };
    expr.exists(|expr| Ok(is_disjunction(expr))).unwrap_or(true)
}

/// Extract time range filter from `WHERE`/`IN (...)`/`BETWEEN` clauses.
/// Return None if no time range can be found in expr.
pub fn extract_time_range_from_expr(
    ts_col_name: &str,
    ts_col_unit: TimeUnit,
    expr: &Expr,
) -> Option<TimestampRange> {
    extract_time_range(ts_col_name, ts_col_unit, expr, DateBinFilters::Derive)
}

/// Whether a filter on `date_bin(stride, ts)` yields a range on `ts`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DateBinFilters {
    /// Derive a range that covers every satisfying row (see [`date_bin_stride`]).
    Derive,
    /// Treat the filter as unparsable.
    Skip,
}

fn extract_time_range(
    ts_col_name: &str,
    ts_col_unit: TimeUnit,
    expr: &Expr,
    date_bin: DateBinFilters,
) -> Option<TimestampRange> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            extract_from_binary_expr(ts_col_name, ts_col_unit, left, op, right, date_bin)
        }
        Expr::Between(Between {
            expr,
            negated,
            low,
            high,
        }) => {
            extract_from_between_expr(ts_col_name, ts_col_unit, expr, negated, low, high, date_bin)
        }
        Expr::InList(InList {
            expr,
            list,
            negated,
        }) => extract_from_in_list_expr(ts_col_name, expr, *negated, list),
        _ => None,
    }
}

fn extract_from_binary_expr(
    ts_col_name: &str,
    ts_col_unit: TimeUnit,
    left: &Expr,
    op: &Operator,
    right: &Expr,
    date_bin: DateBinFilters,
) -> Option<TimestampRange> {
    if date_bin == DateBinFilters::Derive
        && let Some(range) =
            extract_from_date_bin_comparison(ts_col_name, ts_col_unit, left, op, right)
    {
        return Some(range);
    }
    match op {
        Operator::Eq => get_timestamp_filter(ts_col_name, left, right)
            .and_then(|(ts, _)| ts.convert_to(ts_col_unit))
            .map(TimestampRange::single),
        Operator::Lt => {
            let (ts, reverse) = get_timestamp_filter(ts_col_name, left, right)?;
            if reverse {
                // [lit] < ts_col
                strictly_after(ts, ts_col_unit)
            } else {
                // ts_col < [lit]
                ts.convert_to_ceil(ts_col_unit)
                    .map(|ts| TimestampRange::until_end(ts, false))
            }
        }
        Operator::LtEq => {
            let (ts, reverse) = get_timestamp_filter(ts_col_name, left, right)?;
            if reverse {
                // [lit] <= ts_col
                ts.convert_to_ceil(ts_col_unit)
                    .map(TimestampRange::from_start)
            } else {
                // ts_col <= [lit]
                ts.convert_to(ts_col_unit)
                    .map(|ts| TimestampRange::until_end(ts, true))
            }
        }
        Operator::Gt => {
            let (ts, reverse) = get_timestamp_filter(ts_col_name, left, right)?;
            if reverse {
                // [lit] > ts_col
                ts.convert_to_ceil(ts_col_unit)
                    .map(|t| TimestampRange::until_end(t, false))
            } else {
                // ts_col > [lit]
                strictly_after(ts, ts_col_unit)
            }
        }
        Operator::GtEq => {
            let (ts, reverse) = get_timestamp_filter(ts_col_name, left, right)?;
            if reverse {
                // [lit] >= ts_col
                ts.convert_to(ts_col_unit)
                    .map(|t| TimestampRange::until_end(t, true))
            } else {
                // ts_col >= [lit]
                ts.convert_to_ceil(ts_col_unit)
                    .map(TimestampRange::from_start)
            }
        }
        Operator::And => {
            // instead of return none when failed to extract time range from left/right, we unwrap the none into
            // `TimestampRange::min_to_max`.
            let left = extract_time_range(ts_col_name, ts_col_unit, left, date_bin)
                .unwrap_or_else(TimestampRange::min_to_max);
            let right = extract_time_range(ts_col_name, ts_col_unit, right, date_bin)
                .unwrap_or_else(TimestampRange::min_to_max);
            Some(left.and(&right))
        }
        Operator::Or => {
            let left = extract_time_range(ts_col_name, ts_col_unit, left, date_bin)?;
            let right = extract_time_range(ts_col_name, ts_col_unit, right, date_bin)?;
            Some(left.or(&right))
        }
        _ => None,
    }
}

/// Returns the stride of `date_bin(stride, ts_col[, origin])`, converted to `unit`
/// and rounded up, if `expr` is that call with a literal, fixed-width stride.
///
/// `date_bin` floors a timestamp to the start of its bucket, so for every row
/// `bucket_start <= ts < bucket_start + stride`, whatever the origin. That lets a
/// filter on the bucket start bound the time index itself, which is what a
/// filter on a view's `date_bin(...) AS time` column becomes once it is pushed
/// below the view's aggregate.
///
/// This holds in nanoseconds. DataFusion computes the bucket start in
/// nanoseconds and truncates it to the column unit, so on a coarser column the
/// returned value can be up to one unit below the true start; see
/// [`date_bin_range`].
fn date_bin_stride(ts_col_name: &str, unit: TimeUnit, expr: &Expr) -> Option<i64> {
    let Expr::ScalarFunction(ScalarFunction { func, args }) = expr else {
        return None;
    };
    if func.name() != "date_bin" || !(2..=3).contains(&args.len()) {
        return None;
    }
    let Expr::Column(col) = &args[1] else {
        return None;
    };
    if col.name != ts_col_name {
        return None;
    }
    let stride_ns: i128 = match &args[0] {
        // Months have no fixed width.
        Expr::Literal(ScalarValue::IntervalMonthDayNano(Some(v)), _) if v.months == 0 => {
            v.days as i128 * 86_400_000_000_000 + v.nanoseconds as i128
        }
        Expr::Literal(ScalarValue::IntervalDayTime(Some(v)), _) => {
            v.days as i128 * 86_400_000_000_000 + v.milliseconds as i128 * 1_000_000
        }
        _ => return None,
    };
    if stride_ns <= 0 {
        return None;
    }
    let ns_per_unit = match unit {
        TimeUnit::Second => 1_000_000_000,
        TimeUnit::Millisecond => 1_000_000,
        TimeUnit::Microsecond => 1_000,
        TimeUnit::Nanosecond => 1,
    };
    i64::try_from((stride_ns + ns_per_unit - 1) / ns_per_unit).ok()
}

/// Extracts a time range from `date_bin(stride, ts_col) <op> literal`. The range
/// over-approximates the rows that satisfy the comparison (see [`date_bin_stride`]).
fn extract_from_date_bin_comparison(
    ts_col_name: &str,
    ts_col_unit: TimeUnit,
    left: &Expr,
    op: &Operator,
    right: &Expr,
) -> Option<TimestampRange> {
    // Normalize to `date_bin(...) <op> literal`.
    let (bin, op, lit) = match (left, right) {
        (bin, Expr::Literal(lit, _)) => (bin, *op, lit),
        (Expr::Literal(lit, _), bin) => (bin, op.swap()?, lit),
        _ => return None,
    };
    let stride = date_bin_stride(ts_col_name, ts_col_unit, bin)?;
    return_none_if_utf8!(lit);
    let bound = scalar_value_to_timestamp(lit, None)?;
    date_bin_range(op, &bound, stride, ts_col_unit)
}

/// The range of `ts` for which `date_bin(stride, ts) <op> bound` can hold.
fn date_bin_range(
    op: Operator,
    bound: &Timestamp,
    stride: i64,
    unit: TimeUnit,
) -> Option<TimestampRange> {
    // `bin >= bound` and `bin > bound` imply `ts >= bound`, because `ts >= bin`.
    let lower = || bound.convert_to_ceil(unit).map(TimestampRange::from_start);
    // `bin <= bound` and `bin < bound` imply `ts < bound + stride`, because `ts < bin + stride`.
    // On a column coarser than nanoseconds, the returned `bin` is truncated from the true
    // bucket start and can be up to one unit below it, so widen the end by one unit.
    let slack = if unit == TimeUnit::Nanosecond { 0 } else { 1 };
    let upper = || {
        let end = bound
            .convert_to_ceil(unit)?
            .value()
            .checked_add(stride)?
            .checked_add(slack)?;
        Some(TimestampRange::until_end(Timestamp::new(end, unit), false))
    };
    match op {
        Operator::Gt | Operator::GtEq => lower(),
        Operator::Lt | Operator::LtEq => upper(),
        Operator::Eq => Some(lower()?.and(&upper()?)),
        _ => None,
    }
}

/// Returns the timestamps after `ts` in `unit`. The range is empty when `ts`
/// is the largest representable timestamp, rather than wrapping around.
fn strictly_after(ts: Timestamp, unit: TimeUnit) -> Option<TimestampRange> {
    let ts_val = ts.convert_to(unit)?.value();
    Some(match ts_val.checked_add(1) {
        Some(next) => TimestampRange::from_start(Timestamp::new(next, unit)),
        None => TimestampRange::empty(),
    })
}

fn get_timestamp_filter(ts_col_name: &str, left: &Expr, right: &Expr) -> Option<(Timestamp, bool)> {
    let (col, lit, reverse) = match (left, right) {
        (Expr::Column(column), Expr::Literal(scalar, _)) => (column, scalar, false),
        (Expr::Literal(scalar, _), Expr::Column(column)) => (column, scalar, true),
        _ => {
            return None;
        }
    };
    if col.name != ts_col_name {
        return None;
    }

    return_none_if_utf8!(lit);
    scalar_value_to_timestamp(lit, None).map(|t| (t, reverse))
}

fn extract_from_between_expr(
    ts_col_name: &str,
    ts_col_unit: TimeUnit,
    expr: &Expr,
    negated: &bool,
    low: &Expr,
    high: &Expr,
    date_bin: DateBinFilters,
) -> Option<TimestampRange> {
    if *negated {
        return None;
    }

    if date_bin == DateBinFilters::Derive
        && let Some(stride) = date_bin_stride(ts_col_name, ts_col_unit, expr)
    {
        // `low <= date_bin(...) <= high`
        let (Expr::Literal(low, _), Expr::Literal(high, _)) = (low, high) else {
            return None;
        };
        return_none_if_utf8!(low);
        return_none_if_utf8!(high);
        let low = date_bin_range(
            Operator::GtEq,
            &scalar_value_to_timestamp(low, None)?,
            stride,
            ts_col_unit,
        );
        let high = date_bin_range(
            Operator::LtEq,
            &scalar_value_to_timestamp(high, None)?,
            stride,
            ts_col_unit,
        );
        return Some(
            low.unwrap_or_else(TimestampRange::min_to_max)
                .and(&high.unwrap_or_else(TimestampRange::min_to_max)),
        );
    }

    let Expr::Column(col) = expr else {
        return None;
    };
    if col.name != ts_col_name {
        return None;
    }

    match (low, high) {
        (Expr::Literal(low, _), Expr::Literal(high, _)) => {
            return_none_if_utf8!(low);
            return_none_if_utf8!(high);

            let low_opt =
                scalar_value_to_timestamp(low, None).and_then(|ts| ts.convert_to(ts_col_unit));
            let high_opt = scalar_value_to_timestamp(high, None)
                .and_then(|ts| ts.convert_to_ceil(ts_col_unit));
            Some(TimestampRange::new_inclusive(low_opt, high_opt))
        }
        _ => None,
    }
}

/// Extract time range filter from `IN (...)` expr.
fn extract_from_in_list_expr(
    ts_col_name: &str,
    expr: &Expr,
    negated: bool,
    list: &[Expr],
) -> Option<TimestampRange> {
    if negated {
        return None;
    }
    let Expr::Column(col) = expr else {
        return None;
    };
    if col.name != ts_col_name {
        return None;
    }

    if list.is_empty() {
        return Some(TimestampRange::empty());
    }
    let mut init_range = TimestampRange::empty();
    for expr in list {
        if let Expr::Literal(scalar, _) = expr {
            return_none_if_utf8!(scalar);
            // TODO(hl): maybe we should raise an error here since cannot parse
            // timestamp value from in list expr
            let timestamp = scalar_value_to_timestamp(scalar, None)?;
            init_range = init_range.or(&TimestampRange::single(timestamp));
        }
    }
    Some(init_range)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_test_util::temp_dir::{TempDir, create_temp_dir};
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion_common::tree_node::TreeNodeRecursion;
    use datafusion_common::{Column, ScalarValue};
    use datafusion_expr::{BinaryExpr, Literal, Operator, col, lit};
    use datatypes::arrow::array::{
        ArrayRef, DictionaryArray, Int32Array, LargeStringArray, StringViewArray, UInt8Array,
        UInt32Array,
    };
    use datatypes::arrow::datatypes::{DataType, Field, Schema};
    use datatypes::arrow::record_batch::RecordBatch;
    use datatypes::arrow_array::StringArray;
    use parquet::arrow::ParquetRecordBatchStreamBuilder;
    use parquet::file::properties::WriterProperties;

    use super::*;
    use crate::predicate::stats::RowGroupPruningStatistics;

    fn check_build_predicate(expr: Expr, expect: TimestampRange) {
        assert_eq!(
            expect,
            build_time_range_predicate("ts", TimeUnit::Millisecond, &[expr])
        );
    }

    fn date_bin(stride: Expr) -> Expr {
        datafusion::functions::datetime::date_bin().call(vec![stride, col("ts")])
    }

    fn ms(v: i64) -> Expr {
        lit(ScalarValue::TimestampMillisecond(Some(v), None))
    }

    fn minute() -> Expr {
        lit(ScalarValue::new_interval_mdn(0, 0, 60_000_000_000))
    }

    #[test]
    fn test_date_bin() {
        // Upper bounds on a coarser-than-ns column are widened by one unit, because
        // `date_bin` truncates its nanosecond bucket start to the column unit.
        let t = Timestamp::new_millisecond;
        // date_bin(1m, ts) >= 120_000ms  =>  ts >= 120_000ms
        check_build_predicate(
            date_bin(minute()).gt_eq(ms(120_000)),
            TimestampRange::from_start(t(120_000)),
        );
        // date_bin(1m, ts) > 120_000ms  =>  ts > 120_000ms, widened to ts >= 120_000ms
        check_build_predicate(
            date_bin(minute()).gt(ms(120_000)),
            TimestampRange::from_start(t(120_000)),
        );
        // date_bin(1m, ts) < 120_000ms  =>  ts < 180_001ms
        check_build_predicate(
            date_bin(minute()).lt(ms(120_000)),
            TimestampRange::until_end(t(180_001), false),
        );
        // date_bin(1m, ts) <= 120_000ms  =>  ts < 180_001ms
        check_build_predicate(
            date_bin(minute()).lt_eq(ms(120_000)),
            TimestampRange::until_end(t(180_001), false),
        );
        // 120_000ms <= date_bin(1m, ts)  =>  ts >= 120_000ms
        check_build_predicate(
            ms(120_000).lt_eq(date_bin(minute())),
            TimestampRange::from_start(t(120_000)),
        );
        // date_bin(1m, ts) = 120_000ms  =>  120_000ms <= ts < 180_001ms
        check_build_predicate(
            date_bin(minute()).eq(ms(120_000)),
            TimestampRange::new(t(120_000), t(180_001)).unwrap(),
        );
        // date_bin(1m, ts) BETWEEN 60_000ms AND 120_000ms  =>  60_000ms <= ts < 180_001ms
        check_build_predicate(
            date_bin(minute()).between(ms(60_000), ms(120_000)),
            TimestampRange::new(t(60_000), t(180_001)).unwrap(),
        );
        // Combined with other conjuncts.
        check_build_predicate(
            date_bin(minute())
                .gt_eq(ms(60_000))
                .and(date_bin(minute()).lt(ms(120_000)))
                .and(col("v").gt(lit(1))),
            TimestampRange::new(t(60_000), t(180_001)).unwrap(),
        );
        // An origin argument doesn't change the bounds.
        check_build_predicate(
            datafusion::functions::datetime::date_bin()
                .call(vec![minute(), col("ts"), ms(17)])
                .gt_eq(ms(120_000)),
            TimestampRange::from_start(t(120_000)),
        );
        // A month stride has no fixed width: no range for the upper bound.
        check_build_predicate(
            date_bin(lit(ScalarValue::new_interval_mdn(1, 0, 0))).lt(ms(120_000)),
            TimestampRange::min_to_max(),
        );
        // Other columns are ignored.
        check_build_predicate(
            datafusion::functions::datetime::date_bin()
                .call(vec![minute(), col("other")])
                .gt_eq(ms(120_000)),
            TimestampRange::min_to_max(),
        );
        // An upper bound that overflows leaves the range unbounded.
        check_build_predicate(
            date_bin(minute()).lt_eq(ms(i64::MAX - 1)),
            TimestampRange::min_to_max(),
        );
    }

    /// Evaluates `date_bin` with DataFusion over a grid of timestamps, then checks
    /// that every row satisfying a comparison on its result lies in the extracted
    /// range. Covers coarse column units, strides and origins that are not a whole
    /// number of column units, and negative timestamps.
    #[tokio::test]
    async fn test_date_bin_range_keeps_satisfying_rows() {
        /// Whether the comparison holds for a `date_bin` result.
        type BinHolds = Box<dyn Fn(i64) -> bool>;

        use datafusion::prelude::SessionContext;
        use datafusion_expr::cast;
        use datatypes::arrow::array::{AsArray, Int64Array};
        use datatypes::arrow::datatypes::{Int64Type, TimeUnit as ArrowTimeUnit};

        let units = [
            (TimeUnit::Second, ArrowTimeUnit::Second, 1_000_000_000i64),
            (TimeUnit::Millisecond, ArrowTimeUnit::Millisecond, 1_000_000),
            (TimeUnit::Microsecond, ArrowTimeUnit::Microsecond, 1_000),
        ];
        let ctx = SessionContext::new();

        for (unit, arrow_unit, ns_per_unit) in units {
            let ts_lit = |v: i64| {
                lit(match unit {
                    TimeUnit::Second => ScalarValue::TimestampSecond(Some(v), None),
                    TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(Some(v), None),
                    TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(Some(v), None),
                    TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(Some(v), None),
                })
            };
            let ts_type = DataType::Timestamp(arrow_unit, None);
            let ts_array = datatypes::arrow::compute::cast(
                &Int64Array::from((-30..=30).collect::<Vec<i64>>()),
                &ts_type,
            )
            .unwrap();
            let schema = Arc::new(Schema::new(vec![Field::new("ts", ts_type, false)]));
            let batch = RecordBatch::try_new(schema, vec![ts_array]).unwrap();

            let strides_ns = [
                ns_per_unit,
                ns_per_unit * 3,
                ns_per_unit / 2,
                ns_per_unit * 13 / 10,
                ns_per_unit * 26 / 10,
            ];
            let origins_ns = [
                None,
                Some(0),
                Some(ns_per_unit / 2),
                Some(-ns_per_unit * 7 / 3),
            ];
            for stride_ns in strides_ns {
                for origin_ns in origins_ns {
                    let mut args = vec![
                        lit(ScalarValue::new_interval_mdn(0, 0, stride_ns)),
                        col("ts"),
                    ];
                    if let Some(origin) = origin_ns {
                        args.push(lit(ScalarValue::TimestampNanosecond(Some(origin), None)));
                    }
                    let bin = datafusion::functions::datetime::date_bin().call(args);

                    // (ts, date_bin(ts)), both in the column unit.
                    let rows: Vec<(i64, i64)> = ctx
                        .read_batch(batch.clone())
                        .unwrap()
                        .select(vec![
                            cast(col("ts"), DataType::Int64),
                            cast(bin.clone(), DataType::Int64),
                        ])
                        .unwrap()
                        .collect()
                        .await
                        .unwrap()
                        .iter()
                        .flat_map(|b| {
                            let ts = b.column(0).as_primitive::<Int64Type>();
                            let bins = b.column(1).as_primitive::<Int64Type>();
                            ts.values()
                                .iter()
                                .copied()
                                .zip(bins.values().iter().copied())
                                .collect::<Vec<_>>()
                        })
                        .collect();
                    assert_eq!(rows.len(), 61);

                    for c in -35..=35 {
                        let cases: Vec<(Expr, BinHolds)> = vec![
                            (bin.clone().eq(ts_lit(c)), Box::new(move |b| b == c)),
                            (bin.clone().lt(ts_lit(c)), Box::new(move |b| b < c)),
                            (bin.clone().lt_eq(ts_lit(c)), Box::new(move |b| b <= c)),
                            (bin.clone().gt(ts_lit(c)), Box::new(move |b| b > c)),
                            (bin.clone().gt_eq(ts_lit(c)), Box::new(move |b| b >= c)),
                            (ts_lit(c).gt_eq(bin.clone()), Box::new(move |b| c >= b)),
                            (
                                bin.clone().between(ts_lit(c), ts_lit(c + 2)),
                                Box::new(move |b| c <= b && b <= c + 2),
                            ),
                        ];
                        for (expr, holds) in cases {
                            let range =
                                build_time_range_predicate("ts", unit, std::slice::from_ref(&expr));
                            for &(ts, b) in &rows {
                                assert!(
                                    !holds(b) || range.contains(&Timestamp::new(ts, unit)),
                                    "{expr} holds for ts={ts} (bin={b}, {unit:?}, stride={stride_ns}ns, origin={origin_ns:?}ns) but {range:?} excludes it",
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn test_gt() {
        // ts > 1ms
        check_build_predicate(
            col("ts").gt(lit(ScalarValue::TimestampMillisecond(Some(1), None))),
            TimestampRange::from_start(Timestamp::new_millisecond(2)),
        );

        // 1ms > ts
        check_build_predicate(
            lit(ScalarValue::TimestampMillisecond(Some(1), None)).gt(col("ts")),
            TimestampRange::until_end(Timestamp::new_millisecond(1), false),
        );

        // 1001us > ts
        check_build_predicate(
            lit(ScalarValue::TimestampMicrosecond(Some(1001), None)).gt(col("ts")),
            TimestampRange::until_end(Timestamp::new_millisecond(1), true),
        );

        // ts > 1001us
        check_build_predicate(
            col("ts").gt(lit(ScalarValue::TimestampMicrosecond(Some(1001), None))),
            TimestampRange::from_start(Timestamp::new_millisecond(2)),
        );

        // 1s > ts
        check_build_predicate(
            lit(ScalarValue::TimestampSecond(Some(1), None)).gt(col("ts")),
            TimestampRange::until_end(Timestamp::new_millisecond(1000), false),
        );

        // ts > 1s
        check_build_predicate(
            col("ts").gt(lit(ScalarValue::TimestampSecond(Some(1), None))),
            TimestampRange::from_start(Timestamp::new_millisecond(1001)),
        );

        // No timestamp is after the maximum, in either operand order. The
        // successor must not wrap around to the minimum.
        let max = || lit(ScalarValue::TimestampMillisecond(Some(i64::MAX), None));
        for expr in [col("ts").gt(max()), max().lt(col("ts"))] {
            let range = build_time_range_predicate("ts", TimeUnit::Millisecond, &[expr]);
            assert!(range.is_empty(), "{range:?}");
        }
        for (literal, successor) in [
            (
                ScalarValue::TimestampMillisecond(Some(i64::MIN), None),
                i64::MIN + 1,
            ),
            (ScalarValue::TimestampMicrosecond(Some(-1001), None), -1),
            (ScalarValue::TimestampMicrosecond(Some(-1), None), 0),
            (ScalarValue::TimestampSecond(Some(0), None), 1),
        ] {
            let literal = lit(literal);
            for expr in [col("ts").gt(literal.clone()), literal.lt(col("ts"))] {
                check_build_predicate(
                    expr,
                    TimestampRange::from_start(Timestamp::new_millisecond(successor)),
                );
            }
        }
    }

    #[test]
    fn test_gt_eq() {
        // ts >= 1ms
        check_build_predicate(
            col("ts").gt_eq(lit(ScalarValue::TimestampMillisecond(Some(1), None))),
            TimestampRange::from_start(Timestamp::new_millisecond(1)),
        );

        // 1ms >= ts
        check_build_predicate(
            lit(ScalarValue::TimestampMillisecond(Some(1), None)).gt_eq(col("ts")),
            TimestampRange::until_end(Timestamp::new_millisecond(1), true),
        );

        // 1001us >= ts
        check_build_predicate(
            lit(ScalarValue::TimestampMicrosecond(Some(1001), None)).gt_eq(col("ts")),
            TimestampRange::until_end(Timestamp::new_millisecond(1), true),
        );

        // ts >= 1001us
        check_build_predicate(
            col("ts").gt_eq(lit(ScalarValue::TimestampMicrosecond(Some(1001), None))),
            TimestampRange::from_start(Timestamp::new_millisecond(2)),
        );

        // 1s >= ts
        check_build_predicate(
            lit(ScalarValue::TimestampSecond(Some(1), None)).gt_eq(col("ts")),
            TimestampRange::until_end(Timestamp::new_millisecond(1000), true),
        );

        // ts >= 1s
        check_build_predicate(
            col("ts").gt_eq(lit(ScalarValue::TimestampSecond(Some(1), None))),
            TimestampRange::from_start(Timestamp::new_millisecond(1000)),
        );
    }

    #[test]
    fn test_lt() {
        // ts < 1ms
        check_build_predicate(
            col("ts").lt(lit(ScalarValue::TimestampMillisecond(Some(1), None))),
            TimestampRange::until_end(Timestamp::new_millisecond(1), false),
        );

        // 1ms < ts
        check_build_predicate(
            lit(ScalarValue::TimestampMillisecond(Some(1), None)).lt(col("ts")),
            TimestampRange::from_start(Timestamp::new_millisecond(2)),
        );

        // 1001us < ts
        check_build_predicate(
            lit(ScalarValue::TimestampMicrosecond(Some(1001), None)).lt(col("ts")),
            TimestampRange::from_start(Timestamp::new_millisecond(2)),
        );

        // ts < 1001us
        check_build_predicate(
            col("ts").lt(lit(ScalarValue::TimestampMicrosecond(Some(1001), None))),
            TimestampRange::until_end(Timestamp::new_millisecond(1), true),
        );

        // 1s < ts
        check_build_predicate(
            lit(ScalarValue::TimestampSecond(Some(1), None)).lt(col("ts")),
            TimestampRange::from_start(Timestamp::new_millisecond(1001)),
        );

        // ts < 1s
        check_build_predicate(
            col("ts").lt(lit(ScalarValue::TimestampSecond(Some(1), None))),
            TimestampRange::until_end(Timestamp::new_millisecond(1000), false),
        );
    }

    #[test]
    fn test_lt_eq() {
        // ts <= 1ms
        check_build_predicate(
            col("ts").lt_eq(lit(ScalarValue::TimestampMillisecond(Some(1), None))),
            TimestampRange::until_end(Timestamp::new_millisecond(1), true),
        );

        // 1ms <= ts
        check_build_predicate(
            lit(ScalarValue::TimestampMillisecond(Some(1), None)).lt_eq(col("ts")),
            TimestampRange::from_start(Timestamp::new_millisecond(1)),
        );

        // 1001us <= ts
        check_build_predicate(
            lit(ScalarValue::TimestampMicrosecond(Some(1001), None)).lt_eq(col("ts")),
            TimestampRange::from_start(Timestamp::new_millisecond(2)),
        );

        // ts <= 1001us
        check_build_predicate(
            col("ts").lt_eq(lit(ScalarValue::TimestampMicrosecond(Some(1001), None))),
            TimestampRange::until_end(Timestamp::new_millisecond(1), true),
        );

        // 1s <= ts
        check_build_predicate(
            lit(ScalarValue::TimestampSecond(Some(1), None)).lt_eq(col("ts")),
            TimestampRange::from_start(Timestamp::new_millisecond(1000)),
        );

        // ts <= 1s
        check_build_predicate(
            col("ts").lt_eq(lit(ScalarValue::TimestampSecond(Some(1), None))),
            TimestampRange::until_end(Timestamp::new_millisecond(1000), true),
        );
    }

    #[test]
    fn test_extract_time_range_strict() {
        fn ts_lit(ms: i64) -> Expr {
            lit(ScalarValue::TimestampMillisecond(Some(ms), None))
        }
        let extract =
            |filters: &[Expr]| extract_time_range_strict("ts", TimeUnit::Millisecond, filters);
        let range = |start: i64, end: i64| {
            TimestampRange::new(
                Timestamp::new_millisecond(start),
                Timestamp::new_millisecond(end),
            )
            .unwrap()
        };

        // No filter references the column.
        assert_eq!(extract(&[]), TimeRangeExtraction::Absent);
        assert_eq!(
            extract(&[col("host").eq(lit("a"))]),
            TimeRangeExtraction::Absent
        );

        // Both bounds across conjuncts; unrelated filters are ignored.
        assert_eq!(
            extract(&[
                col("ts").gt_eq(ts_lit(1000)),
                col("ts").lt(ts_lit(2000)),
                col("host").eq(lit("a")),
            ]),
            TimeRangeExtraction::Extracted(range(1000, 2000))
        );

        // Lower bound only / upper bound only.
        assert_eq!(
            extract(&[col("ts").gt_eq(ts_lit(1000))]),
            TimeRangeExtraction::Extracted(TimestampRange::from_start(Timestamp::new_millisecond(
                1000
            )))
        );
        assert_eq!(
            extract(&[col("ts").lt(ts_lit(2000))]),
            TimeRangeExtraction::Extracted(TimestampRange::until_end(
                Timestamp::new_millisecond(2000),
                false
            ))
        );

        // BETWEEN is inclusive on both ends.
        assert_eq!(
            extract(&[col("ts").between(ts_lit(1000), ts_lit(2000))]),
            TimeRangeExtraction::Extracted(range(1000, 2001))
        );

        // Equality pins a single point.
        assert_eq!(
            extract(&[col("ts").eq(ts_lit(1500))]),
            TimeRangeExtraction::Extracted(TimestampRange::single(Timestamp::new_millisecond(
                1500
            )))
        );

        // An unparsable side under AND only widens; the extraction stays safe.
        assert_eq!(
            extract(&[col("ts").gt_eq(ts_lit(1000)).and(col("ts").lt(col("t2")))]),
            TimeRangeExtraction::Extracted(TimestampRange::from_start(Timestamp::new_millisecond(
                1000
            )))
        );

        // Contradictory bounds collapse to the empty range, not an error.
        let TimeRangeExtraction::Extracted(empty) =
            extract(&[col("ts").gt_eq(ts_lit(2000)), col("ts").lt(ts_lit(1000))])
        else {
            panic!("expected an extraction");
        };
        assert!(empty.is_empty());

        // Shapes that could widen the satisfying set beyond what is extractable
        // must be refused: OR with an unparsable side, NOT, non-literal bounds.
        assert_eq!(
            extract(&[col("ts").gt(ts_lit(1000)).or(col("host").eq(lit("a")))]),
            TimeRangeExtraction::Unsupported
        );
        assert_eq!(
            extract(&[!col("ts").gt(ts_lit(1000))]),
            TimeRangeExtraction::Unsupported
        );
        assert_eq!(
            extract(&[col("ts").gt_eq(col("t2"))]),
            TimeRangeExtraction::Unsupported
        );

        // A range derived from `date_bin` over-approximates by up to a bucket, so a
        // timestamp synthesized from its bounds may fail the filter: refuse it.
        assert_eq!(
            extract(&[date_bin(minute()).gt(ts_lit(60_000))]),
            TimeRangeExtraction::Unsupported
        );
        assert_eq!(
            extract(&[date_bin(minute()).between(ts_lit(0), ts_lit(60_000))]),
            TimeRangeExtraction::Unsupported
        );
        // Under AND it is dropped like any other unparsable side.
        assert_eq!(
            extract(&[col("ts")
                .lt(ts_lit(2000))
                .and(date_bin(minute()).gt_eq(ts_lit(60_000)))]),
            TimeRangeExtraction::Extracted(TimestampRange::until_end(
                Timestamp::new_millisecond(2000),
                false
            ))
        );
    }

    async fn gen_test_parquet_file(dir: &TempDir, cnt: usize) -> (String, Arc<Schema>) {
        let path = dir
            .path()
            .join("test-prune.parquet")
            .to_string_lossy()
            .to_string();

        let name_field = Field::new("name", DataType::Utf8, true);
        let count_field = Field::new("cnt", DataType::Int32, true);
        let schema = Arc::new(Schema::new(vec![name_field, count_field]));

        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path.clone())
            .unwrap();

        let write_props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(10))
            .build();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(write_props)).unwrap();

        for i in (0..cnt).step_by(10) {
            let name_array = Arc::new(StringArray::from(
                (i..(i + 10).min(cnt))
                    .map(|i| i.to_string())
                    .collect::<Vec<_>>(),
            )) as Arc<_>;
            let count_array = Arc::new(Int32Array::from(
                (i..(i + 10).min(cnt)).map(|i| i as i32).collect::<Vec<_>>(),
            )) as Arc<_>;
            let rb = RecordBatch::try_new(schema.clone(), vec![name_array, count_array]).unwrap();
            writer.write(&rb).unwrap();
        }
        let _ = writer.close().unwrap();
        (path, schema)
    }

    async fn assert_prune(array_cnt: usize, filters: Vec<Expr>, expect: Vec<bool>) {
        let dir = create_temp_dir("prune_parquet");
        let (path, arrow_schema) = gen_test_parquet_file(&dir, array_cnt).await;
        let schema = Arc::new(datatypes::schema::Schema::try_from(arrow_schema.clone()).unwrap());
        let arrow_predicate = Predicate::new(filters);
        let builder = ParquetRecordBatchStreamBuilder::new(
            tokio::fs::OpenOptions::new()
                .read(true)
                .open(path)
                .await
                .unwrap(),
        )
        .await
        .unwrap();
        let metadata = builder.metadata().clone();
        let row_groups = metadata.row_groups();

        let stats = RowGroupPruningStatistics::new(row_groups, &schema);
        let res = arrow_predicate.prune_with_stats(&stats, &arrow_schema);
        assert_eq!(expect, res);
    }

    #[test]
    fn test_clear_dyn_filters_preserves_static_predicates() {
        use datafusion_physical_expr::expressions::lit as physical_lit;

        let static_exprs = vec![col("a").eq(lit(1_i32))];
        let dynamic_filter = Arc::new(DynamicFilterPhysicalExpr::new(vec![], physical_lit(true)));
        let predicate =
            Predicate::with_dyn_filters(static_exprs.clone(), vec![dynamic_filter.clone()]);

        predicate.clear_dyn_filters();
        // An update from the old producer must not put its wrapper back into this execution.
        dynamic_filter.update(physical_lit(false)).unwrap();

        assert_eq!(predicate.exprs(), static_exprs);
        assert!(predicate.dyn_filters().is_empty());
        assert!(predicate.dyn_filter_phy_exprs().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_dynamic_topk_pruning_nullable_lexicographic_keys() {
        use std::cmp::Ordering;

        use datafusion::datasource::memory::MemorySourceConfig;
        use datafusion::execution::context::TaskContext;
        use datafusion::physical_plan::sorts::sort::SortExec;
        use datafusion::physical_plan::{ExecutionPlan, collect};
        use datafusion_physical_expr::PhysicalSortExpr;
        use datafusion_physical_expr::expressions::Column as PhysicalColumn;
        use datatypes::arrow::compute::SortOptions;

        type Row = (Option<i32>, Option<i32>);

        fn compare(left: Option<i32>, right: Option<i32>, options: SortOptions) -> Ordering {
            match (left, right) {
                (None, None) => Ordering::Equal,
                (None, Some(_)) => {
                    if options.nulls_first {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                }
                (Some(_), None) => {
                    if options.nulls_first {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    }
                }
                (Some(left), Some(right)) => {
                    let ordering = left.cmp(&right);
                    if options.descending {
                        ordering.reverse()
                    } else {
                        ordering
                    }
                }
            }
        }

        let groups: [[Row; 2]; 6] = [
            [(Some(11), None), (Some(12), Some(1))],
            [(Some(10), Some(4)), (Some(10), Some(5))],
            [(Some(9), None), (Some(10), Some(6))],
            [(None, Some(1)), (None, Some(2))],
            [(Some(10), None), (Some(10), Some(1))],
            [(Some(10), Some(5)), (Some(10), Some(5))],
        ];
        let dir = create_temp_dir("dynamic_topk_pruning");
        let path = dir.path().join("nullable.parquet");
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]));
        let batch = |rows: &[Row]| {
            RecordBatch::try_new(
                arrow_schema.clone(),
                vec![
                    Arc::new(Int32Array::from(
                        rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int32Array::from(
                        rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap()
        };
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create(&path).unwrap(),
            arrow_schema.clone(),
            None,
        )
        .unwrap();
        for group in &groups {
            writer.write(&batch(group)).unwrap();
            writer.flush().unwrap();
        }
        writer.close().unwrap();
        let builder =
            ParquetRecordBatchStreamBuilder::new(tokio::fs::File::open(path).await.unwrap())
                .await
                .unwrap();
        let schema = Arc::new(datatypes::schema::Schema::try_from(arrow_schema.clone()).unwrap());
        let stats = RowGroupPruningStatistics::new(builder.metadata().row_groups(), &schema);

        for (options, boundary, expected, must_prune_first) in [
            (
                SortOptions {
                    descending: false,
                    nulls_first: false,
                },
                (Some(10), Some(5)),
                [(Some(9), None), (Some(10), Some(1))],
                true,
            ),
            (
                SortOptions {
                    descending: false,
                    nulls_first: true,
                },
                (Some(10), Some(5)),
                [(None, Some(1)), (None, Some(2))],
                false,
            ),
            (
                SortOptions {
                    descending: true,
                    nulls_first: false,
                },
                (Some(10), Some(5)),
                [(Some(12), Some(1)), (Some(11), None)],
                false,
            ),
            (
                SortOptions {
                    descending: false,
                    nulls_first: true,
                },
                (None, Some(5)),
                [(None, Some(1)), (None, Some(2))],
                false,
            ),
            (
                SortOptions {
                    descending: false,
                    nulls_first: false,
                },
                (None, Some(5)),
                [(Some(9), None), (Some(10), Some(1))],
                false,
            ),
            (
                SortOptions {
                    descending: false,
                    nulls_first: false,
                },
                (Some(10), None),
                [(Some(9), None), (Some(10), Some(1))],
                true,
            ),
        ] {
            // Let the real SortExec/TopK producer publish its boundary predicate.
            // Two boundary rows also make dropping later full-key ties safe.
            let seed = [boundary, boundary];
            let input =
                MemorySourceConfig::try_new_exec(&[vec![batch(&seed)]], arrow_schema.clone(), None)
                    .unwrap();
            let ordering = [
                PhysicalSortExpr {
                    expr: Arc::new(PhysicalColumn::new("a", 0)),
                    options,
                },
                PhysicalSortExpr {
                    expr: Arc::new(PhysicalColumn::new("b", 1)),
                    options,
                },
            ];
            let sort = SortExec::new(ordering.into(), input).with_fetch(Some(2));
            let filter = sort
                .dynamic_expressions_produced()
                .into_iter()
                .next()
                .unwrap();
            let filter = (filter as Arc<dyn std::any::Any + Send + Sync>)
                .downcast::<DynamicFilterPhysicalExpr>()
                .unwrap();
            collect(Arc::new(sort), Arc::new(TaskContext::default()))
                .await
                .unwrap();
            let predicate = Predicate::with_dyn_filters(vec![], vec![filter]);
            let keep = predicate.prune_with_stats(&stats, &arrow_schema);
            if must_prune_first {
                assert!(
                    !keep[0],
                    "disjoint first row group must be pruned: {options:?}, {boundary:?}"
                );
            }
            if boundary == (Some(10), Some(5)) && !options.nulls_first && !options.descending {
                assert!(
                    !keep[5],
                    "duplicate full-key ties may be pruned after the heap is full"
                );
            }

            let top_two = |mut rows: Vec<Row>| {
                rows.sort_by(|left, right| {
                    compare(left.0, right.0, options)
                        .then_with(|| compare(left.1, right.1, options))
                });
                rows.truncate(2);
                rows
            };
            let all = seed
                .into_iter()
                .chain(groups.into_iter().flatten())
                .collect();
            let retained = seed
                .into_iter()
                .chain(
                    groups
                        .into_iter()
                        .zip(keep)
                        .filter(|(_, keep)| *keep)
                        .flat_map(|(rows, _)| rows),
                )
                .collect();
            assert_eq!(
                top_two(all),
                expected,
                "explicit sort oracle: {options:?}, {boundary:?}"
            );
            assert_eq!(
                top_two(retained),
                expected,
                "pruned result: {options:?}, {boundary:?}"
            );
            // Seed duplicates must remain available when every candidate ties
            // the full boundary, even if all tied SST rows are pruned.
            assert_eq!(top_two(seed.to_vec()), vec![boundary, boundary]);
        }
    }

    #[tokio::test]
    async fn test_dynamic_aggregate_unknown_to_known_pruning() {
        use std::sync::Mutex;

        use datafusion::execution::context::TaskContext;
        use datafusion::functions_aggregate::min_max::min_udaf;
        use datafusion::physical_expr::aggregate::AggregateExprBuilder;
        use datafusion::physical_plan::aggregates::{
            AggregateExec, AggregateMode, PhysicalGroupBy,
        };
        use datafusion::physical_plan::common::collect;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
        use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
        use datafusion_physical_expr::expressions::col as physical_col;
        use datatypes::arrow::array::Int64Array;
        use futures::{StreamExt, channel::mpsc};

        #[derive(Debug)]
        struct TestInput {
            schema: Arc<Schema>,
            receiver:
                Mutex<Option<mpsc::UnboundedReceiver<datafusion_common::Result<RecordBatch>>>>,
        }
        impl PartitionStream for TestInput {
            fn schema(&self) -> &Arc<Schema> {
                &self.schema
            }

            fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
                let stream = self.receiver.lock().unwrap().take().unwrap();
                Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream))
            }
        }

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]));
        let dir = create_temp_dir("dynamic_aggregate_pruning");
        let path = dir.path().join("nullable.parquet");
        let make_batch = |a: Vec<Option<i64>>, b: Vec<Option<i64>>| {
            RecordBatch::try_new(
                arrow_schema.clone(),
                vec![Arc::new(Int64Array::from(a)), Arc::new(Int64Array::from(b))],
            )
            .unwrap()
        };
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create(&path).unwrap(),
            arrow_schema.clone(),
            None,
        )
        .unwrap();
        for batch in [
            make_batch(vec![Some(101)], vec![Some(5)]),
            make_batch(vec![Some(101)], vec![Some(1)]),
        ] {
            writer.write(&batch).unwrap();
            writer.flush().unwrap();
        }
        writer.close().unwrap();
        let builder =
            ParquetRecordBatchStreamBuilder::new(tokio::fs::File::open(path).await.unwrap())
                .await
                .unwrap();
        let schema = Arc::new(datatypes::schema::Schema::try_from(arrow_schema.clone()).unwrap());
        let stats = RowGroupPruningStatistics::new(builder.metadata().row_groups(), &schema);
        let (sender, receiver) = mpsc::unbounded();
        let input = Arc::new(
            StreamingTableExec::try_new(
                arrow_schema.clone(),
                vec![Arc::new(TestInput {
                    schema: arrow_schema.clone(),
                    receiver: Mutex::new(Some(receiver)),
                })],
                None,
                [],
                false,
                None,
            )
            .unwrap(),
        );
        let aggregates = ["a", "b"]
            .into_iter()
            .map(|name| {
                Arc::new(
                    AggregateExprBuilder::new(
                        min_udaf(),
                        vec![physical_col(name, &arrow_schema).unwrap()],
                    )
                    .schema(arrow_schema.clone())
                    .alias(name)
                    .build()
                    .unwrap(),
                )
            })
            .collect();
        let aggregate = Arc::new(
            AggregateExec::try_new(
                AggregateMode::Partial,
                PhysicalGroupBy::default(),
                aggregates,
                vec![None; 2],
                input,
                arrow_schema.clone(),
            )
            .unwrap(),
        );
        let expr = aggregate.dynamic_expressions_produced().pop().unwrap();
        let dynamic_filter = (expr as Arc<dyn std::any::Any + Send + Sync>)
            .downcast::<DynamicFilterPhysicalExpr>()
            .unwrap();
        let predicate = Predicate::with_dyn_filters(vec![], vec![dynamic_filter]);
        let mut stream = aggregate
            .execute(0, Arc::new(TaskContext::default()))
            .unwrap();

        sender
            .unbounded_send(Ok(make_batch(vec![Some(100)], vec![None])))
            .unwrap();
        // Poll until the input channel becomes pending, after processing this batch.
        assert!(futures::poll!(stream.next()).is_pending());
        assert_eq!(
            predicate.prune_with_stats(&stats, &arrow_schema),
            vec![true, true],
            "unknown min(b) must keep a possible first non-NULL value"
        );
        sender
            .unbounded_send(Ok(make_batch(vec![Some(99)], vec![Some(2)])))
            .unwrap();
        assert!(futures::poll!(stream.next()).is_pending());
        assert_eq!(
            predicate.prune_with_stats(&stats, &arrow_schema),
            vec![false, true],
            "known bounds prune only the non-improving group"
        );
        sender
            .unbounded_send(Ok(make_batch(vec![Some(101)], vec![Some(1)])))
            .unwrap();
        drop(sender);
        let results = collect(stream).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(
            ScalarValue::try_from_array(results[0].column(0), 0).unwrap(),
            ScalarValue::Int64(Some(99))
        );
        assert_eq!(
            ScalarValue::try_from_array(results[0].column(1), 0).unwrap(),
            ScalarValue::Int64(Some(1))
        );
    }

    #[tokio::test]
    async fn test_clear_dyn_filters_restores_static_row_group_pruning() {
        use datafusion_physical_expr::expressions::{
            Column as PhysicalColumn, lit as physical_lit,
        };

        let dir = create_temp_dir("dynamic_pruning_reset");
        let (path, arrow_schema) = gen_test_parquet_file(&dir, 30).await;
        let schema = Arc::new(datatypes::schema::Schema::try_from(arrow_schema.clone()).unwrap());
        let builder =
            ParquetRecordBatchStreamBuilder::new(tokio::fs::File::open(path).await.unwrap())
                .await
                .unwrap();
        let stats = RowGroupPruningStatistics::new(builder.metadata().row_groups(), &schema);
        let dynamic_filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(PhysicalColumn::new("cnt", 1))],
            physical_lit(true),
        ));
        let predicate = Predicate::with_dyn_filters(
            vec![col("cnt").gt_eq(lit(10_i32))],
            vec![dynamic_filter.clone()],
        );

        dynamic_filter
            .update(
                Predicate::to_physical_expr(&col("cnt").gt(lit(100_i32)), &arrow_schema).unwrap(),
            )
            .unwrap();
        assert_eq!(
            predicate.prune_with_stats(&stats, &arrow_schema),
            vec![false; 3]
        );

        // Reset after the prior stream is dropped: no dynamic filter remains, while the
        // static predicate still excludes only the first row group.
        predicate.clear_dyn_filters();
        assert_eq!(
            predicate.prune_with_stats(&stats, &arrow_schema),
            vec![false, true, true]
        );
    }

    fn gen_predicate(max_val: i32, op: Operator) -> Vec<Expr> {
        vec![datafusion_expr::Expr::BinaryExpr(BinaryExpr {
            left: Box::new(datafusion_expr::Expr::Column(Column::from_name("cnt"))),
            op,
            right: Box::new(max_val.lit()),
        })]
    }

    #[tokio::test]
    async fn test_prune_empty() {
        assert_prune(3, vec![], vec![true]).await;
    }

    #[tokio::test]
    async fn test_prune_all_match() {
        let p = gen_predicate(3, Operator::Gt);
        assert_prune(2, p, vec![false]).await;
    }

    #[tokio::test]
    async fn test_prune_gt() {
        let p = gen_predicate(29, Operator::Gt);
        assert_prune(
            100,
            p,
            vec![
                false, false, false, true, true, true, true, true, true, true,
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_prune_eq_expr() {
        let p = gen_predicate(30, Operator::Eq);
        assert_prune(40, p, vec![false, false, false, true]).await;
    }

    #[tokio::test]
    async fn test_prune_neq_expr() {
        let p = gen_predicate(30, Operator::NotEq);
        assert_prune(40, p, vec![true, true, true, true]).await;
    }

    #[tokio::test]
    async fn test_prune_gteq_expr() {
        let p = gen_predicate(29, Operator::GtEq);
        assert_prune(40, p, vec![false, false, true, true]).await;
    }

    #[tokio::test]
    async fn test_prune_lt_expr() {
        let p = gen_predicate(30, Operator::Lt);
        assert_prune(40, p, vec![true, true, true, false]).await;
    }

    #[tokio::test]
    async fn test_prune_lteq_expr() {
        let p = gen_predicate(30, Operator::LtEq);
        assert_prune(40, p, vec![true, true, true, true]).await;
    }

    #[tokio::test]
    async fn test_prune_between_expr() {
        let p = gen_predicate(30, Operator::LtEq);
        assert_prune(40, p, vec![true, true, true, true]).await;
    }

    #[tokio::test]
    async fn test_or() {
        // cnt > 30 or cnt < 20
        let e = datafusion_expr::Expr::Column(Column::from_name("cnt"))
            .gt(30.lit())
            .or(datafusion_expr::Expr::Column(Column::from_name("cnt")).lt(20.lit()));
        assert_prune(40, vec![e], vec![true, true, false, true]).await;
    }

    #[tokio::test]
    async fn test_to_physical_expr() {
        let predicate = Predicate::new(vec![
            col("host").eq(lit("host_a")),
            col("ts").gt(lit(ScalarValue::TimestampMicrosecond(Some(123), None))),
        ]);

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            "host",
            arrow::datatypes::DataType::Utf8,
            false,
        )]));

        let predicates = predicate.to_pruning_physical_exprs(&schema).unwrap();
        assert!(!predicates.is_empty());

        let physical_expr = Predicate::to_physical_expr(&col("host").eq(lit("host_a")), &schema);
        assert!(physical_expr.is_ok());
    }

    fn regex_expr(column: &str, op: Operator, pattern: &str) -> Expr {
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(col(column)),
            op,
            right: Box::new(lit(pattern)),
        })
    }

    /// The expression `create_physical_expr` builds on its own, i.e. the implementation before
    /// regex patterns were precompiled. It recompiles the pattern on every evaluation.
    fn arrow_regex_expr(expr: &Expr, schema: &Arc<Schema>) -> Arc<dyn PhysicalExpr> {
        let df_schema = schema.clone().to_dfschema_ref().unwrap();
        create_physical_expr(
            expr,
            df_schema.as_ref(),
            &ExecutionProps::new(),
            &PhysicalPlanningContext::default(),
        )
        .unwrap()
    }

    fn regex_batch(
        data_type: DataType,
        array: ArrayRef,
        nullable: bool,
    ) -> (Arc<Schema>, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![Field::new("host", data_type, nullable)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
        (schema, batch)
    }

    /// Returns the value of `physical` per row, `None` standing for SQL `NULL`.
    fn evaluate_rows(physical: &Arc<dyn PhysicalExpr>, batch: &RecordBatch) -> Vec<Option<bool>> {
        let array = physical
            .evaluate(batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
        (0..array.len())
            .map(|index| array.is_valid(index).then(|| array.value(index)))
            .collect()
    }

    /// Asserts both implementations agree row by row, as predicate values and as the filter mask a
    /// scan derives from them.
    fn assert_same_regex_result(expr: &Expr, schema: &Arc<Schema>, batch: &RecordBatch) {
        let arrow = arrow_regex_expr(expr, schema);
        let precompiled = Predicate::to_physical_expr(expr, schema).unwrap();

        assert_eq!(
            evaluate_rows(&arrow, batch),
            evaluate_rows(&precompiled, batch),
            "values differ for {expr:?}"
        );
        assert_eq!(
            common_recordbatch::filter::batch_filter(batch, &arrow).unwrap(),
            common_recordbatch::filter::batch_filter(batch, &precompiled).unwrap(),
            "filtered rows differ for {expr:?}"
        );
    }

    /// Differential test: the precompiled predicate must produce exactly what arrow's own regex
    /// kernel produces, for every string layout, with and without `NULL`s, for `=~` and `!~`.
    #[test]
    fn test_precompiled_regex_matches_arrow() {
        let plain_nulls = vec![
            Some("host-a"),
            None,
            Some(""),
            Some("db"),
            Some("日本語"),
            Some("HOST-B"),
        ];
        // A dictionary with a `NULL` key and a `NULL` dictionary value: both are SQL `NULL`, and
        // the dictionary value's `NULL` is what a naive kernel loses.
        let dict_values = Arc::new(StringArray::from(vec![
            Some("host-a"),
            Some("HOST-B"),
            Some(""),
            None,
            Some("日本語"),
        ])) as ArrayRef;
        let cases: Vec<(&str, DataType, ArrayRef, bool, bool)> = vec![
            (
                "utf8",
                DataType::Utf8,
                Arc::new(StringArray::from(vec!["host-a", "db", ""])) as ArrayRef,
                false,
                true,
            ),
            (
                "utf8 with nulls",
                DataType::Utf8,
                Arc::new(StringArray::from(plain_nulls.clone())) as ArrayRef,
                true,
                true,
            ),
            (
                "dictionary(UInt32, utf8) with nulls",
                DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
                Arc::new(DictionaryArray::new(
                    UInt32Array::from(vec![Some(0), Some(1), Some(2), None, Some(3), Some(4)]),
                    dict_values.clone(),
                )) as ArrayRef,
                true,
                true,
            ),
            (
                "large utf8 with nulls",
                DataType::LargeUtf8,
                Arc::new(LargeStringArray::from(plain_nulls.clone())) as ArrayRef,
                true,
                false,
            ),
            (
                "utf8 view with nulls",
                DataType::Utf8View,
                Arc::new(StringViewArray::from(plain_nulls.clone())) as ArrayRef,
                true,
                false,
            ),
            (
                "dictionary(UInt8, utf8) with nulls",
                DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
                Arc::new(DictionaryArray::new(
                    UInt8Array::from(vec![Some(0u8), Some(1), Some(2), None, Some(3), Some(4)]),
                    dict_values.clone(),
                )) as ArrayRef,
                true,
                false,
            ),
        ];

        for (name, data_type, array, nullable, expect_precompiled) in cases {
            let (schema, batch) = regex_batch(data_type.clone(), array, nullable);
            for op in [Operator::RegexMatch, Operator::RegexNotMatch] {
                for pattern in [
                    "host.*", "^host-a$", "^(?:.*)$", "", "(?i)host", "日本", "^$",
                ] {
                    let expr = regex_expr("host", op, pattern);
                    let physical = Predicate::to_physical_expr(&expr, &schema).unwrap();
                    assert_eq!(
                        expect_precompiled,
                        physical.downcast_ref::<PrecompiledRegexExpr>().is_some(),
                        "{name}: {op:?} {pattern:?}"
                    );
                    assert_same_regex_result(&expr, &schema, &batch);
                    let slice = batch.slice(1, batch.num_rows() - 1);
                    assert_same_regex_result(&expr, &schema, &slice);
                }
            }
        }

        // A cast decides the compared type: a dictionary column cast to `Utf8` is precompilable
        // even though the column itself is not `Utf8`.
        let (schema, batch) = regex_batch(
            DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
            Arc::new(DictionaryArray::new(
                UInt32Array::from(vec![Some(0), Some(1), Some(2), None, Some(3), Some(4)]),
                dict_values.clone(),
            )) as ArrayRef,
            true,
        );
        let cast = Expr::Cast(datafusion_expr::expr::Cast {
            expr: Box::new(col("host")),
            field: Arc::new(Field::new("host", DataType::Utf8, true)),
        });
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(cast),
            op: Operator::RegexMatch,
            right: Box::new(lit("host.*")),
        });
        let physical = Predicate::to_physical_expr(&expr, &schema).unwrap();
        assert!(physical.downcast_ref::<PrecompiledRegexExpr>().is_some());
        assert_same_regex_result(&expr, &schema, &batch);
    }

    /// Pruning never evaluates the predicate, so its entry point must keep the plain comparison
    /// instead of the evaluation-only [`PrecompiledRegexExpr`]: pruning runs once per file
    /// (manifest pruning) and once per batch (bulk memtable pruning), and a node the pruning
    /// predicate builder does not recognize would only be replaced by `true`.
    #[test]
    fn test_pruning_keeps_the_plain_regex_expr() {
        let (schema, _) = regex_batch(
            DataType::Utf8,
            Arc::new(StringArray::from(vec![Some("host-a"), None, Some("db")])) as ArrayRef,
            true,
        );
        let expr = regex_expr("host", Operator::RegexMatch, "host.*");

        let pruning = Predicate::new(vec![expr.clone()])
            .to_pruning_physical_exprs(&schema)
            .unwrap();
        assert_eq!(1, pruning.len());
        let physical = &pruning[0];
        let mut rewritten = false;
        physical
            .apply(|node| {
                rewritten |= node.downcast_ref::<PrecompiledRegexExpr>().is_some();
                Ok(TreeNodeRecursion::Continue)
            })
            .unwrap();
        assert!(
            !rewritten,
            "pruning must not build the evaluation-only rewrite: {physical}"
        );
        // It is exactly what `create_physical_expr` plans, i.e. what pruning saw before the
        // rewrite existed.
        assert_eq!(
            arrow_regex_expr(&expr, &schema).to_string(),
            physical.to_string()
        );

        // Evaluation keeps the rewrite.
        let evaluated = Predicate::to_physical_expr(&expr, &schema).unwrap();
        assert!(
            evaluated.downcast_ref::<PrecompiledRegexExpr>().is_some(),
            "evaluation must precompile the pattern: {evaluated}"
        );
    }

    /// Pruning still applies the numeric conjunct of a predicate that also holds a regex, so
    /// keeping the plain comparison does not weaken pruning.
    #[tokio::test]
    async fn test_prune_with_regex_conjunct() {
        let e = regex_expr("name", Operator::RegexMatch, "^1.*").and(col("cnt").gt_eq(lit(10_i32)));
        assert_prune(40, vec![e], vec![false, true, true, true]).await;
    }

    /// The precompiled pattern stays uninitialized until the first evaluation and is then reused.
    /// The per-regex compile counter is tested in common-recordbatch.
    #[test]
    #[allow(clippy::print_stdout)]
    fn test_precompiled_regex_defers_compilation_until_evaluation() {
        let (schema, batch) = regex_batch(
            DataType::Utf8,
            Arc::new(StringArray::from(vec![Some("host-a"), None, Some("db")])) as ArrayRef,
            true,
        );
        let expr = regex_expr("host", Operator::RegexMatch, "host.*");
        let physical = Predicate::to_physical_expr(&expr, &schema).unwrap();
        let regex = physical.downcast_ref::<PrecompiledRegexExpr>().unwrap();
        let initialized = || format!("{:?}", regex.filter);
        assert!(
            initialized().contains("initialized: false"),
            "{}",
            initialized()
        );
        println!("after to_physical_expr: regex initializations = 0");

        assert_eq!(
            vec![Some(true), None, Some(false)],
            evaluate_rows(&physical, &batch)
        );
        assert!(
            initialized().contains("initialized: true"),
            "{}",
            initialized()
        );
        println!("after first evaluate: regex initialized = true");
        for _ in 0..100 {
            assert_eq!(
                vec![Some(true), None, Some(false)],
                evaluate_rows(&physical, &batch)
            );
        }
        assert!(
            initialized().contains("initialized: true"),
            "{}",
            initialized()
        );
        println!("after 100 further evaluations: same OnceLock remains initialized");
    }

    /// A malformed regex must be deferred too, but row evaluation still reports an error instead
    /// of silently accepting the predicate as an exact match.
    #[test]
    #[allow(clippy::print_stdout)]
    fn test_invalid_regex_fails_at_evaluation_not_construction() {
        let (schema, batch) = regex_batch(
            DataType::Utf8,
            Arc::new(StringArray::from(vec![Some("host-a")])) as ArrayRef,
            true,
        );
        let expr = regex_expr("host", Operator::RegexMatch, "host(");
        // The old rewrite fell back to DataFusion on eager compilation failure. That physical
        // planner does not compile the pattern, so it too reports this error on evaluation.
        let old_fallback = arrow_regex_expr(&expr, &schema);
        assert!(old_fallback.evaluate(&batch).is_err());
        let physical = Predicate::to_physical_expr(&expr, &schema)
            .expect("construction must retain the invalid regex");
        let regex = physical.downcast_ref::<PrecompiledRegexExpr>().unwrap();
        assert!(format!("{:?}", regex.filter).contains("initialized: false"));
        let error = physical
            .evaluate(&batch)
            .expect_err("invalid regex must fail on evaluate, not construction");
        let DataFusionError::External(source) = &error else {
            panic!("expected a common-recordbatch error, got: {error}");
        };
        assert!(
            matches!(
                source.downcast_ref::<common_recordbatch::error::Error>(),
                Some(common_recordbatch::error::Error::ArrowCompute { .. })
            ),
            "expected ArrowCompute, got: {error}"
        );
        assert!(physical.evaluate(&batch).is_err());
        assert!(format!("{:?}", regex.filter).contains("initialized: true"));
        println!(
            "invalid regex: 0 initializations after to_physical_expr; first evaluate returned an error"
        );
    }

    /// A scan can repeatedly evaluate the same regex expression without changing its results.
    #[test]
    fn test_regex_pattern_reused_across_batches() {
        let (schema, batch) = regex_batch(
            DataType::Utf8,
            Arc::new(StringArray::from(vec![
                Some("host-a"),
                None,
                Some(""),
                Some("db"),
                Some("host-b"),
            ])) as ArrayRef,
            true,
        );
        let expr = regex_expr("host", Operator::RegexMatch, "host.*");

        let physical = Predicate::to_physical_expr(&expr, &schema).unwrap();
        assert!(
            physical.downcast_ref::<PrecompiledRegexExpr>().is_some(),
            "the scan-side predicate holds the lazy pattern: {physical}"
        );
        assert_eq!("host@0 =~ 'host.*'", physical.to_string());

        for _ in 0..100 {
            assert_eq!(
                vec![Some(true), None, Some(false), Some(false), Some(true)],
                evaluate_rows(&physical, &batch)
            );
        }
    }

    /// The predicate a native `rate(x{host=~'host.*'}[1m])` builds when the label column is
    /// nullable, as [`PromPlanner::matchers_to_expr`] plans it: `is_null(host) OR host =~ '...'`.
    ///
    /// `matchers_to_expr` lives in the query crate; this reproduces the expression it emits so the
    /// rewrite is covered for the shape the series scan actually sees. The logical plan is not
    /// touched by the precompiled path, and the rows are unchanged.
    #[test]
    fn test_nullable_label_matcher_keeps_shape_and_rows() {
        let (schema, batch) = regex_batch(
            DataType::Utf8,
            Arc::new(StringArray::from(vec![
                Some("host-a"),
                None,
                Some(""),
                Some("db"),
                Some("host-b"),
            ])) as ArrayRef,
            true,
        );
        let expr =
            col("host")
                .is_null()
                .or(regex_expr("host", Operator::RegexMatch, "^(?:host.*)$"));

        let physical = Predicate::to_physical_expr(&expr, &schema).unwrap();
        // Shape: the `OR` survives, its null test is untouched, and only the comparison below it
        // is replaced by the precompiled form.
        let or = physical
            .downcast_ref::<PhysicalBinaryExpr>()
            .unwrap_or_else(|| panic!("the OR was rewritten into {physical}"));
        assert_eq!(&Operator::Or, or.op());
        assert!(
            or.left()
                .downcast_ref::<datafusion_physical_expr::expressions::IsNullExpr>()
                .is_some(),
            "the null test is unchanged: {}",
            or.left()
        );
        assert!(
            or.right().downcast_ref::<PrecompiledRegexExpr>().is_some(),
            "the regex below the OR is precompiled: {}",
            or.right()
        );

        // The explicit OR makes a missing label match independently of the regex result.
        assert_eq!(
            vec![Some(true), Some(true), Some(false), Some(false), Some(true)],
            evaluate_rows(&physical, &batch)
        );
        assert_same_regex_result(&expr, &schema, &batch);
    }

    /// A scan hands one physical expr to every thread that evaluates it, and the precompiled
    /// expression provides separate caches to concurrent searches. Sharing it must neither panic
    /// nor change an answer.
    #[test]
    fn test_precompiled_regex_is_shared_between_threads() {
        let (schema, batch) = regex_batch(
            DataType::Utf8,
            Arc::new(StringArray::from(vec![
                Some("host-a"),
                None,
                Some(""),
                Some("db"),
                Some("host-b"),
            ])) as ArrayRef,
            true,
        );
        let expr = regex_expr("host", Operator::RegexMatch, "host.*");
        let physical = Predicate::to_physical_expr(&expr, &schema).unwrap();
        assert!(physical.downcast_ref::<PrecompiledRegexExpr>().is_some());
        let expected = vec![Some(true), None, Some(false), Some(false), Some(true)];
        assert_eq!(expected, evaluate_rows(&physical, &batch));

        std::thread::scope(|scope| {
            for _ in 0..8 {
                let physical = Arc::clone(&physical);
                let batch = batch.clone();
                let expected = expected.clone();
                scope.spawn(move || {
                    for _ in 0..50 {
                        assert_eq!(expected, evaluate_rows(&physical, &batch));
                    }
                });
            }
        });

        // The concurrent evaluations neither compiled nor changed the expression.
        assert_eq!(expected, evaluate_rows(&physical, &batch));
    }
}
