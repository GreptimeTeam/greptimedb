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

use std::collections::HashSet;
use std::sync::Arc;

use common_function::aggrs::aggr_wrapper::{StateMergeHelper, is_all_aggr_exprs_steppable};
use common_telemetry::debug;
use datafusion::error::Result as DfResult;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNode};
use promql::extension_plan::{
    EmptyMetric, FusedSeries, InstantManipulate, RangeManipulate, SeriesDivide, SeriesNormalize,
};
use promql::native::plan::reads_across_series;
use store_api::metric_engine_consts::DATA_SCHEMA_TSID_COLUMN_NAME;

use crate::dist_plan::MergeScanLogicalPlan;
use crate::dist_plan::analyzer::AliasMapping;
use crate::dist_plan::merge_sort::{MergeSortLogicalPlan, merge_sort_transformer};

pub struct StepTransformAction {
    extra_parent_plans: Vec<LogicalPlan>,
    new_child_plan: Option<LogicalPlan>,
}

/// generate the upper aggregation plan that will execute on the frontend.
/// Basically a logical plan resembling the following:
/// Projection:
///     Aggregate:
///
/// from Aggregate
///
/// The upper Projection exists sole to make sure parent plan can recognize the output
/// of the upper aggregation plan.
pub fn step_aggr_to_upper_aggr(
    aggr_plan: &LogicalPlan,
) -> datafusion_common::Result<StepTransformAction> {
    let LogicalPlan::Aggregate(input_aggr) = aggr_plan else {
        return Err(datafusion_common::DataFusionError::Plan(
            "step_aggr_to_upper_aggr only accepts Aggregate plan".to_string(),
        ));
    };
    if !is_all_aggr_exprs_steppable(&input_aggr.aggr_expr) {
        return Err(datafusion_common::DataFusionError::NotImplemented(format!(
            "Some aggregate expressions are not steppable in [{}]",
            input_aggr
                .aggr_expr
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }

    let step_aggr_plan = StateMergeHelper::split_aggr_node(input_aggr.clone())?;

    // TODO(discord9): remove duplication
    let ret = StepTransformAction {
        extra_parent_plans: vec![step_aggr_plan.upper_merge.clone()],
        new_child_plan: Some(step_aggr_plan.lower_state.clone()),
    };
    Ok(ret)
}

#[allow(dead_code)]
pub enum Commutativity {
    Commutative,
    PartialCommutative,
    ConditionalCommutative(Option<Transformer>),
    TransformedCommutative {
        /// Return plans from parent to child order
        transformer: Option<StageTransformer>,
    },
    NonCommutative,
    Unimplemented,
    /// For unrelated plans like DDL
    Unsupported,
}

pub struct Categorizer {}

impl Categorizer {
    pub fn check_plan(
        plan: &LogicalPlan,
        partition_cols: Option<AliasMapping>,
    ) -> DfResult<Commutativity> {
        // Subquery is treated separately in `inspect_plan_with_subquery`. To avoid rewrite the
        // "maybe rewritten" plan, stop the check here.
        if has_subquery(plan)? {
            return Ok(Commutativity::Unimplemented);
        }

        let partition_cols = partition_cols.unwrap_or_default();

        let comm = match plan {
            LogicalPlan::Projection(proj) => {
                for expr in &proj.expr {
                    let commutativity = Self::check_expr(expr);
                    if !matches!(commutativity, Commutativity::Commutative) {
                        return Ok(commutativity);
                    }
                }
                Commutativity::Commutative
            }
            // TODO(ruihang): Change this to Commutative once Like is supported in substrait
            LogicalPlan::Filter(filter) => Self::check_expr(&filter.predicate),
            LogicalPlan::Window(_) => Commutativity::Unimplemented,
            LogicalPlan::Aggregate(aggr) => {
                // The state/merge split maps each group expression to one output column,
                // which doesn't hold for grouping sets.
                let has_grouping_set = aggr
                    .group_expr
                    .iter()
                    .any(|expr| matches!(expr, Expr::GroupingSet(_)));
                let is_all_steppable =
                    !has_grouping_set && is_all_aggr_exprs_steppable(&aggr.aggr_expr);
                let matches_partition = Self::check_partition(&aggr.group_expr, &partition_cols);
                if !matches_partition && is_all_steppable {
                    debug!("Plan is steppable: {plan}");
                    return Ok(Commutativity::TransformedCommutative {
                        transformer: Some(Arc::new(|plan: &LogicalPlan| {
                            debug!("Before Step optimize: {plan}");
                            let ret = step_aggr_to_upper_aggr(plan);
                            ret.inspect_err(|err| {
                                common_telemetry::error!("Failed to step aggregate plan: {err:?}");
                            })
                            .map(|s| TransformerAction {
                                extra_parent_plans: s.extra_parent_plans,
                                new_child_plan: s.new_child_plan,
                            })
                        })),
                    });
                }
                if !matches_partition {
                    return Ok(Commutativity::NonCommutative);
                }
                for expr in &aggr.aggr_expr {
                    let commutativity = Self::check_expr(expr);
                    if !matches!(commutativity, Commutativity::Commutative) {
                        return Ok(commutativity);
                    }
                }
                // all group by expressions are partition columns can push down, unless
                // another push down(including `Limit` or `Sort`) is already in progress(which will then prevent next cond commutative node from being push down).
                // TODO(discord9): This is a temporary solution(that works), a better description of
                // commutativity is needed under this situation.
                Commutativity::ConditionalCommutative(None)
            }
            LogicalPlan::Sort(_sort) => {
                if partition_cols.is_empty() {
                    return Ok(Commutativity::Commutative);
                }

                // sort plan needs to consider column priority
                // Change Sort to MergeSort which assumes the input streams are already sorted hence can be more efficient.
                Commutativity::ConditionalCommutative(Some(Arc::new(merge_sort_transformer)))
            }
            LogicalPlan::Join(_) => Commutativity::NonCommutative,
            LogicalPlan::Repartition(_) => {
                // unsupported? or non-commutative
                Commutativity::Unimplemented
            }
            LogicalPlan::Union(_) => Commutativity::Unimplemented,
            LogicalPlan::TableScan(_) => Commutativity::Commutative,
            LogicalPlan::EmptyRelation(_) => Commutativity::NonCommutative,
            LogicalPlan::Subquery(_) => Commutativity::Unimplemented,
            LogicalPlan::SubqueryAlias(_) => Commutativity::Commutative,
            LogicalPlan::Limit(limit) => {
                // Only execute `fetch` on remote nodes.
                // wait for https://github.com/apache/arrow-datafusion/pull/7669
                if partition_cols.is_empty() && limit.fetch.is_some() {
                    Commutativity::Commutative
                } else if limit.skip.is_none() && limit.fetch.is_some() {
                    Commutativity::PartialCommutative
                } else {
                    Commutativity::Unimplemented
                }
            }
            LogicalPlan::Extension(extension) => {
                Self::check_extension_plan(extension.node.as_ref() as _, &partition_cols)
            }
            LogicalPlan::Distinct(_) => {
                if partition_cols.is_empty() {
                    Commutativity::Commutative
                } else {
                    Commutativity::PartialCommutative
                }
            }
            LogicalPlan::Unnest(_) => Commutativity::Commutative,
            LogicalPlan::Statement(_) => Commutativity::Unsupported,
            LogicalPlan::Values(_) => Commutativity::Unsupported,
            LogicalPlan::Explain(_) => Commutativity::Unsupported,
            LogicalPlan::Analyze(_) => Commutativity::Unsupported,
            LogicalPlan::DescribeTable(_) => Commutativity::Unsupported,
            LogicalPlan::Dml(_) => Commutativity::Unsupported,
            LogicalPlan::Ddl(_) => Commutativity::Unsupported,
            LogicalPlan::Copy(_) => Commutativity::Unsupported,
            LogicalPlan::RecursiveQuery(_) => Commutativity::Unsupported,
        };

        Ok(comm)
    }

    pub fn check_extension_plan(
        plan: &dyn UserDefinedLogicalNode,
        partition_cols: &AliasMapping,
    ) -> Commutativity {
        match plan.name() {
            name if name == SeriesDivide::name() => {
                let series_divide = plan.as_any().downcast_ref::<SeriesDivide>().unwrap();
                // Metric engine `__tsid` uniquely identifies a time-series. Treat a series divide
                // that keys by `__tsid` as commutative across regions so it can be pushed down.
                if series_divide
                    .tags()
                    .iter()
                    .any(|tag| tag == DATA_SCHEMA_TSID_COLUMN_NAME)
                {
                    return Commutativity::Commutative;
                }

                let tags = series_divide.tags().iter().collect::<HashSet<_>>();

                for all_alias in partition_cols.values() {
                    let all_alias = all_alias.iter().map(|c| &c.name).collect::<HashSet<_>>();
                    if tags.intersection(&all_alias).count() == 0 {
                        return Commutativity::NonCommutative;
                    }
                }

                Commutativity::Commutative
            }
            name if name == FusedSeries::name() => {
                let fused = plan.as_any().downcast_ref::<FusedSeries>().unwrap();
                // A region builds this node from the region scan itself: the conversion takes that
                // scan's raw-series entrypoint and cannot look through anything in between, so a
                // node whose input is not that scan has nothing to read where it would be placed
                // and stays where it was planned.
                if !matches!(fused.inputs().as_slice(), [LogicalPlan::TableScan(_)]) {
                    return Commutativity::NonCommutative;
                }
                // The region asks the scan for the series of one named metric, so a node that binds
                // no name cannot be built there at all.
                if fused.binding().metric_name.is_none() {
                    return Commutativity::NonCommutative;
                }
                // A region evaluates the tree over whatever series its own scan returns, so this
                // is only answerable when every node of the tree reads one series at a time. An
                // aggregate or a binary operation would fold or pair across series, and one region
                // holds only part of them.
                if reads_across_series(fused.tree().root()) {
                    return Commutativity::NonCommutative;
                }

                // TEMPORARY, and this is the one thing here that is not proved: the placement is
                // only correct while each series reaches exactly one region. A region evaluates the
                // whole tree over the series its scan returns, so a series whose samples are split
                // across regions is evaluated per fragment, and a range function over a fragment is
                // not the range function over the series. Nothing checks that a series stays in one
                // region: the storage partition key is whatever `partition_column_names()` names and
                // may be a column that varies within one series, and this categorizer runs before
                // region selection, so it has no selected-region vector to consult either. A single
                // region deployment satisfies it; a partitioned table over several regions does not,
                // and would return a wrong answer rather than an error.
                //
                // What would make this unconditional is a partition key the label set determines,
                // checked against the table's physical partition expressions. Until then the
                // placement is a proof of concept over one region.
                Commutativity::Commutative
            }
            name if name == SeriesNormalize::name()
                || name == InstantManipulate::name()
                || name == RangeManipulate::name() =>
            {
                // They should always follows Series Divide.
                // Either all commutative or all non-commutative (which will be blocked by SeriesDivide).
                Commutativity::Commutative
            }
            name if name == EmptyMetric::name()
                || name == MergeScanLogicalPlan::name()
                || name == MergeSortLogicalPlan::name() =>
            {
                Commutativity::Unimplemented
            }
            _ => Commutativity::Unsupported,
        }
    }

    pub fn check_expr(expr: &Expr) -> Commutativity {
        #[allow(deprecated)]
        match expr {
            Expr::Column(_)
            | Expr::ScalarVariable(_, _)
            | Expr::Literal(_, _)
            | Expr::BinaryExpr(_)
            | Expr::Not(_)
            | Expr::IsNotNull(_)
            | Expr::IsNull(_)
            | Expr::IsTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNotFalse(_)
            | Expr::Negative(_)
            | Expr::Between(_)
            | Expr::Exists(_)
            | Expr::InList(_)
            | Expr::Case(_) => Commutativity::Commutative,
            // Annotation collection must not affect distribution. Keep scalar UDFs pushdownable;
            // TODO: Preserve datanode PromQL annotations through plan decoding, return them in
            // Flight stream metadata, and merge them on the frontend.
            Expr::ScalarFunction(_) => Commutativity::Commutative,
            Expr::AggregateFunction(_udaf) => Commutativity::Commutative,

            Expr::Like(_)
            | Expr::SimilarTo(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotUnknown(_)
            | Expr::WindowFunction(_)
            | Expr::InSubquery(_)
            | Expr::ScalarSubquery(_)
            | Expr::HigherOrderFunction(_)
            | Expr::Lambda(_)
            | Expr::LambdaVariable(_)
            | Expr::Wildcard { .. } => Commutativity::Unimplemented,

            Expr::Alias(alias) => Self::check_expr(&alias.expr),
            Expr::Cast(cast) => Self::check_expr(&cast.expr),
            Expr::TryCast(try_cast) => Self::check_expr(&try_cast.expr),

            Expr::Unnest(_)
            | Expr::GroupingSet(_)
            | Expr::Placeholder(_)
            | Expr::OuterReferenceColumn(_, _)
            | Expr::SetComparison(_) => Commutativity::Unimplemented,
        }
    }

    /// Return true if the given expr and partition cols satisfied the rule.
    /// In this case the plan can be treated as fully commutative.
    ///
    /// So only if every partition column is itself one of `exprs`, return true.
    /// Otherwise return false.
    ///
    /// An expression that only references a partition column, like `substr(host, 3, 1)`
    /// or `k % 2`, doesn't count: it can put rows from different partitions into the same
    /// group.
    fn check_partition(exprs: &[Expr], partition_cols: &AliasMapping) -> bool {
        let group_cols = exprs
            .iter()
            .filter_map(|expr| {
                let mut expr = expr;
                while let Expr::Alias(alias) = expr {
                    expr = &alias.expr;
                }
                match expr {
                    Expr::Column(column) => Some(column.name.clone()),
                    _ => None,
                }
            })
            .collect::<HashSet<_>>();
        for all_alias in partition_cols.values() {
            let all_alias = all_alias
                .iter()
                .map(|c| c.name.clone())
                .collect::<HashSet<_>>();
            // check if group columns intersect with all alias of partition columns
            // is empty, if it's empty, not all partition columns show up in `exprs`
            if group_cols.intersection(&all_alias).count() == 0 {
                return false;
            }
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::time::{Duration, UNIX_EPOCH};

    use datafusion::arrow::datatypes::{
        DataType as ArrowDataType, Field, TimeUnit as ArrowTimeUnit,
    };
    use datafusion::common::{DFSchema, DFSchemaRef};
    use datafusion::datasource::DefaultTableSource;
    use datafusion_common::Column;
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::LogicalPlanBuilder;
    use datafusion_expr::expr::ScalarFunction;
    use datafusion_expr::{Extension, LogicalPlan, UserDefinedLogicalNodeCore};
    use datafusion_functions::core::coalesce;
    use datafusion_optimizer::analyzer::AnalyzerRule;
    use datatypes::series::SeriesOrdering;
    use datatypes::series::samples::{FieldSpec, SeriesSchema};
    use promql::extension_plan::{FusedSeries, FusedSeriesBinding};
    use promql::functions::{
        NativeHistogramDrop, NativeHistogramFraction, NativeHistogramQuantile,
    };
    use promql::native::plan::{
        AggregateKind, AggregateStage, AggregateStateSchema, BoundAggregate, BoundInput, Grouping,
        NativeExpr, NativeNode, NativeType,
    };
    use promql_parser::parser;
    use store_api::storage::RegionId;
    use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};

    use super::*;
    use crate::dist_plan::DistPlannerAnalyzer;
    use crate::optimizer::test_util::mock_table_provider;
    use crate::promql::native_compile::{compile, statement_grid};
    use crate::query_engine::DefaultSerializer;

    #[test]
    fn series_divide_by_tsid_is_commutative() {
        let input = LogicalPlanBuilder::empty(false).build().unwrap();
        let series_divide = SeriesDivide::new(
            vec![DATA_SCHEMA_TSID_COLUMN_NAME.to_string()],
            "ts".to_string(),
            input,
        );

        let partition_cols: AliasMapping = BTreeMap::from([(
            "some_partition_col".to_string(),
            BTreeSet::from([Column::from_name("some_partition_col")]),
        )]);

        let commutativity = Categorizer::check_extension_plan(&series_divide, &partition_cols);
        assert!(matches!(commutativity, Commutativity::Commutative));
    }

    #[test]
    fn annotated_histogram_helpers_do_not_block_pushdown() {
        for udf in [
            NativeHistogramQuantile::scalar_udf(),
            NativeHistogramFraction::scalar_udf(),
            NativeHistogramDrop::warning_bool_false_udf(String::new(), None),
        ] {
            let helper = Expr::ScalarFunction(ScalarFunction::new_udf(Arc::new(udf), vec![]));
            let expr = Expr::ScalarFunction(ScalarFunction::new_udf(coalesce(), vec![helper]));
            assert!(matches!(
                Categorizer::check_expr(&expr),
                Commutativity::Commutative
            ));
        }
    }

    /// The statement one `FusedSeries` test node is bound to.
    fn native_statement() -> promql_parser::parser::EvalStmt {
        promql_parser::parser::EvalStmt {
            expr: parser::parse("some_metric").unwrap(),
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        }
    }

    /// A `FusedSeries` node over `input`, bound to the tree a bare selector compiles to.
    ///
    /// The tree is the simplest shape the compiler accepts, because what these tests change is
    /// where the node may be placed, not what it computes.
    fn fused_series_plan(input: LogicalPlan) -> LogicalPlan {
        fused_series_plan_named(input, Some("some_metric"))
    }

    /// The same node, with the metric name it binds chosen by the caller.
    fn fused_series_plan_named(input: LogicalPlan, metric_name: Option<&str>) -> LogicalPlan {
        let stmt = native_statement();
        let series_schema = Arc::new(SeriesSchema {
            fields: Arc::from([FieldSpec { name: "v0".into() }]),
            eval_grid: None,
        });
        let tree = compile(
            &stmt,
            Arc::clone(&series_schema),
            SeriesOrdering::KeyThenTime,
        )
        .unwrap();
        let binding = FusedSeriesBinding {
            series_schema,
            grid: statement_grid(&stmt).unwrap(),
            metric_name: metric_name.map(Arc::from),
            time_index: "ts".to_string(),
            tags: Vec::new(),
        };
        let output_schema: DFSchemaRef = Arc::new(
            DFSchema::new_with_metadata(
                vec![
                    (
                        None,
                        Arc::new(Field::new("v0", ArrowDataType::Float64, true)),
                    ),
                    (
                        None,
                        Arc::new(Field::new(
                            "ts",
                            ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, None),
                            false,
                        )),
                    ),
                ],
                Default::default(),
            )
            .unwrap(),
        );

        LogicalPlan::Extension(Extension {
            node: Arc::new(
                FusedSeries::try_new(
                    Arc::from("some_metric"),
                    tree,
                    binding,
                    input,
                    output_schema,
                )
                .unwrap(),
            ),
        })
    }

    /// The region's own view of the metric's table: a scan whose provider is the region provider.
    fn table_scan_plan() -> LogicalPlan {
        let provider = mock_table_provider(RegionId::new(1, 1));
        LogicalPlanBuilder::scan(
            "some_metric",
            Arc::new(DefaultTableSource::new(Arc::new(provider))),
            None,
        )
        .unwrap()
        .build()
        .unwrap()
    }

    fn holds_fused_series(plan: &LogicalPlan) -> bool {
        if let LogicalPlan::Extension(extension) = plan
            && extension.node.as_any().is::<FusedSeries>()
        {
            return true;
        }
        plan.inputs().into_iter().any(holds_fused_series)
    }

    /// An extension node with no `ExtensionSerializer` branch: neither one under its own name nor
    /// one it shares, since the serializer lists names and not shapes. It stands in for the nodes
    /// that cannot reach a region as bytes for their own reasons, which the encoding gate must keep
    /// honouring. The categorizer refuses it, as it refuses every name it does not know.
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct UnserializableNode {
        input: LogicalPlan,
        schema: DFSchemaRef,
    }

    impl PartialOrd for UnserializableNode {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            let ordering = self.input.partial_cmp(&other.input);
            match ordering {
                Some(std::cmp::Ordering::Equal) if self.schema != other.schema => None,
                ordering => ordering,
            }
        }
    }

    impl UnserializableNode {
        fn new(input: LogicalPlan) -> Self {
            let schema = input.schema().clone();
            Self { input, schema }
        }
    }

    impl UserDefinedLogicalNodeCore for UnserializableNode {
        fn name(&self) -> &str {
            "UnserializableNode"
        }

        fn inputs(&self) -> Vec<&LogicalPlan> {
            vec![&self.input]
        }

        fn schema(&self) -> &DFSchemaRef {
            &self.schema
        }

        fn expressions(&self) -> Vec<Expr> {
            vec![]
        }

        fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "UnserializableNode")
        }

        fn with_exprs_and_inputs(
            &self,
            _exprs: Vec<Expr>,
            mut inputs: Vec<LogicalPlan>,
        ) -> DfResult<Self> {
            Ok(Self::new(inputs.swap_remove(0)))
        }
    }

    /// The plan that node builds over `input`.
    fn unserializable_plan(input: LogicalPlan) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(UnserializableNode::new(input)),
        })
    }

    fn holds_unserializable_node(plan: &LogicalPlan) -> bool {
        if let LogicalPlan::Extension(extension) = plan
            && extension.node.as_any().is::<UnserializableNode>()
        {
            return true;
        }
        plan.inputs().into_iter().any(holds_unserializable_node)
    }

    fn holds_table_scan(plan: &LogicalPlan) -> bool {
        if matches!(plan, LogicalPlan::TableScan(_)) {
            return true;
        }
        plan.inputs().into_iter().any(holds_table_scan)
    }

    /// The merge scan `plan` is, if it is one.
    fn as_merge_scan(plan: &LogicalPlan) -> Option<&MergeScanLogicalPlan> {
        if let LogicalPlan::Extension(extension) = plan {
            extension
                .node
                .as_any()
                .downcast_ref::<MergeScanLogicalPlan>()
        } else {
            None
        }
    }

    /// The first merge scan in `plan`'s visible tree. A merge scan's remote input is not one of its
    /// children, so this does not look into one.
    fn first_merge_scan(plan: &LogicalPlan) -> Option<&MergeScanLogicalPlan> {
        if let Some(merge_scan) = as_merge_scan(plan) {
            return Some(merge_scan);
        }
        plan.inputs().into_iter().find_map(first_merge_scan)
    }

    fn analyze(analyzer: DistPlannerAnalyzer, plan: LogicalPlan) -> LogicalPlan {
        analyzer.analyze(plan, &ConfigOptions::default()).unwrap()
    }

    /// Whether the remote input of any merge scan in `plan` holds a node `wanted` accepts.
    fn any_remote_input_holds(plan: &LogicalPlan, wanted: fn(&LogicalPlan) -> bool) -> bool {
        let mut holds = false;
        plan.apply(|node| {
            if let Some(merge_scan) = as_merge_scan(node) {
                holds |= wanted(merge_scan.input());
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        holds
    }

    #[test]
    fn fused_series_over_its_scan_is_commutative() {
        let plan = fused_series_plan(table_scan_plan());

        assert!(matches!(
            Categorizer::check_plan(&plan, None).unwrap(),
            Commutativity::Commutative
        ));
    }

    /// The region builds the node from the scan's own raw-series entrypoint, so an input that is
    /// not that scan leaves the node with nothing to read where it would be placed.
    #[test]
    fn fused_series_over_anything_but_a_scan_is_not_commutative() {
        let scan = table_scan_plan();
        let input = LogicalPlanBuilder::from(scan)
            .filter(datafusion_expr::col("k0").eq(datafusion_expr::lit("a")))
            .unwrap()
            .build()
            .unwrap();
        let plan = fused_series_plan(input);

        assert!(matches!(
            Categorizer::check_plan(&plan, None).unwrap(),
            Commutativity::NonCommutative
        ));
    }

    /// The region asks the scan for one named metric's series, so a node binding no name cannot
    /// be built there.
    #[test]
    fn fused_series_that_binds_no_metric_name_is_not_commutative() {
        let plan = fused_series_plan_named(table_scan_plan(), None);

        assert!(matches!(
            Categorizer::check_plan(&plan, None).unwrap(),
            Commutativity::NonCommutative
        ));
    }

    /// A tree built by something other than the compiler can hold a node that folds several
    /// series. The check is a property of the tree, so it is exercised on the node itself: the
    /// compiler emits no such node today, and nothing else would build a tree containing one.
    #[test]
    fn a_tree_that_folds_across_series_is_not_per_series() {
        let raw = NativeType::Samples(Arc::new(SeriesSchema {
            fields: Arc::from([FieldSpec { name: "v0".into() }]),
            eval_grid: None,
        }));
        let input = NativeNode {
            expr: NativeExpr::Input(BoundInput {
                output: raw.clone(),
                ordering: SeriesOrdering::KeyThenTime,
            }),
            output: raw,
        };
        let group = Grouping::By(Arc::from(["k0".into()]));
        let aggregate = NativeNode {
            expr: NativeExpr::Aggregate {
                input: Box::new(input),
                op: BoundAggregate {
                    kind: AggregateKind::Sum,
                    group: group.clone(),
                    stage: AggregateStage::Partial,
                },
            },
            output: NativeType::AggregateState(Arc::new(AggregateStateSchema {
                group,
                accumulators: Arc::from([]),
                grid: statement_grid(&native_statement()).unwrap(),
            })),
        };
        assert!(reads_across_series(&aggregate));

        // The same walk over what the compiler emits stays per-series.
        let single_selector = compile(
            &native_statement(),
            Arc::new(SeriesSchema {
                fields: Arc::from([FieldSpec { name: "v0".into() }]),
                eval_grid: None,
            }),
            SeriesOrdering::KeyThenTime,
        )
        .unwrap();
        assert!(!reads_across_series(single_selector.root()));
    }

    /// The encoding gate is what kept the node on the frontend, and it only exists for a plan that
    /// has to be encoded.
    ///
    /// `PlanRewriter::should_expand` asked two questions about a node before it let the descent
    /// continue, and the Substrait one came first: unless the *whole* root plan encoded, every
    /// subtree that failed to encode was expanded on, whichever way `check_plan` answered. A
    /// `FusedSeries` node has no serializer branch — `ExtensionSerializer` answers `NotImplemented`
    /// for every name it does not list — so over the wire the boundary is still drawn at the node
    /// and the region receives the bare scan.
    ///
    /// A plan handed to `RegionServer::handle_read` as a `LogicalPlan` is never encoded, so there
    /// the gate has nothing to say and the commutativity arm decides: the node rides inside the
    /// merge scan.
    #[test]
    fn fused_series_rides_to_the_region_only_when_regions_are_reached_in_process() {
        let plan = fused_series_plan(table_scan_plan());

        assert!(
            DFLogicalSubstraitConvertor
                .encode(&plan, DefaultSerializer)
                .is_err(),
            "the fused node encodes now, so the encoding gate decides nothing"
        );
        assert!(
            DFLogicalSubstraitConvertor
                .encode(&table_scan_plan(), DefaultSerializer)
                .is_ok(),
            "the scan under the fused node encodes, so the fused node is the only reason this plan does not"
        );

        // Over the wire the plan has to encode, so the boundary is drawn at the node.
        let encoded = analyze(DistPlannerAnalyzer::new(false), plan.clone());
        assert!(
            !any_remote_input_holds(&encoded, holds_fused_series),
            "the analyzer placed the node although its plan has to encode: {encoded}"
        );

        // In process nothing is encoded, so the categorizer places the node.
        let in_process = analyze(DistPlannerAnalyzer::new(true), plan);
        assert!(
            any_remote_input_holds(&in_process, holds_fused_series),
            "the analyzer kept the node on the frontend although regions are reached in process: {in_process}"
        );
    }

    /// Skipping the encoding gate removes one reason to expand and adds none: a node that does not
    /// encode for its own reasons, and that the categorizer refuses, still draws the boundary at
    /// itself when regions are reached in process.
    ///
    /// Without this, the skip would amount to placing every unencodable node region-side.
    #[test]
    fn a_plan_that_does_not_encode_for_another_reason_still_expands_at_that_node() {
        let plan = unserializable_plan(table_scan_plan());
        assert!(
            DFLogicalSubstraitConvertor
                .encode(&plan, DefaultSerializer)
                .is_err(),
            "the stand-in node has a serializer branch now, so it no longer proves anything"
        );

        let analyzed = analyze(DistPlannerAnalyzer::new(true), plan);

        assert!(
            !any_remote_input_holds(&analyzed, holds_unserializable_node),
            "the analyzer placed a node the categorizer refuses: {analyzed}"
        );
        let inputs = analyzed.inputs();
        let [input] = inputs.as_slice() else {
            panic!("expected the node to keep one input in: {analyzed}");
        };
        // `expand` keeps a projection over the merge scan to restore the schema of what it wrapped.
        let merge_scan = first_merge_scan(input)
            .unwrap_or_else(|| panic!("expected the boundary right below the node in: {analyzed}"));
        assert!(
            holds_table_scan(merge_scan.input()),
            "expected the scan under the node to be the region input in: {analyzed}"
        );
    }
}

pub type Transformer = Arc<dyn Fn(&LogicalPlan) -> Option<LogicalPlan>>;

/// Returns transformer action that need to be applied
pub type StageTransformer = Arc<dyn Fn(&LogicalPlan) -> DfResult<TransformerAction>>;

/// The Action that a transformer should take on the plan.
pub struct TransformerAction {
    /// list of plans that need to be applied to parent plans, in the order of parent to child.
    /// i.e. if this returns `[Projection, Aggregate]`, then the parent plan should be transformed to
    /// ```ignore
    /// Original Parent Plan:
    ///     Projection:
    ///         Aggregate:
    ///             MergeScan: ...
    /// ```
    pub extra_parent_plans: Vec<LogicalPlan>,
    /// new child plan, if None, use the original plan.
    pub new_child_plan: Option<LogicalPlan>,
}

pub fn partial_commutative_transformer(plan: &LogicalPlan) -> Option<LogicalPlan> {
    Some(plan.clone())
}

fn has_subquery(plan: &LogicalPlan) -> DfResult<bool> {
    let mut found = false;
    plan.apply_expressions(|e| {
        e.apply(|x| {
            if matches!(
                x,
                Expr::Exists(_) | Expr::InSubquery(_) | Expr::ScalarSubquery(_)
            ) {
                found = true;
                Ok(TreeNodeRecursion::Stop)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        })
    })?;
    Ok(found)
}

#[cfg(test)]
mod test {
    use datafusion_expr::{LogicalPlanBuilder, Sort};

    use super::*;

    #[test]
    fn sort_on_empty_partition() {
        let plan = LogicalPlan::Sort(Sort {
            expr: vec![],
            input: Arc::new(LogicalPlanBuilder::empty(false).build().unwrap()),
            fetch: None,
        });
        assert!(matches!(
            Categorizer::check_plan(&plan, Some(Default::default())).unwrap(),
            Commutativity::Commutative
        ));
    }
}
