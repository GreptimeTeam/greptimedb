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

//! Binding between a table-scan position and the sequences it must read after.
//!
//! A [`ScanSequenceBinding`] is a transparent, single-child logical node wrapping
//! exactly one [`TableScan`]. It carries the query-local [`ScanId`] of that scan
//! position plus the per-region sequence lower bounds ([`RegionSeqs`]) the scan
//! must honour, so that a distributed sub-plan can be decoded back into the same
//! scan position and read the identical rows.
//!
//! The node is transparent: it does not filter, reorder, or project rows. That is
//! why [`ScanSequenceBinding::prevent_predicate_push_down_columns`] returns an
//! empty set and [`ScanSequenceBinding::necessary_children_exprs`] passes the
//! parent's column requirements through to the wrapped [`TableScan`]. Without
//! those two overrides the default [`UserDefinedLogicalNodeCore`]
//! implementations keep the scan on a full-column, unfiltered read.
//!
//! This module only defines the plan node and assigns scan ids. Validating the
//! sequence bounds against query parameters (parameter binding) and lowering the
//! node to a physical plan are follow-up steps; this module does not implement
//! serialization either.

use std::collections::{BTreeMap, HashSet};
use std::fmt::{Formatter, Result as FmtResult};
use std::sync::Arc;

use datafusion::error::Result as DfResult;
use datafusion_common::tree_node::Transformed;
use datafusion_common::{DFSchemaRef, TableReference, plan_err};
use datafusion_expr::logical_plan::TableScan;
use datafusion_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore};

/// Identifies one table-scan position inside a single logical plan.
///
/// Ids are assigned per plan, following a fixed post-order walk from `0`, so one
/// id never refers to two different scan positions, and two positions that share
/// the same table provider still get different ids.
pub type ScanId = u32;

/// Per-region sequence lower bounds a scan must read after, keyed by region id.
///
/// An empty map means "not bound yet": the scan reads the visible sequence
/// without a lower bound.
pub type RegionSeqs = BTreeMap<u64, u64>;

/// One row of the scan layout produced by [`identify_sequence_scans`].
///
/// The layout is ordered by assignment, so the entry of `scan_id` is at index
/// `scan_id as usize`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanLayoutEntry {
    scan_id: ScanId,
    source: TableReference,
}

impl ScanLayoutEntry {
    /// Creates a layout entry for the scan position `scan_id` on `source`.
    pub fn new(scan_id: ScanId, source: TableReference) -> Self {
        Self { scan_id, source }
    }

    /// Returns the id of the scan position.
    pub fn scan_id(&self) -> ScanId {
        self.scan_id
    }

    /// Returns the table reference the scan position reads from.
    pub fn source(&self) -> &TableReference {
        &self.source
    }
}

/// Binds a table-scan position to the sequences it reads after.
///
/// The node has exactly one child and its output schema is the child's schema.
///
/// Equality and hashing cover [`ScanSequenceBinding::scan_id`],
/// [`ScanSequenceBinding::after_seqs`] and the child plan, so two bindings that
/// read after different sequences are never treated as the same node by
/// plan-level rewriting or deduplication.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct ScanSequenceBinding {
    scan_id: ScanId,
    after_seqs: RegionSeqs,
    /// The wrapped plan node, normally the [`TableScan`] this binding belongs to.
    ///
    /// It is a [`LogicalPlan`], not a [`TableScan`], because
    /// [`UserDefinedLogicalNodeCore::inputs`] has to hand out a `&LogicalPlan`,
    /// and because rewriting passes may hand over a transient plan on top of the
    /// scan: filter push-down first wraps the scan in a
    /// [`LogicalPlan::Filter`](datafusion_expr::logical_plan::Filter) and only
    /// the next rewrite step moves that predicate into [`TableScan`]'s filters.
    ///
    /// [`ScanSequenceBinding::try_new`] is the constructor that enforces the
    /// wrapped node being a [`TableScan`].
    input: LogicalPlan,
}

impl ScanSequenceBinding {
    /// Name of the plan node, as shown in explain output.
    pub fn name() -> &'static str {
        "ScanSequenceBinding"
    }

    /// Creates a binding over `input` with the given sequence bounds.
    pub fn new(scan_id: ScanId, after_seqs: RegionSeqs, input: TableScan) -> Self {
        Self {
            scan_id,
            after_seqs,
            input: LogicalPlan::TableScan(input),
        }
    }

    /// Creates a binding over `input`, requiring `input` to be a [`TableScan`].
    ///
    /// Returns an error when `input` is not a [`LogicalPlan::TableScan`]: only a
    /// scan is something a distributed sub-plan can be decoded into.
    pub fn try_new(scan_id: ScanId, after_seqs: RegionSeqs, input: LogicalPlan) -> DfResult<Self> {
        if !matches!(input, LogicalPlan::TableScan(_)) {
            return plan_err!(
                "{} requires a TableScan input, got: {}",
                Self::name(),
                input.display_indent()
            );
        }

        Ok(Self {
            scan_id,
            after_seqs,
            input,
        })
    }

    /// Returns the id of this scan position.
    pub fn scan_id(&self) -> ScanId {
        self.scan_id
    }

    /// Returns the per-region sequence lower bounds; empty when not bound yet.
    pub fn after_seqs(&self) -> &RegionSeqs {
        &self.after_seqs
    }

    /// Returns the wrapped plan node, normally a [`LogicalPlan::TableScan`].
    pub fn input(&self) -> &LogicalPlan {
        &self.input
    }

    /// Returns the wrapped [`TableScan`], or `None` while a rewrite pass holds a
    /// transient plan (such as a pushed-down filter) on top of the scan.
    pub fn table_scan(&self) -> Option<&TableScan> {
        match &self.input {
            LogicalPlan::TableScan(scan) => Some(scan),
            _ => None,
        }
    }

    /// Wraps this binding into a [`LogicalPlan::Extension`] node.
    pub fn into_logical_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }

    /// Renders [`ScanSequenceBinding::after_seqs`] for explain output.
    fn fmt_after_seqs(&self) -> String {
        if self.after_seqs.is_empty() {
            return "unbound".to_string();
        }

        let seqs = self
            .after_seqs
            .iter()
            .map(|(region_id, seq)| format!("{region_id}:{seq}"))
            .collect::<Vec<_>>()
            .join(",");

        format!("{{{seqs}}}")
    }
}

impl UserDefinedLogicalNodeCore for ScanSequenceBinding {
    fn name(&self) -> &str {
        Self::name()
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    /// The node does not change rows, so every predicate may be pushed below it.
    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        HashSet::new()
    }

    /// The node's schema is the child's schema, so the child needs exactly the
    /// columns the parent asked for.
    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![output_columns.to_vec()])
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> FmtResult {
        write!(
            f,
            "{}: scan_id={}, after_seqs={}",
            Self::name(),
            self.scan_id,
            self.fmt_after_seqs()
        )?;

        if let Some(scan) = self.table_scan() {
            write!(f, ", source={}", scan.table_name)?;
        }

        Ok(())
    }

    /// Rebuilds the binding with new inputs while keeping `scan_id` and
    /// `after_seqs`: the binding describes a scan position, not the current plan
    /// shape on top of it.
    ///
    /// The new input is not required to be a [`TableScan`] because rewriting
    /// passes hand over intermediate plans (see [`ScanSequenceBinding`]'s `input`
    /// field); the binding only stops being explainable as a scan while such a
    /// plan sits on top of it.
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DfResult<Self> {
        if inputs.len() != 1 {
            return plan_err!(
                "{} expects exactly one input, got {}",
                Self::name(),
                inputs.len()
            );
        }

        Ok(Self {
            scan_id: self.scan_id,
            after_seqs: self.after_seqs.clone(),
            input: inputs.pop().unwrap(),
        })
    }
}

/// Assigns a [`ScanId`] to every table-scan position in `plan`.
///
/// Scans are numbered in a fixed post-order walk: subqueries of a node are
/// visited before its plan inputs and the inputs in order, and every
/// [`TableScan`] position takes the next [`ScanId`], starting from `0`. Two
/// positions sharing the same table provider are numbered separately.
///
/// Returns the numbered plan together with its layout; both come from the same
/// walk, so `layout[i]` is the [`ScanLayoutEntry`] of [`ScanId`] `i`. Bindings
/// are created without sequence bounds; binding them is a later step.
///
/// The plan is expected to be un-numbered: existing bindings are neither
/// detected nor re-numbered. DML write targets are not handled here, since a
/// write target is not a [`LogicalPlan::TableScan`] position to read.
pub fn identify_sequence_scans(plan: LogicalPlan) -> DfResult<(LogicalPlan, Vec<ScanLayoutEntry>)> {
    let mut assigner = ScanIdAssigner::default();
    let plan = plan
        .transform_up_with_subqueries(|plan| assigner.assign(plan))?
        .data;

    Ok((plan, assigner.layout))
}

/// Allocates scan ids during one [`identify_sequence_scans`] walk.
#[derive(Debug, Default)]
struct ScanIdAssigner {
    next_scan_id: ScanId,
    layout: Vec<ScanLayoutEntry>,
}

impl ScanIdAssigner {
    /// Wraps `plan` when it is a [`TableScan`], and returns it unchanged
    /// otherwise.
    fn assign(&mut self, plan: LogicalPlan) -> DfResult<Transformed<LogicalPlan>> {
        let LogicalPlan::TableScan(scan) = plan else {
            return Ok(Transformed::no(plan));
        };

        let scan_id = self.allocate(scan.table_name.clone());
        let binding = ScanSequenceBinding::new(scan_id, RegionSeqs::new(), scan);

        Ok(Transformed::yes(binding.into_logical_plan()))
    }

    /// Moves to the next scan id and records its layout entry.
    fn allocate(&mut self, source: TableReference) -> ScanId {
        let scan_id = self.next_scan_id;
        self.next_scan_id += 1;
        self.layout.push(ScanLayoutEntry::new(scan_id, source));

        scan_id
    }
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    use datafusion::datasource::DefaultTableSource;
    use datafusion::execution::SessionState;
    use datafusion_common::tree_node::TreeNodeRecursion;
    use datafusion_expr::{LogicalPlanBuilder, TableScanBuilder, col, lit};

    use super::*;
    use crate::QueryEngineFactory;
    use crate::dummy_catalog::DummyTableProvider;
    use crate::optimizer::test_util::mock_table_provider;
    use crate::options::QueryOptions;

    /// Builds a [`SessionState`] of a query engine without the dist planner.
    fn session_state() -> SessionState {
        let catalog_list = catalog::memory::new_memory_catalog_manager().unwrap();

        QueryEngineFactory::new(
            catalog_list,
            None,
            None,
            None,
            None,
            false,
            QueryOptions::default(),
        )
        .query_engine()
        .engine_state()
        .session_state()
    }

    /// Runs the analyzer and the logical optimizer of `state` on `plan`.
    fn analyze_and_optimize(state: &SessionState, plan: LogicalPlan) -> LogicalPlan {
        let analyzed = state
            .analyzer()
            .execute_and_check(plan, state.config_options(), |_, _| {})
            .unwrap();

        state
            .optimizer()
            .optimize(analyzed, state, |_, _| {})
            .unwrap()
    }

    fn scan_plan(provider: Arc<DummyTableProvider>, name: &str) -> LogicalPlan {
        LogicalPlanBuilder::scan(name, Arc::new(DefaultTableSource::new(provider)), None)
            .unwrap()
            .build()
            .unwrap()
    }

    fn into_scan(plan: LogicalPlan) -> TableScan {
        let LogicalPlan::TableScan(scan) = plan else {
            panic!("expects a TableScan");
        };

        scan
    }

    fn with_filters(scan: &TableScan, filters: Vec<Expr>) -> LogicalPlan {
        let scan = TableScanBuilder::from(scan.clone())
            .with_filters(filters)
            .build()
            .unwrap();

        LogicalPlan::TableScan(scan)
    }

    fn collect_bindings(plan: &LogicalPlan) -> Vec<ScanSequenceBinding> {
        let mut bindings = Vec::new();
        plan.apply_with_subqueries(|plan| {
            if let LogicalPlan::Extension(extension) = plan
                && let Some(binding) = extension
                    .node
                    .as_any()
                    .downcast_ref::<ScanSequenceBinding>()
            {
                bindings.push(binding.clone());
            }

            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();

        bindings
    }

    fn count_table_scans(plan: &LogicalPlan) -> usize {
        let mut scans = 0;
        plan.apply_with_subqueries(|plan| {
            if matches!(plan, LogicalPlan::TableScan(_)) {
                scans += 1;
            }

            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();

        scans
    }

    fn hash_of(binding: &ScanSequenceBinding) -> u64 {
        let mut hasher = DefaultHasher::new();
        binding.hash(&mut hasher);

        hasher.finish()
    }

    #[test]
    fn test_identify_numbers_each_scan_position() {
        let provider = Arc::new(mock_table_provider(1.into()));
        // Both scans share the same provider, so they are equal `TableScan`s, but
        // they are different scan positions.
        let left = scan_plan(provider.clone(), "devices");
        let right = scan_plan(provider, "devices");
        assert_eq!(left, right);
        let plan = LogicalPlanBuilder::from(left)
            .union(right)
            .unwrap()
            .build()
            .unwrap();

        let (numbered, layout) = identify_sequence_scans(plan).unwrap();

        let bindings = collect_bindings(&numbered);
        assert_eq!(2, bindings.len());
        assert_eq!(0, bindings[0].scan_id());
        assert_eq!(1, bindings[1].scan_id());
        // Every scan is wrapped, none is left bare.
        assert_eq!(2, count_table_scans(&numbered));
        for binding in &bindings {
            assert!(matches!(binding.input(), LogicalPlan::TableScan(_)));
            assert!(binding.after_seqs().is_empty());
        }

        assert_eq!(2, layout.len());
        assert_eq!(0, layout[0].scan_id());
        assert_eq!(1, layout[1].scan_id());
        assert_eq!(layout[0].source(), layout[1].source());
        assert_eq!("devices", layout[0].source().table());
    }

    #[test]
    fn test_optimizer_keeps_binding_adjacent_to_scan() {
        let state = session_state();
        let provider = Arc::new(mock_table_provider(1.into()));
        let scan = into_scan(scan_plan(provider, "devices"));
        let after_seqs = RegionSeqs::from([(1, 42)]);
        let binding = ScanSequenceBinding::new(0, after_seqs.clone(), scan);
        let plan = LogicalPlanBuilder::from(binding.into_logical_plan())
            .project(vec![col("v0")])
            .unwrap()
            .build()
            .unwrap();

        let optimized = analyze_and_optimize(&state, plan);

        let bindings = collect_bindings(&optimized);
        assert_eq!(1, bindings.len());
        let binding = &bindings[0];
        assert_eq!(0, binding.scan_id());
        assert_eq!(&after_seqs, binding.after_seqs());
        // The optimizer may put a projection above the binding, but the binding's
        // own child has to stay the scan.
        assert!(matches!(binding.input(), LogicalPlan::TableScan(_)));
        assert_eq!(1, count_table_scans(&optimized));
    }

    #[test]
    #[allow(clippy::print_stdout)]
    fn test_optimizer_pushes_filters_and_projection_into_bound_scan() {
        let state = session_state();
        let provider = Arc::new(mock_table_provider(1.into()));
        let scan = into_scan(scan_plan(provider, "devices"));
        let binding = ScanSequenceBinding::new(0, RegionSeqs::new(), scan);
        let plan = LogicalPlanBuilder::from(binding.into_logical_plan())
            .filter(col("k0").eq(lit("hello")))
            .unwrap()
            .project(vec![col("v0")])
            .unwrap()
            .build()
            .unwrap();

        let optimized = analyze_and_optimize(&state, plan);
        // Printed to inspect the pushed-down scan by hand; run the test with
        // `--nocapture` to see it.
        println!("optimized plan:\n{}", optimized.display_indent());

        let bindings = collect_bindings(&optimized);
        assert_eq!(1, bindings.len());
        let scan = bindings[0]
            .table_scan()
            .expect("the binding wraps a TableScan");

        assert_eq!(1, scan.filters.len());
        let filter = scan.filters[0].to_string();
        assert!(filter.contains("k0"), "unexpected filter: {filter}");
        assert!(filter.contains("hello"), "unexpected filter: {filter}");
        // `devices` has 3 columns (`k0`, `ts`, `v0`); only `v0` is needed.
        assert_eq!(Some(vec![2]), scan.projection);
    }

    #[test]
    fn test_bindings_with_different_after_seqs_are_distinct() {
        let state = session_state();
        let provider = Arc::new(mock_table_provider(1.into()));
        let binding_a = ScanSequenceBinding::new(
            0,
            RegionSeqs::from([(1, 10)]),
            into_scan(scan_plan(provider.clone(), "devices")),
        );
        let binding_b = ScanSequenceBinding::new(
            0,
            RegionSeqs::from([(1, 20)]),
            into_scan(scan_plan(provider, "devices")),
        );

        // The wrapped scans are equal, so only the binding distinguishes them.
        assert_eq!(binding_a.input(), binding_b.input());
        assert_ne!(binding_a, binding_b);
        assert_ne!(hash_of(&binding_a), hash_of(&binding_b));

        let plan = LogicalPlanBuilder::from(binding_a.into_logical_plan())
            .union(
                LogicalPlanBuilder::from(binding_b.into_logical_plan())
                    .build()
                    .unwrap(),
            )
            .unwrap()
            .build()
            .unwrap();
        let optimized = analyze_and_optimize(&state, plan);

        let bindings = collect_bindings(&optimized);
        assert_eq!(2, bindings.len());
        assert_eq!(2, count_table_scans(&optimized));
        assert_ne!(bindings[0].after_seqs(), bindings[1].after_seqs());
    }

    #[test]
    fn test_with_exprs_and_inputs_preserves_binding() {
        let provider = Arc::new(mock_table_provider(1.into()));
        let scan = scan_plan(provider, "devices");
        let after_seqs = RegionSeqs::from([(1, 5), (2, 6)]);
        let binding = ScanSequenceBinding::new(3, after_seqs.clone(), into_scan(scan.clone()));

        let filtered = with_filters(&into_scan(scan), vec![col("k0").eq(lit("hello"))]);
        let rebuilt = binding
            .with_exprs_and_inputs(vec![], vec![filtered.clone()])
            .unwrap();

        assert_eq!(3, rebuilt.scan_id());
        assert_eq!(&after_seqs, rebuilt.after_seqs());
        assert_eq!(&filtered, rebuilt.input());
        assert_ne!(binding.input(), rebuilt.input());
        assert!(rebuilt.table_scan().is_some());

        // The constructors that establish the binding refuse a non-scan input.
        let not_a_scan = LogicalPlanBuilder::from(filtered)
            .project(vec![col("v0")])
            .unwrap()
            .build()
            .unwrap();
        assert!(ScanSequenceBinding::try_new(0, RegionSeqs::new(), not_a_scan).is_err());
    }
}
