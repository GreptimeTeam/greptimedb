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
//! This module owns the whole in-process lifecycle of a binding:
//!
//! 1. [`identify_sequence_scans`] numbers the scan positions of a query plan.
//! 2. [`bind_sequence_scans`] fills in the per-region sequence lower bounds of
//!    every numbered position from the query parameters.
//! 3. [`lower_sequence_bindings`] removes the node again and moves its bounds
//!    into the wrapped scan's [`ScanRequest`](store_api::storage::ScanRequest),
//!    as the last plan step before physical planning.
//!
//! The module does not implement serialization.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt::{Formatter, Result as FmtResult};
use std::sync::Arc;

use datafusion::datasource::DefaultTableSource;
use datafusion::error::Result as DfResult;
use datafusion_common::tree_node::Transformed;
use datafusion_common::{DFSchemaRef, TableReference, plan_err};
use datafusion_expr::logical_plan::TableScan;
use datafusion_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore};

use crate::dummy_catalog::DummyTableProvider;

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

/// Resolved sequence bounds of one scan position, as supplied by the query
/// parameters.
///
/// Parameter binding requires one of these for every scan position of a
/// template, including historical scans whose lower bound is `0`: a scan that is
/// listed nowhere must not silently fall back to the visible sequence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanSequenceParams {
    /// The scan position the bounds belong to, as assigned by
    /// [`identify_sequence_scans`].
    pub scan_id: ScanId,
    /// Per-region sequence lower bounds the scan must read after.
    pub after_seqs: RegionSeqs,
}

impl ScanSequenceParams {
    /// Creates the bounds of the scan position `scan_id`.
    pub fn new(scan_id: ScanId, after_seqs: RegionSeqs) -> Self {
        Self {
            scan_id,
            after_seqs,
        }
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

    /// Returns this binding with `after_seqs` replaced, keeping the scan id and
    /// the wrapped plan.
    fn with_after_seqs(self, after_seqs: RegionSeqs) -> Self {
        Self {
            scan_id: self.scan_id,
            after_seqs,
            input: self.input,
        }
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

/// Fills in the sequence bounds of every scan position in `identified`.
///
/// `identified` must be the plan returned by [`identify_sequence_scans`], and
/// `params` must cover every [`ScanSequenceBinding`] in it exactly once:
///
/// - A binding without parameters is an error. Historical scans whose lower
///   bound is `0` must be listed explicitly instead of inheriting the visible
///   sequence.
/// - Parameters for an unknown [`ScanId`], and two parameter sets for the same
///   [`ScanId`], are errors.
/// - Scan ids are never renumbered and the plan is neither re-parsed nor
///   optimized here: the returned plan has the same shape as `identified`, only
///   with the bounds of its bindings filled in. Bindings that are no longer
///   directly above their [`TableScan`] (the optimizer may have pushed a
///   [`Projection`](datafusion_expr::logical_plan::Projection) above them) are
///   still found, because positions are matched by node type and carry their
///   own id.
pub fn bind_sequence_scans(
    identified: LogicalPlan,
    params: Vec<ScanSequenceParams>,
) -> DfResult<LogicalPlan> {
    let mut binder = SequenceBinder::default();
    for param in params {
        let ScanSequenceParams {
            scan_id,
            after_seqs,
        } = param;
        if binder.params.insert(scan_id, after_seqs).is_some() {
            return plan_err!(
                "duplicate {} parameters for scan_id {}: every scan position must be bound exactly once",
                ScanSequenceBinding::name(),
                scan_id
            );
        }
    }

    let plan = identified
        .transform_up_with_subqueries(|plan| binder.bind(plan))?
        .data;
    binder.reject_unknown_ids()?;

    Ok(plan)
}

/// Applies the parameters during one [`bind_sequence_scans`] walk.
#[derive(Debug, Default)]
struct SequenceBinder {
    /// Bounds to apply, by scan position.
    params: BTreeMap<ScanId, RegionSeqs>,
    /// Scan positions the walk has already bound.
    bound: BTreeSet<ScanId>,
}

impl SequenceBinder {
    /// Binds `plan` when it is a [`ScanSequenceBinding`], and returns it
    /// unchanged otherwise.
    fn bind(&mut self, plan: LogicalPlan) -> DfResult<Transformed<LogicalPlan>> {
        let LogicalPlan::Extension(extension) = plan else {
            return Ok(Transformed::no(plan));
        };
        let Some(binding) = extension
            .node
            .as_any()
            .downcast_ref::<ScanSequenceBinding>()
        else {
            // Another extension node: hand it back untouched.
            return Ok(Transformed::no(LogicalPlan::Extension(extension)));
        };

        let scan_id = binding.scan_id();
        let Some(after_seqs) = self.params.get(&scan_id).cloned() else {
            return plan_err!(
                "no {} parameters for scan_id {}: every scan position needs an explicit sequence range",
                ScanSequenceBinding::name(),
                scan_id
            );
        };
        self.bound.insert(scan_id);

        // Only the bounds change: the scan id, the wrapped plan and this node's
        // position in the plan are kept as they are.
        Ok(Transformed::yes(
            binding
                .clone()
                .with_after_seqs(after_seqs)
                .into_logical_plan(),
        ))
    }

    /// Rejects parameters that no scan position of the plan claimed.
    fn reject_unknown_ids(&self) -> DfResult<()> {
        let unknown = self
            .params
            .keys()
            .filter(|scan_id| !self.bound.contains(scan_id))
            .copied()
            .collect::<Vec<_>>();
        if unknown.is_empty() {
            return Ok(());
        }

        plan_err!(
            "{} parameters for scan_id(s) {:?} do not match any scan position in the plan",
            ScanSequenceBinding::name(),
            unknown
        )
    }
}

/// Removes every [`ScanSequenceBinding`] from `plan` and lowers its bounds into
/// the wrapped scan.
///
/// A lowered binding position is the wrapped [`TableScan`] itself, reading from
/// a per-scan clone of the region's [`DummyTableProvider`]: the clone owns its
/// own [`ScanRequest`](store_api::storage::ScanRequest) mutex, so two positions
/// that read the same table with different bounds never share a request. The
/// scan reads memtable rows in `(after, snapshot]`, where `snapshot` comes from
/// `snapshot_seqs` and is the same for every position of a region. The lowered
/// request also carries its [`ScanId`], so the engine can attribute the request
/// to the plan position it belongs to and hold its bound to that read view.
///
/// Two positions of one region are never merged, not even when their bounds are
/// equal or when they scan the same table with the same projection and filters:
/// reusing one scan for several positions would make the positions
/// indistinguishable again.
///
/// This must be the last plan step before physical planning: an optimizer pass
/// that runs afterwards sees two ordinary scans of one table and may
/// legitimately collapse them into one.
///
/// Scan-open dependency: the provider's `scan` implementation re-reads its
/// request when it builds the physical scan, and
/// `apply_cached_snapshot_to_request` may then refresh `memtable_max_sequence`
/// from the query's cached per-region snapshot. That refresh leaves
/// `memtable_min_sequence` and `exact_sequence_range` untouched, so the lower
/// bound and the exactness written here survive scan open; the refreshed upper
/// bound is the same snapshot sequence passed in `snapshot_seqs`.
pub fn lower_sequence_bindings(
    plan: LogicalPlan,
    snapshot_seqs: RegionSeqs,
) -> DfResult<LogicalPlan> {
    let mut lowerer = SequenceLowerer { snapshot_seqs };
    let plan = plan
        .transform_up_with_subqueries(|plan| lowerer.lower(plan))?
        .data;

    Ok(plan)
}

/// Lowers bindings during one [`lower_sequence_bindings`] walk.
#[derive(Debug)]
struct SequenceLowerer {
    /// Sequence upper bound of each region, shared by every scan position.
    snapshot_seqs: RegionSeqs,
}

impl SequenceLowerer {
    /// Returns the scan that replaces the [`ScanSequenceBinding`] `plan`, and
    /// leaves any other node unchanged.
    fn lower(&mut self, plan: LogicalPlan) -> DfResult<Transformed<LogicalPlan>> {
        let LogicalPlan::Extension(extension) = plan else {
            return Ok(Transformed::no(plan));
        };
        let Some(binding) = extension
            .node
            .as_any()
            .downcast_ref::<ScanSequenceBinding>()
        else {
            // Another extension node: hand it back untouched.
            return Ok(Transformed::no(LogicalPlan::Extension(extension)));
        };

        let scan_id = binding.scan_id();
        let Some(mut scan) = binding.table_scan().cloned() else {
            return plan_err!(
                "cannot lower {} of scan_id {}: it does not wrap a TableScan",
                ScanSequenceBinding::name(),
                scan_id
            );
        };
        let Some(provider) = region_provider(&scan) else {
            return plan_err!(
                "cannot lower {} of scan_id {} on table {}: the scan does not read a {}",
                ScanSequenceBinding::name(),
                scan_id,
                scan.table_name,
                std::any::type_name::<DummyTableProvider>()
            );
        };

        let region_id = provider.region_metadata().region_id.as_u64();
        let Some(&after) = binding.after_seqs().get(&region_id) else {
            return plan_err!(
                "cannot lower {} of scan_id {}: no lower sequence bound for region {}",
                ScanSequenceBinding::name(),
                scan_id,
                region_id
            );
        };
        let Some(&snapshot) = self.snapshot_seqs.get(&region_id) else {
            return plan_err!(
                "cannot lower {} of scan_id {}: no snapshot sequence for region {}",
                ScanSequenceBinding::name(),
                scan_id,
                region_id
            );
        };
        if after > snapshot {
            return plan_err!(
                "cannot lower {} of scan_id {}: region {} must read after sequence {} but the snapshot sequence is only {}",
                ScanSequenceBinding::name(),
                scan_id,
                region_id,
                after,
                snapshot
            );
        }

        let adapter = provider.clone_for_scan();
        adapter.with_sequence_range(after, snapshot);
        adapter.with_scan_id(scan_id);
        scan.source = Arc::new(DefaultTableSource::new(Arc::new(adapter)));

        Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
    }
}

/// Returns the [`DummyTableProvider`] `scan` reads from, if it reads one.
///
/// A region-server scan is a [`TableScan`] over a [`DefaultTableSource`] that
/// wraps one [`DummyTableProvider`]; the same downcast is used by
/// [`crate::optimizer::scan_hint`] to attach scan hints.
fn region_provider(scan: &TableScan) -> Option<&DummyTableProvider> {
    scan.source
        .downcast_ref::<DefaultTableSource>()?
        .table_provider
        .downcast_ref::<DummyTableProvider>()
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
    use session::context::QueryContext;
    use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};

    use super::*;
    use crate::QueryEngineFactory;
    use crate::dist_plan::MergeScanLogicalPlan;
    use crate::dummy_catalog::{DummyCatalogList, DummyTableProvider};
    use crate::optimizer::test_util::mock_table_provider;
    use crate::options::QueryOptions;
    use crate::query_engine::DefaultSerializer;

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

    /// Builds a [`SessionState`] of a query engine with the dist planner, i.e.
    /// with the frontend analyzer chain that contains
    /// [`DistPlannerAnalyzer`](crate::dist_plan::DistPlannerAnalyzer).
    fn session_state_with_dist_planner() -> SessionState {
        let catalog_list = catalog::memory::new_memory_catalog_manager().unwrap();

        QueryEngineFactory::new(
            catalog_list,
            None,
            None,
            None,
            None,
            true,
            QueryOptions::default(),
        )
        .query_engine()
        .engine_state()
        .session_state()
    }

    /// Applies `visit` to every node of `plan`, including the remote input of a
    /// [`MergeScanLogicalPlan`], which is not part of
    /// [`LogicalPlan::inputs`] and is therefore invisible to a plain walk.
    fn visit_plan(plan: &LogicalPlan, visit: &mut impl FnMut(&LogicalPlan)) {
        visit(plan);

        if let LogicalPlan::Extension(extension) = plan
            && let Some(merge_scan) = extension
                .node
                .as_any()
                .downcast_ref::<MergeScanLogicalPlan>()
        {
            visit_plan(merge_scan.input(), visit);
        }

        for child in plan.inputs() {
            visit_plan(child, visit);
        }
    }

    /// Returns the [`ScanSequenceBinding`] `plan` is, if it is one.
    fn binding_of(plan: &LogicalPlan) -> Option<&ScanSequenceBinding> {
        let LogicalPlan::Extension(extension) = plan else {
            return None;
        };

        extension
            .node
            .as_any()
            .downcast_ref::<ScanSequenceBinding>()
    }

    /// Returns the bindings of `plan` and the number of [`TableScan`]s in it,
    /// counting the part of the plan a `MergeScan` keeps in its remote input.
    fn collect_bindings_and_scans(plan: &LogicalPlan) -> (Vec<ScanSequenceBinding>, usize) {
        let mut bindings = Vec::new();
        let mut scans = 0;
        visit_plan(plan, &mut |plan| {
            if let Some(binding) = binding_of(plan) {
                bindings.push(binding.clone());
            }
            if matches!(plan, LogicalPlan::TableScan(_)) {
                scans += 1;
            }
        });

        (bindings, scans)
    }

    /// Returns the remote input of the only `MergeScan` of `plan`.
    fn merge_scan_input(plan: &LogicalPlan) -> LogicalPlan {
        let mut inputs = Vec::new();
        visit_plan(plan, &mut |plan| {
            if let LogicalPlan::Extension(extension) = plan
                && let Some(merge_scan) = extension
                    .node
                    .as_any()
                    .downcast_ref::<MergeScanLogicalPlan>()
            {
                inputs.push(merge_scan.input().clone());
            }
        });

        assert_eq!(1, inputs.len(), "expects exactly one MergeScan");
        inputs.pop().unwrap()
    }

    /// Reads `plan` back through the production plan serialization: the
    /// [`DefaultSerializer`] and the plan decoder of a query engine, which is the
    /// path a region server takes to decode a remote read.
    #[allow(clippy::print_stdout)]
    async fn round_trip(plan: &LogicalPlan, provider: Arc<DummyTableProvider>) -> LogicalPlan {
        let bytes = DFLogicalSubstraitConvertor
            .encode(plan, DefaultSerializer)
            .unwrap();
        println!("encoded plan: {} bytes", bytes.len());

        let catalog_list = catalog::memory::new_memory_catalog_manager().unwrap();
        let engine = QueryEngineFactory::new(
            catalog_list,
            None,
            None,
            None,
            None,
            false,
            QueryOptions::default(),
        )
        .query_engine();
        let decoder = engine
            .engine_context(QueryContext::arc())
            .unwrap()
            .new_plan_decoder()
            .unwrap();

        decoder
            .decode(
                bytes,
                Arc::new(DummyCatalogList::with_table_provider(provider)),
                false,
            )
            .await
            .unwrap()
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

    /// Collects the [`TableScan`]s of `plan` in walk order.
    fn collect_scans(plan: &LogicalPlan) -> Vec<TableScan> {
        let mut scans = Vec::new();
        plan.apply_with_subqueries(|plan| {
            if let LogicalPlan::TableScan(scan) = plan {
                scans.push(scan.clone());
            }

            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();

        scans
    }

    /// Returns the provider `scan` reads from.
    fn provider_of(scan: &TableScan) -> &DummyTableProvider {
        region_provider(scan).expect("the scan reads a DummyTableProvider")
    }

    /// Returns the table source of `scan`.
    fn source_of(scan: &TableScan) -> &DefaultTableSource {
        scan.source
            .downcast_ref::<DefaultTableSource>()
            .expect("the scan reads a DefaultTableSource")
    }

    /// Builds a plan that reads `name` twice through one provider.
    fn union_of_two_scans(provider: Arc<DummyTableProvider>, name: &str) -> LogicalPlan {
        let left = scan_plan(provider.clone(), name);
        let right = scan_plan(provider, name);
        // Both references are the same table with the same provider, so they are
        // equal `TableScan`s and only their sequence bounds tell them apart.
        assert_eq!(left, right);

        LogicalPlanBuilder::from(left)
            .union(right)
            .unwrap()
            .build()
            .unwrap()
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

    #[test]
    fn test_bind_rejects_unknown_scan_id() {
        let provider = Arc::new(mock_table_provider(1.into()));
        let (identified, layout) = identify_sequence_scans(scan_plan(provider, "devices")).unwrap();
        assert_eq!(1, layout.len());

        let err = bind_sequence_scans(
            identified,
            vec![
                ScanSequenceParams::new(0, RegionSeqs::from([(1, 0)])),
                // No scan position of the plan was numbered 7.
                ScanSequenceParams::new(7, RegionSeqs::from([(1, 0)])),
            ],
        )
        .unwrap_err();

        let message = err.to_string();
        assert!(
            message.contains("scan_id(s) [7]"),
            "unexpected error: {message}"
        );
    }

    #[test]
    fn test_bind_rejects_duplicate_scan_id() {
        let provider = Arc::new(mock_table_provider(1.into()));
        let (identified, _layout) =
            identify_sequence_scans(scan_plan(provider, "devices")).unwrap();

        let err = bind_sequence_scans(
            identified,
            vec![
                ScanSequenceParams::new(0, RegionSeqs::from([(1, 1)])),
                ScanSequenceParams::new(0, RegionSeqs::from([(1, 2)])),
            ],
        )
        .unwrap_err();

        let message = err.to_string();
        assert!(
            message.contains("duplicate ScanSequenceBinding parameters for scan_id 0"),
            "unexpected error: {message}"
        );
    }

    #[test]
    fn test_bind_rejects_missing_parameters() {
        let provider = Arc::new(mock_table_provider(1.into()));
        let (identified, layout) =
            identify_sequence_scans(union_of_two_scans(provider, "devices")).unwrap();
        assert_eq!(2, layout.len());

        // Scan 0 is bound, scan 1 is not: no position may fall back to the
        // visible sequence.
        let err = bind_sequence_scans(
            identified,
            vec![ScanSequenceParams::new(0, RegionSeqs::from([(1, 0)]))],
        )
        .unwrap_err();

        let message = err.to_string();
        assert!(
            message.contains("no ScanSequenceBinding parameters for scan_id 1"),
            "unexpected error: {message}"
        );
    }

    #[test]
    fn test_bind_keeps_plan_shape() {
        let provider = Arc::new(mock_table_provider(1.into()));
        let (identified, _layout) =
            identify_sequence_scans(union_of_two_scans(provider.clone(), "devices")).unwrap();
        let before = collect_bindings(&identified);
        assert_eq!(2, before.len());

        let bound = bind_sequence_scans(
            identified,
            vec![
                ScanSequenceParams::new(0, RegionSeqs::from([(1, 0)])),
                ScanSequenceParams::new(1, RegionSeqs::from([(1, 3)])),
            ],
        )
        .unwrap();

        let after = collect_bindings(&bound);
        assert_eq!(2, after.len());
        for (before, after) in before.iter().zip(&after) {
            assert_eq!(before.scan_id(), after.scan_id());
            // Binding does not re-plan: the wrapped scan is untouched, and it still
            // reads the catalog provider instead of a forked per-scan one.
            assert_eq!(before.input(), after.input());
            let scan = after
                .table_scan()
                .expect("the binding still wraps a TableScan");
            assert!(std::ptr::eq(provider_of(scan), &*provider));
        }
        assert_eq!(2, count_table_scans(&bound));
    }

    #[test]
    fn test_bind_keeps_scan_ids_and_only_replaces_after_seqs() {
        let provider = Arc::new(mock_table_provider(1.into()));
        // Three positions of one provider, numbered `0`, `1`, `2` in walk order.
        let plan = LogicalPlanBuilder::from(scan_plan(provider.clone(), "devices"))
            .union(scan_plan(provider.clone(), "devices"))
            .unwrap()
            .union(scan_plan(provider, "devices"))
            .unwrap()
            .build()
            .unwrap();
        let (identified, layout) = identify_sequence_scans(plan).unwrap();
        assert_eq!(
            vec![0, 1, 2],
            layout
                .iter()
                .map(ScanLayoutEntry::scan_id)
                .collect::<Vec<_>>()
        );
        let before = collect_bindings(&identified);
        assert_eq!(
            vec![0, 1, 2],
            before
                .iter()
                .map(ScanSequenceBinding::scan_id)
                .collect::<Vec<_>>()
        );

        // The parameter order is irrelevant: bounds are matched by scan id.
        let bound = bind_sequence_scans(
            identified,
            vec![
                ScanSequenceParams::new(2, RegionSeqs::from([(1, 30)])),
                ScanSequenceParams::new(0, RegionSeqs::from([(1, 10)])),
                ScanSequenceParams::new(1, RegionSeqs::from([(1, 20)])),
            ],
        )
        .unwrap();

        let after = collect_bindings(&bound);
        assert_eq!(
            vec![0, 1, 2],
            after
                .iter()
                .map(ScanSequenceBinding::scan_id)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            vec![
                RegionSeqs::from([(1, 10)]),
                RegionSeqs::from([(1, 20)]),
                RegionSeqs::from([(1, 30)]),
            ],
            after
                .iter()
                .map(|binding| binding.after_seqs().clone())
                .collect::<Vec<_>>()
        );
        // Nothing but the bounds changed.
        for (before, after) in before.iter().zip(&after) {
            assert_eq!(before.input(), after.input());
        }
        assert_eq!(3, count_table_scans(&bound));
    }

    #[test]
    fn test_lower_removes_every_binding() {
        let state = session_state();
        let provider = Arc::new(mock_table_provider(1.into()));
        let (identified, _layout) =
            identify_sequence_scans(union_of_two_scans(provider, "devices")).unwrap();
        let bound = bind_sequence_scans(
            identified,
            vec![
                ScanSequenceParams::new(0, RegionSeqs::from([(1, 0)])),
                ScanSequenceParams::new(1, RegionSeqs::from([(1, 7)])),
            ],
        )
        .unwrap();
        let optimized = analyze_and_optimize(&state, bound);
        assert_eq!(2, collect_bindings(&optimized).len());

        let lowered = lower_sequence_bindings(optimized, RegionSeqs::from([(1, 100)])).unwrap();

        // Whole-tree check, subqueries included: no binding survives lowering.
        assert_eq!(0, collect_bindings(&lowered).len());
        assert!(
            !lowered
                .display_indent()
                .to_string()
                .contains(ScanSequenceBinding::name())
        );
        assert_eq!(2, count_table_scans(&lowered));
    }

    #[test]
    #[allow(clippy::print_stdout)]
    fn test_lower_two_references_of_one_table_read_different_ranges() {
        const AFTER: u64 = 10;
        const SNAPSHOT: u64 = 42;

        let state = session_state();
        // One provider, so both references read the same region.
        let provider = Arc::new(mock_table_provider(1.into()));
        let region_id = provider.region_metadata().region_id.as_u64();
        assert_eq!(1, region_id);

        let (identified, layout) =
            identify_sequence_scans(union_of_two_scans(provider.clone(), "devices")).unwrap();
        assert_eq!(2, layout.len());
        let bound = bind_sequence_scans(
            identified,
            vec![
                // Scan 0 reads the delta `(C, H]`.
                ScanSequenceParams::new(0, RegionSeqs::from([(region_id, AFTER)])),
                // Scan 1 reads the whole history `(0, H]`.
                ScanSequenceParams::new(1, RegionSeqs::from([(region_id, 0)])),
            ],
        )
        .unwrap();

        let optimized = analyze_and_optimize(&state, bound);
        let bindings = collect_bindings(&optimized);
        // The positions stay apart because their bounds differ.
        assert_eq!(2, bindings.len());
        assert_eq!(0, bindings[0].scan_id());
        assert_eq!(
            &RegionSeqs::from([(region_id, AFTER)]),
            bindings[0].after_seqs()
        );
        assert_eq!(1, bindings[1].scan_id());
        assert_eq!(
            &RegionSeqs::from([(region_id, 0)]),
            bindings[1].after_seqs()
        );

        let lowered =
            lower_sequence_bindings(optimized, RegionSeqs::from([(region_id, SNAPSHOT)])).unwrap();
        assert!(collect_bindings(&lowered).is_empty());

        let scans = collect_scans(&lowered);
        assert_eq!(2, scans.len());
        let (first, second) = (&scans[0], &scans[1]);
        let (first_source, second_source) = (source_of(first), source_of(second));

        // Every position owns a provider, so every position owns a request.
        let first_ptr = Arc::as_ptr(&first_source.table_provider);
        let second_ptr = Arc::as_ptr(&second_source.table_provider);
        println!("scan 0 provider: {first_ptr:p}, scan 1 provider: {second_ptr:p}");
        assert!(
            !Arc::ptr_eq(&first_source.table_provider, &second_source.table_provider),
            "two positions of one table must not share a provider"
        );

        let first_request = provider_of(first).scan_request();
        let second_request = provider_of(second).scan_request();
        // `ScanRequest`'s `Display` omits the lower bound, so the evidence prints
        // the fields it asserts on.
        for (scan, request) in [(0, &first_request), (1, &second_request)] {
            println!(
                "scan {scan}: memtable_min_sequence={:?}, memtable_max_sequence={:?}, \
                 exact_sequence_range={}, sst_min_sequence={:?}, snapshot_on_scan={}",
                request.memtable_min_sequence,
                request.memtable_max_sequence,
                request.exact_sequence_range,
                request.sst_min_sequence,
                request.snapshot_on_scan,
            );
        }

        // Scan 0 reads `(C, H]`.
        assert_eq!(Some(AFTER), first_request.memtable_min_sequence);
        assert_eq!(Some(SNAPSHOT), first_request.memtable_max_sequence);
        // Scan 1 reads `(0, H]`: the whole history keeps an explicit `0` lower
        // bound. `None` is not a wider read: the engine derives the exact-range
        // capability from the lower bound, so `None` makes the capability
        // unavailable and fails the scan instead of returning `(0, H]`.
        assert_eq!(
            Some(0),
            second_request.memtable_min_sequence,
            "the lower bound must be an explicit 0, not a missing bound"
        );
        assert_eq!(Some(SNAPSHOT), second_request.memtable_max_sequence);
        for request in [&first_request, &second_request] {
            assert!(request.exact_sequence_range);
            assert_eq!(None, request.sst_min_sequence);
            assert!(!request.snapshot_on_scan);
        }

        // The catalog provider is untouched: the bounds only live on the clones.
        assert_eq!(None, provider.scan_request().memtable_min_sequence);
        assert_eq!(None, provider.scan_request().memtable_max_sequence);
    }

    #[test]
    fn test_lower_keeps_equal_ranges_of_two_positions_separate() {
        let provider = Arc::new(mock_table_provider(1.into()));
        let (identified, _layout) =
            identify_sequence_scans(union_of_two_scans(provider, "devices")).unwrap();
        // Both positions ask for the same range. They still stay two positions:
        // this step reuses no scan across scan ids.
        let bound = bind_sequence_scans(
            identified,
            vec![
                ScanSequenceParams::new(0, RegionSeqs::from([(1, 5)])),
                ScanSequenceParams::new(1, RegionSeqs::from([(1, 5)])),
            ],
        )
        .unwrap();

        let lowered = lower_sequence_bindings(bound, RegionSeqs::from([(1, 9)])).unwrap();

        let scans = collect_scans(&lowered);
        assert_eq!(2, scans.len());
        assert!(
            !Arc::ptr_eq(&scans[0].source, &scans[1].source),
            "two positions must not share a table source"
        );
        for scan in &scans {
            let request = provider_of(scan).scan_request();
            assert_eq!(Some(5), request.memtable_min_sequence);
            assert_eq!(Some(9), request.memtable_max_sequence);
            assert!(request.exact_sequence_range);
        }
    }

    #[test]
    fn test_lower_rejects_after_greater_than_snapshot() {
        let provider = Arc::new(mock_table_provider(1.into()));
        let (identified, _layout) =
            identify_sequence_scans(scan_plan(provider, "devices")).unwrap();
        let bound = bind_sequence_scans(
            identified,
            vec![ScanSequenceParams::new(0, RegionSeqs::from([(1, 10)]))],
        )
        .unwrap();

        let err = lower_sequence_bindings(bound, RegionSeqs::from([(1, 5)])).unwrap_err();

        let message = err.to_string();
        assert!(
            message.contains("must read after sequence 10"),
            "unexpected error: {message}"
        );
        assert!(message.contains("only 5"), "unexpected error: {message}");
    }

    #[test]
    fn test_lower_rejects_missing_region_bounds() {
        let provider = Arc::new(mock_table_provider(1.into()));

        // The scan reads region 1, but the parameters only cover region 2.
        let (identified, _layout) =
            identify_sequence_scans(scan_plan(provider.clone(), "devices")).unwrap();
        let bound = bind_sequence_scans(
            identified,
            vec![ScanSequenceParams::new(0, RegionSeqs::from([(2, 0)]))],
        )
        .unwrap();
        let err = lower_sequence_bindings(bound, RegionSeqs::from([(1, 100)])).unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("no lower sequence bound for region 1"),
            "unexpected error: {message}"
        );

        // The lower bound is known, but there is no snapshot sequence for the
        // region.
        let (identified, _layout) =
            identify_sequence_scans(scan_plan(provider, "devices")).unwrap();
        let bound = bind_sequence_scans(
            identified,
            vec![ScanSequenceParams::new(0, RegionSeqs::from([(1, 0)]))],
        )
        .unwrap();
        let err = lower_sequence_bindings(bound, RegionSeqs::new()).unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("no snapshot sequence for region 1"),
            "unexpected error: {message}"
        );
    }

    #[tokio::test]
    #[allow(clippy::print_stdout)]
    async fn test_serializer_round_trips_binding_above_scan() {
        let provider = Arc::new(mock_table_provider(1.into()));
        let after_seqs = RegionSeqs::from([(1, 7), (2, 42)]);
        let binding = ScanSequenceBinding::new(
            3,
            after_seqs.clone(),
            into_scan(scan_plan(provider.clone(), "devices")),
        );
        let plan = binding.into_logical_plan();
        let schema = plan.schema().clone();
        // Printed to inspect the encoded size and both plans by hand; run the
        // test with `--nocapture` to see them.
        println!("encoded plan:\n{}", plan.display_indent());

        let decoded = round_trip(&plan, provider).await;
        println!("decoded plan:\n{}", decoded.display_indent());

        let bindings = collect_bindings(&decoded);
        assert_eq!(1, bindings.len());
        let binding = &bindings[0];
        // The node-local state survives the trip.
        assert_eq!(3, binding.scan_id());
        assert_eq!(&after_seqs, binding.after_seqs());
        // The consumer re-attaches the decoded child, so the binding stays
        // adjacent to the scan it belongs to instead of the decode-time
        // placeholder.
        let scan = binding
            .table_scan()
            .expect("the binding wraps the decoded TableScan");
        assert_eq!("devices", scan.table_name.table());
        // A binding's schema is its child's schema, so both come back.
        assert_eq!(&schema, decoded.schema());
    }

    #[test]
    #[allow(clippy::print_stdout)]
    fn test_dist_planner_pushes_binding_with_its_scan() {
        let state = session_state_with_dist_planner();
        let provider = Arc::new(mock_table_provider(1.into()));
        let after_seqs = RegionSeqs::from([(1, 42)]);
        let binding = ScanSequenceBinding::new(
            0,
            after_seqs.clone(),
            into_scan(scan_plan(provider, "devices")),
        );
        let plan = binding.into_logical_plan();

        let analyzed = state
            .analyzer()
            .execute_and_check(plan, state.config_options(), |_, _| {})
            .unwrap();
        // Printed to inspect the split by hand; run the test with `--nocapture`
        // to see it.
        println!("analyzed plan:\n{}", analyzed.display_indent());

        let remote = merge_scan_input(&analyzed);
        println!("MergeScan input:\n{}", remote.display_indent());

        // Being commutative, the binding is remote-plan material instead of
        // staying on the frontend, and it takes its scan with it.
        let remote_binding =
            binding_of(&remote).expect("the remote input of the MergeScan starts at the binding");
        assert_eq!(0, remote_binding.scan_id());
        assert_eq!(&after_seqs, remote_binding.after_seqs());
        assert!(
            matches!(remote_binding.input(), LogicalPlan::TableScan(_)),
            "unexpected binding input: {}",
            remote_binding.input().display_indent()
        );
        assert_eq!(1, count_table_scans(&remote));
    }

    #[test]
    #[allow(clippy::print_stdout)]
    fn test_pushed_down_bindings_with_different_after_seqs_are_not_merged() {
        let state = session_state_with_dist_planner();
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
        // The wrapped scans are equal, so only the bounds tell the positions
        // apart.
        assert_eq!(binding_a.input(), binding_b.input());

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
        println!("optimized plan:\n{}", optimized.display_indent());

        // Being commutative lets both bindings go down, but it must not make them
        // one node: each keeps its own bounds and its own scan.
        let (bindings, scans) = collect_bindings_and_scans(&optimized);
        assert_eq!(2, bindings.len());
        assert_eq!(2, scans);
        assert_ne!(bindings[0].after_seqs(), bindings[1].after_seqs());
    }
}
