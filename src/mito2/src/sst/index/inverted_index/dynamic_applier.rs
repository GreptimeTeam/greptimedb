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

//! Live-handle dynamic filter to inverted-index prefilter.
//!
//! The static inverted-index applier freezes its predicates when the scan opens. This
//! module adds a *separate* applier that keeps the scan's [`Predicate`] alive, re-reads
//! its dynamic filters for every SST file, and translates a newly published filter into
//! inverted-index predicates through the ordinary [`InvertedIndexApplierBuilder`]. Its
//! row selection is intersected with, and never replaces, the static selection.
//!
//! Only positive `IN`/`=` atoms on one directly indexed tag are admitted. Range
//! predicates and unsupported shapes decline; there is no membership-size gate. Each
//! translation is a pruning hint, intersected with the static selection and never cached
//! in the shared result cache. Nullable tags require per-file zero-NULL statistics.
//!
//! A clear invalidates the applier for the rest of its execution, including when another
//! filter is published before the next file. The next execution owns a fresh applier.

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use api::v1::SemanticType;
use common_telemetry::warn;
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::expressions::{
    BinaryExpr, Column as PhysicalColumn, DynamicFilterPhysicalExpr, InListExpr, Literal,
};
use datafusion_common::{Column as LogicalColumn, ScalarValue};
use datafusion_expr::expr::InList;
use datafusion_expr::{Expr, Operator};
use datatypes::arrow::array::{Array, UInt64Array};
use datatypes::arrow::datatypes::{Schema, SchemaRef};
use datatypes::data_type::ConcreteDataType;
use futures::future::{BoxFuture, FutureExt, Shared};
use index::inverted_index::search::index_apply::ApplyOutput;
use object_store::ObjectStore;
use parquet::file::metadata::ParquetMetaData;
use puffin::puffin_manager::cache::PuffinMetadataCacheRef;
use store_api::metadata::{RegionMetadata, RegionMetadataRef};
use store_api::region_request::PathType;
use store_api::storage::ColumnId;
use table::predicate::Predicate;

use crate::cache::file_cache::FileCacheRef;
use crate::cache::index::inverted_index::InvertedIndexCacheRef;
use crate::metrics::DYNAMIC_INDEX_FILTER_TOTAL;
use crate::sst::file::RegionIndexId;
use crate::sst::index::inverted_index::applier::builder::InvertedIndexApplierBuilder;
use crate::sst::index::inverted_index::applier::{
    InvertedIndexApplier, InvertedIndexApplyMetrics, SstApplyPlan,
};
use crate::sst::index::puffin_manager::PuffinManagerFactory;
use crate::sst::parquet::flat_format::FlatReadFormat;
use crate::sst::parquet::format::StatValues;

/// Local approval of a stable, positive single-column membership publication.
///
/// The in-tree join publishes one completed membership of build keys as a pruning hint;
/// `IN`/`=` shape plus the existing stable-generation and NULL guards are sufficient for
/// that producer. Shape alone does not prove monotonicity or completeness for arbitrary
/// external producers; such producers must obey the pruning-only contract of `Predicate`.
pub(crate) struct ApprovedDynamicFilter;

impl ApprovedDynamicFilter {
    fn new(
        handle: &DynamicFilterPhysicalExpr,
        expr: &Arc<dyn PhysicalExpr>,
        output_schema: &Schema,
    ) -> Option<Self> {
        let declared = declared_column_name(handle)?;
        // Physical columns are index-authoritative. Validate both the handle's
        // declared children and its remapped current expression against the actual
        // scan output, not the wider internal read schema.
        if !handle
            .children()
            .iter()
            .all(|child| columns_match_schema(child, output_schema))
            || !columns_match_schema(expr, output_schema)
            || single_translated_column(expr)? != declared
        {
            return None;
        }
        Some(Self)
    }
}

/// Everything the dynamic applier needs to rebuild an [`InvertedIndexApplier`] when the
/// producer publishes a new filter.
pub(crate) struct DynamicInvertedIndexConfig {
    /// Directory of the table.
    pub(crate) table_dir: String,
    /// Path type for generating file paths.
    pub(crate) path_type: PathType,
    /// Object store of the region files.
    pub(crate) store: ObjectStore,
    /// Puffin manager factory for index files.
    pub(crate) puffin_manager_factory: PuffinManagerFactory,
    /// File cache, if enabled.
    pub(crate) file_cache: Option<FileCacheRef>,
    /// Inverted index cache, if enabled.
    pub(crate) inverted_index_cache: Option<InvertedIndexCacheRef>,
    /// Puffin metadata cache, if enabled.
    pub(crate) puffin_metadata_cache: Option<PuffinMetadataCacheRef>,
    /// Latest metadata of the region. Columns are resolved by name here.
    pub(crate) metadata: RegionMetadataRef,
    /// Projected scan output schema to which the producer's physical columns bind.
    pub(crate) output_schema: SchemaRef,
    /// Column ids with a column-level inverted index.
    pub(crate) indexed_column_ids: HashSet<ColumnId>,
    /// The scan's predicate, whose dynamic filters are read live.
    pub(crate) predicate: Predicate,
}

/// The single tag column a dynamic filter is guarded on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GuardedColumn {
    /// Name of the column in the latest region metadata.
    pub(crate) name: String,
    /// Id of the column in the latest region metadata.
    pub(crate) column_id: ColumnId,
    /// Declared concrete type of the column.
    pub(crate) data_type: ConcreteDataType,
    /// Whether the column admits NULLs.
    pub(crate) nullable: bool,
}

/// The atoms admitted by [`admit_dynamic_filters`].
#[derive(Debug, Clone)]
pub(crate) struct AdmittedFilters {
    /// Logical atoms for [`InvertedIndexApplierBuilder`].
    pub(crate) atoms: Vec<Expr>,
    /// The single guarded column.
    pub(crate) column: GuardedColumn,
}

/// Counters of one dynamic applier, i.e. of one scan.
#[derive(Debug, Default)]
pub(crate) struct DynamicIndexCounters {
    applied_files: AtomicUsize,
    declined_files: AtomicUsize,
    failed_files: AtomicUsize,
    translations: AtomicUsize,
    reset_disabled_files: AtomicUsize,
    #[cfg(test)]
    builds: AtomicUsize,
    #[cfg(test)]
    fail_next_build: std::sync::atomic::AtomicBool,
}

/// Point-in-time copy of [`DynamicIndexCounters`].
#[cfg(test)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DynamicIndexStats {
    /// Files where the dynamic index pruned rows.
    pub(crate) applied_files: usize,
    /// Files where the dynamic index declined (no pruning).
    pub(crate) declined_files: usize,
    /// Files where applying the index failed (no pruning).
    pub(crate) failed_files: usize,
    /// Number of filter translations (one per generation change).
    pub(crate) translations: usize,
    /// Files declined because a reset was observed earlier in the scan.
    pub(crate) reset_disabled_files: usize,
    /// Number of single-flight translation builds started.
    pub(crate) builds: usize,
}

impl DynamicIndexCounters {
    #[cfg(test)]
    fn snapshot(&self) -> DynamicIndexStats {
        DynamicIndexStats {
            applied_files: self.applied_files.load(Ordering::Relaxed),
            declined_files: self.declined_files.load(Ordering::Relaxed),
            failed_files: self.failed_files.load(Ordering::Relaxed),
            translations: self.translations.load(Ordering::Relaxed),
            reset_disabled_files: self.reset_disabled_files.load(Ordering::Relaxed),
            builds: self.builds.load(Ordering::Relaxed),
        }
    }
}

/// A translated filter set: the applier built from it and the column it guards.
#[derive(Clone)]
struct TranslatedFilters {
    applier: Arc<InvertedIndexApplier>,
    column: GuardedColumn,
}

/// Result shared by every file waiting for the same publication.
#[derive(Clone)]
enum TranslationOutcome {
    Ready(Arc<TranslatedFilters>),
    Declined,
    Failed,
}

type SharedTranslation = Shared<BoxFuture<'static, TranslationOutcome>>;

/// State shared by the files of one scan.
#[derive(Default)]
struct TranslationState {
    /// Identity and generations of the dynamic filter set `current` was built from.
    filters: Option<Arc<Vec<Arc<DynamicFilterPhysicalExpr>>>>,
    generations: Vec<u64>,
    /// One shared in-flight or completed translation for this epoch and publication.
    current: Option<SharedTranslation>,
    /// Whether the producer cleared its filters during this execution.
    disabled: bool,
}

/// Applies live dynamic filters with the inverted index, per SST file.
///
/// One applier belongs to one scan: it caches the translation of the last observed filter
/// generation and never shares it across scans or through the index result cache.
pub(crate) struct DynamicInvertedIndexApplier {
    config: Arc<DynamicInvertedIndexConfig>,
    state: Mutex<TranslationState>,
    counters: Arc<DynamicIndexCounters>,
    reset_epoch: u64,
}

impl DynamicInvertedIndexApplier {
    /// Creates an applier for one scan.
    pub(crate) fn new(config: DynamicInvertedIndexConfig) -> Self {
        let reset_epoch = config.predicate.dyn_filter_reset_epoch();
        Self {
            config: Arc::new(config),
            state: Mutex::new(TranslationState::default()),
            counters: Arc::new(DynamicIndexCounters::default()),
            reset_epoch,
        }
    }

    /// Returns the counters of this applier.
    #[cfg(test)]
    pub(crate) fn stats(&self) -> DynamicIndexStats {
        self.counters.snapshot()
    }

    /// Returns the generations of the last translation, for tests.
    #[cfg(test)]
    pub(crate) fn generation_for_test(&self) -> Vec<u64> {
        self.lock_state().generations.clone()
    }

    /// Plans and applies the dynamic filter against one SST file.
    ///
    /// Returns `None` whenever the dynamic path declines this file: no dynamic filter, an
    /// unstable publication, a rejected shape, a failed guard, an unknown index or an
    /// index error. Callers then keep the static selection, which keeps every row the
    /// dynamic filter would have kept.
    pub(crate) async fn apply(
        &self,
        read_format: &FlatReadFormat,
        parquet_meta: &ParquetMetaData,
        file_id: RegionIndexId,
        file_size_hint: Option<u64>,
        metrics: Option<&mut InvertedIndexApplyMetrics>,
    ) -> Option<ApplyOutput> {
        if self.config.predicate.dyn_filters().is_empty()
            && self.config.predicate.dyn_filter_reset_epoch() == self.reset_epoch
        {
            return None;
        }
        DYNAMIC_INDEX_FILTER_TOTAL
            .with_label_values(&["inverted", "offered"])
            .inc();
        let Some((plan, applier)) = self.plan_for_file(read_format, parquet_meta).await else {
            DYNAMIC_INDEX_FILTER_TOTAL
                .with_label_values(&["inverted", "declined"])
                .inc();
            return None;
        };

        let result = applier
            .apply_dynamic(file_id, file_size_hint, &plan.index_applier, metrics)
            .await;

        // A reset during I/O also invalidates this result.
        if self.config.predicate.dyn_filter_reset_epoch() != self.reset_epoch {
            self.lock_state().disabled = true;
            self.counters.declined_files.fetch_add(1, Ordering::Relaxed);
            self.counters
                .reset_disabled_files
                .fetch_add(1, Ordering::Relaxed);
            DYNAMIC_INDEX_FILTER_TOTAL
                .with_label_values(&["inverted", "declined"])
                .inc();
            return None;
        }
        match result {
            Ok(output)
                if output.total_row_count != parquet_meta.file_metadata().num_rows() as usize =>
            {
                self.counters.declined_files.fetch_add(1, Ordering::Relaxed);
                DYNAMIC_INDEX_FILTER_TOTAL
                    .with_label_values(&["inverted", "declined"])
                    .inc();
                warn!(
                    "Ignore dynamic inverted index with mismatched row count, file_id: {:?}, index_row_count: {}, parquet_row_count: {}",
                    file_id,
                    output.total_row_count,
                    parquet_meta.file_metadata().num_rows(),
                );
                None
            }
            Ok(output) => {
                self.counters.applied_files.fetch_add(1, Ordering::Relaxed);
                DYNAMIC_INDEX_FILTER_TOTAL
                    .with_label_values(&["inverted", "applied"])
                    .inc();
                Some(output)
            }
            Err(err) => {
                // Fail open: an index error must not drop rows.
                self.counters.failed_files.fetch_add(1, Ordering::Relaxed);
                DYNAMIC_INDEX_FILTER_TOTAL
                    .with_label_values(&["inverted", "declined"])
                    .inc();
                warn!(
                    err;
                    "Failed to apply dynamic inverted index, keep all rows. region: {}, file: {}",
                    file_id.region_id(),
                    file_id.file_id(),
                );
                None
            }
        }
    }

    /// Builds the dynamic apply plan of one file, or declines.
    async fn plan_for_file(
        &self,
        read_format: &FlatReadFormat,
        parquet_meta: &ParquetMetaData,
    ) -> Option<(SstApplyPlan, Arc<InvertedIndexApplier>)> {
        if self.config.predicate.dyn_filter_reset_epoch() != self.reset_epoch {
            self.disable_after_reset();
            return None;
        }
        let filters = self.config.predicate.dyn_filters();
        if filters.is_empty() {
            return None;
        }
        let generations: Vec<_> = filters
            .iter()
            .map(|handle| handle.snapshot_generation())
            .collect();

        // Pruner workers are async query-runtime tasks. Membership translation and
        // N-value encoding can be expensive, so the first file schedules them on the
        // blocking query pool. A oneshot-backed shared future gives the other files the
        // same result without holding a std mutex during the build or an await.
        // The epoch is fixed for this applier and is part of every cache key.
        let shared = {
            let mut state = self.lock_state();
            if state.disabled || self.config.predicate.dyn_filter_reset_epoch() != self.reset_epoch
            {
                state.disabled = true;
                self.counters
                    .reset_disabled_files
                    .fetch_add(1, Ordering::Relaxed);
                self.counters.declined_files.fetch_add(1, Ordering::Relaxed);
                return None;
            }
            // Do not replace a newer publication with a snapshot captured by an older
            // concurrent file. Recheck after the blocking build as well.
            if !self.publication_is_current(&filters, &generations) {
                self.counters.declined_files.fetch_add(1, Ordering::Relaxed);
                return None;
            }
            let changed = state
                .filters
                .as_ref()
                .is_none_or(|old| !Arc::ptr_eq(old, &filters))
                || state.generations != generations;
            if changed {
                let config = Arc::clone(&self.config);
                let counters = Arc::clone(&self.counters);
                let build_filters = Arc::clone(&filters);
                let build_generations = generations.clone();
                let (sender, receiver) = tokio::sync::oneshot::channel();
                #[cfg(test)]
                counters.builds.fetch_add(1, Ordering::Relaxed);
                let task = common_runtime::spawn_blocking_query(move || {
                    build_translation(&config, &counters, &build_filters, &build_generations)
                });
                common_runtime::spawn_query(async move {
                    let result = match task.await {
                        Ok(result) => result,
                        Err(err) => {
                            warn!(err; "Dynamic inverted index translation task failed");
                            TranslationOutcome::Failed
                        }
                    };
                    let _ = sender.send(result);
                });
                state.filters = Some(Arc::clone(&filters));
                state.generations = generations.clone();
                state.current = Some(
                    async move { receiver.await.unwrap_or(TranslationOutcome::Failed) }
                        .boxed()
                        .shared(),
                );
            }
            state.current.as_ref()?.clone()
        };

        let result = shared.await;
        if self.config.predicate.dyn_filter_reset_epoch() != self.reset_epoch {
            self.disable_after_reset();
            return None;
        }
        if !self.publication_is_current(&filters, &generations) {
            self.counters.declined_files.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        let translated = match result {
            TranslationOutcome::Ready(translated) => translated,
            TranslationOutcome::Declined => {
                self.counters.declined_files.fetch_add(1, Ordering::Relaxed);
                return None;
            }
            TranslationOutcome::Failed => {
                self.counters.failed_files.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        };

        let sst_metadata = read_format.metadata();
        if !guards_pass(&translated.column, sst_metadata, read_format, parquet_meta) {
            self.counters.declined_files.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        let Ok(Some(plan)) = translated.applier.plan_for_sst(sst_metadata) else {
            self.counters.declined_files.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        Some((plan, Arc::clone(&translated.applier)))
    }

    fn publication_is_current(
        &self,
        filters: &Arc<Vec<Arc<DynamicFilterPhysicalExpr>>>,
        generations: &[u64],
    ) -> bool {
        Arc::ptr_eq(filters, &self.config.predicate.dyn_filters())
            && filters
                .iter()
                .zip(generations)
                .all(|(handle, generation)| handle.snapshot_generation() == *generation)
    }

    fn disable_after_reset(&self) {
        self.lock_state().disabled = true;
        self.counters
            .reset_disabled_files
            .fetch_add(1, Ordering::Relaxed);
        self.counters.declined_files.fetch_add(1, Ordering::Relaxed);
    }

    fn lock_state(&self) -> MutexGuard<'_, TranslationState> {
        // A poisoned lock only means some other file panicked. The dynamic path fails open
        // by declining every row it cannot reason about, so recovering the state is safe.
        self.state.lock().unwrap_or_else(|err| err.into_inner())
    }
}

/// Runs on the query blocking pool, never while holding the translation-state mutex.
fn build_translation(
    config: &DynamicInvertedIndexConfig,
    counters: &DynamicIndexCounters,
    filters: &[Arc<DynamicFilterPhysicalExpr>],
    generations: &[u64],
) -> TranslationOutcome {
    #[cfg(test)]
    if counters.fail_next_build.swap(false, Ordering::Relaxed) {
        return TranslationOutcome::Failed;
    }
    // `current()` may remap and clone a large physical InList tree on its first
    // read of a generation. Keep that work on the blocking pool too.
    let Some(publications) = filters
        .iter()
        .map(stable_publication)
        .collect::<Option<Vec<_>>>()
    else {
        return TranslationOutcome::Declined;
    };
    if publications
        .iter()
        .zip(generations)
        .any(|((generation, _), expected)| generation != expected)
    {
        return TranslationOutcome::Declined;
    }
    if filters
        .iter()
        .zip(&publications)
        .any(|(handle, (_, expr))| {
            ApprovedDynamicFilter::new(handle, expr, &config.output_schema).is_none()
        })
    {
        return TranslationOutcome::Declined;
    }
    let exprs: Vec<_> = publications.into_iter().map(|(_, expr)| expr).collect();
    let Some(admitted) =
        admit_dynamic_filters(&config.metadata, &config.indexed_column_ids, &exprs)
    else {
        return TranslationOutcome::Declined;
    };
    let applier = match build_applier(config, &admitted.atoms) {
        Ok(Some(applier)) => applier,
        Ok(None) => return TranslationOutcome::Failed,
        Err(err) => {
            warn!(err; "Failed to build dynamic inverted index translation");
            return TranslationOutcome::Failed;
        }
    };
    counters.translations.fetch_add(1, Ordering::Relaxed);
    TranslationOutcome::Ready(Arc::new(TranslatedFilters {
        applier: Arc::new(applier),
        column: admitted.column,
    }))
}

/// Reads one dynamic filter with a stable generation.
///
/// Returns `None` when the producer published a new expression while it was read, or when
/// the expression cannot be read at all.
fn stable_publication(
    handle: &Arc<DynamicFilterPhysicalExpr>,
) -> Option<(u64, Arc<dyn PhysicalExpr>)> {
    let before = handle.snapshot_generation();
    let expr = handle.current().ok()?;
    let after = handle.snapshot_generation();
    if before != after {
        return None;
    }
    Some((before, expr))
}

/// Check physical column indexes and names against the projected scan output.
fn columns_match_schema(expr: &Arc<dyn PhysicalExpr>, schema: &Schema) -> bool {
    collect_columns(expr).iter().all(|column| {
        schema
            .fields()
            .get(column.index())
            .is_some_and(|field| field.name() == column.name())
    })
}

/// Returns the single column the dynamic filter declares it depends on.
fn declared_column_name(handle: &DynamicFilterPhysicalExpr) -> Option<String> {
    let mut column_name: Option<String> = None;
    let children = handle.children();
    if children.len() != 1 || children[0].downcast_ref::<PhysicalColumn>().is_none() {
        return None;
    }
    for child in children {
        for column in collect_columns(child) {
            match &column_name {
                Some(name) if name != column.name() => return None,
                Some(_) => {}
                None => column_name = Some(column.name().to_string()),
            }
        }
    }
    column_name
}

/// Returns the column referenced by every top-level conjunct of `expr`.
fn single_translated_column(expr: &Arc<dyn PhysicalExpr>) -> Option<String> {
    let mut atoms = Vec::new();
    collect_atoms(expr, &mut atoms)?;
    let mut column_name: Option<String> = None;
    for (name, _) in atoms {
        match &column_name {
            Some(existing) if *existing != name => return None,
            Some(_) => {}
            None => column_name = Some(name),
        }
    }
    column_name
}

/// Splits top-level conjunctions and translates each atom.
fn collect_atoms(expr: &Arc<dyn PhysicalExpr>, out: &mut Vec<(String, Expr)>) -> Option<()> {
    if let Some(binary) = expr.downcast_ref::<BinaryExpr>()
        && *binary.op() == Operator::And
    {
        collect_atoms(binary.left(), out)?;
        collect_atoms(binary.right(), out)?;
        return Some(());
    }
    out.push(translate_conjunct(expr)?);
    Some(())
}

/// Translates one admitted physical atom back to the equivalent logical expression.
///
/// Returns the referenced column name and the logical atom. Every other shape is rejected:
/// this is the phase-1 admission matrix of the prototype.
fn translate_conjunct(expr: &Arc<dyn PhysicalExpr>) -> Option<(String, Expr)> {
    if let Some(in_list) = expr.downcast_ref::<InListExpr>() {
        if in_list.negated() || in_list.list().is_empty() {
            return None;
        }
        let name = physical_column_name(in_list.expr())?;
        let mut list = Vec::with_capacity(in_list.list().len());
        for item in in_list.list() {
            list.push(Expr::Literal(nonnull_literal(item)?, None));
        }
        let atom = Expr::InList(InList {
            expr: Box::new(logical_column(&name)),
            list,
            negated: false,
        });
        return Some((name, atom));
    }

    let binary = expr.downcast_ref::<BinaryExpr>()?;
    let op = *binary.op();

    if op == Operator::Eq {
        // `col = lit` or `lit = col`: the physical shape decides, not the operand order.
        if let (Some(name), Some(value)) = (
            physical_column_name(binary.left()),
            nonnull_literal(binary.right()),
        ) {
            let atom = logical_column(&name).eq(Expr::Literal(value, None));
            return Some((name, atom));
        }
        if let (Some(value), Some(name)) = (
            nonnull_literal(binary.left()),
            physical_column_name(binary.right()),
        ) {
            let atom = logical_column(&name).eq(Expr::Literal(value, None));
            return Some((name, atom));
        }
        return None;
    }

    // Range expressions are deliberately declined by this index.
    None
}

fn logical_column(name: &str) -> Expr {
    Expr::Column(LogicalColumn::new_unqualified(name))
}

/// Returns the name of a physical column reference.
fn physical_column_name(expr: &Arc<dyn PhysicalExpr>) -> Option<String> {
    expr.downcast_ref::<PhysicalColumn>()
        .map(|column| column.name().to_string())
}

/// Returns the value of a non-NULL literal atom.
fn nonnull_literal(expr: &Arc<dyn PhysicalExpr>) -> Option<ScalarValue> {
    let literal = expr.downcast_ref::<Literal>()?;
    let value = literal.value();
    if value.is_null() {
        return None;
    }
    Some(value.clone())
}

/// Admits the dynamic filters of phase 1 and translates them to logical atoms.
///
/// Rejects everything but positive, top-level-conjunct, single-column `IN` lists and
/// equalities over one direct tag column, and rejects a literal whose concrete type is not
/// exactly the column's declared `String`/`Int64` type.
pub(crate) fn admit_dynamic_filters(
    metadata: &RegionMetadata,
    indexed_column_ids: &HashSet<ColumnId>,
    filters: &[Arc<dyn PhysicalExpr>],
) -> Option<AdmittedFilters> {
    let mut pairs = Vec::new();
    for filter in filters {
        collect_atoms(filter, &mut pairs)?;
    }

    let mut column_name: Option<String> = None;
    for (name, _) in &pairs {
        match &column_name {
            // More than one guarded column: decline instead of guessing how multi-column
            // index selections combine.
            Some(existing) if existing != name => return None,
            Some(_) => {}
            None => column_name = Some(name.clone()),
        }
    }
    let name = column_name?;

    // Resolve by name in the latest metadata.
    let column = metadata.column_by_name(&name)?;
    if column.semantic_type != SemanticType::Tag {
        return None;
    }
    if !column.column_schema.is_inverted_indexed()
        || !indexed_column_ids.contains(&column.column_id)
    {
        return None;
    }
    let data_type = column.column_schema.data_type.clone();
    if !matches!(
        data_type,
        ConcreteDataType::String(_) | ConcreteDataType::Int64(_)
    ) {
        return None;
    }

    let mut atoms = Vec::with_capacity(pairs.len());
    for (_, atom) in pairs {
        // Every literal must carry exactly the declared type: no float on an integer
        // column and no dictionary or view reinterpretation.
        if !atom_literals_match(&atom, &data_type) {
            return None;
        }
        atoms.push(atom);
    }
    Some(AdmittedFilters {
        atoms,
        column: GuardedColumn {
            name,
            column_id: column.column_id,
            data_type,
            nullable: column.column_schema.is_nullable(),
        },
    })
}

/// Returns true if every literal of `atom` has exactly the type of the guarded column.
fn atom_literals_match(atom: &Expr, data_type: &ConcreteDataType) -> bool {
    let literals: Vec<&ScalarValue> = match atom {
        Expr::InList(in_list) => in_list
            .list
            .iter()
            .filter_map(|item| match item {
                Expr::Literal(value, _) => Some(value),
                _ => None,
            })
            .collect(),
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => match &*binary.right {
            Expr::Literal(value, _) => vec![value],
            _ => return false,
        },
        _ => return false,
    };
    if literals.is_empty() {
        return false;
    }
    literals.into_iter().all(|value| {
        matches!(
            (value, data_type),
            (ScalarValue::Utf8(Some(_)), ConcreteDataType::String(_))
                | (ScalarValue::Int64(Some(_)), ConcreteDataType::Int64(_))
        )
    })
}

/// Checks that the file still carries the guarded column with the same name and type, and
/// that a nullable guarded column has no NULL in this file.
fn guards_pass(
    column: &GuardedColumn,
    sst_metadata: &RegionMetadata,
    read_format: &FlatReadFormat,
    parquet_meta: &ParquetMetaData,
) -> bool {
    // The predicate addresses the column by its latest name; a renamed column in this file
    // would make the name-addressed guard meaningless.
    let Some(sst_column) = sst_metadata.column_by_id(column.column_id) else {
        return false;
    };
    if sst_column.column_schema.name != column.name {
        return false;
    }
    // Type drift between the latest metadata and this file: the encoded keys would not
    // match the file's values.
    if sst_column.column_schema.data_type != column.data_type {
        return false;
    }
    if sst_column.semantic_type != SemanticType::Tag {
        return false;
    }

    // The index only stores non-NULL values, so a nullable tag needs a per-file proof.
    (!column.nullable && !sst_column.column_schema.is_nullable())
        || file_has_no_nulls(read_format, parquet_meta, column.column_id)
}

/// Returns true if every row group of the file proves `null_count == 0` for `column_id`.
///
/// UNKNOWN DOES NOT COUNT AS ZERO: a missing column or missing statistics declines.
pub(crate) fn file_has_no_nulls(
    read_format: &FlatReadFormat,
    parquet_meta: &ParquetMetaData,
    column_id: ColumnId,
) -> bool {
    let StatValues::Values(values) = read_format.null_counts(parquet_meta.row_groups(), column_id)
    else {
        return false;
    };
    let Some(values) = values.as_any().downcast_ref::<UInt64Array>() else {
        return false;
    };
    values.null_count() == 0 && values.values().iter().all(|count| *count == 0)
}

#[cfg(test)]
#[path = "dynamic_applier_test.rs"]
mod tests;

/// Builds the index applier of the admitted atoms.
///
/// Any encode error declines: the caller keeps the static selection.
fn build_applier(
    config: &DynamicInvertedIndexConfig,
    atoms: &[Expr],
) -> crate::error::Result<Option<InvertedIndexApplier>> {
    InvertedIndexApplierBuilder::new(
        config.table_dir.clone(),
        config.path_type,
        config.store.clone(),
        &config.metadata,
        config.indexed_column_ids.clone(),
        config.puffin_manager_factory.clone(),
    )
    .with_dynamic_metrics()
    .with_file_cache(config.file_cache.clone())
    .with_inverted_index_cache(config.inverted_index_cache.clone())
    .with_puffin_metadata_cache(config.puffin_metadata_cache.clone())
    .build(atoms)
}
