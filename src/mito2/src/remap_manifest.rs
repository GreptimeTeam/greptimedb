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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use partition::expr::{Operand, PartitionExpr, RestrictedOp};
use snafu::{OptionExt, ResultExt, ensure};
use store_api::storage::RegionId;

use crate::error;
pub use crate::error::{Error, Result};
use crate::manifest::action::{RegionManifest, RemovedFilesRecord};
use crate::sst::file::{FileFormat, FileMetaRef, PartitionExprRef};

/// Nesting depth a persisted ownership expression may reach before it is
/// published as a canonical operand list instead of one level deeper per hop.
/// The JSON decoder rejects deeply nested input, so the shape has to stay small
/// however often a reference is re-authorized.
const OWNED_RANGE_MAX_DEPTH: usize = 8;

/// Upper bound on the number of disjuncts an ownership expression may expand to
/// before it is persisted. A conjunction of `n` disjunctions has `n^k` disjuncts,
/// so the count is computed on the tree and rejected before anything is
/// allocated: expanding first and deduplicating later can allocate an
/// astronomical number of terms from a tiny input.
const OWNED_RANGE_MAX_TERMS: usize = 64;

/// Remaps file references from old region manifests to new region manifests.
pub struct RemapManifest {
    /// Old region manifests indexed by region ID
    old_manifests: HashMap<RegionId, RegionManifest>,
    /// New partition expressions indexed by region ID
    new_partition_exprs: HashMap<RegionId, PartitionExpr>,
    /// For each old region, which new regions should receive its files
    region_mapping: HashMap<RegionId, Vec<RegionId>>,
    /// Newly generated manifests for target regions
    new_manifests: HashMap<RegionId, RegionManifest>,
}

impl RemapManifest {
    pub fn new(
        old_manifests: HashMap<RegionId, RegionManifest>,
        new_partition_exprs: HashMap<RegionId, PartitionExpr>,
        region_mapping: HashMap<RegionId, Vec<RegionId>>,
    ) -> Self {
        Self {
            old_manifests,
            new_partition_exprs,
            region_mapping,
            new_manifests: HashMap::new(),
        }
    }

    /// Remaps manifests from old regions to new regions.
    ///
    /// Main entry point. It copies files from old regions to new regions based on
    /// partition expression overlaps.
    pub fn remap_manifests(&mut self) -> Result<RemapResult> {
        // initialize new manifests
        self.initialize_new_manifests()?;

        // remap files
        self.do_remap()?;

        // merge and set metadata for all new manifests
        self.finalize_manifests()?;

        // validate and compute statistics
        let stats = self.compute_stats();
        self.validate_result(&stats)?;

        let new_manifests = std::mem::take(&mut self.new_manifests);

        Ok(RemapResult {
            new_manifests,
            stats,
        })
    }

    /// Initializes empty manifests for all new regions.
    fn initialize_new_manifests(&mut self) -> Result<()> {
        let mut new_manifests = HashMap::new();

        // Get any old manifest as template (they all share the same table schema)
        let template_manifest = self
            .old_manifests
            .values()
            .next()
            .context(error::NoOldManifestsSnafu)?;
        let template_metadata = (*template_manifest.metadata).clone();
        let sst_format = template_manifest.sst_format;

        // Create empty manifest for each new region
        for region_id in self.new_partition_exprs.keys() {
            // Derive region metadata from any old manifest as template
            let mut new_metadata = template_metadata.clone();

            new_metadata.region_id = *region_id;
            let new_partition_expr = self
                .new_partition_exprs
                .get(region_id)
                .context(error::MissingPartitionExprSnafu {
                    region_id: *region_id,
                })?
                .as_json_str()
                .context(error::SerializePartitionExprSnafu)?;
            new_metadata.set_partition_expr(Some(new_partition_expr));

            let manifest = RegionManifest {
                metadata: Arc::new(new_metadata),
                files: HashMap::new(),
                removed_files: RemovedFilesRecord::default(),
                flushed_entry_id: 0,
                flushed_sequence: 0,
                committed_sequence: None,
                manifest_version: 0,
                truncated_entry_id: None,
                compaction_time_window: None,
                sst_format,
                append_mode: None,
                pending_tails: Default::default(),
                series_indexes: Default::default(),
                handoff_floors: Default::default(),
            };

            new_manifests.insert(*region_id, manifest);
        }

        self.new_manifests = new_manifests;

        Ok(())
    }

    /// Remaps files from old regions to new regions according to the region mapping.
    fn do_remap(&mut self) -> Result<()> {
        // For each old region and its target new regions
        for (&from_region_id, target_region_ids) in &self.region_mapping {
            // Get old manifest
            let from_manifest = self.old_manifests.get(&from_region_id).context(
                error::MissingOldManifestSnafu {
                    region_id: from_region_id,
                },
            )?;

            // Copy files to all target new regions
            for &to_region_id in target_region_ids {
                let target_manifest = self.new_manifests.get_mut(&to_region_id).context(
                    error::MissingNewManifestSnafu {
                        region_id: to_region_id,
                    },
                )?;

                Self::copy_files_to_region(from_manifest, target_manifest)?;
            }
        }

        Ok(())
    }

    /// Copies files from a source region to a target region.
    fn copy_files_to_region(
        source_manifest: &RegionManifest,
        target_manifest: &mut RegionManifest,
    ) -> Result<()> {
        ensure!(
            (source_manifest.sst_format == crate::sst::FormatType::Narthex)
                == (target_manifest.sst_format == crate::sst::FormatType::Narthex),
            error::InvalidRequestSnafu {
                region_id: target_manifest.metadata.region_id,
                reason: "Cannot remap between Parquet and Narthex regions",
            }
        );
        // Rows outside these domains never reach the target, so the target reference
        // only needs to authorize what both domains admit.
        let source_domain = crate::region::parse_partition_expr(
            source_manifest.metadata.partition_expr.as_deref(),
        )?;
        let target_domain = crate::region::parse_partition_expr(
            target_manifest.metadata.partition_expr.as_deref(),
        )?;
        // The target region names every reference this remap creates or widens.
        let target_region_id = target_manifest.metadata.region_id;
        for (file_id, file_meta) in &source_manifest.files {
            if let Some(existing) = target_manifest.files.get(file_id)
                && source_manifest.sst_format == crate::sst::FormatType::Narthex
            {
                // Admission sequences may differ by logical region; physical
                // ownership and object inventory must still agree in all builds.
                let same_inventory = match (&existing.format, &file_meta.format) {
                    (FileFormat::NarthexSamples(left), FileFormat::NarthexSamples(right)) => {
                        left.manifest_size == right.manifest_size && left.parts == right.parts
                    }
                    _ => false,
                };
                ensure!(
                    existing.region_id == file_meta.region_id && same_inventory,
                    error::InconsistentFileSnafu {
                        file_id: *file_id,
                        reason: "narthex samples identity mismatch",
                    }
                );
            }
            let owned_range = Self::narrowed_owned_range(
                target_region_id,
                file_meta,
                source_domain.as_ref(),
                target_domain.as_ref(),
            )?;
            // Keep sharing the source reference when the authorization is unchanged.
            let file_meta_clone =
                if Self::exact_eq(owned_range.as_ref(), file_meta.owned_range.as_deref()) {
                    file_meta.clone()
                } else {
                    let mut meta = (**file_meta).clone();
                    meta.owned_range = owned_range.map(PartitionExprRef::from);
                    FileMetaRef::from(meta)
                };

            // Insert or merge into target manifest
            // Same file might be added from multiple overlapping old regions
            use std::collections::hash_map::Entry;
            match target_manifest.files.entry(*file_id) {
                Entry::Vacant(e) => {
                    e.insert(file_meta_clone);
                }
                Entry::Occupied(mut e) => {
                    #[cfg(debug_assertions)]
                    {
                        // File already exists - verify it's the same physical file
                        Self::verify_file_consistency(e.get(), &file_meta_clone)?;
                    }
                    // One physical file, several references: the target may read the
                    // union of what each reference was allowed to read.
                    let union = Self::union_owned_range(
                        target_region_id,
                        e.get().owned_range.as_deref(),
                        file_meta_clone.owned_range.as_deref(),
                    )?;
                    if !Self::exact_eq(union.as_ref(), e.get().owned_range.as_deref()) {
                        let mut meta = (**e.get()).clone();
                        meta.owned_range = union.map(PartitionExprRef::from);
                        *e.get_mut() = FileMetaRef::from(meta);
                    }
                }
            }
        }

        // Remap creates references, not physical copies: retain index owner,
        // inventory and exact source identities together with the samples.
        for (id, index) in &source_manifest.series_indexes {
            if let Some(existing) = target_manifest.series_indexes.get(id) {
                ensure!(
                    existing == index,
                    error::InconsistentFileSnafu {
                        file_id: *id,
                        reason: "narthex series identity mismatch",
                    }
                );
            } else {
                target_manifest.series_indexes.insert(*id, index.clone());
            }
        }
        Ok(())
    }

    /// Computes the `owned_range` for a reference to `file_meta` in a target region.
    ///
    /// A row is readable through the new reference only if the old reference allowed
    /// it (`file_meta.owned_range`), the source region's domain admitted it and the
    /// target region's domain admits it, so the three are intersected. Operands that
    /// cannot remove a row are dropped: `None` (every physical row) and the file
    /// creator's own `partition_expr`; `combine` then flattens and deduplicates the
    /// result so repeated repartitions cannot grow the expression. `None` in the
    /// result means "every physical row".
    fn narrowed_owned_range(
        region_id: RegionId,
        file_meta: &crate::sst::file::FileMeta,
        source_domain: Option<&PartitionExpr>,
        target_domain: Option<&PartitionExpr>,
    ) -> Result<Option<PartitionExpr>> {
        fn push(operands: &mut Vec<PartitionExpr>, candidate: Option<&PartitionExpr>) {
            if let Some(candidate) = candidate {
                operands.push(candidate.clone());
            }
        }

        let creator = file_meta.partition_expr.as_deref();
        let mut operands: Vec<PartitionExpr> = Vec::with_capacity(3);

        // An explicit range is enforced even when it equals the creator: the
        // source may have been narrowed for reasons the creator expression does
        // not capture.
        push(&mut operands, file_meta.owned_range.as_deref());

        // The source's *effective* filter is the region expression, except that
        // the reader skips the region predicate when it equals the creator's
        // expression. In that case the source saw every physical row, so the
        // source term is TRUE and must not be added back as a restriction. Only
        // an exact key match may drop it: two predicates that merely compare
        // equal as Rust `Value`s (`TIME 1 second` and `TIME 1000 milliseconds`)
        // are different physical filters, and the source never admitted the rows
        // the second one excludes.
        if !Self::exact_eq(source_domain, creator) {
            push(&mut operands, source_domain);
        }

        // The target domain always applies: it is what routes a row to this
        // target. Dropping it because it happens to equal the creator would let
        // two targets both keep a row the creator expression does not match.
        push(&mut operands, target_domain);

        Self::combine(region_id, RestrictedOp::And, operands)
    }

    fn union_owned_range(
        region_id: RegionId,
        existing: Option<&PartitionExpr>,
        incoming: Option<&PartitionExpr>,
    ) -> Result<Option<PartitionExpr>> {
        match (existing, incoming) {
            // Two holders of the same physical file publish the *union* of their
            // authorizations: the target region may read either range. `None`
            // means "every physical row", so it absorbs the union.
            (Some(existing), Some(incoming)) => Ok(Self::combine(
                region_id,
                RestrictedOp::Or,
                [existing.clone(), incoming.clone()],
            )?),
            _ => Ok(None),
        }
    }

    /// Exact, persistable identity of one predicate.
    ///
    /// [PartitionExpr]'s own equality compares literals as Rust `Value`s, where
    /// two coercible literals (`TIME 1 second` and `TIME 1000 milliseconds`) are
    /// equal although they are different physical predicates. Deduplicating those
    /// would widen a reference, so every leaf is keyed by its serialized form,
    /// which keeps the literal's type tag. It is also the form that gets persisted,
    /// so the key and the decoder agree on what "the same predicate" means; the
    /// expression is canonicalized first because decoding canonicalizes, and a
    /// predicate built in memory must compare equal to the one that was stored.
    fn exact_key(expr: &PartitionExpr) -> String {
        expr.exact_key()
    }

    /// Compares two optional predicates by [PartitionExpr::same_exact].
    ///
    /// Every decision that drops or replaces a term goes through this instead of
    /// `PartitionExpr` equality, which cannot tell two literals of different
    /// physical types apart. The reader (`ParquetFileReader::is_same_region_partition`)
    /// and the scan planner (`ScanInput::should_skip_region_partition`) ask the same
    /// question with the same helper, so a source that skipped its region predicate
    /// is exactly one whose creator expression keys equal to the source domain.
    fn exact_eq(left: Option<&PartitionExpr>, right: Option<&PartitionExpr>) -> bool {
        PartitionExpr::same_exact(left, right)
    }

    /// Folds `terms` into one expression of `op` with a bounded shape.
    ///
    /// Every remap intersects or unions the tree it produced last time, so a
    /// naive `reduce` duplicates the ownership tree on each hop and builds a
    /// left-deep chain whose persisted JSON eventually exceeds the decoder's
    /// recursion limit. Exact duplicates are dropped and the remaining terms
    /// combined pairwise, so the depth grows with `log2(terms)` instead of with
    /// the number of hops; past [OWNED_RANGE_MAX_DEPTH] the expression is
    /// rewritten into one canonical operand list, which keeps the persisted shape
    /// small however often the reference is re-authorized. Neither rewrite can
    /// widen what a reference authorizes: they use only `A AND A = A`,
    /// `A OR A = A`, associativity and distributivity.
    ///
    /// The canonical rewrite multiplies disjuncts, so its size is computed on the
    /// tree and rejected past [OWNED_RANGE_MAX_TERMS] *before* anything is
    /// allocated. A shape that cannot be published exactly within that budget is
    /// an error, never a widened or truncated range.
    fn combine(
        region_id: RegionId,
        op: RestrictedOp,
        terms: impl IntoIterator<Item = PartitionExpr>,
    ) -> Result<Option<PartitionExpr>> {
        let mut keys = HashSet::new();
        let mut flat: Vec<PartitionExpr> = Vec::new();
        for term in terms {
            if keys.insert(Self::exact_key(&term)) {
                flat.push(term);
            }
        }
        let Some(combined) = Self::nest(op, flat) else {
            return Ok(None);
        };
        if Self::depth(&combined) <= OWNED_RANGE_MAX_DEPTH {
            return Ok(Some(combined));
        }
        // Every hop re-applies the same intersection and union, so nesting one
        // level deeper per hop would grow without bound. Past a small depth the
        // operands are published as one canonical list instead: distributing the
        // tree into disjuncts of conjuncts, dropping duplicated and subsumed
        // terms, and re-nesting that list into a balanced tree.
        let disjuncts = Self::disjunct_count(&combined);
        ensure!(
            disjuncts <= OWNED_RANGE_MAX_TERMS,
            error::InvalidRequestSnafu {
                region_id,
                reason: format!(
                    "An ownership range would expand to {disjuncts} disjuncts, the budget is {OWNED_RANGE_MAX_TERMS}"
                ),
            }
        );
        let mut dnf = Vec::with_capacity(disjuncts);
        Self::to_dnf(&combined, &mut dnf);
        Self::from_dnf(region_id, dnf)
    }

    /// Number of disjuncts `expr` expands to, saturated.
    ///
    /// Computed without allocating, so a tiny pathological input cannot expand
    /// into an astronomical term list before it is rejected.
    fn disjunct_count(expr: &PartitionExpr) -> usize {
        match (expr.op(), expr.lhs(), expr.rhs()) {
            (RestrictedOp::Or, Operand::Expr(lhs), Operand::Expr(rhs)) => {
                Self::disjunct_count(lhs).saturating_add(Self::disjunct_count(rhs))
            }
            (RestrictedOp::And, Operand::Expr(lhs), Operand::Expr(rhs)) => {
                Self::disjunct_count(lhs).saturating_mul(Self::disjunct_count(rhs))
            }
            _ => 1,
        }
    }

    /// Nests `terms` with `op` into a balanced tree, so the depth grows with
    /// `log2(terms)` instead of with the number of hops.
    fn nest(op: RestrictedOp, mut terms: Vec<PartitionExpr>) -> Option<PartitionExpr> {
        while terms.len() > 1 {
            let mut combined = Vec::with_capacity(terms.len().div_ceil(2));
            let mut iter = terms.into_iter();
            while let Some(lhs) = iter.next() {
                combined.push(match iter.next() {
                    Some(rhs) => {
                        PartitionExpr::new(Operand::Expr(lhs), op.clone(), Operand::Expr(rhs))
                    }
                    None => lhs,
                });
            }
            terms = combined;
        }
        terms.pop()
    }

    /// Rewrites `expr` as disjuncts of conjuncts. `AND` and `OR` are associative
    /// and distributive, so this is an exact logical rewrite, never a widening.
    /// Callers bound the result with [Self::disjunct_count] first.
    fn to_dnf(expr: &PartitionExpr, terms: &mut Vec<Vec<PartitionExpr>>) {
        match (expr.op(), expr.lhs(), expr.rhs()) {
            (RestrictedOp::Or, Operand::Expr(lhs), Operand::Expr(rhs)) => {
                Self::to_dnf(lhs, terms);
                Self::to_dnf(rhs, terms);
            }
            (RestrictedOp::And, Operand::Expr(lhs), Operand::Expr(rhs)) => {
                let (mut left, mut right) = (Vec::new(), Vec::new());
                Self::to_dnf(lhs, &mut left);
                Self::to_dnf(rhs, &mut right);
                for left in &left {
                    for right in &right {
                        let term = left.iter().chain(right).cloned().collect();
                        terms.push(term);
                    }
                }
            }
            // A comparison (or a node this form cannot decompose) is one conjunct.
            _ => terms.push(vec![expr.clone()]),
        }
    }

    /// Rebuilds one expression from `terms` (`OR` of `AND`s), dropping duplicated
    /// conjuncts and terms subsumed by a shorter one: `A OR (A AND B) = A`.
    ///
    /// Everything is ordered and compared by [Self::exact_key]: `PartitionExpr`
    /// ordering reaches `Value::cmp`, which panics on mixed numeric widths and
    /// treats coercible literals of different physical types as equal.
    fn from_dnf(
        region_id: RegionId,
        terms: Vec<Vec<PartitionExpr>>,
    ) -> Result<Option<PartitionExpr>> {
        let mut keyed: Vec<(Vec<String>, Vec<PartitionExpr>)> = Vec::with_capacity(terms.len());
        for term in terms {
            let mut conjuncts: Vec<(String, PartitionExpr)> = term
                .into_iter()
                .map(|expr| (Self::exact_key(&expr), expr))
                .collect();
            conjuncts.sort_by(|left, right| left.0.cmp(&right.0));
            conjuncts.dedup_by(|left, right| left.0 == right.0);
            let (keys, exprs) = conjuncts.into_iter().unzip();
            keyed.push((keys, exprs));
        }
        // Shorter terms first, so a term can only be subsumed by an earlier one.
        keyed.sort_by(|left, right| {
            left.0
                .len()
                .cmp(&right.0.len())
                .then_with(|| left.0.cmp(&right.0))
        });
        let mut kept: Vec<(Vec<String>, Vec<PartitionExpr>)> = Vec::with_capacity(keyed.len());
        for (keys, exprs) in keyed {
            if kept
                .iter()
                .any(|(kept_keys, _)| kept_keys.iter().all(|key| keys.binary_search(key).is_ok()))
            {
                continue;
            }
            kept.push((keys, exprs));
        }
        let operands = kept
            .into_iter()
            .map(|(_, term)| Self::nest(RestrictedOp::And, term))
            .collect::<Option<Vec<_>>>();
        let Some(operands) = operands else {
            return Ok(None);
        };
        let Some(rebuilt) = Self::nest(RestrictedOp::Or, operands) else {
            return Ok(None);
        };
        ensure!(
            Self::depth(&rebuilt) <= OWNED_RANGE_MAX_DEPTH,
            error::InvalidRequestSnafu {
                region_id,
                reason: format!(
                    "An ownership range cannot be published within depth {OWNED_RANGE_MAX_DEPTH}"
                ),
            }
        );
        Ok(Some(rebuilt))
    }

    /// Operator nesting of `expr`, used only to decide when to publish it flat.
    fn depth(expr: &PartitionExpr) -> usize {
        1 + Self::operand_depth(expr.lhs()).max(Self::operand_depth(expr.rhs()))
    }

    fn operand_depth(operand: &Operand) -> usize {
        match operand {
            Operand::Expr(expr) => Self::depth(expr),
            _ => 0,
        }
    }

    /// Verifies that two file metadata entries are consistent.
    #[cfg(debug_assertions)]
    fn verify_file_consistency(
        existing: &crate::sst::file::FileMeta,
        new: &crate::sst::file::FileMeta,
    ) -> Result<()> {
        // When the same file appears from multiple overlapping old regions,
        // verify they are actually the same physical file with identical metadata

        ensure!(
            existing.region_id == new.region_id,
            error::InconsistentFileSnafu {
                file_id: existing.file_id,
                reason: "region_id mismatch",
            }
        );

        ensure!(
            existing.file_id == new.file_id,
            error::InconsistentFileSnafu {
                file_id: existing.file_id,
                reason: "file_id mismatch",
            }
        );

        ensure!(
            existing.time_range == new.time_range,
            error::InconsistentFileSnafu {
                file_id: existing.file_id,
                reason: "time_range mismatch",
            }
        );

        ensure!(
            existing.level == new.level,
            error::InconsistentFileSnafu {
                file_id: existing.file_id,
                reason: "level mismatch",
            }
        );

        ensure!(
            existing.file_size == new.file_size,
            error::InconsistentFileSnafu {
                file_id: existing.file_id,
                reason: "file_size mismatch",
            }
        );

        ensure!(
            existing.partition_expr == new.partition_expr,
            error::InconsistentFileSnafu {
                file_id: existing.file_id,
                reason: "partition_expr mismatch",
            }
        );

        Ok(())
    }

    /// Finalizes manifests by merging metadata from source regions.
    fn finalize_manifests(&mut self) -> Result<()> {
        for (region_id, manifest) in self.new_manifests.iter_mut() {
            if let Some(previous_manifest) = self.old_manifests.get(region_id) {
                manifest.flushed_entry_id = previous_manifest.flushed_entry_id;
                manifest.flushed_sequence = previous_manifest.flushed_sequence;
                manifest.manifest_version = previous_manifest.manifest_version;
                manifest.truncated_entry_id = previous_manifest.truncated_entry_id;
                manifest.compaction_time_window = previous_manifest.compaction_time_window;
                manifest.committed_sequence = previous_manifest.committed_sequence;
            } else {
                // new region
                manifest.flushed_entry_id = 0;
                manifest.flushed_sequence = 0;
                manifest.manifest_version = 0;
                manifest.truncated_entry_id = None;
                manifest.compaction_time_window = None;
                manifest.committed_sequence = None;
            }

            // removed_files are tracked by old manifests, don't copy
            manifest.removed_files = RemovedFilesRecord::default();
        }

        Ok(())
    }

    /// Computes statistics about the remapping.
    fn compute_stats(&self) -> RemapStats {
        let mut files_per_region = HashMap::with_capacity(self.new_manifests.len());
        let mut total_file_refs = 0;
        let mut empty_regions = Vec::new();
        let mut all_files = HashSet::new();

        for (&region_id, manifest) in &self.new_manifests {
            let file_count = manifest.files.len();
            files_per_region.insert(region_id, file_count);
            total_file_refs += file_count;

            if file_count == 0 {
                empty_regions.push(region_id);
            }

            for file_id in manifest.files.keys() {
                all_files.insert(*file_id);
            }
        }

        RemapStats {
            files_per_region,
            total_file_refs,
            empty_regions,
            unique_files: all_files.len(),
        }
    }

    /// Validates the remapping result.
    fn validate_result(&self, stats: &RemapStats) -> Result<()> {
        // all new regions have manifests
        for region_id in self.new_partition_exprs.keys() {
            ensure!(
                self.new_manifests.contains_key(region_id),
                error::MissingNewManifestSnafu {
                    region_id: *region_id
                }
            );
        }

        // no unique files were lost
        // Count unique files in old manifests (files may be duplicated across regions)
        let mut old_unique_files = HashSet::new();
        for manifest in self.old_manifests.values() {
            for file_id in manifest.files.keys() {
                old_unique_files.insert(*file_id);
            }
        }

        ensure!(
            stats.unique_files >= old_unique_files.len(),
            error::FilesLostSnafu {
                old_count: old_unique_files.len(),
                new_count: stats.unique_files,
            }
        );

        // 3. Log warning about empty regions (not an error)
        if !stats.empty_regions.is_empty() {
            common_telemetry::warn!(
                "Repartition resulted in {} empty regions: {:?}, new partition exprs: {:?}",
                stats.empty_regions.len(),
                stats.empty_regions,
                self.new_partition_exprs.keys().collect::<Vec<_>>()
            );
        }

        Ok(())
    }
}

/// Result of manifest remapping, including new manifests and statistics.
#[derive(Debug)]
pub struct RemapResult {
    /// New manifests for all new regions
    pub new_manifests: HashMap<RegionId, RegionManifest>,
    /// Statistics about the remapping
    pub stats: RemapStats,
}

/// Statistical information about the manifest remapping.
#[derive(Debug)]
pub struct RemapStats {
    /// Number of files per region in new manifests
    pub files_per_region: HashMap<RegionId, usize>,
    /// Total number of file references created across all new regions
    pub total_file_refs: usize,
    /// Regions that received no files (potentially empty after repartition)
    pub empty_regions: Vec<RegionId>,
    /// Total number of unique files processed
    pub unique_files: usize,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::num::NonZeroU64;
    use std::sync::Arc;
    use std::time::Duration;

    use api::v1::SemanticType;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use datatypes::value::Value;
    use partition::expr::{PartitionExpr, col};
    use smallvec::SmallVec;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder, RegionMetadataRef};
    use store_api::storage::{FileId, RegionId, SequenceNumber};

    use super::*;
    use crate::manifest::action::RegionManifest;
    use crate::sst::FormatType;
    use crate::sst::file::{FileMeta, FileTimeRange};
    use crate::wal::EntryId;

    /// Helper to create a basic region metadata for testing.
    fn create_region_metadata(region_id: RegionId) -> RegionMetadataRef {
        let mut builder = RegionMetadataBuilder::new(region_id);
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("pk", ConcreteDataType::int64_datatype(), false),
                semantic_type: SemanticType::Tag,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "val",
                    ConcreteDataType::float64_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Field,
                column_id: 3,
            })
            .primary_key(vec![2]);
        Arc::new(builder.build().unwrap())
    }

    /// Helper to create a FileMeta for testing.
    fn create_file_meta(
        region_id: RegionId,
        file_id: FileId,
        partition_expr: Option<PartitionExpr>,
    ) -> FileMeta {
        FileMeta {
            region_id,
            file_id,
            time_range: FileTimeRange::default(),
            level: 0,
            file_size: 1024,
            max_row_group_uncompressed_size: 1024,
            available_indexes: SmallVec::new(),
            indexes: Default::default(),
            index_file_size: 0,
            index_version: 0,
            num_rows: 100,
            num_row_groups: 1,
            sequence: NonZeroU64::new(1),
            partition_expr: partition_expr.map(Into::into),
            num_series: 1,
            ..Default::default()
        }
    }

    /// Helper to create a manifest with specified number of files.
    fn create_manifest(
        region_id: RegionId,
        num_files: usize,
        partition_expr: Option<PartitionExpr>,
        flushed_entry_id: EntryId,
        flushed_sequence: SequenceNumber,
    ) -> RegionManifest {
        let mut files = HashMap::new();
        for _ in 0..num_files {
            let file_id = FileId::random();
            let file_meta = create_file_meta(region_id, file_id, partition_expr.clone());
            files.insert(file_id, file_meta.into());
        }

        RegionManifest {
            metadata: create_region_metadata(region_id),
            files,
            removed_files: RemovedFilesRecord::default(),
            flushed_entry_id,
            flushed_sequence,
            manifest_version: 1,
            truncated_entry_id: None,
            compaction_time_window: None,
            committed_sequence: None,
            sst_format: FormatType::PrimaryKey,
            append_mode: None,
            pending_tails: Default::default(),
            series_indexes: Default::default(),
            handoff_floors: Default::default(),
        }
    }

    #[rstest::rstest]
    #[case(FormatType::Narthex, FormatType::Flat)]
    #[case(FormatType::PrimaryKey, FormatType::Narthex)]
    #[case(FormatType::Flat, FormatType::PrimaryKey)]
    #[test]
    fn test_remap_storage_format(
        #[case] source_format: FormatType,
        #[case] target_format: FormatType,
    ) {
        let mut source = create_manifest(RegionId::new(1, 0), 0, None, 0, 0);
        let mut target = create_manifest(RegionId::new(1, 1), 0, None, 0, 0);
        source.sst_format = source_format;
        target.sst_format = target_format;
        let result = RemapManifest::copy_files_to_region(&source, &mut target);
        if source_format == FormatType::Narthex || target_format == FormatType::Narthex {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("Cannot remap between")
            );
        } else {
            result.unwrap();
        }
        assert!(target.files.is_empty());
        assert_eq!(target.sst_format, target_format);
    }

    /// Helper to create partition expression: col >= start AND col < end
    fn range_expr(col_name: &str, start: i64, end: i64) -> PartitionExpr {
        col(col_name)
            .gt_eq(Value::Int64(start))
            .and(col(col_name).lt(Value::Int64(end)))
    }

    #[test]
    fn test_simple_split() {
        // One region [0, 100) splits into two regions: [0, 50) and [50, 100)
        let old_region_id = RegionId::new(1, 1);
        let new_region_id_1 = RegionId::new(1, 2);
        let new_region_id_2 = RegionId::new(1, 3);

        let old_expr = range_expr("x", 0, 100);
        let new_expr_1 = range_expr("x", 0, 50);
        let new_expr_2 = range_expr("x", 50, 100);

        let old_manifest = create_manifest(old_region_id, 10, Some(old_expr.clone()), 100, 200);

        let mut old_manifests = HashMap::new();
        old_manifests.insert(old_region_id, old_manifest);

        let mut new_partition_exprs = HashMap::new();
        new_partition_exprs.insert(new_region_id_1, new_expr_1);
        new_partition_exprs.insert(new_region_id_2, new_expr_2);

        // Direct mapping: old region -> both new regions
        let mut region_mapping = HashMap::new();
        region_mapping.insert(old_region_id, vec![new_region_id_1, new_region_id_2]);

        let mut remapper = RemapManifest::new(old_manifests, new_partition_exprs, region_mapping);

        let result = remapper.remap_manifests().unwrap();

        // Both new regions should have all 10 files
        assert_eq!(result.new_manifests.len(), 2);
        assert_eq!(result.new_manifests[&new_region_id_1].files.len(), 10);
        assert_eq!(result.new_manifests[&new_region_id_2].files.len(), 10);
        assert_eq!(result.stats.total_file_refs, 20);
        assert_eq!(result.stats.unique_files, 10);
        assert!(result.stats.empty_regions.is_empty());

        // Verify FileMeta is immutable - region_id stays as old_region_id
        for file_meta in result.new_manifests[&new_region_id_1].files.values() {
            assert_eq!(file_meta.region_id, old_region_id);
        }
        for file_meta in result.new_manifests[&new_region_id_2].files.values() {
            assert_eq!(file_meta.region_id, old_region_id);
        }
    }

    #[test]
    fn test_simple_merge() {
        // Two regions [0, 50) and [50, 100) merge into one region [0, 100)
        let old_region_id_1 = RegionId::new(1, 1);
        let old_region_id_2 = RegionId::new(1, 2);
        let new_region_id = RegionId::new(1, 3);

        let old_expr_1 = range_expr("x", 0, 50);
        let old_expr_2 = range_expr("x", 50, 100);
        let new_expr = range_expr("x", 0, 100);

        let manifest_1 = create_manifest(old_region_id_1, 5, Some(old_expr_1.clone()), 100, 200);
        let manifest_2 = create_manifest(old_region_id_2, 5, Some(old_expr_2.clone()), 150, 250);

        let mut old_manifests = HashMap::new();
        old_manifests.insert(old_region_id_1, manifest_1);
        old_manifests.insert(old_region_id_2, manifest_2);

        let mut new_partition_exprs = HashMap::new();
        new_partition_exprs.insert(new_region_id, new_expr);

        // Direct mapping: both old regions -> same new region
        let mut region_mapping = HashMap::new();
        region_mapping.insert(old_region_id_1, vec![new_region_id]);
        region_mapping.insert(old_region_id_2, vec![new_region_id]);

        let mut remapper = RemapManifest::new(old_manifests, new_partition_exprs, region_mapping);

        let result = remapper.remap_manifests().unwrap();

        // New region should have all 10 files
        assert_eq!(result.new_manifests.len(), 1);
        assert_eq!(result.new_manifests[&new_region_id].files.len(), 10);
        assert_eq!(result.stats.total_file_refs, 10);
        assert_eq!(result.stats.unique_files, 10);
        assert!(result.stats.empty_regions.is_empty());

        // Verify metadata falls back to defaults when no prior manifest exists for the region
        let new_manifest = &result.new_manifests[&new_region_id];
        assert_eq!(new_manifest.flushed_entry_id, 0);
        assert_eq!(new_manifest.flushed_sequence, 0);
        assert_eq!(new_manifest.manifest_version, 0);
        assert_eq!(new_manifest.truncated_entry_id, None);
        assert_eq!(new_manifest.compaction_time_window, None);
    }

    #[test]
    fn test_metadata_preserved_for_existing_region() {
        // Test that metadata is preserved when a previous manifest exists for the same region id
        let old_region_id_1 = RegionId::new(1, 1);
        let old_region_id_2 = RegionId::new(1, 2);
        let old_region_id_3 = RegionId::new(1, 3);
        let new_region_id = RegionId::new(1, 4);

        let new_expr = range_expr("x", 0, 100);

        let mut manifest_1 = create_manifest(old_region_id_1, 2, None, 10, 20);
        manifest_1.truncated_entry_id = Some(5);
        manifest_1.compaction_time_window = Some(Duration::from_secs(3600));

        let mut manifest_2 = create_manifest(old_region_id_2, 2, None, 25, 15); // Higher entry_id, lower sequence
        manifest_2.truncated_entry_id = Some(20);
        manifest_2.compaction_time_window = Some(Duration::from_secs(7200)); // Larger window

        let manifest_3 = create_manifest(old_region_id_3, 2, None, 15, 30); // Lower entry_id, higher sequence
        let mut previous_manifest = create_manifest(new_region_id, 0, None, 200, 300);
        previous_manifest.truncated_entry_id = Some(40);
        previous_manifest.compaction_time_window = Some(Duration::from_secs(1800));
        previous_manifest.manifest_version = 7;
        let expected_flushed_entry_id = previous_manifest.flushed_entry_id;
        let expected_flushed_sequence = previous_manifest.flushed_sequence;
        let expected_truncated_entry_id = previous_manifest.truncated_entry_id;
        let expected_compaction_window = previous_manifest.compaction_time_window;
        let expected_manifest_version = previous_manifest.manifest_version;

        let mut old_manifests = HashMap::new();
        old_manifests.insert(old_region_id_1, manifest_1);
        old_manifests.insert(old_region_id_2, manifest_2);
        old_manifests.insert(old_region_id_3, manifest_3);
        old_manifests.insert(new_region_id, previous_manifest);

        let mut new_partition_exprs = HashMap::new();
        new_partition_exprs.insert(new_region_id, new_expr);

        // Direct mapping: all old regions -> same new region
        let mut region_mapping = HashMap::new();
        region_mapping.insert(old_region_id_1, vec![new_region_id]);
        region_mapping.insert(old_region_id_2, vec![new_region_id]);
        region_mapping.insert(old_region_id_3, vec![new_region_id]);

        let mut remapper = RemapManifest::new(old_manifests, new_partition_exprs, region_mapping);

        let result = remapper.remap_manifests().unwrap();

        let new_manifest = &result.new_manifests[&new_region_id];
        // Should reuse metadata from previous manifest of the same region id
        assert_eq!(new_manifest.flushed_entry_id, expected_flushed_entry_id);
        assert_eq!(new_manifest.flushed_sequence, expected_flushed_sequence);
        assert_eq!(new_manifest.truncated_entry_id, expected_truncated_entry_id);
        assert_eq!(
            new_manifest.compaction_time_window,
            expected_compaction_window
        );
        assert_eq!(new_manifest.manifest_version, expected_manifest_version);
    }

    #[test]
    fn test_file_consistency_check() {
        // Test that duplicate files are verified for consistency
        let old_region_id_1 = RegionId::new(1, 1);
        let old_region_id_2 = RegionId::new(1, 2);
        let new_region_id = RegionId::new(1, 3);

        let new_expr = range_expr("x", 0, 100);

        // Create manifests with same file (overlapping regions)
        let shared_file_id = FileId::random();
        let file_meta = create_file_meta(old_region_id_1, shared_file_id, None);

        let mut manifest_1 = create_manifest(old_region_id_1, 0, None, 100, 200);
        manifest_1
            .files
            .insert(shared_file_id, file_meta.clone().into());

        let mut manifest_2 = create_manifest(old_region_id_2, 0, None, 100, 200);
        manifest_2.files.insert(shared_file_id, file_meta.into());

        let mut old_manifests = HashMap::new();
        old_manifests.insert(old_region_id_1, manifest_1);
        old_manifests.insert(old_region_id_2, manifest_2);

        let mut new_partition_exprs = HashMap::new();
        new_partition_exprs.insert(new_region_id, new_expr);

        // Direct mapping: both old regions -> same new region
        let mut region_mapping = HashMap::new();
        region_mapping.insert(old_region_id_1, vec![new_region_id]);
        region_mapping.insert(old_region_id_2, vec![new_region_id]);

        let mut remapper = RemapManifest::new(old_manifests, new_partition_exprs, region_mapping);

        let result = remapper.remap_manifests().unwrap();

        // Should succeed - same file is consistent
        assert_eq!(result.new_manifests[&new_region_id].files.len(), 1);
        assert_eq!(result.stats.total_file_refs, 1);
        assert_eq!(result.stats.unique_files, 1);
    }

    #[test]
    fn test_empty_regions() {
        // Test that empty regions are detected and logged (but not an error)
        let old_region_id = RegionId::new(1, 1);
        let new_region_id_1 = RegionId::new(1, 2);
        let new_region_id_2 = RegionId::new(1, 3);

        let old_expr = range_expr("x", 0, 50);
        let new_expr_1 = range_expr("x", 0, 50);
        let new_expr_2 = range_expr("x", 100, 200); // No overlap with old region

        let old_manifest = create_manifest(old_region_id, 5, Some(old_expr.clone()), 100, 200);

        let mut old_manifests = HashMap::new();
        old_manifests.insert(old_region_id, old_manifest);

        let mut new_partition_exprs = HashMap::new();
        new_partition_exprs.insert(new_region_id_1, new_expr_1);
        new_partition_exprs.insert(new_region_id_2, new_expr_2);

        // Direct mapping: old region -> new region 1 only (region 2 is isolated)
        let mut region_mapping = HashMap::new();
        region_mapping.insert(old_region_id, vec![new_region_id_1]);

        let mut remapper = RemapManifest::new(old_manifests, new_partition_exprs, region_mapping);

        let result = remapper.remap_manifests().unwrap();

        // Region 2 should be empty
        assert_eq!(result.new_manifests[&new_region_id_1].files.len(), 5);
        assert_eq!(result.new_manifests[&new_region_id_2].files.len(), 0);
        assert_eq!(result.stats.empty_regions, vec![new_region_id_2]);
    }

    #[test]
    fn test_n_to_m_complex_repartition() {
        // Test complex N-to-M transition: 2 old regions -> 3 new regions
        let old_region_1 = RegionId::new(1, 1);
        let old_region_2 = RegionId::new(1, 2);
        let new_region_1 = RegionId::new(1, 3);
        let new_region_2 = RegionId::new(1, 4);
        let new_region_3 = RegionId::new(1, 5);

        // Old: [0, 100), [100, 200)
        // New: [0, 50), [50, 150), [150, 250)
        let old_expr_1 = range_expr("u", 0, 100);
        let old_expr_2 = range_expr("u", 100, 200);
        let new_expr_1 = range_expr("u", 0, 50);
        let new_expr_2 = range_expr("u", 50, 150);
        let new_expr_3 = range_expr("u", 150, 250);

        let manifest_1 = create_manifest(old_region_1, 3, Some(old_expr_1.clone()), 100, 200);
        let manifest_2 = create_manifest(old_region_2, 4, Some(old_expr_2.clone()), 150, 250);

        let mut old_manifests = HashMap::new();
        old_manifests.insert(old_region_1, manifest_1);
        old_manifests.insert(old_region_2, manifest_2);

        let mut new_partition_exprs = HashMap::new();
        new_partition_exprs.insert(new_region_1, new_expr_1);
        new_partition_exprs.insert(new_region_2, new_expr_2);
        new_partition_exprs.insert(new_region_3, new_expr_3);

        // Direct mapping:
        // old region 1 -> new regions 1, 2
        // old region 2 -> new regions 2, 3
        let mut region_mapping = HashMap::new();
        region_mapping.insert(old_region_1, vec![new_region_1, new_region_2]);
        region_mapping.insert(old_region_2, vec![new_region_2, new_region_3]);

        let mut remapper = RemapManifest::new(old_manifests, new_partition_exprs, region_mapping);

        let result = remapper.remap_manifests().unwrap();

        assert_eq!(result.new_manifests.len(), 3);
        assert_eq!(result.new_manifests[&new_region_1].files.len(), 3);
        assert_eq!(result.new_manifests[&new_region_2].files.len(), 7); // 3 + 4
        assert_eq!(result.new_manifests[&new_region_3].files.len(), 4);
        assert_eq!(result.stats.total_file_refs, 14); // 3 + 7 + 4
        assert_eq!(result.stats.unique_files, 7); // 3 + 4 unique
    }

    /// Sets the region domain recorded in a manifest's metadata.
    fn set_manifest_domain(manifest: &mut RegionManifest, domain: Option<PartitionExpr>) {
        let mut metadata = (*manifest.metadata).clone();
        metadata.set_partition_expr(domain.map(|expr| expr.as_json_str().unwrap()));
        manifest.metadata = Arc::new(metadata);
    }

    /// Remaps a single source manifest into a single target domain.
    fn remap_into(
        source: RegionId,
        source_manifest: RegionManifest,
        target: RegionId,
        target_domain: PartitionExpr,
    ) -> RegionManifest {
        let mut remapper = RemapManifest::new(
            HashMap::from([(source, source_manifest)]),
            HashMap::from([(target, target_domain)]),
            HashMap::from([(source, vec![target])]),
        );
        remapper.remap_manifests().unwrap().new_manifests[&target].clone()
    }

    #[test]
    fn test_remap_owned_range_narrows_and_unions() {
        let source = RegionId::new(1, 1);
        let other = RegionId::new(1, 2);
        let target = RegionId::new(1, 3);

        // Creator expression of the shared file: the region that flushed it.
        let file_domain = range_expr("u", 0, 100);
        let left = range_expr("u", 0, 50);
        let right = range_expr("u", 50, 100);

        let mut owning = create_manifest(source, 1, Some(file_domain.clone()), 100, 200);
        set_manifest_domain(&mut owning, Some(file_domain.clone()));
        let file_id = *owning.files.keys().next().unwrap();

        // A target whose domain equals the creator still keeps that domain: write
        // admission does not constrain a file's physical rows to the creator
        // expression, so dropping it would let a sibling target keep a row this
        // target must not see.
        let remapped = remap_into(source, owning.clone(), target, file_domain.clone());
        assert_eq!(
            Some(&file_domain),
            remapped.files[&file_id].owned_range.as_deref()
        );

        // Narrowing by the target domain; the source domain is dropped because it is
        // the creator expression.
        let remapped = remap_into(source, owning.clone(), target, left.clone());
        assert_eq!(Some(&left), remapped.files[&file_id].owned_range.as_deref());

        // An unpartitioned source narrows too: `None` imposes nothing, it does not
        // authorize the rows the target domain excludes.
        let mut unpartitioned = create_manifest(source, 1, None, 100, 200);
        set_manifest_domain(&mut unpartitioned, None);
        let file_id = *unpartitioned.files.keys().next().unwrap();
        let remapped = remap_into(source, unpartitioned, target, left.clone());
        assert_eq!(Some(&left), remapped.files[&file_id].owned_range.as_deref());

        // The same physical file reaches the target from two source regions, so the
        // target may read the union of both ranges.
        let mut left_manifest = create_manifest(source, 1, Some(file_domain.clone()), 100, 200);
        set_manifest_domain(&mut left_manifest, Some(left.clone()));
        let file_id = *left_manifest.files.keys().next().unwrap();
        let mut right_manifest = create_manifest(other, 0, Some(file_domain.clone()), 100, 200);
        set_manifest_domain(&mut right_manifest, Some(right.clone()));
        right_manifest
            .files
            .insert(file_id, left_manifest.files[&file_id].clone());
        let mut remapper = RemapManifest::new(
            HashMap::from([(source, left_manifest), (other, right_manifest)]),
            HashMap::from([(target, file_domain.clone())]),
            HashMap::from([(source, vec![target]), (other, vec![target])]),
        );
        let remapped = remapper.remap_manifests().unwrap().new_manifests[&target].clone();
        let owned = remapped.files[&file_id]
            .owned_range
            .as_deref()
            .expect("the union must authorize rows");
        // The two source regions are visited in hash order, so either operand order is
        // correct for the `Or` chain.
        // Each contribution is restricted by the target domain as well, because a
        // target domain always applies (`whole` here), so the union is over
        // `left AND whole` and `right AND whole`.
        let left_contribution = left.clone().and(file_domain.clone());
        let right_contribution = right.clone().and(file_domain.clone());
        let forward = PartitionExpr::new(
            Operand::Expr(left_contribution.clone()),
            RestrictedOp::Or,
            Operand::Expr(right_contribution.clone()),
        );
        let backward = PartitionExpr::new(
            Operand::Expr(right_contribution),
            RestrictedOp::Or,
            Operand::Expr(left_contribution),
        );
        assert!(
            owned == &forward || owned == &backward,
            "unexpected union: {owned:?}"
        );
    }

    /// A remap never hands the apply path an entry it would refuse: every entry of
    /// every target carries a range, because the target expression is always one of
    /// its operands.
    ///
    /// Together with
    /// `worker::handle_apply_staging::tests::test_remap_install_without_an_owned_range_is_refused`
    /// (an entry without a range is refused, which is how an old remap's output is
    /// stopped by a new node) this pins the upgrade boundary from both sides: what a
    /// build that understands `owned_range` publishes, and what it accepts.
    #[test]
    fn test_every_remapped_entry_carries_an_owned_range() {
        let source = RegionId::new(1, 1);
        let co_holder = RegionId::new(1, 2);
        let split_left = RegionId::new(1, 3);
        let split_right = RegionId::new(1, 4);
        let merged = RegionId::new(1, 5);

        let whole = range_expr("k", 0, 100);
        let left = range_expr("k", 0, 50);
        let right = range_expr("k", 50, 100);

        // One physical file, two holders, each authorized for one half; its creator
        // expression is the whole domain, so no holder's range is redundant.
        let mut holder = create_manifest(source, 1, Some(whole.clone()), 1, 1);
        set_manifest_domain(&mut holder, Some(left.clone()));
        let file_id = *holder.files.keys().next().unwrap();
        let mut held = (*holder.files[&file_id]).clone();
        held.owned_range = Some(PartitionExprRef::from(left.clone()));
        holder.files.insert(file_id, FileMetaRef::from(held));
        let mut co = create_manifest(co_holder, 0, Some(whole.clone()), 1, 1);
        set_manifest_domain(&mut co, Some(right.clone()));
        let mut co_held = (*holder.files[&file_id]).clone();
        co_held.owned_range = Some(PartitionExprRef::from(right.clone()));
        co.files.insert(file_id, FileMetaRef::from(co_held));

        // Both halves of a split, and a merge of both holders into one region.
        let remapped = RemapManifest::new(
            HashMap::from([(source, holder), (co_holder, co)]),
            HashMap::from([
                (split_left, left.clone()),
                (split_right, right.clone()),
                (merged, whole.clone()),
            ]),
            HashMap::from([
                (source, vec![split_left, merged]),
                (co_holder, vec![split_right, merged]),
            ]),
        )
        .remap_manifests()
        .unwrap()
        .new_manifests;

        for (region_id, manifest) in &remapped {
            assert!(
                !manifest.files.is_empty(),
                "region {region_id} kept no file"
            );
            for (file_id, entry) in &manifest.files {
                assert!(
                    entry.owned_range.is_some(),
                    "region {region_id} entry {file_id:?} must carry a range"
                );
            }
        }
        // A target that sees the reference once only publishes its own domain.
        assert_eq!(
            Some(&left),
            remapped[&split_left].files[&file_id].owned_range.as_deref()
        );
        assert_eq!(
            Some(&right),
            remapped[&split_right].files[&file_id]
                .owned_range
                .as_deref()
        );
        // The merge sees both holders, so the survivor may read their union.
        let merged_range = remapped[&merged].files[&file_id]
            .owned_range
            .as_deref()
            .expect("the merge must authorize rows");
        assert!(
            merged_range != &left && merged_range != &right,
            "the merged reference must keep both halves, got {merged_range:?}"
        );
    }

    /// Nesting depth of an ownership expression.
    fn owned_range_depth(expr: &PartitionExpr) -> usize {
        RemapManifest::depth(expr)
    }

    #[test]
    fn test_repeated_split_and_merge_keeps_the_owned_range_shallow() {
        let source = RegionId::new(1, 1);
        let left_region = RegionId::new(1, 2);
        let right_region = RegionId::new(1, 3);
        let merged_region = RegionId::new(1, 4);
        let whole = range_expr("k", 0, 100);
        let left = range_expr("k", 0, 50);
        let right = range_expr("k", 50, 100);

        let mut manifest = create_manifest(source, 1, Some(whole.clone()), 1, 1);
        set_manifest_domain(&mut manifest, Some(whole.clone()));
        let file_id = *manifest.files.keys().next().unwrap();

        for cycle in 0..10 {
            // Split the current domain in half...
            let mut halves = HashMap::new();
            for (region, domain) in [(left_region, &left), (right_region, &right)] {
                let remapped = remap_into(source, manifest.clone(), region, domain.clone());
                halves.insert(region, remapped);
            }
            // ...and merge both halves back, which re-applies the same range.
            manifest = RemapManifest::new(
                halves,
                HashMap::from([(merged_region, whole.clone())]),
                HashMap::from([
                    (left_region, vec![merged_region]),
                    (right_region, vec![merged_region]),
                ]),
            )
            .remap_manifests()
            .unwrap()
            .new_manifests[&merged_region]
                .clone();
            let owned_range = manifest.files[&file_id]
                .owned_range
                .as_deref()
                .expect("the merged reference must be restricted to its domain");
            assert!(
                owned_range_depth(owned_range) <= OWNED_RANGE_MAX_DEPTH + 4,
                "cycle {cycle} grew the ownership expression to depth {}",
                owned_range_depth(owned_range)
            );
        }
    }

    #[test]
    fn test_wide_merge_round_trips_through_the_manifest_json() {
        let source = RegionId::new(1, 1);
        let target = RegionId::new(1, 100);
        let whole = range_expr("k", 0, 1000);

        // 32 overlapping holders of the same physical file merge into one region.
        let mut old_manifests = HashMap::new();
        let mut region_mapping = HashMap::new();
        let mut template = create_manifest(source, 1, Some(whole.clone()), 1, 1);
        set_manifest_domain(&mut template, Some(whole.clone()));
        let file_id = *template.files.keys().next().unwrap();
        let mut targets = Vec::new();
        for hop in 0..32i64 {
            let region = RegionId::new(1, hop as u32 + 1);
            let mut manifest = template.clone();
            set_manifest_domain(&mut manifest, Some(range_expr("k", hop, hop + 100)));
            old_manifests.insert(region, manifest);
            region_mapping.insert(region, vec![target]);
            targets.push(region);
        }

        let remapped = RemapManifest::new(
            old_manifests,
            HashMap::from([(target, whole)]),
            region_mapping,
        )
        .remap_manifests()
        .unwrap()
        .new_manifests[&target]
            .clone();
        let owned_range = remapped.files[&file_id]
            .owned_range
            .clone()
            .expect("the merged reference must be restricted to its domain");
        assert!(owned_range_depth(&owned_range) <= OWNED_RANGE_MAX_DEPTH + 4);

        // The persisted form is the one a reopen decodes.
        let encoded = serde_json::to_vec(&remapped).unwrap();
        let decoded = RegionManifest::decode(&encoded).unwrap();
        assert_eq!(
            Some(owned_range.as_ref()),
            decoded.files[&file_id].owned_range.as_deref()
        );
        assert!(owned_range_depth(owned_range.as_ref()) <= OWNED_RANGE_MAX_DEPTH + 4);
    }

    /// Two literals that are equal as Rust `Value`s are still different physical
    /// predicates, so an exact dedupe must keep both: dropping one would widen what
    /// the reference authorizes.
    #[test]
    fn test_ownership_dedupe_keeps_coercible_literals_of_different_types() {
        use common_time::time::Time;

        let region_id = RegionId::new(1, 1);
        let seconds = PartitionExpr::new(
            Operand::Column("t".to_string()),
            RestrictedOp::Gt,
            Operand::Value(Value::Time(Time::new_second(1))),
        );
        let millis = PartitionExpr::new(
            Operand::Column("t".to_string()),
            RestrictedOp::Gt,
            Operand::Value(Value::Time(Time::new_millisecond(1000))),
        );
        // Rust equality treats these as the same predicate.
        assert_eq!(seconds, millis);

        let combined = RemapManifest::combine(
            region_id,
            RestrictedOp::And,
            [seconds.clone(), millis.clone()],
        )
        .unwrap()
        .unwrap();
        let json = combined.as_json_str().unwrap();
        assert!(json.contains("\"Second\""), "{json}");
        assert!(json.contains("\"Millisecond\""), "{json}");
    }

    /// A source domain that is only expression-equal to the creator's own
    /// expression is still applied as a restriction.
    ///
    /// The source term may only be dropped when it *is* the creator expression,
    /// because that is when the reader skips the region predicate and the source
    /// saw every physical row. Expression equality is not enough to decide that:
    /// `TIME 1 second` and `TIME 1000 milliseconds` compare equal as Rust
    /// `Value`s but are different physical filters, so dropping the source term
    /// would hand back rows the source never admitted.
    ///
    /// This pins the expression a remap publishes. The rows it decides are checked
    /// end to end (write, remap twice, scan) by
    /// `engine::repartition_dup_row_test::test_respelled_domain_keeps_the_same_rows_across_a_remap`.
    #[test]
    fn test_expression_equal_but_distinct_source_domain_still_restricts() {
        use common_time::time::Time;

        let source = RegionId::new(1, 1);
        let target = RegionId::new(1, 2);
        let creator = PartitionExpr::new(
            Operand::Column("t".to_string()),
            RestrictedOp::Gt,
            Operand::Value(Value::Time(Time::new_second(1))),
        );
        let source_domain = PartitionExpr::new(
            Operand::Column("t".to_string()),
            RestrictedOp::Gt,
            Operand::Value(Value::Time(Time::new_millisecond(1000))),
        );
        // Rust equality cannot tell the two predicates apart.
        assert_eq!(creator, source_domain);

        let mut manifest = create_manifest(source, 1, Some(creator.clone()), 1, 1);
        set_manifest_domain(&mut manifest, Some(source_domain.clone()));
        let file_id = *manifest.files.keys().next().unwrap();

        // The target domain equals the creator expression, so only the exact-key
        // decision on the source domain is left to keep it out of the result.
        let remapped = remap_into(source, manifest, target, creator.clone());
        let owned_range = remapped.files[&file_id]
            .owned_range
            .as_deref()
            .expect("a distinct source term must survive as a restriction");
        let expected = PartitionExpr::new(
            Operand::Expr(source_domain),
            RestrictedOp::And,
            Operand::Expr(creator),
        );
        assert_eq!(&expected, owned_range);
        let json = owned_range.as_json_str().unwrap();
        assert!(json.contains("\"Second\""), "{json}");
        assert!(json.contains("\"Millisecond\""), "{json}");
    }

    /// A tiny pathological input expands to an astronomical number of disjuncts.
    /// The budget is computed on the tree, so the input is refused instead of
    /// allocating it.
    #[test]
    fn test_ownership_expansion_budget_fails_closed() {
        let region_id = RegionId::new(1, 1);
        let disjunction = |base: i64| {
            let mut expr = PartitionExpr::new(
                Operand::Column("k".to_string()),
                RestrictedOp::Eq,
                Operand::Value(Value::Int64(base)),
            );
            for offset in 1..32 {
                expr = PartitionExpr::new(
                    Operand::Expr(expr),
                    RestrictedOp::Or,
                    Operand::Expr(PartitionExpr::new(
                        Operand::Column("k".to_string()),
                        RestrictedOp::Eq,
                        Operand::Value(Value::Int64(base + offset)),
                    )),
                );
            }
            expr
        };
        // 32 disjuncts, intersected nine times: 32^9 disjuncts.
        let mut pathological = disjunction(0);
        for step in 1..9 {
            pathological = PartitionExpr::new(
                Operand::Expr(pathological),
                RestrictedOp::And,
                Operand::Expr(disjunction(step * 100)),
            );
        }
        assert!(RemapManifest::depth(&pathological) > OWNED_RANGE_MAX_DEPTH);
        let leaf = PartitionExpr::new(
            Operand::Column("k".to_string()),
            RestrictedOp::GtEq,
            Operand::Value(Value::Int64(0)),
        );
        let error = RemapManifest::combine(region_id, RestrictedOp::And, [leaf, pathological])
            .expect_err("an unexpandable ownership range must fail closed");
        assert!(error.to_string().contains("disjuncts"), "{error}");

        // A deep but small input still becomes publishable within the depth budget.
        let mut chain = PartitionExpr::new(
            Operand::Column("k".to_string()),
            RestrictedOp::GtEq,
            Operand::Value(Value::Int64(0)),
        );
        for step in 1..12 {
            chain = PartitionExpr::new(
                Operand::Expr(chain),
                RestrictedOp::And,
                Operand::Expr(PartitionExpr::new(
                    Operand::Column("k".to_string()),
                    RestrictedOp::NotEq,
                    Operand::Value(Value::Int64(step)),
                )),
            );
        }
        assert!(RemapManifest::depth(&chain) > OWNED_RANGE_MAX_DEPTH);
        let combined = RemapManifest::combine(region_id, RestrictedOp::And, [chain])
            .unwrap()
            .unwrap();
        assert!(RemapManifest::depth(&combined) <= OWNED_RANGE_MAX_DEPTH);
    }

    #[test]
    fn test_two_hop_split_keeps_the_target_domain_of_a_creator_matching_target() {
        // Write admission does not evaluate the region's partition expression, so
        // a file created under `C` can physically contain a row outside `C`.
        // Readers skip the region predicate when the expressions match, which
        // means that row is visible in `C`.
        //
        // Remapping `C` to the superset `S` must therefore keep the row; but
        // splitting `S` back into `C` and `U` must not let *both* halves keep it.
        // That only holds if the target domain is never dropped, even when the
        // target's expression happens to equal the creator's.
        let creator = range_expr("k", 0, 5); // C
        let sup = range_expr("k", 0, 10); // S
        let upper = range_expr("k", 5, 100); // U

        let source = RegionId::new(1, 1);
        let sup_region = RegionId::new(1, 2);
        let c_target = RegionId::new(1, 3);
        let u_target = RegionId::new(1, 4);

        let mut source_manifest = create_manifest(source, 1, Some(creator.clone()), 1, 1);
        set_manifest_domain(&mut source_manifest, Some(creator.clone()));
        let file_id = *source_manifest.files.keys().next().unwrap();

        // First hop: C -> S. The creator-equal *source* domain contributes
        // nothing (the source saw every physical row), so the range is `S`.
        let first = remap_into(source, source_manifest, sup_region, sup.clone());
        assert_eq!(
            Some(&sup),
            first.files[&file_id].owned_range.as_deref(),
            "remapping to a superset must keep rows the creator expression excludes"
        );

        // Second hop: S -> {C, U}. Both targets overlap S.
        let mut second = create_manifest(sup_region, 0, Some(creator.clone()), 1, 1);
        set_manifest_domain(&mut second, Some(sup.clone()));
        second.files.insert(file_id, first.files[&file_id].clone());
        let remapped = RemapManifest::new(
            HashMap::from([(sup_region, second)]),
            HashMap::from([(c_target, creator.clone()), (u_target, upper.clone())]),
            HashMap::from([(sup_region, vec![c_target, u_target])]),
        )
        .remap_manifests()
        .unwrap()
        .new_manifests
        .clone();

        assert_eq!(
            Some(&sup.clone().and(creator.clone())),
            remapped[&c_target].files[&file_id].owned_range.as_deref(),
            "a target whose domain equals the creator must still restrict to its own domain"
        );
        assert_eq!(
            Some(&sup.clone().and(upper.clone())),
            remapped[&u_target].files[&file_id].owned_range.as_deref(),
            "the other target keeps its own domain too"
        );
    }
}
