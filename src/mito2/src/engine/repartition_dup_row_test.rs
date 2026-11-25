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

//! Pins a **storage-side** gap, not a query-side one.
//!
//! A repartition (`RemapManifests`) hands the *same* `file_id` to several live
//! regions; the manifests dedup by `file_id` (`remap_manifest.rs:copy_files_to_region`)
//! but nothing marks the file as co-owned. Region compaction admission only looks
//! at the file level (`compactor.rs::validate_region_compaction_output` rejects
//! `LEVEL_SHARED` and nothing else), so one holder can materialize the shared file's
//! rows into a brand-new `file_id` while the other holder keeps the original. Merging
//! the holders back then keeps *both* copies, because the remap dedups by `file_id`
//! and the two copies obviously carry different ids.
//!
//! The test drives the real path end to end: create -> put -> flush -> split
//! (`remap_manifests` + `ApplyStagingManifest`) -> region compaction -> merge
//! (`remap_manifests` over both halves) -> plain scan.
//!
//! Two read-path conditions make the duplicate observable instead of being hidden:
//!
//! * The region is `append_mode`, so the scan returns every visible row. A last-row
//!   region would collapse the two copies while deduping by `(primary key, timestamp)`,
//!   which hides the gap from the read side without removing it from storage.
//! * GC is enabled (the production setting) so retiring the input SST from the left
//!   holder's version only *unlinks* it; `GcConfig::lingering_time` keeps the object.
//!   With the test-default `gc.enable = false`, the co-holder's purge deletes the
//!   physical object immediately, which is the same gap seen from its worst side: the
//!   other holder's rows become unreadable instead of being counted twice.

use std::collections::{HashMap, HashSet};

use api::v1::Rows;
use api::v1::region::{StrictWindow, compact_request};
use common_recordbatch::RecordBatches;
use datatypes::arrow::array::AsArray;
use datatypes::arrow::datatypes::Float64Type;
use datatypes::value::Value;
use partition::expr::{Operand, PartitionExpr, RestrictedOp, col};
use store_api::region_engine::{RegionEngine, RemapManifestsRequest, SettableRegionRoleState};
use store_api::region_request::{
    ApplyStagingManifestRequest, EnterStagingRequest, RegionCompactRequest, RegionCreateRequest,
    RegionFlushRequest, RegionRequest, StagingPartitionDirective,
};
use store_api::storage::{FileId, RegionId};

use super::ScanRequest;
use crate::config::MitoConfig;
use crate::gc::GcConfig;
use crate::sst::file::LEVEL_SHARED;
use crate::test_util::{CreateRequestBuilder, TestEnv, build_rows, put_rows, rows_schema};

/// A tag domain `[start, end)`; `None` for the upper bound means "no upper bound".
///
/// The builder creates `tag_0` as a *string* column, so the split has to be written
/// with string literals. Writing it with `Value::Int64` would not partition anything:
/// the predicate cannot be evaluated against a string column, so both halves would
/// read every row and the "one row per side" premise below would silently be false.
fn str_range_expr(col_name: &str, start: &str, end: Option<&str>) -> PartitionExpr {
    let lower = col(col_name).gt_eq(Value::String(start.into()));
    match end {
        Some(end) => lower.and(col(col_name).lt(Value::String(end.into()))),
        None => lower,
    }
}

/// The file ids a live region's persisted manifest holds.
async fn manifest_files(
    engine: &crate::engine::MitoEngine,
    region_id: RegionId,
) -> HashSet<FileId> {
    let region = engine.get_region(region_id).unwrap();
    region
        .manifest_ctx
        .manifest()
        .await
        .files
        .keys()
        .copied()
        .collect()
}

/// A plain region scan as a sorted multiset of `(tag_0, field_0)`.
///
/// A multiset, not a count and not a sum, is the point: the invariant under test is
/// "every committed write event is visible exactly once", so a duplicated row must
/// show up as an extra element even when its field value is zero (a sum would stay
/// unchanged for the row used here).
async fn scan_rows(engine: &crate::engine::MitoEngine, region_id: RegionId) -> Vec<(String, f64)> {
    let stream = engine
        .scan_to_stream(region_id, ScanRequest::default())
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let mut rows = Vec::new();
    for batch in batches.iter() {
        let tags = batch.column_by_name("tag_0").unwrap().as_string::<i32>();
        let fields = batch
            .column_by_name("field_0")
            .unwrap()
            .as_primitive::<Float64Type>();
        for i in 0..batch.num_rows() {
            rows.push((tags.value(i).to_string(), fields.value(i)));
        }
    }
    // `f64` is not `Ord`, so sort by tag first and then by the field value.
    rows.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.total_cmp(&b.1)));
    common_telemetry::info!(
        "Scan of region {} returned {} rows: {:?}",
        region_id,
        rows.len(),
        rows
    );
    rows
}

/// Copies `manifest_paths[&region_id]` from the central region's staging storage into a
/// freshly created region and exits staging, i.e. what a repartition coordinator does.
async fn apply_staging_manifest(
    engine: &crate::engine::MitoEngine,
    region_id: RegionId,
    partition_expr: &str,
    central_region_id: RegionId,
    manifest_path: &str,
) {
    // The new region must exist and be in staging state before it consumes the layout.
    engine
        .handle_request(
            region_id,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_directive: StagingPartitionDirective::UpdatePartitionExpr(
                    partition_expr.to_string(),
                ),
            }),
        )
        .await
        .unwrap();
    engine
        .handle_request(
            region_id,
            RegionRequest::ApplyStagingManifest(ApplyStagingManifestRequest {
                partition_expr: partition_expr.to_string(),
                central_region_id,
                manifest_path: manifest_path.to_string(),
            }),
        )
        .await
        .unwrap();
}

/// One shared SST, two live holders, one holder rewrites it, then the holders merge:
/// the same logical row is visible twice inside the merged region.
///
/// The gap is entirely in the storage layer, so the assertion is on a plain scan of
/// the merged region (an aggregate over every visible row), no flow/query involved.
#[tokio::test]
async fn test_split_shared_file_single_side_compaction_double_counts_after_merge() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("split-shared-file-compaction").await;
    let engine = env
        .create_engine(MitoConfig {
            // Compaction must only run when the test asks for it.
            schedule_compaction_after_edit: false,
            // Keep retired SSTs for `lingering_time` (the production path) instead of
            // letting the co-holder's purge delete the shared object on the spot.
            gc: GcConfig {
                enable: true,
                ..Default::default()
            },
            ..Default::default()
        })
        .await;

    let source = RegionId::new(1, 1);
    let left = RegionId::new(1, 2);
    let right = RegionId::new(1, 3);
    let merged = RegionId::new(1, 4);

    let whole = str_range_expr("tag_0", "0", None).as_json_str().unwrap();
    let request = CreateRequestBuilder::new()
        // Append-only: the scan returns every visible row instead of deduping the two
        // copies away. The gap is in storage either way; dedup only hides it.
        .insert_option("append_mode", "true")
        .partition_expr_json(Some(whole.clone()))
        .build();
    let schema = rows_schema(&request);
    engine
        .handle_request(source, RegionRequest::Create(request.clone()))
        .await
        .unwrap();

    // Two logical rows, one per side of the split: sum(field_0) == 1, 2 rows.
    put_rows(
        &engine,
        source,
        Rows {
            schema: schema.clone(),
            rows: build_rows(0, 2),
        },
    )
    .await;
    engine
        .handle_request(source, RegionRequest::Flush(RegionFlushRequest::default()))
        .await
        .unwrap();

    // ---- Step 1: split. Both halves' manifests list the same physical SST. ----
    let source_files = manifest_files(&engine, source).await;
    assert_eq!(1, source_files.len(), "the source must flush one SST");
    let shared = *source_files.iter().next().unwrap();

    engine
        .set_region_role_state_gracefully(source, SettableRegionRoleState::StagingLeader)
        .await
        .unwrap();
    let left_expr = str_range_expr("tag_0", "0", Some("1"))
        .as_json_str()
        .unwrap();
    let right_expr = str_range_expr("tag_0", "1", None).as_json_str().unwrap();
    let split = engine
        .remap_manifests(RemapManifestsRequest {
            region_id: source,
            input_regions: vec![source],
            region_mapping: [(source, vec![left, right])].into_iter().collect(),
            new_partition_exprs: [(left, left_expr.clone()), (right, right_expr.clone())]
                .into_iter()
                .collect(),
        })
        .await
        .unwrap();

    for (region_id, expr) in [(left, &left_expr), (right, &right_expr)] {
        engine
            .handle_request(region_id, RegionRequest::Create(request.clone()))
            .await
            .unwrap();
        apply_staging_manifest(
            &engine,
            region_id,
            expr,
            source,
            &split.manifest_paths[&region_id],
        )
        .await;
    }

    let left_files = manifest_files(&engine, left).await;
    let right_files = manifest_files(&engine, right).await;
    assert_eq!(
        HashSet::from([shared]),
        left_files,
        "the left half must hold the split SST"
    );
    assert_eq!(
        left_files, right_files,
        "both live halves must hold the same file_id after the split"
    );

    // One row per side, so this is a real check that the split domain filters rows.
    // `build_rows(0, 2)` writes `tag_0 = "0"` (field 0.0) and `tag_0 = "1"` (field 1.0).
    assert_eq!(
        vec![("0".to_string(), 0.0)],
        scan_rows(&engine, left).await,
        "the left half must read only the row in its own domain"
    );
    assert_eq!(
        vec![("1".to_string(), 1.0)],
        scan_rows(&engine, right).await,
        "the right half must read only the row in its own domain"
    );

    // ---- Step 2: one holder runs region compaction over the shared SST. ----
    // Admission only inspects the level, and the shared file sits at L0, so this
    // is allowed even though the right half still holds the very same file.
    let shared_level = {
        let region = engine.get_region(left).unwrap();
        region.version().ssts.levels()[0].files[&shared].level()
    };
    assert!(
        shared_level < LEVEL_SHARED,
        "the split keeps the original level, so the level-only admission check passes"
    );
    engine
        .handle_request(
            left,
            RegionRequest::Compact(RegionCompactRequest {
                options: compact_request::Options::StrictWindow(StrictWindow {
                    window_seconds: 60,
                }),
                parallelism: None,
                time_range: None,
            }),
        )
        .await
        .unwrap();

    let left_after = manifest_files(&engine, left).await;
    assert_eq!(1, left_after.len(), "the rewrite replaces the input SST");
    assert!(
        !left_after.contains(&shared),
        "the left half replaced the shared file with its own copy"
    );
    let copy = *left_after.iter().next().unwrap();
    assert_ne!(shared, copy, "the copy is a new file_id");
    assert_eq!(
        right_files,
        manifest_files(&engine, right).await,
        "the other holder is untouched by its co-holder's compaction"
    );

    // ---- Step 3: merge both halves back into one region. ----
    engine
        .handle_request(merged, RegionRequest::Create(request.clone()))
        .await
        .unwrap();
    apply_staging_manifest_prepare(&engine, merged, &whole).await;
    let merge = engine
        .remap_manifests(RemapManifestsRequest {
            region_id: merged,
            input_regions: vec![left, right],
            region_mapping: [(left, vec![merged]), (right, vec![merged])]
                .into_iter()
                .collect(),
            new_partition_exprs: [(merged, whole.clone())].into_iter().collect(),
        })
        .await
        .unwrap();
    engine
        .handle_request(
            merged,
            RegionRequest::ApplyStagingManifest(ApplyStagingManifestRequest {
                partition_expr: whole.clone(),
                central_region_id: merged,
                manifest_path: merge.manifest_paths[&merged].clone(),
            }),
        )
        .await
        .unwrap();

    let merged_files = manifest_files(&engine, merged).await;
    assert_eq!(
        HashSet::from([shared, copy]),
        merged_files,
        "the merge only dedups by file_id, so the original and the copy both survive"
    );

    // ---- Step 4: a plain read of the merged region. ----
    let expected = vec![("0".to_string(), 0.0), ("1".to_string(), 1.0)];
    let observed = scan_rows(&engine, merged).await;
    assert_eq!(
        expected,
        observed,
        "each logical row must be visible exactly once, but the merged region holds both \
         the original SST and the single-side compaction's copy, so one logical row is \
         visible twice (original + materialized copy): got {} rows {:?}. \
         同一逻辑行在 merge 后的 region 内可见两次（原件 + 单侧 compaction 的实体副本）。",
        observed.len(),
        observed
    );
}

/// Puts a freshly created region into the staging state a repartition needs when it is
/// the region that owns the staging storage (the merge's central region).
async fn apply_staging_manifest_prepare(
    engine: &crate::engine::MitoEngine,
    region_id: RegionId,
    partition_expr: &str,
) {
    engine
        .handle_request(
            region_id,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_directive: StagingPartitionDirective::UpdatePartitionExpr(
                    partition_expr.to_string(),
                ),
            }),
        )
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// Two-generation regression: split -> compact -> merge -> split -> compact -> merge
//
// The single-generation case above is not enough to pin the invariant. A merge hands
// the surviving references to a fresh region, that region repartitions again, and one
// of *its* halves materializes a reference into a new physical file once more. Each
// hop rewrites `owned_range` (intersections on a split, unions on a merge), so a
// second generation can lose the authorization that kept a row single-counted even
// when the first one preserves it.
// ---------------------------------------------------------------------------

/// Which half of the second generation the scenario rewrites.
#[derive(Clone, Copy, Debug)]
enum Gen2Compaction {
    /// Rewrites the half that holds generation 1's materialized copy.
    CompactedHalf,
    /// Rewrites the half that still holds the shared original.
    OriginalHalf,
}

/// One persisted manifest entry, reduced to what a reference-overlap check needs.
#[derive(Debug, Clone)]
struct ManifestEntry {
    file_id: FileId,
    /// The region that created the physical file, not the region currently holding it.
    creator: RegionId,
    level: u8,
    /// `None` means "every physical row".
    owned_range: Option<PartitionExpr>,
}

impl ManifestEntry {
    /// `file_id / creator / level / owned_range` on one line, for failure messages.
    fn describe(&self) -> String {
        format!(
            "file_id={}, creator={}, level={}, owned_range={}",
            self.file_id,
            self.creator,
            self.level,
            self.owned_range
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "None (every physical row)".to_string()),
        )
    }
}

/// The persisted manifest of a live region, sorted by file id.
async fn manifest_entries(
    engine: &crate::engine::MitoEngine,
    region_id: RegionId,
) -> Vec<ManifestEntry> {
    let region = engine.get_region(region_id).unwrap();
    let manifest = region.manifest_ctx.manifest().await;
    let mut entries: Vec<ManifestEntry> = manifest
        .files
        .values()
        .map(|meta| ManifestEntry {
            file_id: meta.file_id,
            creator: meta.region_id,
            level: meta.level,
            owned_range: meta.owned_range.as_deref().cloned(),
        })
        .collect();
    entries.sort_by_key(|entry| entry.file_id);
    for entry in &entries {
        common_telemetry::info!(
            "Manifest entry of region {}: {}",
            region_id,
            entry.describe()
        );
    }
    entries
}

/// Whether the row `tag_0 = tag` is inside `expr`.
///
/// Every ownership range this test creates is a boolean combination of `tag_0`
/// comparisons with string literals, so a direct walk of the tree is exact. An
/// unexpected shape panics instead of guessing: a silently "admitting" fallback
/// would turn the overlap check below into a no-op.
fn admits_tag(expr: &PartitionExpr, tag: &str) -> bool {
    match (&*expr.lhs, &expr.op, &*expr.rhs) {
        (Operand::Expr(left), RestrictedOp::And, Operand::Expr(right)) => {
            admits_tag(left, tag) && admits_tag(right, tag)
        }
        (Operand::Expr(left), RestrictedOp::Or, Operand::Expr(right)) => {
            admits_tag(left, tag) || admits_tag(right, tag)
        }
        (Operand::Column(column), op, Operand::Value(Value::String(bound))) => {
            assert_eq!("tag_0", column, "the scenario only splits on tag_0");
            let bound = bound.as_utf8();
            match op {
                RestrictedOp::Eq => tag == bound,
                RestrictedOp::NotEq => tag != bound,
                RestrictedOp::Lt => tag < bound,
                RestrictedOp::LtEq => tag <= bound,
                RestrictedOp::Gt => tag > bound,
                RestrictedOp::GtEq => tag >= bound,
                other => panic!("unsupported comparison {other} in {expr}"),
            }
        }
        _ => panic!("unsupported ownership expression {expr}"),
    }
}

/// The `tag_0` values from `universe` this entry may read.
fn authorized_tags(entry: &ManifestEntry, universe: &[String]) -> Vec<String> {
    universe
        .iter()
        .filter(|tag| match &entry.owned_range {
            Some(expr) => admits_tag(expr, tag.as_str()),
            None => true,
        })
        .map(|tag| String::from(tag.as_str()))
        .collect()
}

/// The `tag_0` values of a scan result.
fn tags(rows: &[(String, f64)]) -> Vec<String> {
    rows.iter().map(|(tag, _)| tag.clone()).collect()
}

/// Fails if two entries of one manifest can both serve the same physical row.
///
/// Two references to the same `file_id` are the same entry, so the check is about
/// distinct files. Comparing `owned_range`s alone is not enough: a range only
/// authorizes rows and says nothing about what a file contains, and after a rewrite a
/// holder may legitimately authorize a wider domain than its file physically carries
/// (`tag_0 >= "1"` over a file that also holds `tag_0 = "0"` is not a duplicate). So
/// each entry's authorized tags are intersected with the tags its file physically
/// holds, tracked by [`run_two_generations`] as the files are written: a pair may
/// overlap only if one row is both physically present in both files and authorized
/// through both references.
fn assert_no_overlapping_references(
    region_id: RegionId,
    entries: &[ManifestEntry],
    physical_rows: &HashMap<FileId, Vec<String>>,
    universe: &[String],
) {
    let described: Vec<String> = entries.iter().map(ManifestEntry::describe).collect();
    for (index, left) in entries.iter().enumerate() {
        let left_physical = physical_rows.get(&left.file_id).unwrap_or_else(|| {
            panic!(
                "no recorded rows for the file of {} in region {}, the scenario must track \
                 every file it writes; entries: {described:?}",
                left.describe(),
                region_id,
            )
        });
        let left_tags = authorized_tags(left, universe);
        for right in &entries[index + 1..] {
            if left.file_id == right.file_id {
                continue;
            }
            let right_physical = physical_rows.get(&right.file_id).unwrap_or_else(|| {
                panic!(
                    "no recorded rows for the file of {} in region {}, the scenario must track \
                     every file it writes; entries: {described:?}",
                    right.describe(),
                    region_id,
                )
            });
            let right_tags = authorized_tags(right, universe);
            let overlapping: Vec<String> = universe
                .iter()
                .filter(|tag| {
                    left_tags.contains(*tag)
                        && left_physical.contains(*tag)
                        && right_tags.contains(*tag)
                        && right_physical.contains(*tag)
                })
                .map(|tag| String::from(tag.as_str()))
                .collect();
            assert!(
                overlapping.is_empty(),
                "region {} holds two references that both serve rows {overlapping:?}: \
                 [{}] (rows {left_physical:?}) and [{}] (rows {right_physical:?}). A row \
                 authorized through two references is read twice; entries: {described:?}",
                region_id,
                left.describe(),
                right.describe(),
            );
        }
    }
}

/// Runs a region compaction exactly when the test asks for it.
async fn compact_region(engine: &crate::engine::MitoEngine, region_id: RegionId) {
    engine
        .handle_request(
            region_id,
            RegionRequest::Compact(RegionCompactRequest {
                options: compact_request::Options::StrictWindow(StrictWindow {
                    window_seconds: 60,
                }),
                parallelism: None,
                time_range: None,
            }),
        )
        .await
        .unwrap();
}

/// Splits `from` into `left`/`right` over the `"0"` / `"1"` cut: the coordinator's half
/// of a repartition (staging role, remap, create each half, install its manifest).
///
/// Both halves keep the same physical SSTs; only their `owned_range`s are narrowed.
async fn split_region(
    engine: &crate::engine::MitoEngine,
    from: RegionId,
    left: RegionId,
    right: RegionId,
    create: &RegionCreateRequest,
    left_expr: &str,
    right_expr: &str,
) {
    // A no-op when `from` is already a staging leader (the second generation's source
    // is a merged region that never left staging).
    engine
        .set_region_role_state_gracefully(from, SettableRegionRoleState::StagingLeader)
        .await
        .unwrap();
    let split = engine
        .remap_manifests(RemapManifestsRequest {
            region_id: from,
            input_regions: vec![from],
            region_mapping: [(from, vec![left, right])].into_iter().collect(),
            new_partition_exprs: [
                (left, left_expr.to_string()),
                (right, right_expr.to_string()),
            ]
            .into_iter()
            .collect(),
        })
        .await
        .unwrap();
    for (region_id, expr) in [(left, left_expr), (right, right_expr)] {
        engine
            .handle_request(region_id, RegionRequest::Create(create.clone()))
            .await
            .unwrap();
        apply_staging_manifest(
            engine,
            region_id,
            expr,
            from,
            &split.manifest_paths[&region_id],
        )
        .await;
    }
}

/// Merges `sources` into the fresh region `target`: the coordinator's half of a merge,
/// i.e. a remap that maps every holder onto one whole-domain region.
async fn merge_regions(
    engine: &crate::engine::MitoEngine,
    target: RegionId,
    sources: [RegionId; 2],
    whole_expr: &str,
    create: &RegionCreateRequest,
) {
    engine
        .handle_request(target, RegionRequest::Create(create.clone()))
        .await
        .unwrap();
    apply_staging_manifest_prepare(engine, target, whole_expr).await;
    let merge = engine
        .remap_manifests(RemapManifestsRequest {
            region_id: target,
            input_regions: sources.to_vec(),
            region_mapping: sources
                .iter()
                .map(|source| (*source, vec![target]))
                .collect(),
            new_partition_exprs: [(target, whole_expr.to_string())].into_iter().collect(),
        })
        .await
        .unwrap();
    engine
        .handle_request(
            target,
            RegionRequest::ApplyStagingManifest(ApplyStagingManifestRequest {
                partition_expr: whole_expr.to_string(),
                central_region_id: target,
                manifest_path: merge.manifest_paths[&target].clone(),
            }),
        )
        .await
        .unwrap();
}

/// Two full generations over the same schema and cut: the first one is the existing
/// regression, the second one repeats it on the merged region.
///
/// One generation is not enough: the merge publishes references whose `owned_range`s
/// the next split narrows again, and a rewrite inside that narrower second generation
/// materializes a row for the second time. The rows are asserted as a multiset after
/// every merge, and the final manifest is checked for two references that both serve
/// the same row.
async fn run_two_generations(case: Gen2Compaction, suffix: &str) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix(&format!("split-shared-file-gen2-{suffix}")).await;
    let engine = env
        .create_engine(MitoConfig {
            // Compaction must only run when the scenario asks for it.
            schedule_compaction_after_edit: false,
            gc: GcConfig {
                enable: true,
                ..Default::default()
            },
            ..Default::default()
        })
        .await;

    let source = RegionId::new(1, 1);
    let left = RegionId::new(1, 2);
    let right = RegionId::new(1, 3);
    let gen1 = RegionId::new(1, 4);
    let left2 = RegionId::new(1, 5);
    let right2 = RegionId::new(1, 6);
    let gen2 = RegionId::new(1, 7);

    let whole = str_range_expr("tag_0", "0", None).as_json_str().unwrap();
    let left_expr = str_range_expr("tag_0", "0", Some("1"))
        .as_json_str()
        .unwrap();
    let right_expr = str_range_expr("tag_0", "1", None).as_json_str().unwrap();
    let request = CreateRequestBuilder::new()
        .insert_option("append_mode", "true")
        .partition_expr_json(Some(whole.clone()))
        .build();

    // `build_rows(0, 2)` writes `tag_0 = "0"` (field 0.0) and `tag_0 = "1"` (field 1.0).
    let expected = vec![("0".to_string(), 0.0), ("1".to_string(), 1.0)];
    let universe = vec!["0".to_string(), "1".to_string()];

    // The `tag_0` values each physical file contains. A range only authorizes rows, so
    // proving that two references cannot serve the same row needs the row sets too;
    // every file this scenario writes is recorded with the rows its writer read.
    let mut physical_rows: HashMap<FileId, Vec<String>> = HashMap::new();

    // ---- Setup: two rows in one flushed SST. ----
    let schema = rows_schema(&request);
    engine
        .handle_request(source, RegionRequest::Create(request.clone()))
        .await
        .unwrap();
    put_rows(
        &engine,
        source,
        Rows {
            schema,
            rows: build_rows(0, 2),
        },
    )
    .await;
    engine
        .handle_request(source, RegionRequest::Flush(RegionFlushRequest::default()))
        .await
        .unwrap();
    let source_files = manifest_files(&engine, source).await;
    assert_eq!(1, source_files.len(), "the source must flush one SST");
    let shared = *source_files.iter().next().unwrap();
    physical_rows.insert(shared, tags(&scan_rows(&engine, source).await));

    // ---- Generation 0: split, rewrite the left half, merge into `gen1`. ----
    split_region(
        &engine,
        source,
        left,
        right,
        &request,
        &left_expr,
        &right_expr,
    )
    .await;
    assert_eq!(
        vec![("0".to_string(), 0.0)],
        scan_rows(&engine, left).await,
        "the left half must read only the row in its own domain"
    );
    assert_eq!(
        vec![("1".to_string(), 1.0)],
        scan_rows(&engine, right).await,
        "the right half must read only the row in its own domain"
    );

    compact_region(&engine, left).await;
    let left_files = manifest_files(&engine, left).await;
    assert_eq!(1, left_files.len(), "the rewrite replaces the input SST");
    let copy = *left_files.iter().next().unwrap();
    assert_ne!(shared, copy, "the copy is a new file_id");
    assert_eq!(
        HashSet::from([shared]),
        manifest_files(&engine, right).await,
        "the survivor state is: the left half holds the compacted copy, the right half \
         holds the shared original"
    );
    physical_rows.insert(copy, tags(&scan_rows(&engine, left).await));

    merge_regions(&engine, gen1, [left, right], &whole, &request).await;
    assert_eq!(
        expected,
        scan_rows(&engine, gen1).await,
        "generation 1: split + single-side compaction + merge must expose each row once"
    );

    // ---- Generation 1: split the merged region again over the same cut. ----
    split_region(
        &engine,
        gen1,
        left2,
        right2,
        &request,
        &left_expr,
        &right_expr,
    )
    .await;
    assert_eq!(
        vec![("0".to_string(), 0.0)],
        scan_rows(&engine, left2).await,
        "the second generation's left half must read only the row in its own domain"
    );
    assert_eq!(
        vec![("1".to_string(), 1.0)],
        scan_rows(&engine, right2).await,
        "the second generation's right half must read only the row in its own domain"
    );

    // ---- Generation 2: rewrite one half, then merge into `gen2`. ----
    let victim = match case {
        Gen2Compaction::CompactedHalf => left2,
        Gen2Compaction::OriginalHalf => right2,
    };
    let before_rewrite = scan_rows(&engine, victim).await;
    let victim_files = manifest_files(&engine, victim).await;
    assert_eq!(
        2,
        victim_files.len(),
        "{case:?}: the half must hold the shared original and generation 1's copy"
    );
    compact_region(&engine, victim).await;
    let victim_after = manifest_files(&engine, victim).await;
    assert_eq!(
        1,
        victim_after.len(),
        "{case:?}: the rewrite replaces the half's references with one file"
    );
    let rewritten = *victim_after.iter().next().unwrap();
    assert!(
        !victim_files.contains(&rewritten),
        "{case:?}: the rewrite produces a new file_id"
    );
    physical_rows.insert(rewritten, tags(&before_rewrite));
    assert_eq!(
        before_rewrite,
        scan_rows(&engine, victim).await,
        "{case:?}: the rewrite must keep the half's own row"
    );

    merge_regions(&engine, gen2, [left2, right2], &whole, &request).await;
    let observed = scan_rows(&engine, gen2).await;
    let entries = manifest_entries(&engine, gen2).await;
    assert_eq!(
        expected,
        observed,
        "{case:?}: generation 2 must expose each row exactly once, got {} rows {observed:?}; \
         manifest entries: {:?}",
        observed.len(),
        entries
            .iter()
            .map(ManifestEntry::describe)
            .collect::<Vec<_>>()
    );
    assert_no_overlapping_references(gen2, &entries, &physical_rows, &universe);
}

/// A second generation repeats split -> one-side compaction -> merge on the region the
/// first generation produced. Both choices of the rewritten half are covered: the one
/// holding generation 1's materialized copy, and the one holding the shared original.
#[tokio::test]
async fn test_two_generation_split_compaction_merge_does_not_duplicate() {
    run_two_generations(Gen2Compaction::CompactedHalf, "compacted-half").await;
    run_two_generations(Gen2Compaction::OriginalHalf, "original-half").await;
}
