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

use chrono::Utc;
use common_telemetry::{debug, info};
use partition::expr::{Operand, PartitionExpr, RestrictedOp};
use store_api::logstore::LogStore;
use store_api::region_request::ApplyStagingManifestRequest;
use store_api::storage::{FileId, RegionId};
use tokio::sync::oneshot;

use crate::error::{
    InvalidRequestSnafu, RegionStateSnafu, Result, StagingPartitionExprMismatchSnafu,
    UnexpectedSnafu,
};
use crate::manifest::action::{RegionEdit, RegionManifest};
use crate::manifest::storage::manifest_dir;
use crate::manifest::storage::staging::{StagingBlobStorage, staging_blob_path};
use crate::region::{MitoRegionRef, RegionLeaderState, RegionRoleState, parse_partition_expr};
use crate::request::{OptionOutputTx, RegionEditRequest, WorkerRequest, WorkerRequestWithTime};
use crate::sst::location::region_dir_from_table_dir;
use crate::worker::RegionWorkerLoop;
use crate::worker::drain_ended_staging;
use crate::worker::handle_reload::notify_if_fenced;

impl<S: LogStore> RegionWorkerLoop<S> {
    pub(crate) async fn handle_apply_staging_manifest_request(
        &mut self,
        region_id: RegionId,
        request: ApplyStagingManifestRequest,
        sender: OptionOutputTx,
    ) {
        let region = match self.regions.writable_region(region_id) {
            Ok(region) => region,
            Err(e) => {
                sender.send(Err(e));
                return;
            }
        };

        if !region.is_staging() {
            let manifest_partition_expr = region.metadata().partition_expr.as_ref().cloned();
            let is_match = manifest_partition_expr.as_ref() == Some(&request.partition_expr);
            debug!(
                "region {} manifest partition expr: {:?}, request partition expr: {:?}",
                region_id, manifest_partition_expr, request.partition_expr
            );
            if is_match {
                // If current partition expr is already the same as the request,
                // treats the region already applied the staging manifest.
                info!(
                    "Region {} already applied the staging manifest, partition expr: {}, ignore the apply staging manifest request",
                    region_id, request.partition_expr
                );
                sender.send(Ok(0));
                return;
            }

            sender.send(
                RegionStateSnafu {
                    region_id,
                    state: region.state(),
                    expect: RegionRoleState::Leader(RegionLeaderState::Staging),
                }
                .fail(),
            );
            return;
        }

        let staging_partition_info = region.manifest_ctx.staging_partition_info();

        let staging_partition_expr = staging_partition_info
            .as_ref()
            .and_then(|info| info.partition_expr());
        // If the partition expr mismatch, return error.
        if staging_partition_expr != Some(request.partition_expr.as_str()) {
            sender.send(
                StagingPartitionExprMismatchSnafu {
                    manifest_expr: staging_partition_info
                        .as_ref()
                        .and_then(|info| info.partition_expr().map(ToString::to_string)),
                    request_expr: request.partition_expr,
                }
                .fail(),
            );
            return;
        }

        let worker_sender = self.sender.clone();
        common_runtime::spawn_global(async move {
            let staging_manifest = match Self::fetch_staging_manifest(
                &region,
                request.central_region_id,
                &request.manifest_path,
            )
            .await
            {
                Ok(staging_manifest) => staging_manifest,
                Err(e) => {
                    sender.send(Err(e));
                    return;
                }
            };
            if staging_manifest.metadata.partition_expr.as_ref() != Some(&request.partition_expr) {
                sender.send(Err(StagingPartitionExprMismatchSnafu {
                    manifest_expr: staging_manifest.metadata.partition_expr.clone(),
                    request_expr: request.partition_expr,
                }
                .build()));
                return;
            }

            // `ApplyStagingManifestRequest` names the target partition expression
            // the remap used, and a remap conjoins that expression into every entry
            // it produces. The manifest itself is supplied by the request, so each
            // entry is checked against that expression here: the ordinary-edit guard
            // never sees a whole manifest installed through this path.
            let target_domain = match parse_partition_expr(Some(request.partition_expr.as_str())) {
                Ok(Some(target_domain)) => target_domain,
                Ok(None) => {
                    sender.send(Err(InvalidRequestSnafu {
                        region_id,
                        reason: "apply staging manifest request carries no target partition expression",
                    }
                    .build()));
                    return;
                }
                Err(e) => {
                    sender.send(Err(e));
                    return;
                }
            };
            for (file_id, incoming) in &staging_manifest.files {
                if let Err(e) = check_remap_owned_range(
                    region.region_id,
                    *file_id,
                    incoming.owned_range.as_deref(),
                    &target_domain,
                ) {
                    sender.send(Err(e));
                    return;
                }
            }

            let files_to_add = staging_manifest
                .files
                .values()
                .map(|meta| meta.as_ref().clone())
                .collect::<Vec<_>>();
            let edit = RegionEdit {
                series_indexes_to_add: staging_manifest
                    .series_indexes
                    .values()
                    .map(|index| index.as_ref().clone())
                    .collect(),
                files_to_add,
                files_to_remove: vec![],
                timestamp_ms: Some(Utc::now().timestamp_millis()),
                compaction_time_window: None,
                flushed_entry_id: None,
                flushed_sequence: None,
                committed_sequence: None,
                ..Default::default()
            };

            let (tx, rx) = oneshot::channel();
            info!(
                "Applying staging manifest request to region {}",
                region.region_id,
            );
            let _ = worker_sender
                .send(WorkerRequestWithTime::new(WorkerRequest::EditRegion(
                    RegionEditRequest::new(
                        region_id, edit,
                        // we don't need to preload sst cache during repartition, as it may cause extra network overhead.
                        false, tx,
                    )
                    .for_staging(request.partition_expr),
                )))
                .await;

            // Await the result from the region edit and forward the outcome to the original sender.
            // If the operation completes successfully, respond with Ok(0); otherwise, respond with an appropriate error.
            if let Ok(result) = rx.await {
                let Ok(()) = result else {
                    sender.send(result.map(|_| 0));
                    return;
                };
                let manager = region.manifest_ctx.manifest_manager.write().await;
                let drain = drain_ended_staging(&worker_sender, region_id);
                let hook_payload = match region.end_staging(manager, drain).await {
                    Ok(payload) => payload,
                    Err(e) => {
                        notify_if_fenced(&worker_sender, &region).await;
                        sender.send(Err(e));
                        return;
                    }
                };
                if let Some(pending) = hook_payload {
                    pending.fire().await;
                }
                sender.send(Ok(0));
            } else {
                sender.send(
                    UnexpectedSnafu {
                        reason: "edit region receiver channel closed",
                    }
                    .fail(),
                );
            }
        });
    }

    /// Fetches the staging manifest from the central region's staging blob storage.
    ///
    /// The `central_region_id` is used to locate the staging directory because the staging
    /// manifest was created by the central region during `remap_manifests` operation.
    async fn fetch_staging_manifest(
        region: &MitoRegionRef,
        central_region_id: RegionId,
        manifest_path: &str,
    ) -> Result<RegionManifest> {
        let region_dir =
            region_dir_from_table_dir(region.table_dir(), central_region_id, region.path_type());
        let staging_blob_path = staging_blob_path(&manifest_dir(&region_dir));
        let staging_blob_storage = StagingBlobStorage::new(
            staging_blob_path,
            region.access_layer().object_store().clone(),
        );
        let staging_manifest = staging_blob_storage.get(manifest_path).await?;

        RegionManifest::decode(&staging_manifest)
    }
}

/// Upper bound on the disjuncts an installed range may expand to while its
/// containment is proved. The manifest decoder already bounds the input depth;
/// this bounds the width, so a pathological request cannot be expanded.
const MAX_INSTALL_RANGE_DISJUNCTS: usize = 1024;

/// Checks that one entry a staging manifest installs is what a remap produces
/// for this target.
///
/// A remap creates each entry by conjoining the target partition expression, so
/// two things hold of every legitimate entry: it carries a range (the target
/// expression is always one of its operands, and the request always names one),
/// and that range admits no row the target expression excludes. A merge is
/// legitimate even though it *widens* the surviving region: two holders with
/// disjoint halves union into the target's own range, which is wider than what
/// either holder had. This therefore checks the entry against the target
/// expression, never against the range the region already holds.
fn check_remap_owned_range(
    region_id: RegionId,
    file_id: FileId,
    owned_range: Option<&PartitionExpr>,
    target_domain: &PartitionExpr,
) -> Result<()> {
    let Some(owned_range) = owned_range else {
        return InvalidRequestSnafu {
            region_id,
            reason: format!(
                "staging manifest installs file {file_id:?} without an owned range, but a remap \
                 entry is always restricted by the target partition expression {}",
                describe_range(Some(target_domain)),
            ),
        }
        .fail();
    };

    if implies(owned_range, target_domain) {
        return Ok(());
    }

    InvalidRequestSnafu {
        region_id,
        reason: format!(
            "staging manifest installs file {file_id:?} with owned range {}, which does not imply \
             the target partition expression {}; an installed reference may not authorize rows \
             the target does not own",
            describe_range(Some(owned_range)),
            describe_range(Some(target_domain)),
        ),
    }
    .fail()
}

/// Whether `narrower` provably admits no row that `broader` excludes.
///
/// Both sides are rewritten into disjuncts of conjuncts, which is exact (`AND`
/// and `OR` are associative and distributive), and every disjunct of `narrower`
/// must be covered by a disjunct of `broader`, i.e. contain all of its conjuncts.
/// The comparison is on exact serialized keys, so `TIME 1 second` and
/// `TIME 1000 milliseconds` stay distinct. A shape that cannot be expanded within
/// [MAX_INSTALL_RANGE_DISJUNCTS] is not proven, so the caller fails closed.
fn implies(narrower: &PartitionExpr, broader: &PartitionExpr) -> bool {
    if disjunct_count(narrower) > MAX_INSTALL_RANGE_DISJUNCTS
        || disjunct_count(broader) > MAX_INSTALL_RANGE_DISJUNCTS
    {
        return false;
    }

    let (mut narrower_terms, mut broader_terms) = (Vec::new(), Vec::new());
    if !collect_disjuncts(narrower, &mut narrower_terms)
        || !collect_disjuncts(broader, &mut broader_terms)
    {
        return false;
    }

    narrower_terms.iter().all(|narrowed| {
        broader_terms
            .iter()
            .any(|broader| broader.iter().all(|key| narrowed.contains(key)))
    })
}

/// Number of disjuncts `expr` expands to, saturated.
///
/// Computed on the tree so a tiny input whose expansion is astronomical is
/// refused before anything is allocated.
fn disjunct_count(expr: &PartitionExpr) -> usize {
    match (expr.op(), expr.lhs(), expr.rhs()) {
        (RestrictedOp::Or, Operand::Expr(lhs), Operand::Expr(rhs)) => {
            disjunct_count(lhs).saturating_add(disjunct_count(rhs))
        }
        (RestrictedOp::And, Operand::Expr(lhs), Operand::Expr(rhs)) => {
            disjunct_count(lhs).saturating_mul(disjunct_count(rhs))
        }
        _ => 1,
    }
}

/// Appends every disjunct of `expr`, each as exact serialized conjunct keys.
fn collect_disjuncts(expr: &PartitionExpr, out: &mut Vec<Vec<String>>) -> bool {
    match (expr.op(), expr.lhs(), expr.rhs()) {
        (RestrictedOp::Or, Operand::Expr(lhs), Operand::Expr(rhs)) => {
            collect_disjuncts(lhs, out) && collect_disjuncts(rhs, out)
        }
        (RestrictedOp::And, Operand::Expr(lhs), Operand::Expr(rhs)) => {
            let (mut left, mut right) = (Vec::new(), Vec::new());
            if !collect_disjuncts(lhs, &mut left) || !collect_disjuncts(rhs, &mut right) {
                return false;
            }
            for left in &left {
                for right in &right {
                    let mut term = left.clone();
                    term.extend(right.iter().cloned());
                    out.push(term);
                }
            }
            true
        }
        _ => match range_key(expr) {
            Some(key) => {
                out.push(vec![key]);
                true
            }
            None => false,
        },
    }
}

/// Exact identity of one predicate: the serialized form, which keeps the
/// literal's physical type. `PartitionExpr`'s own equality cannot tell
/// `TIME 1 second` from `TIME 1000 milliseconds` apart. Canonicalizing first
/// matches what the decoder produces, so a range this node built and the same
/// range read back from storage compare equal.
fn range_key(expr: &PartitionExpr) -> Option<String> {
    expr.clone().canonicalize().as_json_str().ok()
}

/// Renders a range for an error message.
fn describe_range(range: Option<&PartitionExpr>) -> String {
    match range {
        None => "every physical row (unrestricted)".to_string(),
        Some(expr) => range_key(expr).unwrap_or_else(|| expr.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use datatypes::value::Value;
    use partition::expr::col;

    use super::*;
    use crate::error::Error;

    /// `tag >= start AND tag < end`.
    fn range(start: i64, end: i64) -> PartitionExpr {
        col("tag")
            .gt_eq(Value::Int64(start))
            .and(col("tag").lt(Value::Int64(end)))
    }

    fn or(left: PartitionExpr, right: PartitionExpr) -> PartitionExpr {
        PartitionExpr::new(Operand::Expr(left), RestrictedOp::Or, Operand::Expr(right))
    }

    /// A remap always conjoins the target expression, so an entry without a range
    /// did not come from one: installing it would authorize every physical row.
    #[test]
    fn test_remap_install_without_an_owned_range_is_refused() {
        let region_id = RegionId::new(1, 1);
        let file_id = FileId::random();
        let target = range(0, 100);
        let error = check_remap_owned_range(region_id, file_id, None, &target)
            .expect_err("an entry without a range is not a remap result");
        let Error::InvalidRequest { reason, .. } = error else {
            panic!("unexpected error: {error}");
        };
        assert!(reason.contains(&format!("{file_id:?}")), "{reason}");
        assert!(reason.contains("without an owned range"), "{reason}");
    }

    /// The installed range may never authorize a row the target does not own.
    #[test]
    fn test_remap_install_range_must_imply_the_target_expression() {
        let region_id = RegionId::new(1, 1);
        let file_id = FileId::random();
        let target = range(0, 100);

        // An identical range and an added conjunct both imply the target.
        check_remap_owned_range(region_id, file_id, Some(&target), &target)
            .expect("the target's own range implies it");
        let narrowed = target.clone().and(col("tag").lt(Value::Int64(50)));
        assert!(implies(&narrowed, &target), "adding a conjunct narrows");

        // A range that reaches past the target does not.
        let wider = range(0, 200);
        assert!(!implies(&wider, &target));
        let error = check_remap_owned_range(region_id, file_id, Some(&wider), &target)
            .expect_err("a range outside the target must be refused");
        let Error::InvalidRequest { reason, .. } = error else {
            panic!("unexpected error: {error}");
        };
        assert!(reason.contains("does not imply the target"), "{reason}");
        assert!(reason.contains(&format!("{file_id:?}")), "{reason}");

        // A disjoint range does not either.
        assert!(!implies(&range(100, 200), &target));
    }

    /// Merging two holders widens the survivor, which is a legitimate remap
    /// result: each holder's contribution is restricted by the target expression,
    /// and only their union reaches the target's full range.
    #[test]
    fn test_remap_install_accepts_a_union_of_holders_widening_the_survivor() {
        let region_id = RegionId::new(1, 1);
        let file_id = FileId::random();
        let target = range(0, 100);

        // Survivor at `[0, 50)` and the other holder at `[50, 100)`, as a remap
        // publishes them: the union of two conjunctions, each containing the target.
        let left = range(0, 50).and(target.clone());
        let right = range(50, 100).and(target.clone());
        let union = or(left.clone(), right.clone());
        // The union is wider than the survivor's own range, yet it implies the
        // target, so the install is admitted.
        assert!(
            implies(&left, &union),
            "the survivor's range is part of the union"
        );
        assert!(
            implies(&union, &target),
            "every holder is inside the target"
        );
        check_remap_owned_range(region_id, file_id, Some(&union), &target)
            .expect("a merge may widen the surviving reference");

        // A distributed union of the same contributions is the same range.
        let distributed =
            PartitionExpr::new(Operand::Expr(left), RestrictedOp::Or, Operand::Expr(right));
        assert!(implies(&distributed, &target));
        check_remap_owned_range(region_id, file_id, Some(&distributed), &target)
            .expect("a distributed union keeps implying the target");
    }

    /// An expansion that cannot be bounded is refused instead of attempted.
    #[test]
    fn test_remap_install_refuses_an_unbounded_expansion() {
        // Four disjoint ranges intersected twelve times: 4^12 disjuncts, a count
        // computed on the tree and never allocated.
        let quarter = |step: i64| {
            let mut disjunction = range(step, step + 1);
            for offset in 1..4 {
                let bound = step + offset;
                disjunction = or(disjunction, range(bound, bound + 1));
            }
            disjunction
        };
        let mut expr = quarter(0);
        for step in 1..12 {
            expr = expr.and(quarter(step * 10));
        }
        assert!(disjunct_count(&expr) > MAX_INSTALL_RANGE_DISJUNCTS);
        assert!(!implies(&expr, &expr));
    }
}
