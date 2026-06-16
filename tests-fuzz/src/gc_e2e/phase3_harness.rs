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

use std::collections::{BTreeSet, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use common_meta::key::TableMetadataManagerRef;
use common_meta::key::table_repart::TableRepartValue;
use common_procedure::{Procedure, ProcedureWithId, Status, watcher};
use common_procedure_test::new_test_procedure_context;
use common_telemetry::info;
use common_test_util::temp_dir::create_temp_dir;
use common_wal::config::DatanodeWalConfig;
use meta_srv::gc::{BatchGcProcedure, GcSchedulerOptions, Region2Peers};
use mito2::gc::GcConfig;
use mito2::sst::location::parse_file_id_type_from_path;
use store_api::path_utils::{region_dir, region_name};
use store_api::storage::{FileId, FileRef, FileRefsManifest, GcReport, RegionId};
use tests_integration::cluster::{GreptimeDbCluster, GreptimeDbClusterBuilder};
use tests_integration::test_util::{StorageType, execute_sql, get_test_store_config};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase3E2eTableShape {
    SingleRegion,
    MultiRegion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase3E2eScenarioKind {
    CompactGc,
    RepartitionLike,
    FollowerLike,
    RepartitionPreGcProtection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase3GcOutcomeKind {
    DeletedFiles,
    NoOpSuccess,
    NeedRetry,
}

impl Phase3GcOutcomeKind {
    fn as_evidence_value(&self) -> &'static str {
        match self {
            Self::DeletedFiles => "deleted_files",
            Self::NoOpSuccess => "no_op_success",
            Self::NeedRetry => "need_retry",
        }
    }
}

impl Phase3E2eScenarioKind {
    fn as_seed_value(&self) -> &'static str {
        match self {
            Self::CompactGc => "compact_gc",
            Self::RepartitionLike => "repartition_like",
            Self::FollowerLike => "follower_like",
            Self::RepartitionPreGcProtection => "repartition_pre_gc_protection",
        }
    }

    fn from_seed_value(value: &str) -> Result<Self, String> {
        match value {
            "compact_gc" => Ok(Self::CompactGc),
            "repartition_like" => Ok(Self::RepartitionLike),
            "follower_like" => Ok(Self::FollowerLike),
            "repartition_pre_gc_protection" => Ok(Self::RepartitionPreGcProtection),
            other => Err(format!("unknown phase3 scenario_kind: {other}")),
        }
    }
}

impl Phase3E2eTableShape {
    fn as_seed_value(&self) -> &'static str {
        match self {
            Self::SingleRegion => "single_region",
            Self::MultiRegion => "multi_region",
        }
    }

    fn from_seed_value(value: &str) -> Result<Self, String> {
        match value {
            "single_region" => Ok(Self::SingleRegion),
            "multi_region" => Ok(Self::MultiRegion),
            other => Err(format!("unknown phase3 table_shape: {other}")),
        }
    }

    fn rows_per_flush(&self) -> usize {
        match self {
            Self::SingleRegion => 3,
            Self::MultiRegion => 4,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Phase3E2eInput {
    pub seed: u64,
    pub flush_rounds: usize,
    pub full_file_listing: bool,
    pub compaction_wait_secs: u64,
    pub table_shape: Phase3E2eTableShape,
    pub scenario_kind: Phase3E2eScenarioKind,
}

impl Phase3E2eInput {
    pub fn smoke(seed: u64) -> Self {
        Self {
            seed,
            flush_rounds: 4,
            full_file_listing: true,
            compaction_wait_secs: 2,
            table_shape: Phase3E2eTableShape::SingleRegion,
            scenario_kind: Phase3E2eScenarioKind::CompactGc,
        }
    }

    pub fn summary(&self) -> String {
        format!(
            "seed={} flush_rounds={} full_file_listing={} compaction_wait_secs={} table_shape={} scenario_kind={}",
            self.seed,
            self.flush_rounds,
            self.full_file_listing,
            self.compaction_wait_secs,
            self.table_shape.as_seed_value(),
            self.scenario_kind.as_seed_value(),
        )
    }

    pub fn from_seed_metadata(input: &str) -> Result<Self, String> {
        let mut seed = None;
        let mut flush_rounds = None;
        let mut full_file_listing = None;
        let mut compaction_wait_secs = None;
        let mut table_shape = None;
        let mut scenario_kind = None;

        for raw_line in input.lines() {
            let line = raw_line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            let (key, value) = line
                .split_once('=')
                .ok_or_else(|| format!("invalid phase3 seed metadata line: {line}"))?;
            let value = value.trim();

            match key.trim() {
                "seed" => seed = Some(value.parse::<u64>().map_err(|e| e.to_string())?),
                "flush_rounds" => {
                    flush_rounds = Some(value.parse::<usize>().map_err(|e| e.to_string())?)
                }
                "full_file_listing" => {
                    full_file_listing = Some(value.parse::<bool>().map_err(|e| e.to_string())?)
                }
                "compaction_wait_secs" => {
                    compaction_wait_secs = Some(value.parse::<u64>().map_err(|e| e.to_string())?)
                }
                "table_shape" => table_shape = Some(Phase3E2eTableShape::from_seed_value(value)?),
                "scenario_kind" => {
                    scenario_kind = Some(Phase3E2eScenarioKind::from_seed_value(value)?)
                }
                other => return Err(format!("unknown phase3 seed metadata key: {other}")),
            }
        }

        Ok(Self {
            seed: seed.ok_or_else(|| "missing seed".to_string())?,
            flush_rounds: flush_rounds.ok_or_else(|| "missing flush_rounds".to_string())?,
            full_file_listing: full_file_listing
                .ok_or_else(|| "missing full_file_listing".to_string())?,
            compaction_wait_secs: compaction_wait_secs
                .ok_or_else(|| "missing compaction_wait_secs".to_string())?,
            table_shape: table_shape.unwrap_or(Phase3E2eTableShape::SingleRegion),
            scenario_kind: scenario_kind.unwrap_or(Phase3E2eScenarioKind::CompactGc),
        })
    }
}

#[derive(Debug)]
pub struct Phase3RepartitionEvidence {
    pub reconciliation_stage: &'static str,
    pub protects_gc_deletion_decision: bool,
    pub source_region: RegionId,
    pub old_destination_region: RegionId,
    pub related_destination_region: RegionId,
    pub manifest_cross_region_refs: BTreeSet<RegionId>,
    pub repartition_after_update: BTreeSet<RegionId>,
}

#[derive(Debug)]
pub struct Phase3PreGcProtectionEvidence {
    pub dropped_source_region: RegionId,
    pub destination_region: RegionId,
    pub protected_file_ids: HashSet<FileId>,
    pub acquired_file_refs_source: HashSet<FileRef>,
    pub acquired_cross_region_refs_source: HashSet<RegionId>,
    pub source_paths_before_gc: BTreeSet<String>,
    pub source_paths_after_gc: BTreeSet<String>,
    pub destination_manifest_protected_after_gc: HashSet<FileId>,
    pub source_in_route_before: bool,
    pub source_in_route_after: bool,
    pub table_repart_before_gc: Option<TableRepartValue>,
    pub gc_report: GcReport,
    /// Orphan pressure file written into the dropped source region to guarantee
    /// GC has unprotected files to delete during full-listing.
    pub orphan_pressure_file_id: FileId,
    pub orphan_pressure_path: String,
}

#[derive(Debug)]
pub struct Phase3E2eEvidence {
    pub target_name: &'static str,
    pub seed: u64,
    pub flush_rounds: usize,
    pub full_file_listing: bool,
    pub compaction_wait_secs: u64,
    pub table_shape: Phase3E2eTableShape,
    pub scenario_kind: Phase3E2eScenarioKind,
    pub table_name: String,
    pub regions: Vec<RegionId>,
    pub sst_before_compaction: BTreeSet<String>,
    pub sst_after_compaction: BTreeSet<String>,
    pub sst_after_gc: BTreeSet<String>,
    pub manifest_after_gc: BTreeSet<String>,
    pub gc_report: GcReport,
    pub repartition_evidence: Option<Phase3RepartitionEvidence>,
    pub pre_gc_protection_evidence: Option<Phase3PreGcProtectionEvidence>,
    pub follower_required_files: HashSet<FileId>,
    pub count_output: String,
    pub replay_trace: Vec<String>,
}

impl Phase3E2eEvidence {
    pub fn concise_summary(&self) -> String {
        format!(
            "target={} seed={} mode={} table_shape={} scenario={} table={} regions={:?} deleted_regions={:?} retry_regions={:?} follower_required_files={} before={} compacted={} after_gc={}",
            self.target_name,
            self.seed,
            self.gc_mode_label(),
            self.table_shape.as_seed_value(),
            self.scenario_kind.as_seed_value(),
            self.table_name,
            self.regions,
            self.gc_report.deleted_files.keys().collect::<Vec<_>>(),
            self.gc_report.need_retry_regions,
            self.follower_required_files.len(),
            self.sst_before_compaction.len(),
            self.sst_after_compaction.len(),
            self.sst_after_gc.len(),
        )
    }

    pub fn gc_mode_label(&self) -> &'static str {
        if self.full_file_listing {
            "full_listing"
        } else {
            "fast"
        }
    }

    pub fn mode_evidence_summary(&self) -> Result<String, String> {
        Ok(format!(
            "{} outcome={} report_deleted_files={} object_store_diff_files={} follower_required_files={} sst_after_gc={} manifest_after_gc={} retry_regions={:?}",
            self.gc_mode_label(),
            self.outcome_kind()?.as_evidence_value(),
            collect_deleted_file_ids(&self.gc_report).len(),
            collect_object_store_deleted_file_ids(self)?.len(),
            self.follower_required_files.len(),
            self.sst_after_gc.len(),
            self.manifest_after_gc.len(),
            self.gc_report.need_retry_regions,
        ))
    }

    pub fn outcome_kind(&self) -> Result<Phase3GcOutcomeKind, String> {
        classify_phase3_gc_outcome(
            &self.gc_report,
            collect_object_store_deleted_file_ids(self)?.len(),
        )
    }
}

pub fn classify_phase3_gc_outcome(
    report: &GcReport,
    object_store_deleted_count: usize,
) -> Result<Phase3GcOutcomeKind, String> {
    if !report.need_retry_regions.is_empty() {
        return Ok(Phase3GcOutcomeKind::NeedRetry);
    }

    let report_deleted_count = collect_deleted_file_ids(report).len();
    if report_deleted_count == 0 && object_store_deleted_count == 0 {
        return Ok(Phase3GcOutcomeKind::NoOpSuccess);
    }
    if report_deleted_count > 0 && object_store_deleted_count > 0 {
        return Ok(Phase3GcOutcomeKind::DeletedFiles);
    }

    Err(format!(
        "inconsistent GC outcome: report_deleted_files={report_deleted_count} object_store_deleted_files={object_store_deleted_count} need_retry_regions={:?}",
        report.need_retry_regions,
    ))
}

pub fn validate_phase3_mode_comparison(
    fast: &Phase3E2eEvidence,
    full: &Phase3E2eEvidence,
) -> Result<(), String> {
    if fast.full_file_listing || !full.full_file_listing {
        return Err(format!(
            "invalid mode comparison inputs; fast={} full={}",
            fast.mode_evidence_summary()?,
            full.mode_evidence_summary()?,
        ));
    }

    if fast.flush_rounds != full.flush_rounds
        || fast.compaction_wait_secs != full.compaction_wait_secs
        || fast.table_shape != full.table_shape
        || fast.scenario_kind != full.scenario_kind
        || fast.regions.len() != full.regions.len()
    {
        return Err(format!(
            "mode comparison inputs are not comparable; fast={} full={}",
            fast.concise_summary(),
            full.concise_summary(),
        ));
    }

    let fast_report_deleted = collect_deleted_file_ids(&fast.gc_report).len();
    let full_report_deleted = collect_deleted_file_ids(&full.gc_report).len();
    let fast_store_deleted = collect_object_store_deleted_file_ids(fast)?.len();
    let full_store_deleted = collect_object_store_deleted_file_ids(full)?.len();

    if fast_report_deleted > full_report_deleted || fast_store_deleted > full_store_deleted {
        return Err(format!(
            "fast GC deleted more files than full-listing GC for comparable inputs; fast={} full={}",
            fast.mode_evidence_summary()?,
            full.mode_evidence_summary()?,
        ));
    }

    if fast.sst_after_gc.len() < full.sst_after_gc.len() {
        return Err(format!(
            "fast GC left fewer SSTs than full-listing GC for comparable inputs; fast={} full={}",
            fast.mode_evidence_summary()?,
            full.mode_evidence_summary()?,
        ));
    }

    Ok(())
}

pub async fn run_phase3_e2e_gc_cycle(input: Phase3E2eInput) -> Phase3E2eEvidence {
    if input.scenario_kind == Phase3E2eScenarioKind::RepartitionPreGcProtection {
        return run_repartition_pre_gc_protection_scenario(input).await;
    }

    common_telemetry::init_default_ut_logging();

    let cluster_name = format!("phase3-gc-e2e-{}", input.seed);
    let (store_config, _store_guard) = get_test_store_config(&StorageType::File);
    let home_dir = Arc::new(create_temp_dir("phase3_gc_e2e_home"));
    let cluster = GreptimeDbClusterBuilder::new(&cluster_name)
        .await
        .with_datanodes(2)
        .with_shared_home_dir(home_dir)
        .with_store_config(store_config)
        .with_metasrv_gc_config(GcSchedulerOptions {
            enable: true,
            ..Default::default()
        })
        .with_datanode_gc_config(GcConfig {
            enable: true,
            lingering_time: Some(Duration::ZERO),
            ..Default::default()
        })
        .with_datanode_wal_config(DatanodeWalConfig::Noop)
        .build(false)
        .await;

    let instance = cluster.fe_instance().clone();
    let metasrv = cluster.metasrv.clone();
    let table_name = format!("phase3_gc_table_{}", input.seed);

    create_append_mode_table(&instance, &table_name, input.table_shape).await;
    generate_ssts(
        &instance,
        &table_name,
        input.flush_rounds,
        input.table_shape,
    )
    .await;

    let sst_before_compaction = cluster.list_sst_files_from_all_datanodes().await;
    let regions = get_table_regions(metasrv.table_metadata_manager(), &instance, &table_name).await;
    assert_expected_region_shape(input.table_shape, &regions);
    assert_eq!(
        sst_before_compaction.len(),
        input.flush_rounds * regions.len()
    );

    compact_table(&instance, &table_name, input.compaction_wait_secs).await;

    let sst_after_compaction = cluster.list_sst_files_from_all_datanodes().await;
    assert_eq!(
        sst_after_compaction.len(),
        (input.flush_rounds + 1) * regions.len()
    );

    let gc_report = run_batch_gc(&cluster, regions.clone(), input.full_file_listing).await;
    let repartition_evidence = match input.scenario_kind {
        Phase3E2eScenarioKind::CompactGc
        | Phase3E2eScenarioKind::FollowerLike
        | Phase3E2eScenarioKind::RepartitionPreGcProtection => None,
        Phase3E2eScenarioKind::RepartitionLike => {
            Some(run_repartition_like_update(&cluster, &regions).await)
        }
    };

    let sst_after_gc = cluster.list_sst_files_from_all_datanodes().await;
    let manifest_after_gc = cluster.list_sst_files_from_manifests().await;
    let follower_required_files = match input.scenario_kind {
        Phase3E2eScenarioKind::FollowerLike => {
            collect_file_ids_from_paths(manifest_after_gc.clone()).unwrap_or_else(|err| {
                panic!("failed to collect follower-like protected files: {err}")
            })
        }
        Phase3E2eScenarioKind::CompactGc
        | Phase3E2eScenarioKind::RepartitionLike
        | Phase3E2eScenarioKind::RepartitionPreGcProtection => HashSet::new(),
    };
    let count_output = query_count_output(&instance, &table_name).await;

    info!(
        "phase3 e2e gc cycle finished: seed={}, table={}, report={:?}",
        input.seed, table_name, gc_report
    );

    let evidence = Phase3E2eEvidence {
        target_name: "fuzz_gc_e2e_cross_region",
        seed: input.seed,
        flush_rounds: input.flush_rounds,
        full_file_listing: input.full_file_listing,
        compaction_wait_secs: input.compaction_wait_secs,
        table_shape: input.table_shape,
        scenario_kind: input.scenario_kind,
        table_name,
        regions,
        sst_before_compaction,
        sst_after_compaction,
        sst_after_gc,
        manifest_after_gc,
        gc_report,
        repartition_evidence,
        pre_gc_protection_evidence: None,
        follower_required_files,
        count_output,
        replay_trace: vec![
            format!(
                "create {} append-mode table",
                input.table_shape.as_seed_value()
            ),
            format!("generate {} flush rounds", input.flush_rounds),
            "compact table to create removable SSTs".to_string(),
            format!(
                "run BatchGcProcedure(full_file_listing={})",
                input.full_file_listing
            ),
            format!("{}", phase3_scenario_trace_step(input.scenario_kind)),
            "capture post-GC SST/manifest/data checkpoints".to_string(),
        ],
    };

    validate_phase3_e2e_evidence(&evidence)
        .unwrap_or_else(|err| panic!("Phase 3 E2E invariant violation: {err}"));

    evidence
}

pub fn validate_phase3_e2e_evidence(evidence: &Phase3E2eEvidence) -> Result<(), String> {
    let deleted_file_ids = collect_deleted_file_ids(&evidence.gc_report);
    let deleted_path_ids = collect_file_ids_from_paths(
        evidence
            .sst_after_compaction
            .difference(&evidence.sst_after_gc)
            .cloned()
            .collect(),
    )?;
    let reachable_after_gc = collect_file_ids_from_paths(evidence.manifest_after_gc.clone())?;
    let object_store_after_gc = collect_file_ids_from_paths(evidence.sst_after_gc.clone())?;

    if evidence
        .gc_report
        .need_retry_regions
        .iter()
        .any(|r| !evidence.regions.contains(r))
    {
        return Err(format!(
            "retry region escaped requested scope; {} trace={:?}",
            evidence.concise_summary(),
            evidence.replay_trace,
        ));
    }

    if evidence
        .gc_report
        .processed_regions
        .iter()
        .any(|r| !evidence.regions.contains(r))
    {
        return Err(format!(
            "processed region escaped requested scope; {} trace={:?}",
            evidence.concise_summary(),
            evidence.replay_trace,
        ));
    }

    let reachable_violation = intersect(&deleted_file_ids, &reachable_after_gc);
    if !reachable_violation.is_empty() {
        return Err(format!(
            "deleted files still reachable after GC: {:?}; {} trace={:?}",
            reachable_violation,
            evidence.concise_summary(),
            evidence.replay_trace,
        ));
    }

    let follower_violation = intersect(&deleted_file_ids, &evidence.follower_required_files);
    if !follower_violation.is_empty() {
        return Err(format!(
            "deleted follower-protected files: {:?}; {} trace={:?}",
            follower_violation,
            evidence.concise_summary(),
            evidence.replay_trace,
        ));
    }

    if evidence.scenario_kind == Phase3E2eScenarioKind::FollowerLike
        && evidence.follower_required_files.is_empty()
    {
        return Err(format!(
            "follower_like scenario did not record follower-required files; {} trace={:?}",
            evidence.concise_summary(),
            evidence.replay_trace,
        ));
    }

    if !evidence
        .follower_required_files
        .is_subset(&object_store_after_gc)
    {
        return Err(format!(
            "follower-required files missing from object store after GC; follower_required={:?} object_store={:?}; {} trace={:?}",
            evidence.follower_required_files,
            object_store_after_gc,
            evidence.concise_summary(),
            evidence.replay_trace,
        ));
    }

    if evidence.scenario_kind != Phase3E2eScenarioKind::RepartitionPreGcProtection
        && deleted_file_ids != deleted_path_ids
    {
        return Err(format!(
            "gc_report deleted_files mismatch object-store diff; report={:?} diff={:?}; {} trace={:?}",
            deleted_file_ids,
            deleted_path_ids,
            evidence.concise_summary(),
            evidence.replay_trace,
        ));
    }

    if evidence.scenario_kind != Phase3E2eScenarioKind::RepartitionPreGcProtection
        && evidence.manifest_after_gc != evidence.sst_after_gc
    {
        return Err(format!(
            "manifest/object-store SST sets diverged after GC; {} trace={:?}",
            evidence.concise_summary(),
            evidence.replay_trace,
        ));
    }

    if !reachable_after_gc.is_subset(&object_store_after_gc) {
        return Err(format!(
            "manifest references missing from object store after GC; reachable={:?} object_store={:?}; {} trace={:?}",
            reachable_after_gc,
            object_store_after_gc,
            evidence.concise_summary(),
            evidence.replay_trace,
        ));
    }

    let expected_row_count =
        (evidence.flush_rounds * evidence.table_shape.rows_per_flush()).to_string();
    if !evidence.count_output.contains(&expected_row_count) {
        return Err(format!(
            "unexpected row-count output: expected {} in {}; {} trace={:?}",
            expected_row_count,
            evidence.count_output,
            evidence.concise_summary(),
            evidence.replay_trace,
        ));
    }

    validate_repartition_evidence(evidence)?;
    validate_pre_gc_protection_evidence(evidence)?;

    Ok(())
}

fn validate_repartition_evidence(evidence: &Phase3E2eEvidence) -> Result<(), String> {
    match evidence.scenario_kind {
        Phase3E2eScenarioKind::CompactGc
        | Phase3E2eScenarioKind::FollowerLike
        | Phase3E2eScenarioKind::RepartitionPreGcProtection => {
            if evidence.repartition_evidence.is_some() {
                return Err(format!(
                    "{} scenario unexpectedly recorded repartition evidence; {} trace={:?}",
                    evidence.scenario_kind.as_seed_value(),
                    evidence.concise_summary(),
                    evidence.replay_trace,
                ));
            }
        }
        Phase3E2eScenarioKind::RepartitionLike => {
            let Some(repart) = &evidence.repartition_evidence else {
                return Err(format!(
                    "repartition_like scenario missing post-GC repartition metadata evidence; {} trace={:?}",
                    evidence.concise_summary(),
                    evidence.replay_trace,
                ));
            };

            if repart.reconciliation_stage != "post_gc_metadata_reconciliation"
                || repart.protects_gc_deletion_decision
            {
                return Err(format!(
                    "repartition_like evidence overstated GC deletion-decision protection: {:?}; {} trace={:?}",
                    repart,
                    evidence.concise_summary(),
                    evidence.replay_trace,
                ));
            }

            if !evidence.regions.contains(&repart.source_region)
                || !evidence
                    .regions
                    .contains(&repart.related_destination_region)
            {
                return Err(format!(
                    "post-GC repartition metadata evidence escaped requested regions: {:?}; {} trace={:?}",
                    repart,
                    evidence.concise_summary(),
                    evidence.replay_trace,
                ));
            }

            if !repart
                .manifest_cross_region_refs
                .contains(&repart.related_destination_region)
            {
                return Err(format!(
                    "post-GC repartition metadata refs missing related destination: {:?}; {} trace={:?}",
                    repart,
                    evidence.concise_summary(),
                    evidence.replay_trace,
                ));
            }

            if repart
                .repartition_after_update
                .contains(&repart.old_destination_region)
                || !repart
                    .repartition_after_update
                    .contains(&repart.related_destination_region)
            {
                return Err(format!(
                    "post-GC repartition metadata was not reconciled from old destination to related destination: {:?}; {} trace={:?}",
                    repart,
                    evidence.concise_summary(),
                    evidence.replay_trace,
                ));
            }
        }
    }

    Ok(())
}

fn validate_pre_gc_protection_evidence(evidence: &Phase3E2eEvidence) -> Result<(), String> {
    if evidence.scenario_kind != Phase3E2eScenarioKind::RepartitionPreGcProtection {
        if evidence.pre_gc_protection_evidence.is_some() {
            return Err(format!(
                "non-protection scenario {} unexpectedly recorded pre-GC protection evidence; {} trace={:?}",
                evidence.scenario_kind.as_seed_value(),
                evidence.concise_summary(),
                evidence.replay_trace,
            ));
        }
        return Ok(());
    }

    let Some(prot) = &evidence.pre_gc_protection_evidence else {
        return Err(format!(
            "repartition_pre_gc_protection scenario missing pre-GC protection evidence; {} trace={:?}",
            evidence.concise_summary(),
            evidence.replay_trace,
        ));
    };

    if !evidence.full_file_listing {
        return Err(format!(
            "repartition_pre_gc_protection must use full-listing GC; {} trace={:?}",
            evidence.concise_summary(),
            evidence.replay_trace,
        ));
    }

    if !prot.source_in_route_before {
        return Err(format!(
            "dropped source region {:?} was not in table route before merge; evidence={:?}",
            prot.dropped_source_region, prot,
        ));
    }

    if prot.source_in_route_after {
        return Err(format!(
            "dropped source region {:?} still in table route after merge; evidence={:?}",
            prot.dropped_source_region, prot,
        ));
    }

    let repart = prot.table_repart_before_gc.as_ref().ok_or_else(|| {
        format!(
            "missing table_repart metadata before GC; evidence={:?}",
            prot,
        )
    })?;

    let dst_set = repart
        .src_to_dst
        .get(&prot.dropped_source_region)
        .ok_or_else(|| {
            format!(
                "table_repart does not contain dropped source {:?}; repart={:?}",
                prot.dropped_source_region, repart,
            )
        })?;

    if !dst_set.contains(&prot.destination_region) {
        return Err(format!(
            "table_repart for source {:?} does not contain destination {:?}; repart={:?}",
            prot.dropped_source_region, prot.destination_region, repart,
        ));
    }

    if prot.protected_file_ids.is_empty() {
        return Err(format!(
            "no protected file ids found in destination {:?} manifest; evidence={:?}",
            prot.destination_region, prot,
        ));
    }

    let source_before_ids = collect_file_ids_from_paths(prot.source_paths_before_gc.clone())?;
    if !source_before_ids.is_superset(&prot.protected_file_ids) {
        let missing: Vec<_> = prot
            .protected_file_ids
            .difference(&source_before_ids)
            .collect();
        return Err(format!(
            "protected file ids were not present in source object-store paths before GC: {:?}; evidence={:?}",
            missing, prot,
        ));
    }

    // Validate the orphan pressure file exists before GC and is not protected.
    if prot
        .protected_file_ids
        .contains(&prot.orphan_pressure_file_id)
    {
        return Err(format!(
            "orphan pressure file {:?} must NOT be in protected_file_ids; evidence={:?}",
            prot.orphan_pressure_file_id, prot,
        ));
    }
    if !source_before_ids.contains(&prot.orphan_pressure_file_id) {
        return Err(format!(
            "orphan pressure file {:?} not found in source object-store paths before GC; evidence={:?}",
            prot.orphan_pressure_file_id, prot,
        ));
    }

    let unprotected_source_before_ids: HashSet<FileId> = source_before_ids
        .difference(&prot.protected_file_ids)
        .copied()
        .collect();
    if !unprotected_source_before_ids.contains(&prot.orphan_pressure_file_id) {
        return Err(format!(
            "orphan pressure file {:?} was not classified as unprotected before GC; unprotected={:?}; evidence={:?}",
            prot.orphan_pressure_file_id, unprotected_source_before_ids, prot,
        ));
    }

    if !prot
        .acquired_cross_region_refs_source
        .contains(&prot.destination_region)
    {
        return Err(format!(
            "acquired cross_region_refs[{:?}] does not contain destination {:?}; cross_region_refs={:?}",
            prot.dropped_source_region,
            prot.destination_region,
            prot.acquired_cross_region_refs_source,
        ));
    }

    if prot.acquired_file_refs_source.is_empty() {
        return Err(format!(
            "acquired file_refs[{:?}] is empty; evidence={:?}",
            prot.dropped_source_region, prot,
        ));
    }

    if !prot
        .acquired_file_refs_source
        .iter()
        .all(|file_ref| file_ref.region_id == prot.dropped_source_region)
    {
        return Err(format!(
            "acquired file_refs contained refs not attributed to dropped source {:?}; refs={:?}",
            prot.dropped_source_region, prot.acquired_file_refs_source,
        ));
    }

    let acquired_file_ids: HashSet<FileId> = prot
        .acquired_file_refs_source
        .iter()
        .map(|r| r.file_id)
        .collect();
    if !acquired_file_ids.is_superset(&prot.protected_file_ids) {
        return Err(format!(
            "acquired file_refs[{:?}] does not cover all protected file ids; acquired={:?} protected={:?}",
            prot.dropped_source_region, acquired_file_ids, prot.protected_file_ids,
        ));
    }

    if !prot
        .gc_report
        .processed_regions
        .contains(&prot.dropped_source_region)
    {
        return Err(format!(
            "gc_report.processed_regions does not contain dropped source {:?}; report={:?}",
            prot.dropped_source_region, prot.gc_report,
        ));
    }

    if !prot.gc_report.need_retry_regions.is_empty() {
        return Err(format!(
            "gc_report has unexpected need_retry_regions: {:?}; evidence={:?}",
            prot.gc_report.need_retry_regions, prot,
        ));
    }

    let deleted: HashSet<FileId> = prot
        .gc_report
        .deleted_files
        .get(&prot.dropped_source_region)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .collect();

    let deleted_protected: Vec<_> = deleted.intersection(&prot.protected_file_ids).collect();
    if !deleted_protected.is_empty() {
        return Err(format!(
            "GC deleted protected files from dropped source {:?}: deleted={:?} protected={:?}",
            prot.dropped_source_region, deleted_protected, prot.protected_file_ids,
        ));
    }

    let source_after_ids = collect_file_ids_from_paths(prot.source_paths_after_gc.clone())?;

    let deleted_unprotected = deleted
        .intersection(&unprotected_source_before_ids)
        .next()
        .is_some();
    let removed_unprotected = unprotected_source_before_ids
        .difference(&source_after_ids)
        .next()
        .is_some();
    if !deleted_unprotected && !removed_unprotected {
        return Err(format!(
            "dropped source GC did not show deletion pressure on unprotected files; unprotected_before={:?} deleted={:?} source_after={:?}; evidence={:?}",
            unprotected_source_before_ids, deleted, source_after_ids, prot,
        ));
    }

    // The orphan pressure file must have been removed by GC.
    let orphan_deleted_by_report = deleted.contains(&prot.orphan_pressure_file_id);
    let orphan_missing_from_storage = !source_after_ids.contains(&prot.orphan_pressure_file_id);
    if !orphan_deleted_by_report && !orphan_missing_from_storage {
        return Err(format!(
            "orphan pressure file {:?} was not deleted by GC; in-report={} in-storage-after={}; evidence={:?}",
            prot.orphan_pressure_file_id,
            orphan_deleted_by_report,
            source_after_ids.contains(&prot.orphan_pressure_file_id),
            prot,
        ));
    }

    if !source_after_ids.is_superset(&prot.protected_file_ids) {
        let missing: Vec<_> = prot
            .protected_file_ids
            .difference(&source_after_ids)
            .collect();
        return Err(format!(
            "protected file ids missing from object store after GC: {:?}; evidence={:?}",
            missing, prot,
        ));
    }

    if !prot
        .destination_manifest_protected_after_gc
        .is_superset(&prot.protected_file_ids)
    {
        let missing: Vec<_> = prot
            .protected_file_ids
            .difference(&prot.destination_manifest_protected_after_gc)
            .collect();
        return Err(format!(
            "destination manifest stopped referencing protected origin-source files after GC: {:?}; evidence={:?}",
            missing, prot,
        ));
    }

    Ok(())
}

fn phase3_scenario_trace_step(scenario_kind: Phase3E2eScenarioKind) -> &'static str {
    match scenario_kind {
        Phase3E2eScenarioKind::CompactGc => "run compact_gc scenario checks",
        Phase3E2eScenarioKind::RepartitionLike => {
            "run post-GC repartition metadata reconciliation checks"
        }
        Phase3E2eScenarioKind::FollowerLike => "run follower_like protected-set checks",
        Phase3E2eScenarioKind::RepartitionPreGcProtection => {
            "run pre-GC cross-region deletion protection checks"
        }
    }
}

fn collect_object_store_deleted_file_ids(
    evidence: &Phase3E2eEvidence,
) -> Result<HashSet<FileId>, String> {
    collect_file_ids_from_paths(
        evidence
            .sst_after_compaction
            .difference(&evidence.sst_after_gc)
            .cloned()
            .collect(),
    )
}

fn default_phase3_seed_corpus_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("corpus/phase3_e2e")
}

/// Write an orphan `.parquet` file into the dropped source region's object-store
/// directory. The file is intentionally absent from every manifest and
/// `FileRefsManifest`, so full-listing GC will detect it as deletable orphan.
///
/// Returns the orphan `FileId` and the full object-store path.
async fn write_orphan_pressure_file(
    cluster: &GreptimeDbCluster,
    dropped_source: RegionId,
) -> (FileId, String) {
    let datanode = cluster
        .datanode_instances
        .values()
        .next()
        .expect("cluster must have at least one datanode");
    let mito = datanode
        .region_server()
        .mito_engine()
        .expect("datanode must have a mito engine");
    let object_store = mito.object_store_manager().default_object_store().clone();

    let orphan_file_id = FileId::random();
    let region_dir_path = region_dir("greptime/public", dropped_source);
    let orphan_path = format!("{}{}.parquet", region_dir_path, orphan_file_id);

    object_store
        .write(&orphan_path, "orphan")
        .await
        .expect("writing orphan pressure file must succeed");

    info!(
        "Wrote orphan pressure file: path={} file_id={:?}",
        orphan_path, orphan_file_id
    );

    (orphan_file_id, orphan_path)
}

async fn run_repartition_pre_gc_protection_scenario(input: Phase3E2eInput) -> Phase3E2eEvidence {
    assert!(
        input.full_file_listing,
        "repartition_pre_gc_protection requires full_file_listing=true"
    );
    assert_eq!(
        input.table_shape,
        Phase3E2eTableShape::MultiRegion,
        "repartition_pre_gc_protection requires a multi-region table"
    );

    common_telemetry::init_default_ut_logging();

    let cluster_name = format!("phase3-gc-e2e-{}", input.seed);
    let (store_config, _store_guard) = get_test_store_config(&StorageType::File);
    let home_dir = Arc::new(create_temp_dir("phase3_gc_e2e_home"));
    let cluster = GreptimeDbClusterBuilder::new(&cluster_name)
        .await
        .with_datanodes(2)
        .with_shared_home_dir(home_dir)
        .with_store_config(store_config)
        .with_metasrv_gc_config(GcSchedulerOptions {
            enable: true,
            ..Default::default()
        })
        .with_datanode_gc_config(GcConfig {
            enable: true,
            lingering_time: Some(Duration::ZERO),
            ..Default::default()
        })
        .with_datanode_wal_config(DatanodeWalConfig::Noop)
        .build(false)
        .await;

    let instance = cluster.fe_instance().clone();
    let metasrv = cluster.metasrv.clone();
    let table_name = format!("phase3_gc_table_{}", input.seed);

    // --- Step 1: Create multi-region append mode table ---
    create_append_mode_table(&instance, &table_name, Phase3E2eTableShape::MultiRegion).await;

    // --- Step 2: Insert and flush data ---
    generate_ssts(
        &instance,
        &table_name,
        input.flush_rounds,
        Phase3E2eTableShape::MultiRegion,
    )
    .await;

    // --- Step 3: Compact ---
    compact_table(&instance, &table_name, input.compaction_wait_secs).await;

    let regions_before =
        get_table_regions(metasrv.table_metadata_manager(), &instance, &table_name).await;
    assert!(
        regions_before.len() > 1,
        "pre-merge multi-region table must have more than 1 region, got {:?}",
        regions_before
    );

    let sst_before_compaction = cluster.list_sst_files_from_all_datanodes().await;

    // --- Step 4: Execute real SQL MERGE PARTITION ---
    let merge_sql = format!("ALTER TABLE {table_name} MERGE PARTITION (host < 'm', host >= 'm')");
    let _ = execute_sql(&instance, &merge_sql).await;

    // Wait for the merge procedure to fully complete and routes to converge
    tokio::time::sleep(Duration::from_secs(3)).await;

    let regions_after =
        get_table_regions(metasrv.table_metadata_manager(), &instance, &table_name).await;
    let regions_after_set: BTreeSet<RegionId> = regions_after.iter().copied().collect();

    // --- Step 5: Identify dropped source and destination ---
    let table_repart_mgr = metasrv.table_metadata_manager().table_repart_manager();
    let table_id = regions_before[0].table_id();
    let table_repart_before_gc = table_repart_mgr.get(table_id).await.unwrap();

    // The dropped source is a region that was in table route before merge but not after
    let dropped_source = regions_before
        .iter()
        .copied()
        .find(|r| !regions_after_set.contains(r))
        .expect("there must be a dropped source region after merge");

    let source_in_route_before = regions_before.contains(&dropped_source);
    let source_in_route_after = regions_after_set.contains(&dropped_source);

    // The destination is any region that is active after merge and in the same table
    let destination_region = regions_after[0];

    // --- Step 6: Inspect destination manifest for cross-region files ---
    let all_regions = cluster.list_all_regions().await;
    let dest_region_ref = all_regions
        .get(&destination_region)
        .expect("destination region must be present in cluster after merge");
    let dest_manifest_entries = dest_region_ref.manifest_sst_entries().await;

    let protected_file_ids: HashSet<FileId> = dest_manifest_entries
        .iter()
        .filter(|e| e.origin_region_id == dropped_source)
        .filter_map(|e| FileId::parse_str(&e.file_id).ok())
        .collect();

    info!(
        "Pre-GC protection scenario: dropped_source={:?} destination={:?} protected_file_ids={:?}",
        dropped_source, destination_region, protected_file_ids,
    );

    assert!(
        !protected_file_ids.is_empty(),
        "destination manifest must contain files from the dropped source region; \
         dropped_source={:?} destination={:?} entries_cnt={}",
        dropped_source,
        destination_region,
        dest_manifest_entries.len(),
    );

    // --- Write an orphan pressure file so full-listing GC always has deletable files ---
    let (orphan_pressure_file_id, orphan_pressure_path) =
        write_orphan_pressure_file(&cluster, dropped_source).await;

    let sst_before_gc = cluster.list_sst_files_from_all_datanodes().await;

    // Collect source region paths after merge but before GC.
    let source_paths_before_gc: BTreeSet<String> = sst_before_gc
        .iter()
        .filter(|path| path_belongs_to_region(path, dropped_source))
        .cloned()
        .collect();

    // --- Step 7: Build route override for the dropped source ---
    let (_, table_route) = metasrv
        .table_metadata_manager()
        .table_route_manager()
        .get_physical_table_route(table_id)
        .await
        .unwrap();

    let leader_peer = table_route
        .region_routes
        .iter()
        .find_map(|r| r.leader_peer.clone())
        .expect("table route must have at least one leader peer");

    let mut region_routes_override = Region2Peers::new();
    region_routes_override.insert(dropped_source, (leader_peer, Vec::new()));

    // --- Step 8: Run full-listing BatchGcProcedure for the dropped source ---
    // Manually drive state machine to inspect intermediate state
    let mut procedure = BatchGcProcedure::new(
        cluster.metasrv.mailbox().clone(),
        cluster.metasrv.table_metadata_manager().clone(),
        cluster.metasrv.options().grpc.server_addr.clone(),
        vec![dropped_source],
        true, // full_file_listing
        Duration::from_secs(10),
        region_routes_override,
    );

    let procedure_ctx = new_test_procedure_context();

    // Execute Start -> Acquiring
    let status = procedure.execute(&procedure_ctx).await.unwrap();
    assert!(
        matches!(status, Status::Executing { .. }),
        "expected Executing after Start, got {:?}",
        status
    );

    // Execute Acquiring -> Gcing
    let status = procedure.execute(&procedure_ctx).await.unwrap();
    assert!(
        matches!(status, Status::Executing { .. }),
        "expected Executing after Acquiring, got {:?}",
        status
    );

    // Inspect acquired file_refs
    let file_refs = procedure.file_refs_for_test().clone();
    let acquired_file_refs_source: HashSet<FileRef> = file_refs
        .file_refs
        .get(&dropped_source)
        .cloned()
        .unwrap_or_default();
    let acquired_cross_region_refs_source: HashSet<RegionId> = file_refs
        .cross_region_refs
        .get(&dropped_source)
        .cloned()
        .unwrap_or_default();

    info!(
        "Acquired after Acquiring state: file_refs[{:?}]={:?} cross_region_refs[{:?}]={:?}",
        dropped_source,
        acquired_file_refs_source,
        dropped_source,
        acquired_cross_region_refs_source,
    );

    // Execute Gcing -> UpdateRepartition
    let status = procedure.execute(&procedure_ctx).await.unwrap();
    assert!(
        matches!(status, Status::Executing { .. }),
        "expected Executing after Gcing, got {:?}",
        status
    );

    // Execute UpdateRepartition -> Done
    let status = procedure.execute(&procedure_ctx).await.unwrap();
    let gc_report = match status {
        Status::Done { output } => {
            let report =
                BatchGcProcedure::cast_result(output.expect("GC procedure must return output"))
                    .unwrap();
            info!("GC report: {:?}", report);
            report
        }
        other => panic!("expected Done after UpdateRepartition, got {:?}", other),
    };

    // --- Step 9: Collect post-GC evidence ---
    let sst_after_gc = cluster.list_sst_files_from_all_datanodes().await;
    let manifest_after_gc = cluster.list_sst_files_from_manifests().await;
    let count_output = query_count_output(&instance, &table_name).await;

    let source_paths_after_gc: BTreeSet<String> = sst_after_gc
        .iter()
        .filter(|path| path_belongs_to_region(path, dropped_source))
        .cloned()
        .collect();

    let all_regions_after_gc = cluster.list_all_regions().await;
    let destination_manifest_protected_after_gc: HashSet<FileId> = all_regions_after_gc
        .get(&destination_region)
        .expect("destination region must still be present after GC")
        .manifest_sst_entries()
        .await
        .iter()
        .filter(|e| e.origin_region_id == dropped_source)
        .filter_map(|e| FileId::parse_str(&e.file_id).ok())
        .collect();

    let pre_gc_protection_evidence = Phase3PreGcProtectionEvidence {
        dropped_source_region: dropped_source,
        destination_region,
        protected_file_ids,
        acquired_file_refs_source,
        acquired_cross_region_refs_source,
        source_paths_before_gc,
        source_paths_after_gc: source_paths_after_gc.clone(),
        destination_manifest_protected_after_gc,
        source_in_route_before,
        source_in_route_after,
        table_repart_before_gc,
        gc_report: gc_report.clone(),
        orphan_pressure_file_id,
        orphan_pressure_path: orphan_pressure_path.clone(),
    };

    let all_regions_evidence: Vec<RegionId> = regions_before.clone();

    let evidence = Phase3E2eEvidence {
        target_name: "fuzz_gc_e2e_cross_region",
        seed: input.seed,
        flush_rounds: input.flush_rounds,
        full_file_listing: input.full_file_listing,
        compaction_wait_secs: input.compaction_wait_secs,
        table_shape: input.table_shape,
        scenario_kind: input.scenario_kind,
        table_name: table_name.clone(),
        regions: all_regions_evidence,
        sst_before_compaction,
        sst_after_compaction: sst_before_gc,
        sst_after_gc,
        manifest_after_gc,
        gc_report,
        repartition_evidence: None,
        pre_gc_protection_evidence: Some(pre_gc_protection_evidence),
        follower_required_files: HashSet::new(),
        count_output,
        replay_trace: vec![
            format!("create multi_region append-mode table"),
            format!("generate {} flush rounds", input.flush_rounds),
            "compact table to create removable SSTs".to_string(),
            format!("execute ALTER TABLE MERGE PARTITION (host < 'm', host >= 'm')"),
            format!(
                "identify dropped source {:?} and destination {:?}",
                dropped_source, destination_region
            ),
            format!("run pre-GC cross-region deletion protection checks"),
        ],
    };

    validate_phase3_e2e_evidence(&evidence)
        .unwrap_or_else(|err| panic!("Phase 3 Pre-GC Protection invariant violation: {err}"));

    evidence
}

fn load_phase3_seed_corpus_inputs() -> Result<Vec<Phase3E2eInput>, String> {
    let seed_dir = std::env::var("GT_PHASE3_SEED_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| default_phase3_seed_corpus_dir());
    let entries = fs::read_dir(&seed_dir).map_err(|error| {
        format!(
            "failed to read phase3 seed dir {}: {error}",
            seed_dir.display()
        )
    })?;

    let mut paths = entries
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            format!(
                "failed to iterate phase3 seed dir {}: {error}",
                seed_dir.display()
            )
        })?
        .into_iter()
        .map(|entry| entry.path())
        .filter(|path| path.is_file())
        .collect::<Vec<_>>();
    paths.sort();

    let mut inputs = Vec::new();
    for path in paths {
        let content = fs::read_to_string(&path).map_err(|error| {
            format!(
                "failed to read phase3 seed file {}: {error}",
                path.display()
            )
        })?;
        let input = Phase3E2eInput::from_seed_metadata(&content).map_err(|error| {
            format!(
                "failed to parse phase3 seed file {}: {error}",
                path.display()
            )
        })?;
        inputs.push(input);
    }

    if inputs.is_empty() {
        return Err(format!(
            "no phase3 seed files found in {}",
            seed_dir.display()
        ));
    }

    Ok(inputs)
}

fn collect_deleted_file_ids(report: &GcReport) -> HashSet<FileId> {
    report
        .deleted_files
        .values()
        .flat_map(|files| files.iter().copied())
        .collect()
}

fn collect_file_ids_from_paths(paths: BTreeSet<String>) -> Result<HashSet<FileId>, String> {
    paths
        .into_iter()
        .map(|path| {
            parse_file_id_type_from_path(&path)
                .map(|(file_id, _)| file_id)
                .map_err(|err| format!("failed to parse file id from path {path}: {err}"))
        })
        .collect()
}

fn path_belongs_to_region(path: &str, region_id: RegionId) -> bool {
    let expected_region_dir = region_name(region_id.table_id(), region_id.region_sequence());
    path.split('/')
        .any(|segment| segment == expected_region_dir)
}

fn intersect(left: &HashSet<FileId>, right: &HashSet<FileId>) -> Vec<FileId> {
    left.iter()
        .copied()
        .filter(|file_id| right.contains(file_id))
        .collect()
}

async fn create_append_mode_table(
    instance: &Arc<frontend::instance::Instance>,
    table_name: &str,
    table_shape: Phase3E2eTableShape,
) {
    let sql = match table_shape {
        Phase3E2eTableShape::SingleRegion => format!(
            r#"
            CREATE TABLE {table_name} (
                ts TIMESTAMP TIME INDEX,
                val DOUBLE,
                host STRING
            ) WITH (append_mode = 'true')
            "#
        ),
        Phase3E2eTableShape::MultiRegion => format!(
            r#"
            CREATE TABLE {table_name} (
                ts TIMESTAMP TIME INDEX,
                val DOUBLE,
                host STRING
            ) PARTITION ON COLUMNS (host) (
                host < 'm',
                host >= 'm'
            ) WITH (append_mode = 'true')
            "#
        ),
    };
    let _ = execute_sql(instance, &sql).await;
}

async fn generate_ssts(
    instance: &Arc<frontend::instance::Instance>,
    table_name: &str,
    flush_rounds: usize,
    table_shape: Phase3E2eTableShape,
) {
    for i in 0..flush_rounds {
        let day = i + 1;
        let insert_sql = match table_shape {
            Phase3E2eTableShape::SingleRegion => format!(
                r#"
                INSERT INTO {table_name} (ts, val, host) VALUES
                ('2023-01-{day:02} 10:00:00', {}, 'host{}'),
                ('2023-01-{day:02} 11:00:00', {}, 'host{}'),
                ('2023-01-{day:02} 12:00:00', {}, 'host{}')
                "#,
                10.0 + i as f64,
                i,
                20.0 + i as f64,
                i,
                30.0 + i as f64,
                i,
            ),
            Phase3E2eTableShape::MultiRegion => format!(
                r#"
                INSERT INTO {table_name} (ts, val, host) VALUES
                ('2023-01-{day:02} 10:00:00', {}, 'host{}'),
                ('2023-01-{day:02} 11:00:00', {}, 'host{}'),
                ('2023-01-{day:02} 12:00:00', {}, 'zhost{}'),
                ('2023-01-{day:02} 13:00:00', {}, 'zhost{}')
                "#,
                10.0 + i as f64,
                i,
                20.0 + i as f64,
                i,
                30.0 + i as f64,
                i,
                40.0 + i as f64,
                i,
            ),
        };
        let _ = execute_sql(instance, &insert_sql).await;

        let flush_sql = format!("ADMIN FLUSH_TABLE('{table_name}')");
        let _ = execute_sql(instance, &flush_sql).await;
    }
}

fn assert_expected_region_shape(table_shape: Phase3E2eTableShape, regions: &[RegionId]) {
    match table_shape {
        Phase3E2eTableShape::SingleRegion => assert_eq!(regions.len(), 1),
        Phase3E2eTableShape::MultiRegion => {
            assert!(
                regions.len() > 1,
                "multi-region phase3 seed must create more than one region, got {regions:?}"
            );
        }
    }
}

async fn compact_table(
    instance: &Arc<frontend::instance::Instance>,
    table_name: &str,
    compaction_wait_secs: u64,
) {
    let compact_sql = format!("ADMIN COMPACT_TABLE('{table_name}')");
    let _ = execute_sql(instance, &compact_sql).await;
    tokio::time::sleep(Duration::from_secs(compaction_wait_secs)).await;
}

async fn get_table_regions(
    table_metadata_manager: &TableMetadataManagerRef,
    instance: &Arc<frontend::instance::Instance>,
    table_name: &str,
) -> Vec<RegionId> {
    let table = instance
        .catalog_manager()
        .table("greptime", "public", table_name, None)
        .await
        .unwrap()
        .unwrap();
    let table_id = table.table_info().table_id();
    let (_, table_route) = table_metadata_manager
        .table_route_manager()
        .get_physical_table_route(table_id)
        .await
        .unwrap();

    table_route
        .region_routes
        .into_iter()
        .map(|route| route.region.id)
        .collect()
}

async fn run_repartition_like_update(
    cluster: &GreptimeDbCluster,
    regions: &[RegionId],
) -> Phase3RepartitionEvidence {
    assert!(
        regions.len() > 1,
        "repartition_like phase3 seed requires multiple regions, got {regions:?}"
    );

    let source_region = regions[0];
    let related_destination_region = regions[1];
    let old_destination_region = RegionId::new(
        source_region.table_id(),
        source_region.region_number() + 100,
    );
    let repart_mgr = cluster
        .metasrv
        .table_metadata_manager()
        .table_repart_manager();
    let current = repart_mgr
        .get_with_raw_bytes(source_region.table_id())
        .await
        .unwrap();
    let mut initial_value = TableRepartValue::new();
    initial_value.update_mappings(source_region, &[old_destination_region]);
    repart_mgr
        .upsert_value(source_region.table_id(), current, &initial_value)
        .await
        .unwrap();

    let mut file_refs_manifest = FileRefsManifest::default();
    file_refs_manifest
        .cross_region_refs
        .insert(source_region, [related_destination_region].into());
    let manifest_cross_region_refs = file_refs_manifest
        .cross_region_refs
        .get(&source_region)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .collect();

    let mut procedure = BatchGcProcedure::new_update_repartition_for_test(
        cluster.metasrv.mailbox().clone(),
        cluster.metasrv.table_metadata_manager().clone(),
        cluster.metasrv.options().grpc.server_addr.clone(),
        vec![source_region],
        file_refs_manifest,
        Duration::from_secs(5),
    );
    let procedure_ctx = new_test_procedure_context();
    let status = procedure.execute(&procedure_ctx).await.unwrap();
    assert!(matches!(status, Status::Done { .. }));

    let repart_after = repart_mgr
        .get(source_region.table_id())
        .await
        .unwrap()
        .unwrap();
    let repartition_after_update = repart_after
        .src_to_dst
        .get(&source_region)
        .cloned()
        .unwrap_or_default();

    Phase3RepartitionEvidence {
        reconciliation_stage: "post_gc_metadata_reconciliation",
        protects_gc_deletion_decision: false,
        source_region,
        old_destination_region,
        related_destination_region,
        manifest_cross_region_refs,
        repartition_after_update,
    }
}

async fn run_batch_gc(
    cluster: &GreptimeDbCluster,
    regions: Vec<RegionId>,
    full_file_listing: bool,
) -> GcReport {
    let procedure = BatchGcProcedure::new(
        cluster.metasrv.mailbox().clone(),
        cluster.metasrv.table_metadata_manager().clone(),
        cluster.metasrv.options().grpc.server_addr.clone(),
        regions,
        full_file_listing,
        Duration::from_secs(10),
        Default::default(),
    );
    let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));
    let procedure_id = procedure_with_id.id;

    let _ = cluster
        .metasrv
        .procedure_manager()
        .submit(procedure_with_id)
        .await
        .unwrap();

    let mut watcher = cluster
        .metasrv
        .procedure_manager()
        .procedure_watcher(procedure_id)
        .unwrap();
    let output = watcher::wait(&mut watcher).await.unwrap().unwrap();

    BatchGcProcedure::cast_result(output).unwrap()
}

async fn query_count_output(
    instance: &Arc<frontend::instance::Instance>,
    table_name: &str,
) -> String {
    let sql = format!("SELECT COUNT(*) FROM {table_name}");
    execute_sql(instance, &sql).await.data.pretty_print().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_phase3_e2e_smoke_full_listing_gc_cycle() {
        let evidence = run_phase3_e2e_gc_cycle(Phase3E2eInput::smoke(7)).await;

        assert_eq!(evidence.regions.len(), 1);
        assert_eq!(evidence.sst_before_compaction.len(), 4);
        assert_eq!(evidence.sst_after_compaction.len(), 5);
        assert_eq!(evidence.sst_after_gc.len(), 1);
        assert_eq!(evidence.manifest_after_gc, evidence.sst_after_gc);
        assert!(evidence.gc_report.need_retry_regions.is_empty());
        assert_eq!(evidence.gc_report.deleted_files.len(), 1);
        assert!(evidence.count_output.contains("12"));
        assert!(
            evidence
                .concise_summary()
                .contains("fuzz_gc_e2e_cross_region")
        );
        assert_eq!(evidence.replay_trace.len(), 6);
    }

    #[test]
    fn test_phase3_e2e_seed_metadata_parser() {
        let input = Phase3E2eInput::from_seed_metadata(
            r#"
            seed=11
            flush_rounds=3
            full_file_listing=true
            compaction_wait_secs=2
            table_shape=multi_region
            scenario_kind=follower_like
            "#,
        )
        .unwrap();

        assert_eq!(
            input,
            Phase3E2eInput {
                seed: 11,
                flush_rounds: 3,
                full_file_listing: true,
                compaction_wait_secs: 2,
                table_shape: Phase3E2eTableShape::MultiRegion,
                scenario_kind: Phase3E2eScenarioKind::FollowerLike,
            }
        );
    }

    #[tokio::test]
    async fn test_phase3_e2e_smoke_multi_region_gc_cycle() {
        let evidence = run_phase3_e2e_gc_cycle(Phase3E2eInput {
            seed: 23,
            flush_rounds: 4,
            full_file_listing: true,
            compaction_wait_secs: 2,
            table_shape: Phase3E2eTableShape::MultiRegion,
            scenario_kind: Phase3E2eScenarioKind::CompactGc,
        })
        .await;

        assert!(evidence.regions.len() > 1);
        assert_eq!(
            evidence.sst_before_compaction.len(),
            4 * evidence.regions.len()
        );
        assert_eq!(
            evidence.sst_after_compaction.len(),
            5 * evidence.regions.len()
        );
        assert_eq!(evidence.sst_after_gc.len(), evidence.regions.len());
        assert_eq!(evidence.manifest_after_gc, evidence.sst_after_gc);
        assert!(evidence.gc_report.need_retry_regions.is_empty());
        assert_eq!(
            evidence.gc_report.deleted_files.len(),
            evidence.regions.len()
        );
        assert!(evidence.count_output.contains("16"));
    }

    #[tokio::test]
    async fn test_phase3_e2e_smoke_repartition_like_metadata_reconciliation() {
        let evidence = run_phase3_e2e_gc_cycle(Phase3E2eInput {
            seed: 31,
            flush_rounds: 4,
            full_file_listing: true,
            compaction_wait_secs: 2,
            table_shape: Phase3E2eTableShape::MultiRegion,
            scenario_kind: Phase3E2eScenarioKind::RepartitionLike,
        })
        .await;

        let repart = evidence
            .repartition_evidence
            .as_ref()
            .expect("repartition-like seed records post-GC repartition metadata evidence");
        assert_eq!(
            repart.reconciliation_stage,
            "post_gc_metadata_reconciliation"
        );
        assert!(!repart.protects_gc_deletion_decision);
        assert!(evidence.regions.contains(&repart.source_region));
        assert!(
            evidence
                .regions
                .contains(&repart.related_destination_region)
        );
        assert!(
            repart
                .manifest_cross_region_refs
                .contains(&repart.related_destination_region)
        );
        assert!(
            repart
                .repartition_after_update
                .contains(&repart.related_destination_region)
        );
        assert!(
            !repart
                .repartition_after_update
                .contains(&repart.old_destination_region)
        );
    }

    #[tokio::test]
    async fn test_phase3_e2e_compare_fast_and_full_listing_modes() {
        let full = run_phase3_e2e_gc_cycle(Phase3E2eInput {
            seed: 41,
            flush_rounds: 4,
            full_file_listing: true,
            compaction_wait_secs: 2,
            table_shape: Phase3E2eTableShape::MultiRegion,
            scenario_kind: Phase3E2eScenarioKind::CompactGc,
        })
        .await;
        let fast = run_phase3_e2e_gc_cycle(Phase3E2eInput {
            seed: 42,
            flush_rounds: 4,
            full_file_listing: false,
            compaction_wait_secs: 2,
            table_shape: Phase3E2eTableShape::MultiRegion,
            scenario_kind: Phase3E2eScenarioKind::CompactGc,
        })
        .await;

        validate_phase3_mode_comparison(&fast, &full).unwrap_or_else(|err| {
            panic!(
                "Phase 3 E2E fast/full mode comparison failed: {err}; fast={}; full={}",
                fast.mode_evidence_summary().unwrap(),
                full.mode_evidence_summary().unwrap(),
            )
        });
        assert!(fast.mode_evidence_summary().unwrap().contains("fast"));
        assert!(
            full.mode_evidence_summary()
                .unwrap()
                .contains("full_listing")
        );
    }

    #[tokio::test]
    async fn test_phase3_e2e_smoke_follower_like_protection() {
        let evidence = run_phase3_e2e_gc_cycle(Phase3E2eInput {
            seed: 53,
            flush_rounds: 4,
            full_file_listing: true,
            compaction_wait_secs: 2,
            table_shape: Phase3E2eTableShape::MultiRegion,
            scenario_kind: Phase3E2eScenarioKind::FollowerLike,
        })
        .await;

        assert!(evidence.regions.len() > 1);
        assert!(!evidence.follower_required_files.is_empty());
        assert_eq!(evidence.manifest_after_gc, evidence.sst_after_gc);
        assert!(evidence.gc_report.need_retry_regions.is_empty());
        assert!(
            evidence
                .mode_evidence_summary()
                .unwrap()
                .contains("follower_required_files=")
        );
    }

    #[tokio::test]
    async fn test_phase3_e2e_repartition_pre_gc_protection() {
        let evidence = run_phase3_e2e_gc_cycle(Phase3E2eInput {
            seed: 61,
            flush_rounds: 4,
            full_file_listing: true,
            compaction_wait_secs: 2,
            table_shape: Phase3E2eTableShape::MultiRegion,
            scenario_kind: Phase3E2eScenarioKind::RepartitionPreGcProtection,
        })
        .await;

        let prot = evidence
            .pre_gc_protection_evidence
            .as_ref()
            .expect("pre-GC protection evidence must be present");

        assert!(prot.source_in_route_before);
        assert!(!prot.source_in_route_after);
        assert!(!prot.protected_file_ids.is_empty());
        assert!(
            prot.acquired_cross_region_refs_source
                .contains(&prot.destination_region)
        );
        assert!(!prot.acquired_file_refs_source.is_empty());
        assert!(
            prot.gc_report
                .processed_regions
                .contains(&prot.dropped_source_region)
        );
        assert!(prot.gc_report.need_retry_regions.is_empty());

        let deleted: HashSet<FileId> = prot
            .gc_report
            .deleted_files
            .get(&prot.dropped_source_region)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect();
        let deleted_protected: HashSet<_> =
            deleted.intersection(&prot.protected_file_ids).collect();
        assert!(
            deleted_protected.is_empty(),
            "protected files should not be deleted: {:?}",
            deleted_protected
        );

        let source_after_ids =
            collect_file_ids_from_paths(prot.source_paths_after_gc.clone()).unwrap();
        assert!(
            source_after_ids.is_superset(&prot.protected_file_ids),
            "protected file ids must still exist in object store after GC"
        );
        assert!(
            !source_after_ids.contains(&prot.orphan_pressure_file_id),
            "orphan pressure file {:?} must have been removed from storage by GC",
            prot.orphan_pressure_file_id,
        );

        assert!(evidence.count_output.contains("16"));
    }

    #[tokio::test]
    async fn test_phase3_e2e_replay_corpus() {
        let inputs = load_phase3_seed_corpus_inputs().unwrap();

        for input in inputs {
            let evidence = run_phase3_e2e_gc_cycle(input).await;
            assert!(
                evidence.concise_summary().contains(&input.seed.to_string()),
                "phase3 replay summary should include seed; input={}",
                input.summary()
            );
        }
    }

    #[test]
    fn test_phase3_e2e_validator_rejects_reachable_overlap() {
        let file_id = FileId::random();
        let mut deleted_files = std::collections::HashMap::new();
        deleted_files.insert(RegionId::new(1024, 1), vec![file_id]);

        let report = GcReport {
            deleted_files,
            deleted_indexes: Default::default(),
            need_retry_regions: Default::default(),
            processed_regions: HashSet::from([RegionId::new(1024, 1)]),
        };

        let path = format!("phase3/1024_0000000001/{}.parquet", file_id);
        let evidence = Phase3E2eEvidence {
            target_name: "fuzz_gc_e2e_cross_region",
            seed: 9,
            flush_rounds: 4,
            full_file_listing: true,
            compaction_wait_secs: 2,
            table_shape: Phase3E2eTableShape::SingleRegion,
            scenario_kind: Phase3E2eScenarioKind::CompactGc,
            table_name: "phase3_gc_table_9".to_string(),
            regions: vec![RegionId::new(1024, 1)],
            sst_before_compaction: BTreeSet::from([path.clone()]),
            sst_after_compaction: BTreeSet::from([path.clone()]),
            sst_after_gc: BTreeSet::new(),
            manifest_after_gc: BTreeSet::from([path]),
            gc_report: report,
            repartition_evidence: None,
            pre_gc_protection_evidence: None,
            follower_required_files: HashSet::new(),
            count_output: "12".to_string(),
            replay_trace: vec!["replay".to_string()],
        };

        let err = validate_phase3_e2e_evidence(&evidence).unwrap_err();
        assert!(err.contains("deleted files still reachable after GC"));
    }

    #[test]
    fn test_phase3_e2e_validator_rejects_follower_overlap() {
        let file_id = FileId::random();
        let region = RegionId::new(1024, 1);
        let mut deleted_files = std::collections::HashMap::new();
        deleted_files.insert(region, vec![file_id]);

        let report = GcReport {
            deleted_files,
            deleted_indexes: Default::default(),
            need_retry_regions: Default::default(),
            processed_regions: HashSet::from([region]),
        };

        let path = format!("phase3/1024_0000000001/{}.parquet", file_id);
        let evidence = Phase3E2eEvidence {
            target_name: "fuzz_gc_e2e_cross_region",
            seed: 53,
            flush_rounds: 4,
            full_file_listing: true,
            compaction_wait_secs: 2,
            table_shape: Phase3E2eTableShape::SingleRegion,
            scenario_kind: Phase3E2eScenarioKind::FollowerLike,
            table_name: "phase3_gc_table_53".to_string(),
            regions: vec![region],
            sst_before_compaction: BTreeSet::from([path.clone()]),
            sst_after_compaction: BTreeSet::from([path]),
            sst_after_gc: BTreeSet::new(),
            manifest_after_gc: BTreeSet::new(),
            gc_report: report,
            repartition_evidence: None,
            pre_gc_protection_evidence: None,
            follower_required_files: HashSet::from([file_id]),
            count_output: "12".to_string(),
            replay_trace: vec!["replay".to_string()],
        };

        let err = validate_phase3_e2e_evidence(&evidence).unwrap_err();
        assert!(err.contains("deleted follower-protected files"));
    }

    #[test]
    fn test_phase3_gc_outcome_classifies_retry_and_no_op() {
        let region = RegionId::new(1024, 1);
        let retry_report = GcReport {
            need_retry_regions: HashSet::from([region]),
            ..Default::default()
        };
        assert_eq!(
            classify_phase3_gc_outcome(&retry_report, 0).unwrap(),
            Phase3GcOutcomeKind::NeedRetry,
        );

        let no_op_report = GcReport {
            deleted_files: std::collections::HashMap::from([(region, vec![])]),
            processed_regions: HashSet::from([region]),
            ..Default::default()
        };
        assert_eq!(
            classify_phase3_gc_outcome(&no_op_report, 0).unwrap(),
            Phase3GcOutcomeKind::NoOpSuccess,
        );
    }
}
