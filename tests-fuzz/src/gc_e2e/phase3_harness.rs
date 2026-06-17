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
use mito2::sst::location::{parse_file_id_type_from_path, parse_index_file_info};
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
    RepartitionAdminGcDroppedRegion,
    PostMigrationAdminGc,
    BuildIndexGc,
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
            Self::RepartitionAdminGcDroppedRegion => "admin_gc_dropped_region",
            Self::PostMigrationAdminGc => "post_migration_admin_gc",
            Self::BuildIndexGc => "build_index_gc",
        }
    }

    fn from_seed_value(value: &str) -> Result<Self, String> {
        match value {
            "compact_gc" => Ok(Self::CompactGc),
            "repartition_like" => Ok(Self::RepartitionLike),
            "follower_like" => Ok(Self::FollowerLike),
            "repartition_pre_gc_protection" => Ok(Self::RepartitionPreGcProtection),
            "admin_gc_dropped_region" => Ok(Self::RepartitionAdminGcDroppedRegion),
            "post_migration_admin_gc" => Ok(Self::PostMigrationAdminGc),
            "build_index_gc" => Ok(Self::BuildIndexGc),
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
pub struct Phase3AdminGcDroppedRegionEvidence {
    pub dropped_source_region: RegionId,
    pub destination_region: RegionId,
    pub protected_file_ids: HashSet<FileId>,
    pub source_paths_before_gc: BTreeSet<String>,
    pub source_paths_after_gc: BTreeSet<String>,
    pub destination_manifest_protected_after_gc: HashSet<FileId>,
    pub source_in_route_before: bool,
    pub source_in_route_after: bool,
    pub source_region_physically_dropped_before_admin_gc: bool,
    pub table_repart_before_gc: Option<TableRepartValue>,
    pub table_repart_after_gc: Option<TableRepartValue>,
    pub admin_gc_output: String,
    pub orphan_pressure_file_id: FileId,
    pub orphan_pressure_path: String,
}

#[derive(Debug)]
pub struct Phase3PostMigrationAdminGcEvidence {
    pub migrated_region: RegionId,
    pub from_peer_id: u64,
    pub to_peer_id: u64,
    pub migration_procedure_id: String,
    pub migration_procedure_state: String,
    pub leader_after_migration: u64,
    pub leader_after_gc: u64,
    pub admin_gc_output: String,
    pub orphan_pressure_file_id: FileId,
    pub orphan_pressure_path: String,
    pub obsolete_source_file_ids_before_gc: HashSet<FileId>,
    pub source_paths_before_gc: BTreeSet<String>,
    pub source_paths_after_gc: BTreeSet<String>,
}

#[derive(Debug)]
pub struct Phase3BuildIndexGcEvidence {
    pub region: RegionId,
    pub manifest_after_build_puffins: BTreeSet<String>,
    pub storage_after_build_puffins: BTreeSet<String>,
    pub manifest_before_gc_puffins: BTreeSet<String>,
    pub storage_before_gc_puffins: BTreeSet<String>,
    pub manifest_after_gc_puffins: BTreeSet<String>,
    pub storage_after_gc_puffins: BTreeSet<String>,
    pub obsolete_puffins_before_gc: BTreeSet<String>,
    pub active_puffins_before_gc: BTreeSet<String>,
    pub reported_deleted_index_puffins: BTreeSet<String>,
    pub fox_count_before_gc: String,
    pub fox_count_after_gc: String,
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
    pub admin_gc_dropped_evidence: Option<Phase3AdminGcDroppedRegionEvidence>,
    pub post_migration_admin_gc_evidence: Option<Phase3PostMigrationAdminGcEvidence>,
    pub build_index_gc_evidence: Option<Phase3BuildIndexGcEvidence>,
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

    if input.scenario_kind == Phase3E2eScenarioKind::RepartitionAdminGcDroppedRegion {
        return run_repartition_admin_gc_dropped_region_scenario(input).await;
    }

    if input.scenario_kind == Phase3E2eScenarioKind::PostMigrationAdminGc {
        return run_post_migration_admin_gc_scenario(input).await;
    }

    if input.scenario_kind == Phase3E2eScenarioKind::BuildIndexGc {
        return run_build_index_gc_scenario(input).await;
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
        | Phase3E2eScenarioKind::RepartitionPreGcProtection
        | Phase3E2eScenarioKind::RepartitionAdminGcDroppedRegion
        | Phase3E2eScenarioKind::PostMigrationAdminGc
        | Phase3E2eScenarioKind::BuildIndexGc => None,
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
        | Phase3E2eScenarioKind::RepartitionPreGcProtection
        | Phase3E2eScenarioKind::RepartitionAdminGcDroppedRegion
        | Phase3E2eScenarioKind::PostMigrationAdminGc
        | Phase3E2eScenarioKind::BuildIndexGc => HashSet::new(),
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
        admin_gc_dropped_evidence: None,
        post_migration_admin_gc_evidence: None,
        build_index_gc_evidence: None,
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
        && evidence.scenario_kind != Phase3E2eScenarioKind::RepartitionAdminGcDroppedRegion
        && evidence.scenario_kind != Phase3E2eScenarioKind::PostMigrationAdminGc
        && evidence.scenario_kind != Phase3E2eScenarioKind::BuildIndexGc
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
        && evidence.scenario_kind != Phase3E2eScenarioKind::RepartitionAdminGcDroppedRegion
        && evidence.scenario_kind != Phase3E2eScenarioKind::PostMigrationAdminGc
        && evidence.scenario_kind != Phase3E2eScenarioKind::BuildIndexGc
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
    validate_admin_gc_dropped_evidence(evidence)?;
    validate_post_migration_admin_gc_evidence(evidence)?;
    validate_build_index_gc_evidence(evidence)?;

    Ok(())
}

fn validate_repartition_evidence(evidence: &Phase3E2eEvidence) -> Result<(), String> {
    match evidence.scenario_kind {
        Phase3E2eScenarioKind::CompactGc
        | Phase3E2eScenarioKind::FollowerLike
        | Phase3E2eScenarioKind::RepartitionPreGcProtection
        | Phase3E2eScenarioKind::RepartitionAdminGcDroppedRegion
        | Phase3E2eScenarioKind::PostMigrationAdminGc
        | Phase3E2eScenarioKind::BuildIndexGc => {
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
        Phase3E2eScenarioKind::RepartitionAdminGcDroppedRegion => {
            "run admin GC_REGIONS on dropped source region"
        }
        Phase3E2eScenarioKind::PostMigrationAdminGc => {
            "run admin GC_REGIONS after region migration"
        }
        Phase3E2eScenarioKind::BuildIndexGc => {
            "run build_index_gc known-gap scenario with BUILD_INDEX + index GC"
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
        admin_gc_dropped_evidence: None,
        post_migration_admin_gc_evidence: None,
        build_index_gc_evidence: None,
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

async fn run_repartition_admin_gc_dropped_region_scenario(
    input: Phase3E2eInput,
) -> Phase3E2eEvidence {
    assert!(
        input.full_file_listing,
        "repartition_admin_gc_dropped_region requires full_file_listing=true"
    );
    assert_eq!(
        input.table_shape,
        Phase3E2eTableShape::MultiRegion,
        "repartition_admin_gc_dropped_region requires a multi-region table"
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

    let dropped_source = regions_before
        .iter()
        .copied()
        .find(|r| !regions_after_set.contains(r))
        .expect("there must be a dropped source region after merge");

    let source_in_route_before = regions_before.contains(&dropped_source);
    let source_in_route_after = regions_after_set.contains(&dropped_source);

    let destination_region = regions_after[0];

    // --- Step 6: Wait until the dropped source is physically gone from datanodes ---
    // This ensures ADMIN GC exercises the dropped-region full-listing path instead
    // of accidentally hitting an active/open region that only disappeared from routes.
    let mut all_regions = cluster.list_all_regions().await;
    for _ in 0..20 {
        if !all_regions.contains_key(&dropped_source) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        all_regions = cluster.list_all_regions().await;
    }
    let source_region_physically_dropped_before_admin_gc =
        !all_regions.contains_key(&dropped_source);
    assert!(
        source_region_physically_dropped_before_admin_gc,
        "dropped source region must be physically removed from datanodes before ADMIN GC; \
         dropped_source={:?} active_regions={:?}",
        dropped_source,
        all_regions.keys().collect::<Vec<_>>(),
    );

    // --- Step 7: Inspect destination manifest for cross-region files ---
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
        "Admin-GC-dropped scenario: dropped_source={:?} destination={:?} protected_file_ids={:?}",
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

    let source_paths_before_gc: BTreeSet<String> = sst_before_gc
        .iter()
        .filter(|path| path_belongs_to_region(path, dropped_source))
        .cloned()
        .collect();

    // --- Step 8: Execute ADMIN GC_REGIONS SQL for the dropped source with full listing ---
    let gc_sql = format!("ADMIN GC_REGIONS({}, true)", dropped_source.as_u64());
    info!("Executing admin GC: {}", gc_sql);
    let gc_output = execute_sql(&instance, &gc_sql).await;
    let admin_gc_output = gc_output.data.pretty_print().await;
    info!("Admin GC output: {}", admin_gc_output);

    // Wait for the GC procedure to fully complete
    tokio::time::sleep(Duration::from_secs(2)).await;

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

    let table_repart_after_gc = table_repart_mgr.get(table_id).await.unwrap();

    let admin_gc_dropped_evidence = Phase3AdminGcDroppedRegionEvidence {
        dropped_source_region: dropped_source,
        destination_region,
        protected_file_ids,
        source_paths_before_gc,
        source_paths_after_gc: source_paths_after_gc.clone(),
        destination_manifest_protected_after_gc,
        source_in_route_before,
        source_in_route_after,
        source_region_physically_dropped_before_admin_gc,
        table_repart_before_gc,
        table_repart_after_gc,
        admin_gc_output: admin_gc_output.clone(),
        orphan_pressure_file_id,
        orphan_pressure_path: orphan_pressure_path.clone(),
    };

    let all_regions_evidence: Vec<RegionId> = regions_before.clone();

    let gc_report = GcReport::default();

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
        pre_gc_protection_evidence: None,
        admin_gc_dropped_evidence: Some(admin_gc_dropped_evidence),
        post_migration_admin_gc_evidence: None,
        build_index_gc_evidence: None,
        follower_required_files: HashSet::new(),
        count_output,
        replay_trace: vec![
            "create multi_region append-mode table".to_string(),
            format!("generate {} flush rounds", input.flush_rounds),
            "compact table to create removable SSTs".to_string(),
            "execute ALTER TABLE MERGE PARTITION (host < 'm', host >= 'm')".to_string(),
            format!(
                "identify dropped source {:?} and destination {:?}",
                dropped_source, destination_region
            ),
            format!(
                "execute ADMIN GC_REGIONS({}, true) for dropped source",
                dropped_source.as_u64()
            ),
            "run admin GC_REGIONS validation checks".to_string(),
        ],
    };

    validate_phase3_e2e_evidence(&evidence)
        .unwrap_or_else(|err| panic!("Phase 3 Admin-GC-Dropped invariant violation: {err}"));

    evidence
}

fn validate_admin_gc_dropped_evidence(evidence: &Phase3E2eEvidence) -> Result<(), String> {
    if evidence.scenario_kind != Phase3E2eScenarioKind::RepartitionAdminGcDroppedRegion {
        if evidence.admin_gc_dropped_evidence.is_some() {
            return Err(format!(
                "non-admin-gc-dropped scenario {} unexpectedly recorded admin GC dropped evidence; {} trace={:?}",
                evidence.scenario_kind.as_seed_value(),
                evidence.concise_summary(),
                evidence.replay_trace,
            ));
        }
        return Ok(());
    }

    let Some(ev) = &evidence.admin_gc_dropped_evidence else {
        return Err(format!(
            "admin_gc_dropped_region scenario missing admin GC dropped evidence; {} trace={:?}",
            evidence.concise_summary(),
            evidence.replay_trace,
        ));
    };

    if !evidence.full_file_listing {
        return Err(format!(
            "admin_gc_dropped_region must use full-listing GC; {} trace={:?}",
            evidence.concise_summary(),
            evidence.replay_trace,
        ));
    }

    // 1. source in route before merge, not after
    if !ev.source_in_route_before {
        return Err(format!(
            "dropped source region {:?} was not in table route before merge; evidence={:?}",
            ev.dropped_source_region, ev,
        ));
    }
    if ev.source_in_route_after {
        return Err(format!(
            "dropped source region {:?} still in table route after merge; evidence={:?}",
            ev.dropped_source_region, ev,
        ));
    }
    if !ev.source_region_physically_dropped_before_admin_gc {
        return Err(format!(
            "dropped source region {:?} was still physically open before ADMIN GC; evidence={:?}",
            ev.dropped_source_region, ev,
        ));
    }

    // 2. table_repart_before_gc contains dropped_source -> destination
    let repart_before = ev
        .table_repart_before_gc
        .as_ref()
        .ok_or_else(|| format!("missing table_repart metadata before GC; evidence={:?}", ev,))?;
    let dst_set_before = repart_before
        .src_to_dst
        .get(&ev.dropped_source_region)
        .ok_or_else(|| {
            format!(
                "table_repart does not contain dropped source {:?}; repart={:?}",
                ev.dropped_source_region, repart_before,
            )
        })?;
    if !dst_set_before.contains(&ev.destination_region) {
        return Err(format!(
            "table_repart for source {:?} does not contain destination {:?}; repart={:?}",
            ev.dropped_source_region, ev.destination_region, repart_before,
        ));
    }

    // 3. destination manifest protected file ids non-empty
    if ev.protected_file_ids.is_empty() {
        return Err(format!(
            "no protected file ids found in destination {:?} manifest; evidence={:?}",
            ev.destination_region, ev,
        ));
    }

    // 4. Before GC: source object-store paths contain both protected ids and orphan id
    let source_before_ids = collect_file_ids_from_paths(ev.source_paths_before_gc.clone())?;
    if !source_before_ids.is_superset(&ev.protected_file_ids) {
        let missing: Vec<_> = ev
            .protected_file_ids
            .difference(&source_before_ids)
            .collect();
        return Err(format!(
            "protected file ids were not present in source object-store paths before GC: {:?}; evidence={:?}",
            missing, ev,
        ));
    }
    if !source_before_ids.contains(&ev.orphan_pressure_file_id) {
        return Err(format!(
            "orphan pressure file {:?} not found in source object-store paths before GC; evidence={:?}",
            ev.orphan_pressure_file_id, ev,
        ));
    }

    // 5. admin GC output indicates at least 1 processed region
    //    The pretty_print output for a single u64 result is a table where
    //    the value "1" appears as a row value.
    if !ev.admin_gc_output.contains("1") {
        return Err(format!(
            "admin GC output does not indicate processed_regions=1; output={}",
            ev.admin_gc_output,
        ));
    }

    // 6. After GC: orphan not in source paths
    let source_after_ids = collect_file_ids_from_paths(ev.source_paths_after_gc.clone())?;
    if source_after_ids.contains(&ev.orphan_pressure_file_id) {
        return Err(format!(
            "orphan pressure file {:?} still present in source object-store after GC; evidence={:?}",
            ev.orphan_pressure_file_id, ev,
        ));
    }

    // 7. After GC: protected ids still in source paths
    if !source_after_ids.is_superset(&ev.protected_file_ids) {
        let missing: Vec<_> = ev
            .protected_file_ids
            .difference(&source_after_ids)
            .collect();
        return Err(format!(
            "protected file ids missing from object store after GC: {:?}; evidence={:?}",
            missing, ev,
        ));
    }

    // 8. Destination manifest after GC still references protected ids
    if !ev
        .destination_manifest_protected_after_gc
        .is_superset(&ev.protected_file_ids)
    {
        let missing: Vec<_> = ev
            .protected_file_ids
            .difference(&ev.destination_manifest_protected_after_gc)
            .collect();
        return Err(format!(
            "destination manifest stopped referencing protected origin-source files after GC: {:?}; evidence={:?}",
            missing, ev,
        ));
    }

    // 9. table_repart_after_gc still contains dropped_source -> destination
    let repart_after = ev
        .table_repart_after_gc
        .as_ref()
        .ok_or_else(|| format!("missing table_repart metadata after GC; evidence={:?}", ev,))?;
    let dst_set_after = repart_after
        .src_to_dst
        .get(&ev.dropped_source_region)
        .ok_or_else(|| {
            format!(
                "table_repart_after_gc does not contain dropped source {:?}; repart={:?}",
                ev.dropped_source_region, repart_after,
            )
        })?;
    if !dst_set_after.contains(&ev.destination_region) {
        return Err(format!(
            "table_repart_after_gc for source {:?} does not contain destination {:?}; repart={:?}",
            ev.dropped_source_region, ev.destination_region, repart_after,
        ));
    }

    // 10. Count still correct (validated in outer validator via count_output check)

    Ok(())
}

async fn run_post_migration_admin_gc_scenario(input: Phase3E2eInput) -> Phase3E2eEvidence {
    assert!(
        input.full_file_listing,
        "post_migration_admin_gc requires full_file_listing=true"
    );
    assert_eq!(
        input.table_shape,
        Phase3E2eTableShape::MultiRegion,
        "post_migration_admin_gc requires a multi-region table"
    );

    common_telemetry::init_default_ut_logging();

    let cluster_name = format!("phase3-gc-e2e-{}", input.seed);
    let (store_config, _store_guard) = get_test_store_config(&StorageType::File);
    let home_dir = Arc::new(create_temp_dir("phase3_gc_e2e_home"));
    let cluster = GreptimeDbClusterBuilder::new(&cluster_name)
        .await
        .with_datanodes(3)
        .with_shared_home_dir(home_dir)
        .with_store_config(store_config)
        .with_metasrv_gc_config(GcSchedulerOptions {
            enable: true,
            ..Default::default()
        })
        .with_datanode_gc_config(GcConfig {
            enable: true,
            // `None` means files tracked in manifest.removed_files are eligible immediately.
            // `Some(Duration::ZERO)` can still race same-timestamp remove records.
            lingering_time: None,
            unknown_file_lingering_time: Duration::ZERO,
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
    let flush_rounds = input.flush_rounds.max(4);
    generate_ssts(
        &instance,
        &table_name,
        flush_rounds,
        Phase3E2eTableShape::MultiRegion,
    )
    .await;

    // --- Step 3: Compact ---
    compact_table(&instance, &table_name, input.compaction_wait_secs).await;

    let regions = get_table_regions(metasrv.table_metadata_manager(), &instance, &table_name).await;
    assert!(
        regions.len() > 1,
        "multi-region table must have more than 1 region, got {:?}",
        regions
    );

    let sst_before_compaction = cluster.list_sst_files_from_all_datanodes().await;

    // --- Step 4: Pick an active region and get its current leader ---
    let table_id = regions[0].table_id();
    let (_, table_route) = metasrv
        .table_metadata_manager()
        .table_route_manager()
        .get_physical_table_route(table_id)
        .await
        .unwrap();

    // Pick the first region that has a leader
    let target_region_route = table_route
        .region_routes
        .iter()
        .find(|r| r.leader_peer.is_some())
        .expect("must have at least one region with a leader");
    let migrated_region = target_region_route.region.id;
    let from_peer = target_region_route
        .leader_peer
        .as_ref()
        .expect("leader peer must be present");
    let from_peer_id = from_peer.id;

    // Pick a different datanode as target (datanode IDs are 1..=3 with 3 datanodes)
    let to_peer_id = if from_peer_id == 1 { 2 } else { 1 };

    info!(
        "Post-migration-admin-gc scenario: migrating region={:?} from={} to={}",
        migrated_region, from_peer_id, to_peer_id,
    );

    // --- Step 5: Execute SQL ADMIN migrate_region ---
    let migrate_sql = format!(
        "ADMIN migrate_region({}, {}, {}, 60)",
        migrated_region.as_u64(),
        from_peer_id,
        to_peer_id
    );
    info!("Executing migration: {}", migrate_sql);
    let migrate_output = execute_sql(&instance, &migrate_sql).await;
    let migration_output = migrate_output.data.pretty_print().await;
    let migration_procedure_id = first_pretty_table_value(&migration_output).unwrap_or_else(|| {
        panic!("failed to parse migration procedure id from {migration_output}")
    });
    info!(
        "Migration procedure submitted: id={} output={}",
        migration_procedure_id, migration_output,
    );

    let migration_procedure_state =
        wait_procedure_done(&instance, &migration_procedure_id, Duration::from_secs(60)).await;
    info!(
        "Migration procedure finished: id={} state={}",
        migration_procedure_id, migration_procedure_state,
    );

    // --- Step 6: Confirm leader changed to to_peer ---
    let wait_start = tokio::time::Instant::now();
    let wait_timeout = Duration::from_secs(30);
    let leader_after_migration;
    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let (_, route) = metasrv
            .table_metadata_manager()
            .table_route_manager()
            .get_physical_table_route(table_id)
            .await
            .unwrap();
        let region_route = route
            .region_routes
            .iter()
            .find(|r| r.region.id == migrated_region)
            .expect("migrated region must still be in table route");
        if let Some(peer) = &region_route.leader_peer {
            if peer.id == to_peer_id {
                leader_after_migration = peer.id;
                info!(
                    "Migration completed: leader for region {:?} is now {:?}",
                    migrated_region, peer,
                );
                break;
            }
        }
        if wait_start.elapsed() > wait_timeout {
            panic!(
                "Timed out waiting for region {:?} leader to become {}; current leader={:?}",
                migrated_region, to_peer_id, region_route.leader_peer,
            );
        }
    }

    // --- Step 7: Verify region still exists in cluster ---
    let all_regions = cluster.list_all_regions().await;
    assert!(
        all_regions.contains_key(&migrated_region),
        "migrated region {:?} must still exist in cluster after migration",
        migrated_region,
    );

    // --- Step 8: Write orphan pressure file to the active migrated region ---
    let (orphan_pressure_file_id, orphan_pressure_path) =
        write_orphan_pressure_file(&cluster, migrated_region).await;

    let sst_before_gc = cluster.list_sst_files_from_all_datanodes().await;
    let manifest_before_gc = cluster.list_sst_files_from_manifests().await;

    let source_paths_before_gc: BTreeSet<String> = sst_before_gc
        .iter()
        .filter(|path| path_belongs_to_region(path, migrated_region))
        .cloned()
        .collect();
    let source_before_ids = collect_file_ids_from_paths(source_paths_before_gc.clone()).unwrap();
    let manifest_before_ids = collect_file_ids_from_paths(manifest_before_gc).unwrap();
    let mut obsolete_source_file_ids_before_gc: HashSet<FileId> = source_before_ids
        .difference(&manifest_before_ids)
        .copied()
        .collect();
    obsolete_source_file_ids_before_gc.remove(&orphan_pressure_file_id);
    assert!(
        !obsolete_source_file_ids_before_gc.is_empty(),
        "post-migration scenario must create compacted obsolete source files before GC; \
         migrated_region={:?} source_before={:?} manifest_before={:?} orphan={:?}",
        migrated_region,
        source_before_ids,
        manifest_before_ids,
        orphan_pressure_file_id,
    );

    // --- Step 9: Execute ADMIN GC_REGIONS SQL ---
    let gc_sql = format!("ADMIN GC_REGIONS({}, true)", migrated_region.as_u64());
    info!("Executing admin GC after migration: {}", gc_sql);
    let gc_output = execute_sql(&instance, &gc_sql).await;
    let admin_gc_output = gc_output.data.pretty_print().await;
    info!("Admin GC output: {}", admin_gc_output);

    // Wait for the GC procedure to fully complete
    tokio::time::sleep(Duration::from_secs(2)).await;

    // --- Step 10: Collect post-GC evidence ---
    let sst_after_gc = cluster.list_sst_files_from_all_datanodes().await;
    let manifest_after_gc = cluster.list_sst_files_from_manifests().await;
    let count_output = query_count_output(&instance, &table_name).await;

    let source_paths_after_gc: BTreeSet<String> = sst_after_gc
        .iter()
        .filter(|path| path_belongs_to_region(path, migrated_region))
        .cloned()
        .collect();

    // Verify leader still to_peer after GC
    let (_, route_after_gc) = metasrv
        .table_metadata_manager()
        .table_route_manager()
        .get_physical_table_route(table_id)
        .await
        .unwrap();
    let region_route_after_gc = route_after_gc
        .region_routes
        .iter()
        .find(|r| r.region.id == migrated_region)
        .expect("migrated region must still be in table route after GC");
    let leader_after_gc = region_route_after_gc
        .leader_peer
        .as_ref()
        .map(|p| p.id)
        .unwrap_or(0);

    let post_migration_admin_gc_evidence = Phase3PostMigrationAdminGcEvidence {
        migrated_region,
        from_peer_id,
        to_peer_id,
        migration_procedure_id,
        migration_procedure_state,
        leader_after_migration,
        leader_after_gc,
        admin_gc_output: admin_gc_output.clone(),
        orphan_pressure_file_id,
        orphan_pressure_path: orphan_pressure_path.clone(),
        obsolete_source_file_ids_before_gc,
        source_paths_before_gc: source_paths_before_gc.clone(),
        source_paths_after_gc: source_paths_after_gc.clone(),
    };

    let all_regions_evidence: Vec<RegionId> = regions.clone();
    let gc_report = GcReport::default();

    let evidence = Phase3E2eEvidence {
        target_name: "fuzz_gc_e2e_cross_region",
        seed: input.seed,
        flush_rounds,
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
        pre_gc_protection_evidence: None,
        admin_gc_dropped_evidence: None,
        post_migration_admin_gc_evidence: Some(post_migration_admin_gc_evidence),
        build_index_gc_evidence: None,
        follower_required_files: HashSet::new(),
        count_output,
        replay_trace: vec![
            "create multi_region append-mode table".to_string(),
            format!("generate {} flush rounds", flush_rounds),
            "compact table".to_string(),
            format!(
                "migrate region {:?} from peer {} to peer {}",
                migrated_region, from_peer_id, to_peer_id
            ),
            format!(
                "execute ADMIN GC_REGIONS({}, true)",
                migrated_region.as_u64()
            ),
            "run post-migration admin GC validation checks".to_string(),
        ],
    };

    validate_phase3_e2e_evidence(&evidence)
        .unwrap_or_else(|err| panic!("Phase 3 Post-Migration Admin GC invariant violation: {err}"));

    evidence
}

fn validate_post_migration_admin_gc_evidence(evidence: &Phase3E2eEvidence) -> Result<(), String> {
    if evidence.scenario_kind != Phase3E2eScenarioKind::PostMigrationAdminGc {
        if evidence.post_migration_admin_gc_evidence.is_some() {
            return Err(format!(
                "non-post-migration scenario {} unexpectedly recorded post-migration-admin-gc evidence; {} trace={:?}",
                evidence.scenario_kind.as_seed_value(),
                evidence.concise_summary(),
                evidence.replay_trace,
            ));
        }
        return Ok(());
    }

    let Some(ev) = &evidence.post_migration_admin_gc_evidence else {
        return Err(format!(
            "post_migration_admin_gc scenario missing post-migration admin GC evidence; {} trace={:?}",
            evidence.concise_summary(),
            evidence.replay_trace,
        ));
    };

    if !evidence.full_file_listing {
        return Err(format!(
            "post_migration_admin_gc must use full-listing GC; {} trace={:?}",
            evidence.concise_summary(),
            evidence.replay_trace,
        ));
    }

    // 1. Migration leader is to_peer
    if ev.leader_after_migration != ev.to_peer_id {
        return Err(format!(
            "region {:?} leader after migration {} != to_peer {}; evidence={:?}",
            ev.migrated_region, ev.leader_after_migration, ev.to_peer_id, ev,
        ));
    }
    if !ev.migration_procedure_state.contains("Done") {
        return Err(format!(
            "migration procedure {} did not finish successfully before GC; state={}; evidence={:?}",
            ev.migration_procedure_id, ev.migration_procedure_state, ev,
        ));
    }
    if ev.leader_after_gc != ev.to_peer_id {
        return Err(format!(
            "region {:?} leader after GC {} != to_peer {}; evidence={:?}",
            ev.migrated_region, ev.leader_after_gc, ev.to_peer_id, ev,
        ));
    }

    // 2. admin GC output indicates at least 1 processed region
    if !ev.admin_gc_output.contains("1") {
        return Err(format!(
            "admin GC output does not indicate processed_regions=1; output={}",
            ev.admin_gc_output,
        ));
    }

    // 3. orphan exists before GC
    let source_before_ids = collect_file_ids_from_paths(ev.source_paths_before_gc.clone())?;
    if !source_before_ids.contains(&ev.orphan_pressure_file_id) {
        return Err(format!(
            "orphan pressure file {:?} not found before GC; evidence={:?}",
            ev.orphan_pressure_file_id, ev,
        ));
    }
    if ev.obsolete_source_file_ids_before_gc.is_empty() {
        return Err(format!(
            "post-migration scenario did not record compacted obsolete source files before GC; evidence={:?}",
            ev,
        ));
    }
    if !ev
        .obsolete_source_file_ids_before_gc
        .is_subset(&source_before_ids)
    {
        return Err(format!(
            "recorded obsolete source files were not present before GC; obsolete={:?} before={:?}; evidence={:?}",
            ev.obsolete_source_file_ids_before_gc, source_before_ids, ev,
        ));
    }

    // 4. The post-migration active-region GC should still do real deletion work
    // by removing compacted obsolete SSTs. Unknown orphan files are recorded as
    // a probe but are not required to be deleted for active regions.
    let source_after_ids = collect_file_ids_from_paths(ev.source_paths_after_gc.clone())?;
    let deleted_source_ids: HashSet<FileId> = source_before_ids
        .difference(&source_after_ids)
        .copied()
        .collect();
    if deleted_source_ids.is_empty() {
        return Err(format!(
            "post-migration ADMIN GC did not delete any source-region files; before={:?} after={:?}; evidence={:?}",
            source_before_ids, source_after_ids, ev,
        ));
    }
    let deleted_obsolete_ids: HashSet<FileId> = deleted_source_ids
        .intersection(&ev.obsolete_source_file_ids_before_gc)
        .copied()
        .collect();
    if deleted_obsolete_ids.is_empty() {
        return Err(format!(
            "post-migration ADMIN GC did not delete recorded compacted obsolete files; obsolete={:?} deleted={:?}; evidence={:?}",
            ev.obsolete_source_file_ids_before_gc, deleted_source_ids, ev,
        ));
    }
    if !source_after_ids.contains(&ev.orphan_pressure_file_id) {
        return Err(format!(
            "active-region orphan probe {:?} was unexpectedly deleted; deleted={:?}; evidence={:?}",
            ev.orphan_pressure_file_id, deleted_source_ids, ev,
        ));
    }

    // 5. manifest-reachable file ids after GC are all in object-store after GC
    let manifest_after_ids = collect_file_ids_from_paths(evidence.manifest_after_gc.clone())?;
    let object_store_after_ids = collect_file_ids_from_paths(evidence.sst_after_gc.clone())?;
    if !manifest_after_ids.is_subset(&object_store_after_ids) {
        let missing: Vec<_> = manifest_after_ids
            .difference(&object_store_after_ids)
            .collect();
        return Err(format!(
            "manifest-reachable files missing from object store after GC: {:?}; {} trace={:?}",
            missing,
            evidence.concise_summary(),
            evidence.replay_trace,
        ));
    }

    // 6. Row count still correct (checked in outer validator)

    Ok(())
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

fn first_pretty_table_value(output: &str) -> Option<String> {
    output
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if !line.starts_with('|') {
                return None;
            }
            let cells = line
                .trim_matches('|')
                .split('|')
                .map(str::trim)
                .collect::<Vec<_>>();
            let value = cells.first().copied().unwrap_or_default();
            let lower_value = value.to_ascii_lowercase();
            if value.is_empty()
                || lower_value.contains("migrate_region")
                || lower_value.contains("procedure_state")
                || lower_value.contains("gc_regions")
            {
                None
            } else {
                Some(value.to_string())
            }
        })
        .next()
}

async fn wait_procedure_done(
    instance: &Arc<frontend::instance::Instance>,
    procedure_id: &str,
    timeout: Duration,
) -> String {
    let wait_start = tokio::time::Instant::now();
    loop {
        let sql = format!("ADMIN procedure_state('{procedure_id}')");
        let output = execute_sql(instance, &sql).await.data.pretty_print().await;
        let state = first_pretty_table_value(&output)
            .unwrap_or_else(|| panic!("failed to parse procedure_state output: {output}"));
        if state.contains("\"status\":\"Done\"") || state.contains("Done") {
            return state;
        }
        if state.contains("Failed")
            || state.contains("Retrying")
            || state.contains("RollingBack")
            || state.contains("PrepareRollback")
            || state.contains("Poisoned")
        {
            panic!("procedure {procedure_id} entered non-success state: {state}");
        }
        assert!(
            wait_start.elapsed() <= timeout,
            "timed out waiting for procedure {procedure_id} to finish; last state={state}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

// ---------------------------------------------------------------------------
//  BuildIndexGc scenario helpers
// ---------------------------------------------------------------------------

/// Filter a set of file paths to only those ending with `.puffin`.
fn puffin_paths(paths: &BTreeSet<String>) -> BTreeSet<String> {
    paths
        .iter()
        .filter(|p| p.ends_with(".puffin"))
        .cloned()
        .collect()
}

/// Poll until manifest and storage both contain at least one `.puffin` file
/// and every manifest puffin is also present in storage (manifest subset storage).
async fn wait_for_build_index_puffins(
    cluster: &GreptimeDbCluster,
    min_manifest_puffins: usize,
    timeout: Duration,
) -> (BTreeSet<String>, BTreeSet<String>) {
    let start = tokio::time::Instant::now();
    loop {
        let manifest = cluster.list_sst_files_from_manifests().await;
        let storage = cluster.list_sst_files_from_all_datanodes().await;
        let mp = puffin_paths(&manifest);
        let sp = puffin_paths(&storage);

        if mp.len() >= min_manifest_puffins && mp.is_subset(&sp) {
            return (mp, sp);
        }

        if start.elapsed() > timeout {
            panic!(
                "timed out waiting for at least {min_manifest_puffins} build-index puffins; manifest={:?} storage={:?}",
                mp, sp,
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Execute `SELECT COUNT(*) FROM {table} WHERE msg @@ 'fox'` and return the
/// pretty-printed output.
async fn query_fox_count_output(
    instance: &Arc<frontend::instance::Instance>,
    table_name: &str,
) -> String {
    let sql = format!("SELECT COUNT(*) FROM {table_name} WHERE msg @@ 'fox'");
    execute_sql(instance, &sql).await.data.pretty_print().await
}

/// Check whether a GreptimeDB pretty-printed table output contains a specific
/// expected value as one of its data cells (not just a fragile substring).
fn pretty_table_contains_cell(output: &str, expected: &str) -> bool {
    for line in output.lines() {
        let line = line.trim();
        if !line.starts_with('|') {
            continue;
        }
        for cell in line.trim_matches('|').split('|') {
            if cell.trim() == expected {
                return true;
            }
        }
    }
    false
}

/// Poll until the manifest and storage have diverging `.puffin` files:
/// active (v1) puffins in manifest are non-empty, and obsolete (v0) puffins
/// exist only in storage. Returns (manifest_puffins, storage_puffins, all_storage_files).
async fn wait_for_obsolete_index_puffins(
    cluster: &GreptimeDbCluster,
    timeout: Duration,
) -> (BTreeSet<String>, BTreeSet<String>, BTreeSet<String>) {
    let start = tokio::time::Instant::now();
    loop {
        let manifest = cluster.list_sst_files_from_manifests().await;
        let storage = cluster.list_sst_files_from_all_datanodes().await;
        let manifest_puffins = puffin_paths(&manifest);
        let storage_puffins = puffin_paths(&storage);
        let obsolete_puffins: BTreeSet<String> = storage_puffins
            .difference(&manifest_puffins)
            .cloned()
            .collect();

        if !manifest_puffins.is_empty()
            && !obsolete_puffins.is_empty()
            && manifest_puffins.is_subset(&storage_puffins)
        {
            return (manifest_puffins, storage_puffins, storage);
        }

        if start.elapsed() > timeout {
            panic!(
                "timed out waiting for obsolete build-index puffins; manifest={:?} storage={:?} obsolete={:?}",
                manifest_puffins, storage_puffins, obsolete_puffins,
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Extracts the set of `.puffin` paths that were reported as deleted in
/// `GcReport.deleted_indexes` for `region_id`, with path/version precision.
fn reported_deleted_index_paths(
    report: &GcReport,
    region_id: RegionId,
    candidate_puffins: &BTreeSet<String>,
) -> Result<BTreeSet<String>, String> {
    let deleted = report
        .deleted_indexes
        .get(&region_id)
        .cloned()
        .unwrap_or_default();
    let deleted_set: HashSet<(FileId, u64)> = deleted.into_iter().collect();

    let mut result = BTreeSet::new();
    for path in candidate_puffins {
        if !path_belongs_to_region(path, region_id) {
            continue;
        }
        match parse_index_file_info(path) {
            Ok((file_id, version)) => {
                if deleted_set.contains(&(file_id, version)) {
                    result.insert(path.clone());
                }
            }
            Err(_) => {
                // Skip paths that cannot be parsed as index files
            }
        }
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
//  run_build_index_gc_scenario
// ---------------------------------------------------------------------------
//
// Flow (oracle-reviewed):
//   1. CREATE single-region append-mode table with msg STRING (no index preset)
//   2. INSERT / ADMIN FLUSH_TABLE for flush_rounds
//   3. ALTER TABLE … SET INVERTED INDEX
//   4. First ADMIN BUILD_INDEX  →  creates v0 .puffin files
//   5. wait_for_build_index_puffins
//   6. ALTER TABLE … SET FULLTEXT INDEX WITH (analyzer='English', case_sensitive='false')
//   7. Second ADMIN BUILD_INDEX → creates v1 files; v0 files become RemovedFile::Index
//   8. wait_for_obsolete_index_puffins (storage \ manifest non-empty)
//   9. Run full-listing batch GC
//  10. Validate strict invariants (deleted_indexes non-empty, v0 removed, v1 preserved)
async fn run_build_index_gc_scenario(input: Phase3E2eInput) -> Phase3E2eEvidence {
    assert!(
        input.full_file_listing,
        "build_index_gc requires full_file_listing=true"
    );
    assert_eq!(
        input.table_shape,
        Phase3E2eTableShape::SingleRegion,
        "build_index_gc requires a single-region table"
    );

    common_telemetry::init_default_ut_logging();

    let flush_rounds = input.flush_rounds.max(2);

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
            unknown_file_lingering_time: Duration::ZERO,
            ..Default::default()
        })
        .with_datanode_wal_config(DatanodeWalConfig::Noop)
        .build(false)
        .await;

    let instance = cluster.fe_instance().clone();
    let metasrv = cluster.metasrv.clone();
    let table_name = format!("phase3_gc_table_{}", input.seed);

    // --- Step 1: Create append-mode table (no index preset) ---
    let create_sql = format!(
        r#"
        CREATE TABLE {table_name} (
            ts TIMESTAMP TIME INDEX,
            msg STRING
        ) WITH (append_mode = 'true')
        "#,
    );
    let _ = execute_sql(&instance, &create_sql).await;

    // --- Step 2: Insert data with `fox` keyword and flush ---
    for i in 0..flush_rounds {
        let day = i + 1;
        let insert_sql = format!(
            r#"
            INSERT INTO {table_name} (ts, msg) VALUES
            ('2023-01-{day:02} 10:00:00', 'the quick brown fox jumps'),
            ('2023-01-{day:02} 11:00:00', 'lazy dog sleeps'),
            ('2023-01-{day:02} 12:00:00', 'another fox appears')
            "#,
        );
        let _ = execute_sql(&instance, &insert_sql).await;
        let flush_sql = format!("ADMIN FLUSH_TABLE('{table_name}')");
        let _ = execute_sql(&instance, &flush_sql).await;
    }

    // --- Step 3: Set INVERTED INDEX (not FULLTEXT yet) ---
    let alter_inverted_sql =
        format!("ALTER TABLE {table_name} MODIFY COLUMN msg SET INVERTED INDEX");
    let _ = execute_sql(&instance, &alter_inverted_sql).await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    // --- Step 4: First BUILD_INDEX → creates v0 .puffin files ---
    let build_index_sql = format!("ADMIN BUILD_INDEX('{table_name}')");
    let _ = execute_sql(&instance, &build_index_sql).await;

    // --- Step 5: Wait for v0 .puffin files ---
    let (manifest_after_build_puffins, storage_after_build_puffins) =
        wait_for_build_index_puffins(&cluster, 1, Duration::from_secs(120)).await;
    info!(
        "BuildIndexGc: after first BUILD_INDEX (INVERTED, v0) — manifest={:?} storage={:?}",
        manifest_after_build_puffins, storage_after_build_puffins,
    );

    // --- Step 6: Switch to FULLTEXT INDEX with options (creates space for version bump) ---
    let sst_before_compaction = cluster.list_sst_files_from_all_datanodes().await;
    let alter_fulltext_sql = format!(
        "ALTER TABLE {table_name} MODIFY COLUMN msg SET FULLTEXT INDEX WITH(analyzer = 'English', case_sensitive = 'false')",
    );
    let _ = execute_sql(&instance, &alter_fulltext_sql).await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    // --- Step 7: Second BUILD_INDEX → creates v1 .puffin, v0 becomes RemovedFile::Index ---
    let _ = execute_sql(&instance, &build_index_sql).await;

    // --- Step 8: Wait until storage has orphan (v0) puffins not referenced by manifest ---
    let (manifest_before_gc_puffins, storage_before_gc_puffins, sst_after_compaction) =
        wait_for_obsolete_index_puffins(&cluster, Duration::from_secs(120)).await;

    let obsolete_puffins_before_gc: BTreeSet<String> = storage_before_gc_puffins
        .difference(&manifest_before_gc_puffins)
        .cloned()
        .collect();
    let active_puffins_before_gc: BTreeSet<String> = manifest_before_gc_puffins.clone();

    info!(
        "BuildIndexGc: before GC — manifest(v1)={manifest_before_gc_puffins:?} \
         storage={storage_before_gc_puffins:?} obsolete(v0)={obsolete_puffins_before_gc:?}",
    );

    // --- Step 9: Pre-GC fox count ---
    let fox_count_before_gc = query_fox_count_output(&instance, &table_name).await;

    // --- Step 10: Run batch GC ---
    let regions = get_table_regions(metasrv.table_metadata_manager(), &instance, &table_name).await;
    assert_expected_region_shape(Phase3E2eTableShape::SingleRegion, &regions);
    let region = regions[0];

    let gc_report = run_batch_gc(&cluster, regions.clone(), true).await;

    // --- Step 11: Post-GC collection ---
    let manifest_after_gc_all = cluster.list_sst_files_from_manifests().await;
    let sst_after_gc_all = cluster.list_sst_files_from_all_datanodes().await;

    let manifest_after_gc_puffins = puffin_paths(&manifest_after_gc_all);
    let storage_after_gc_puffins = puffin_paths(&sst_after_gc_all);
    let fox_count_after_gc = query_fox_count_output(&instance, &table_name).await;
    let total_count_output = query_count_output(&instance, &table_name).await;

    let candidate_puffins_before_gc = {
        let mut s = storage_before_gc_puffins.clone();
        s.extend(manifest_before_gc_puffins.iter().cloned());
        s
    };
    let reported_deleted_index_puffins =
        reported_deleted_index_paths(&gc_report, region, &candidate_puffins_before_gc)
            .unwrap_or_else(|err| panic!("failed to resolve reported deleted index paths: {err}"));

    info!(
        "BuildIndexGc: after GC — manifest={manifest_after_gc_puffins:?} \
         storage={storage_after_gc_puffins:?} reported_deleted={reported_deleted_index_puffins:?}",
    );

    let build_index_gc_evidence = Phase3BuildIndexGcEvidence {
        region,
        manifest_after_build_puffins: manifest_after_build_puffins.clone(),
        storage_after_build_puffins: storage_after_build_puffins.clone(),
        manifest_before_gc_puffins: manifest_before_gc_puffins.clone(),
        storage_before_gc_puffins: storage_before_gc_puffins.clone(),
        manifest_after_gc_puffins: manifest_after_gc_puffins.clone(),
        storage_after_gc_puffins: storage_after_gc_puffins.clone(),
        obsolete_puffins_before_gc: obsolete_puffins_before_gc.clone(),
        active_puffins_before_gc: active_puffins_before_gc.clone(),
        reported_deleted_index_puffins: reported_deleted_index_puffins.clone(),
        fox_count_before_gc: fox_count_before_gc.clone(),
        fox_count_after_gc: fox_count_after_gc.clone(),
    };

    let evidence = Phase3E2eEvidence {
        target_name: "fuzz_gc_e2e_cross_region",
        seed: input.seed,
        flush_rounds,
        full_file_listing: input.full_file_listing,
        compaction_wait_secs: input.compaction_wait_secs,
        table_shape: input.table_shape,
        scenario_kind: input.scenario_kind,
        table_name: table_name.clone(),
        regions: regions.clone(),
        sst_before_compaction,
        sst_after_compaction,
        sst_after_gc: sst_after_gc_all,
        manifest_after_gc: manifest_after_gc_all,
        gc_report: gc_report.clone(),
        repartition_evidence: None,
        pre_gc_protection_evidence: None,
        admin_gc_dropped_evidence: None,
        post_migration_admin_gc_evidence: None,
        build_index_gc_evidence: Some(build_index_gc_evidence),
        follower_required_files: HashSet::new(),
        count_output: total_count_output,
        replay_trace: vec![
            "create append-mode table with msg STRING".to_string(),
            format!("insert {flush_rounds} flush rounds (2 fox rows each)"),
            "ALTER TABLE SET INVERTED INDEX".to_string(),
            "ADMIN BUILD_INDEX (v0)".to_string(),
            "ALTER TABLE SET FULLTEXT INDEX WITH(analyzer='English', case_sensitive='false')"
                .to_string(),
            "ADMIN BUILD_INDEX (v1, obsolete v0)".to_string(),
            "wait for obsolete index puffins".to_string(),
            format!(
                "run BatchGcProcedure(full_file_listing={})",
                input.full_file_listing
            ),
            "capture post-GC state and fox count".to_string(),
        ],
    };

    validate_phase3_e2e_evidence(&evidence)
        .unwrap_or_else(|err| panic!("Phase 3 BuildIndexGc invariant violation: {err}"));

    evidence
}

// ---------------------------------------------------------------------------
//  validate_build_index_gc_evidence
// ---------------------------------------------------------------------------

fn validate_build_index_gc_evidence(evidence: &Phase3E2eEvidence) -> Result<(), String> {
    if evidence.scenario_kind != Phase3E2eScenarioKind::BuildIndexGc {
        if evidence.build_index_gc_evidence.is_some() {
            return Err(format!(
                "non-build-index-gc scenario {} unexpectedly recorded build-index-gc evidence; {} trace={:?}",
                evidence.scenario_kind.as_seed_value(),
                evidence.concise_summary(),
                evidence.replay_trace,
            ));
        }
        return Ok(());
    }

    let Some(ev) = &evidence.build_index_gc_evidence else {
        return Err(format!(
            "build_index_gc scenario missing build-index-gc evidence; {} trace={:?}",
            evidence.concise_summary(),
            evidence.replay_trace,
        ));
    };

    // (1) regions.len() == 1
    if evidence.regions.len() != 1 {
        return Err(format!(
            "build_index_gc must have exactly 1 region; got {:?}",
            evidence.regions,
        ));
    }

    // (2) need_retry_regions is empty and processed_regions contains the region
    if !evidence.gc_report.need_retry_regions.is_empty() {
        return Err(format!(
            "build_index_gc must have empty need_retry_regions; got {:?}",
            evidence.gc_report.need_retry_regions,
        ));
    }
    if !evidence.gc_report.processed_regions.contains(&ev.region) {
        return Err(format!(
            "build_index_gc processed_regions must contain {:?}; got {:?}",
            ev.region, evidence.gc_report.processed_regions,
        ));
    }

    // (3) after build manifest/storage puffins non-empty and manifest subset storage
    if ev.manifest_after_build_puffins.is_empty() {
        return Err(format!(
            "build_index_gc manifest_after_build_puffins is empty; evidence={:?}",
            ev,
        ));
    }
    if ev.storage_after_build_puffins.is_empty() {
        return Err(format!(
            "build_index_gc storage_after_build_puffins is empty; evidence={:?}",
            ev,
        ));
    }
    if !ev
        .manifest_after_build_puffins
        .is_subset(&ev.storage_after_build_puffins)
    {
        return Err(format!(
            "build_index_gc manifest_after_build not subset of storage_after_build; manifest={:?} storage={:?}",
            ev.manifest_after_build_puffins, ev.storage_after_build_puffins,
        ));
    }

    // (4) before GC: active (manifest) v1 puffins and obsolete (storage-only) v0 puffins are non-empty.
    if ev.active_puffins_before_gc.is_empty() {
        return Err(format!(
            "build_index_gc active_puffins_before_gc (manifest, v1) is empty; evidence={:?}",
            ev,
        ));
    }
    if ev.obsolete_puffins_before_gc.is_empty() {
        return Err(format!(
            "build_index_gc obsolete_puffins_before_gc (storage-only, v0) is empty; \
             the second BUILD_INDEX did not produce version bumps; evidence={:?}",
            ev,
        ));
    }

    // (5) gc_report.deleted_indexes must contain a non-empty entry for this region.
    let has_deleted_index = evidence
        .gc_report
        .deleted_indexes
        .get(&ev.region)
        .map(|v| !v.is_empty())
        .unwrap_or(false);
    if !has_deleted_index {
        return Err(format!(
            "build_index_gc expects non-empty deleted_indexes for region {:?}; \
             got deleted_indexes={:?}",
            ev.region, evidence.gc_report.deleted_indexes,
        ));
    }

    // (6) reported_deleted_index_puffins must be non-empty and derived exclusively
    // from GcReport.deleted_indexes via parse_index_file_info (no storage-diff fallback).
    if ev.reported_deleted_index_puffins.is_empty() {
        return Err(format!(
            "build_index_gc reported_deleted_index_puffins is empty; \
             deleted_indexes is non-empty but path mapping produced no results; \
             report={:?}",
            evidence.gc_report,
        ));
    }

    // (7) reported_deleted_index_puffins must intersect obsolete (v0) puffins.
    let intersect_obsolete: BTreeSet<_> = ev
        .reported_deleted_index_puffins
        .intersection(&ev.obsolete_puffins_before_gc)
        .cloned()
        .collect();
    if intersect_obsolete.is_empty() {
        return Err(format!(
            "build_index_gc reported_deleted does not intersect obsolete (v0); \
             reported={:?} obsolete={:?}",
            ev.reported_deleted_index_puffins, ev.obsolete_puffins_before_gc,
        ));
    }

    // (8) reported_deleted_index_puffins must NOT intersect active (v1) puffins.
    let intersect_active: BTreeSet<_> = ev
        .reported_deleted_index_puffins
        .intersection(&ev.active_puffins_before_gc)
        .cloned()
        .collect();
    if !intersect_active.is_empty() {
        return Err(format!(
            "build_index_gc reported_deleted_index_puffins intersects active_puffins_before_gc (v1); \
             intersection={:?}; evidence={:?}",
            intersect_active, ev,
        ));
    }

    // (9) At least one obsolete (v0) puffin must have been removed from storage.
    let removed_obsolete: BTreeSet<_> = ev
        .obsolete_puffins_before_gc
        .difference(&ev.storage_after_gc_puffins)
        .cloned()
        .collect();
    if removed_obsolete.is_empty() {
        return Err(format!(
            "build_index_gc no obsolete (v0) puffins were removed from storage; \
             obsolete_before={:?} storage_after={:?}",
            ev.obsolete_puffins_before_gc, ev.storage_after_gc_puffins,
        ));
    }

    // (10) active_puffins_before_gc subset of storage_after_gc_puffins
    if !ev
        .active_puffins_before_gc
        .is_subset(&ev.storage_after_gc_puffins)
    {
        let missing: Vec<_> = ev
            .active_puffins_before_gc
            .difference(&ev.storage_after_gc_puffins)
            .collect();
        return Err(format!(
            "build_index_gc active puffins missing from storage after GC: {:?}; evidence={:?}",
            missing, ev,
        ));
    }

    // (11) manifest_after_gc_puffins subset storage_after_gc_puffins
    if !ev
        .manifest_after_gc_puffins
        .is_subset(&ev.storage_after_gc_puffins)
    {
        let missing: Vec<_> = ev
            .manifest_after_gc_puffins
            .difference(&ev.storage_after_gc_puffins)
            .collect();
        return Err(format!(
            "build_index_gc manifest puffins missing from storage after GC: {:?}; evidence={:?}",
            missing, ev,
        ));
    }

    // (12) For single-region full-listing, manifest_after_gc must equal sst_after_gc.
    if evidence.manifest_after_gc != evidence.sst_after_gc {
        return Err(format!(
            "build_index_gc manifest_after_gc / sst_after_gc sets diverged; {} trace={:?}",
            evidence.concise_summary(),
            evidence.replay_trace,
        ));
    }

    // (13) fox_count_before_gc and fox_count_after_gc contain expected value
    let expected_fox = (evidence.flush_rounds * 2).to_string();
    if !pretty_table_contains_cell(&ev.fox_count_before_gc, &expected_fox) {
        return Err(format!(
            "build_index_gc fox_count_before_gc does not contain expected '{}'; output=\n{}",
            expected_fox, ev.fox_count_before_gc,
        ));
    }
    if !pretty_table_contains_cell(&ev.fox_count_after_gc, &expected_fox) {
        return Err(format!(
            "build_index_gc fox_count_after_gc does not contain expected '{}'; output=\n{}",
            expected_fox, ev.fox_count_after_gc,
        ));
    }

    Ok(())
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
    async fn test_phase3_e2e_repartition_admin_gc_dropped_region() {
        let evidence = run_phase3_e2e_gc_cycle(Phase3E2eInput {
            seed: 71,
            flush_rounds: 4,
            full_file_listing: true,
            compaction_wait_secs: 2,
            table_shape: Phase3E2eTableShape::MultiRegion,
            scenario_kind: Phase3E2eScenarioKind::RepartitionAdminGcDroppedRegion,
        })
        .await;

        let ev = evidence
            .admin_gc_dropped_evidence
            .as_ref()
            .expect("admin GC dropped evidence must be present");

        // source was in route before merge, not after
        assert!(ev.source_in_route_before);
        assert!(!ev.source_in_route_after);

        // protected file ids non-empty
        assert!(!ev.protected_file_ids.is_empty());

        // table_repart before GC contains dropped_source -> destination
        let repart_before = ev.table_repart_before_gc.as_ref().unwrap();
        let destinations = repart_before
            .src_to_dst
            .get(&ev.dropped_source_region)
            .expect("table_repart must contain dropped source");
        assert!(destinations.contains(&ev.destination_region));

        // admin GC output indicates processed_regions >= 1
        assert!(
            ev.admin_gc_output.contains("1"),
            "admin GC output should indicate processed_regions=1, got: {}",
            ev.admin_gc_output
        );

        // orphan was deleted by GC
        let source_after_ids =
            collect_file_ids_from_paths(ev.source_paths_after_gc.clone()).unwrap();
        assert!(
            !source_after_ids.contains(&ev.orphan_pressure_file_id),
            "orphan pressure file {:?} must have been removed by GC",
            ev.orphan_pressure_file_id,
        );

        // protected files still exist after GC
        assert!(
            source_after_ids.is_superset(&ev.protected_file_ids),
            "protected file ids must still exist in object store after GC"
        );

        // destination manifest after GC still references protected ids
        assert!(
            ev.destination_manifest_protected_after_gc
                .is_superset(&ev.protected_file_ids),
            "destination manifest must still reference protected origin-source files after GC"
        );

        // table_repart_after_gc still contains dropped_source -> destination
        let repart_after = ev.table_repart_after_gc.as_ref().unwrap();
        let destinations_after = repart_after
            .src_to_dst
            .get(&ev.dropped_source_region)
            .expect("table_repart_after_gc must contain dropped source");
        assert!(destinations_after.contains(&ev.destination_region));

        // count still correct
        assert!(evidence.count_output.contains("16"));
    }

    #[tokio::test]
    async fn test_phase3_e2e_post_migration_admin_gc() {
        let evidence = run_phase3_e2e_gc_cycle(Phase3E2eInput {
            seed: 81,
            flush_rounds: 4,
            full_file_listing: true,
            compaction_wait_secs: 2,
            table_shape: Phase3E2eTableShape::MultiRegion,
            scenario_kind: Phase3E2eScenarioKind::PostMigrationAdminGc,
        })
        .await;

        let ev = evidence
            .post_migration_admin_gc_evidence
            .as_ref()
            .expect("post-migration admin GC evidence must be present");

        // leader after migration is to_peer
        assert_eq!(ev.leader_after_migration, ev.to_peer_id);
        assert_eq!(ev.leader_after_gc, ev.to_peer_id);

        // admin GC output indicates processed_regions >= 1
        assert!(
            ev.admin_gc_output.contains("1"),
            "admin GC output should indicate processed_regions=1, got: {}",
            ev.admin_gc_output
        );

        // Active-region GC should still remove compacted obsolete SSTs after migration.
        let source_before_ids =
            collect_file_ids_from_paths(ev.source_paths_before_gc.clone()).unwrap();
        let source_after_ids =
            collect_file_ids_from_paths(ev.source_paths_after_gc.clone()).unwrap();
        assert!(!ev.obsolete_source_file_ids_before_gc.is_empty());
        let deleted_source_ids: HashSet<FileId> = source_before_ids
            .difference(&source_after_ids)
            .copied()
            .collect();
        let deleted_obsolete_ids: HashSet<FileId> = deleted_source_ids
            .intersection(&ev.obsolete_source_file_ids_before_gc)
            .copied()
            .collect();
        assert!(
            !deleted_obsolete_ids.is_empty(),
            "post-migration GC should delete at least one compacted obsolete source-region file"
        );
        assert!(
            source_after_ids.contains(&ev.orphan_pressure_file_id),
            "active-region orphan probe should remain until active unknown-file GC semantics change"
        );

        // manifest-reachable files all still exist in object store after GC
        let manifest_after_ids =
            collect_file_ids_from_paths(evidence.manifest_after_gc.clone()).unwrap();
        let object_store_after_ids =
            collect_file_ids_from_paths(evidence.sst_after_gc.clone()).unwrap();
        assert!(
            manifest_after_ids.is_subset(&object_store_after_ids),
            "manifest-reachable files must all be in object store after GC"
        );

        // count still correct
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
            admin_gc_dropped_evidence: None,
            post_migration_admin_gc_evidence: None,
            build_index_gc_evidence: None,
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
            admin_gc_dropped_evidence: None,
            post_migration_admin_gc_evidence: None,
            build_index_gc_evidence: None,
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

    #[tokio::test]
    async fn test_phase3_e2e_build_index_gc() {
        let evidence = run_phase3_e2e_gc_cycle(Phase3E2eInput {
            seed: 91,
            flush_rounds: 4,
            full_file_listing: true,
            compaction_wait_secs: 2,
            table_shape: Phase3E2eTableShape::SingleRegion,
            scenario_kind: Phase3E2eScenarioKind::BuildIndexGc,
        })
        .await;

        let ev = evidence
            .build_index_gc_evidence
            .as_ref()
            .expect("build-index-gc evidence must be present");

        // Basic evidence assertions
        assert_eq!(evidence.regions.len(), 1);
        assert!(evidence.gc_report.need_retry_regions.is_empty());
        assert!(evidence.gc_report.processed_regions.contains(&ev.region));

        // Strict index-deletion proof
        assert!(!ev.obsolete_puffins_before_gc.is_empty());
        assert!(
            evidence
                .gc_report
                .deleted_indexes
                .get(&ev.region)
                .map(|v| !v.is_empty())
                .unwrap_or(false),
            "deleted_indexes must be non-empty for this region"
        );
        assert!(
            !ev.reported_deleted_index_puffins.is_empty(),
            "reported_deleted_index_puffins must be non-empty"
        );
        let intersect_obsolete: BTreeSet<_> = ev
            .reported_deleted_index_puffins
            .intersection(&ev.obsolete_puffins_before_gc)
            .cloned()
            .collect();
        assert!(
            !intersect_obsolete.is_empty(),
            "reported deleted must intersect obsolete (v0) puffins"
        );
        let intersect_active: BTreeSet<_> = ev
            .reported_deleted_index_puffins
            .intersection(&ev.active_puffins_before_gc)
            .cloned()
            .collect();
        assert!(
            intersect_active.is_empty(),
            "reported deleted must not intersect active (v1) puffins"
        );
        let removed_obsolete: BTreeSet<_> = ev
            .obsolete_puffins_before_gc
            .difference(&ev.storage_after_gc_puffins)
            .cloned()
            .collect();
        assert!(
            !removed_obsolete.is_empty(),
            "at least one obsolete (v0) puffin must be removed from storage"
        );
        assert!(
            ev.active_puffins_before_gc
                .is_subset(&ev.storage_after_gc_puffins),
            "active (v1) puffins must be preserved in storage"
        );
        assert_eq!(
            evidence.manifest_after_gc, evidence.sst_after_gc,
            "manifest and storage sets must be identical after GC"
        );

        // Fox count correct: flush_rounds=4, 2 fox rows each = 8
        let expected_fox = "8";
        assert!(
            pretty_table_contains_cell(&ev.fox_count_before_gc, expected_fox),
            "fox_count_before_gc should contain '{expected_fox}', got:\n{}",
            ev.fox_count_before_gc,
        );
        assert!(
            pretty_table_contains_cell(&ev.fox_count_after_gc, expected_fox),
            "fox_count_after_gc should contain '{expected_fox}', got:\n{}",
            ev.fox_count_after_gc,
        );
    }
}
