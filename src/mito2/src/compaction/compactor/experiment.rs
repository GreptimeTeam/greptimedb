// Copyright 2023-2026 GrepTime Inc.
//
// This file is part of the GreptimeDB Enterprise Edition and is licensed under
// the GreptimeDB Enterprise License. You may not use this file except in
// compliance with that license. A copy of the license is available at the root
// of this repository in the file LICENSE-ENTERPRISE.
//
// Unless required by applicable law or agreed to in writing, this software is
// distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied.

//! Fixed-topology two-stage experiment: local shared L0 routing, remote per-region L1.
//! Round objects are immutable; each leader publishes only its own region manifest.
mod batch_window;
mod parallelism;
pub(crate) use parallelism::{automatic_parallelism, merge_parallelism};

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Instant;

use common_base::cancellation::CancellationHandle;
use object_store::{ErrorKind, ObjectStore};
use partition::multi_dim::MultiDimPartitionRule;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use snafu::{ResultExt, ensure};
use store_api::metadata::RegionMetadata;
use store_api::region_request::PathType;
use store_api::storage::{FileId, RegionId};

use super::{CompactionContext, CompactionRegion, MergeOutput};
use crate::compaction::CompactionOutput;
use crate::compaction::multi_region::{
    MultiRegionCompactionContext, MultiRegionCompactionPlan, MultiRegionTarget, compact_regions,
};
use crate::compaction::picker::PickerOutput;
use crate::compaction::unit::CompactionUnit;
use crate::error::{
    InvalidMultiRegionCompactionSnafu as Invalid, OpenDalSnafu, Result, SerdeJsonSnafu,
};
use crate::region::options::RegionOptions;
use crate::sst::file::{FileMeta, RegionFileId};
use crate::sst::parquet::writer::SstWriteGuard;

static WIDTH: LazyLock<usize> = LazyLock::new(|| {
    std::env::var("TESTBED_SHARED_BATCH_FILES")
        .unwrap_or_else(|_| "32".into())
        .parse::<std::num::NonZeroUsize>()
        .expect("invalid TESTBED_SHARED_BATCH_FILES")
        .get()
});
static REGIONS: LazyLock<u32> = LazyLock::new(|| {
    std::env::var("TESTBED_SHARED_TABLE_REGIONS")
        .unwrap_or_else(|_| "12".into())
        .parse::<std::num::NonZeroU32>()
        .expect("invalid TESTBED_SHARED_TABLE_REGIONS")
        .get()
});
static REGION_COUNTS: LazyLock<HashMap<u32, u32>> = LazyLock::new(|| {
    std::env::var("TESTBED_SHARED_TABLE_LAYOUT")
        .unwrap_or_default()
        .split(',')
        .filter(|s| !s.trim().is_empty())
        .map(|s| {
            let (table, count) = s.split_once(':').expect("expected table_id:region_count");
            (
                table.trim().parse().expect("invalid table id"),
                count
                    .trim()
                    .parse::<std::num::NonZeroU32>()
                    .expect("invalid region count")
                    .get(),
            )
        })
        .collect()
});

fn region_count(id: RegionId) -> u32 {
    REGION_COUNTS
        .get(&id.table_id())
        .copied()
        .unwrap_or(*REGIONS)
}
const DAY: i64 = 86400;
static TABLES: LazyLock<Vec<u32>> = LazyLock::new(|| {
    std::env::var("TESTBED_SHARED_TABLE_ID")
        .ok()
        .map(|s| {
            s.split(',')
                .map(|id| id.trim().parse().expect("invalid TESTBED_SHARED_TABLE_ID"))
                .collect()
        })
        .unwrap_or_default()
});
static BATCHES: LazyLock<std::num::NonZeroUsize> = LazyLock::new(|| {
    std::env::var("TESTBED_SHARED_TABLE_BATCHES")
        .unwrap_or_else(|_| "1".into())
        .parse()
        .expect("TESTBED_SHARED_TABLE_BATCHES must be a positive integer")
});

pub(crate) fn batch_limit() -> usize {
    BATCHES.get()
}

// Single-batch rounds keep the original object layout for replay.
fn batch_root(root: &str, files: usize, index: usize) -> String {
    if files == *WIDTH {
        root.into()
    } else {
        format!("{root}/batch-{index}")
    }
}

static CURSORS: LazyLock<Mutex<HashMap<(RegionId, u32), u64>>> = LazyLock::new(Default::default);
pub(crate) fn enabled(id: RegionId) -> bool {
    uses_shared_compaction(id, &TABLES, region_count(id))
}

// A single target has no duplicated scan to share. Keep its picker, merge and
// file lifecycle on the ordinary path, including unpartitioned metric tables.
fn uses_shared_compaction(id: RegionId, tables: &[u32], regions: u32) -> bool {
    regions > 1 && id.region_number() < regions && tables.contains(&id.table_id())
}

/// Only mixed bulk L0s belong to the shared scan. Routed L1s keep the regular picker.
pub(crate) fn is_shared(file: &FileMeta) -> bool {
    file.level == 0 && file.partition_expr.is_none()
}

pub(crate) fn shared_inputs<'a>(files: impl Iterator<Item = &'a FileMeta>) -> bool {
    let mut found = false;
    for file in files {
        if !is_shared(file) {
            return false;
        }
        found = true;
    }
    found
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_region_uses_ordinary_compaction() {
        assert!(!uses_shared_compaction(RegionId::new(42, 0), &[42], 1));
        assert!(!uses_shared_compaction(RegionId::new(42, 1), &[42], 1));
        assert!(!uses_shared_compaction(RegionId::new(42, 0), &[42], 0));
    }

    #[test]
    fn shared_compaction_only_matches_selected_data_regions() {
        for n in 0..12 {
            assert!(uses_shared_compaction(RegionId::new(42, n), &[42], 12));
        }
        assert!(!uses_shared_compaction(RegionId::new(42, 12), &[42], 12));
        assert!(!uses_shared_compaction(
            RegionId::new(42, 1 << 24),
            &[42],
            12
        ));
        assert!(!uses_shared_compaction(RegionId::new(43, 0), &[42], 12));
        assert!(!uses_shared_compaction(RegionId::new(42, 0), &[], 12));
    }

    #[test]
    fn routed_l1_never_reenters_the_shared_scan() {
        use crate::compaction::test_util::new_file_handle;
        let input = new_file_handle(FileId::random(), 0, 100, 0);
        let mut l0 = input.meta_ref().clone();
        l0.region_id = RegionId::new(42, 0);
        l0.partition_expr = None;
        assert!(shared_inputs([&l0].into_iter()));
        let mut l1 = l0.clone();
        l1.level = 1;
        assert!(!shared_inputs([&l1].into_iter()));
        assert!(!shared_inputs([&l0, &l1].into_iter()));
        assert!(!shared_inputs(std::iter::empty()));
    }

    #[test]
    fn l1_picker_view_preserves_live_reservations_without_deleting_l0() {
        use crate::compaction::test_util::{new_file_handle, primary_key_metadata_for_test};
        use crate::sst::file_purger::NoopFilePurger;
        use crate::sst::version::SstVersion;
        let l0 = new_file_handle(FileId::random(), 0, 100, 0)
            .meta_ref()
            .clone();
        let l1 = new_file_handle(FileId::random(), 0, 100, 1)
            .meta_ref()
            .clone();
        let mut version = SstVersion::new(primary_key_metadata_for_test());
        version.add_files(Arc::new(NoopFilePurger), [l0, l1].into_iter());
        let live_l0 = version.levels()[0].files().next().unwrap().clone();
        let live_l1 = version.levels()[1].files().next().unwrap().clone();
        let filtered = version.without_shared_l0();
        assert_eq!(0, filtered.levels()[0].files().count());
        let selected = filtered.levels()[1].files().next().unwrap();
        assert!(selected.try_set_compacting());
        assert!(live_l1.compacting());
        assert!(!live_l0.is_deleted());
        selected.set_compacting(false);
        assert_eq!(1, version.levels()[0].files().count());
    }
}
fn cursor(id: RegionId, owner: u32) -> u64 {
    *CURSORS.lock().unwrap().get(&(id, owner)).unwrap_or(&0)
}
fn prefix(id: RegionId, owner: u32, round: u64) -> String {
    // Keep the old Region-0 layout readable after upgrading/restarting.
    if owner == 0 {
        format!("_testbed_table_jobs/{}/{round}", id.table_id())
    } else {
        format!(
            "_testbed_table_jobs/{}/owners/{owner}/{round}",
            id.table_id()
        )
    }
}

fn source_owner<'a>(mut inputs: impl Iterator<Item = &'a FileMeta>) -> Result<RegionId> {
    let first = inputs.next().ok_or_else(|| {
        Invalid {
            reason: "empty shared input",
        }
        .build()
    })?;
    ensure!(
        is_shared(first) && inputs.all(|f| is_shared(f) && f.region_id == first.region_id),
        Invalid {
            reason: "shared batch mixes source owners"
        }
    );
    Ok(first.region_id)
}

async fn read<T: DeserializeOwned>(store: &ObjectStore, key: &str) -> Result<Option<T>> {
    match store.read(key).await {
        Ok(buf) => serde_json::from_slice(&buf.to_bytes())
            .map(Some)
            .context(SerdeJsonSnafu),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).context(OpenDalSnafu),
    }
}

async fn create<T: Serialize>(store: &ObjectStore, key: &str, value: &T) -> Result<()> {
    let bytes = serde_json::to_vec(&serde_json::to_value(value).context(SerdeJsonSnafu)?)
        .context(SerdeJsonSnafu)?;
    match store
        .write_with(key, bytes.clone())
        .if_not_exists(true)
        .await
    {
        Ok(_) => Ok(()),
        Err(e) => {
            // OSS reports FileAlreadyExists as a conflict. Verify the exact payload
            // also for an ambiguous response to our own successful first write.
            if let Ok(buf) = store.read(key).await
                && buf.to_bytes().as_ref() == bytes
            {
                return Ok(());
            }
            Err(e).context(OpenDalSnafu)
        }
    }
}

#[derive(Serialize, Deserialize)]
struct Reservation {
    metadata: RegionMetadata,
    options: RegionOptions,
    inputs: Vec<FileMeta>,
}

type Outputs = BTreeMap<RegionId, Vec<FileMeta>>;

/// Only a complete, registered shared batch needs a readiness retry.
/// Routed L1, unsequenced files and an undersized tail do not keep polling.
pub(crate) fn has_pending_shared_batch(ssts: &crate::sst::version::SstVersion) -> bool {
    let mut counts = HashMap::new();
    for f in ssts.levels()[0]
        .files()
        .filter(|f| is_shared(f.meta_ref()) && f.meta_ref().sequence.is_some())
    {
        let n = counts.entry(f.meta_ref().region_id).or_insert(0usize);
        *n += 1;
        if *n >= *WIDTH {
            return true;
        }
    }
    false
}

pub(crate) async fn pick(region: &CompactionRegion) -> Result<Option<PickerOutput>> {
    let id = region.region_id;
    ensure!(
        region.access_layer.path_type() == PathType::Data && id.region_number() < region_count(id),
        Invalid {
            reason: "expected a fixed physical metric data region"
        }
    );
    let store = region.access_layer.object_store();
    let handles: HashMap<_, _> = region.current_version.ssts.levels()[0]
        .files()
        .filter(|f| is_shared(f.meta_ref()) && f.meta_ref().sequence.is_some())
        .map(|f| (f.meta_ref().file_id, f))
        .collect();
    let mut pending = Vec::new();
    // Every participant advertises readiness without taking an execution slot.
    // Only the source owner creates a roster and executes its shared scan.
    for owner in 0..region_count(id) {
        let mut round = cursor(id, owner);
        loop {
            let root = prefix(id, owner, round);
            let key = format!("{root}/sources");
            let ids: Vec<FileId> = if let Some(ids) = read(store, &key).await? {
                ids
            } else {
                if id.region_number() != owner {
                    break;
                }
                let mut files: Vec<_> = handles
                    .values()
                    .map(|f| f.meta_ref())
                    .filter(|f| f.region_id == id)
                    .collect();
                if files.len() < *WIDTH {
                    break;
                }
                files.sort_by(|a, b| {
                    (a.sequence, a.file_id.as_bytes()).cmp(&(b.sequence, b.file_id.as_bytes()))
                });
                let ids = batch_window::new_roster(
                    files.iter().map(|f| f.file_id),
                    *WIDTH,
                    batch_limit(),
                );
                create(store, &key, &ids).await?;
                ids
            };
            ensure!(
                batch_window::valid_roster(&ids, *WIDTH),
                Invalid {
                    reason: "invalid immutable round roster"
                }
            );
            let mut outstanding = false;
            for (index, batch) in ids.chunks_exact(*WIDTH).enumerate() {
                let job = batch_root(&root, ids.len(), index);
                let applied_key = format!("{job}/applied-{}", id.region_number());
                if read::<bool>(store, &applied_key).await? == Some(true) {
                    continue;
                }
                let inputs: Vec<_> = batch
                    .iter()
                    .filter_map(|k| handles.get(k).map(|f| (**f).clone()))
                    .collect();
                if inputs.len() == *WIDTH {
                    ensure!(
                        source_owner(inputs.iter().map(|f| f.meta_ref()))?
                            == RegionId::new(id.table_id(), owner),
                        Invalid {
                            reason: "roster source owner mismatch"
                        }
                    );
                    let ready_key = format!("{job}/ready-{}", id.region_number());
                    // The experiment's fixed topology excludes DDL/migration;
                    // ordinary L1 selection excludes these L0 files entirely.
                    if !store.exists(&ready_key).await.context(OpenDalSnafu)? {
                        let reservation = Reservation {
                            metadata: (*region.region_metadata).clone(),
                            options: region.region_options.clone(),
                            inputs: inputs.iter().map(|f| f.meta_ref().clone()).collect(),
                        };
                        create(store, &ready_key, &reservation).await?;
                    }
                    let result_exists = store
                        .exists(&format!("{job}/result"))
                        .await
                        .context(OpenDalSnafu)?;
                    let executable = result_exists
                        || (id.region_number() == owner
                            && all_ready(store, &job, region_count(id)).await?);
                    if executable {
                        pending.push(CompactionOutput {
                            output_level: 1,
                            inputs,
                            filter_deleted: false,
                            output_time_range: None,
                        });
                    }
                    outstanding = true;
                    continue;
                }
                let Some(result): Option<Outputs> = read(store, &format!("{job}/result")).await?
                else {
                    outstanding = true;
                    continue;
                };
                let expected = result.get(&id).ok_or_else(|| {
                    Invalid {
                        reason: "region missing from table result",
                    }
                    .build()
                })?;
                let live: HashMap<_, _> = region.current_version.ssts.levels()[1]
                    .files()
                    .map(|f| (f.meta_ref().file_id, f.meta_ref()))
                    .collect();
                ensure!(
                    inputs.is_empty() && expected.iter().all(|f| live.get(&f.file_id) == Some(&f)),
                    Invalid {
                        reason: "partial or conflicting region publication"
                    }
                );
                create(store, &applied_key, &true).await?;
            }
            if outstanding {
                break;
            }
            round += 1;
            CURSORS.lock().unwrap().insert((id, owner), round);
        }
    }
    if pending.is_empty() {
        return Ok(None);
    }
    Ok(Some(PickerOutput {
        outputs: pending,
        expired_ssts: vec![],
        time_window_size: DAY,
        max_file_size: Some(256 << 20),
    }))
}

async fn ready_regions(
    store: &ObjectStore,
    root: &str,
    count: u32,
) -> Result<Option<BTreeMap<u32, Reservation>>> {
    let mut ready = BTreeMap::new();
    for n in 0..count {
        let Some(r) = read::<Reservation>(store, &format!("{root}/ready-{n}")).await? else {
            return Ok(None);
        };
        ready.insert(n, r);
    }
    Ok(Some(ready))
}

struct Guard {
    store: ObjectStore,
    root: String,
}
#[async_trait::async_trait]
impl SstWriteGuard for Guard {
    async fn before_create(&self, id: RegionFileId) -> Result<()> {
        create(
            &self.store,
            &format!(
                "{}/protected/{}/{}",
                self.root,
                id.region_id().as_u64(),
                id.file_id()
            ),
            &true,
        )
        .await
    }
}

pub(crate) async fn merge(
    ctx: &CompactionContext,
    unit: &CompactionUnit,
    cancel: Arc<CancellationHandle>,
) -> Result<MergeOutput> {
    let id = ctx.region_id;
    let owner = source_owner(unit.inputs.iter().map(|f| f.meta_ref()))?.region_number();
    let round = cursor(id, owner);
    let root = prefix(id, owner, round);
    let store = ctx.access_layer.object_store();
    let ids: Vec<FileId> = read(store, &format!("{root}/sources"))
        .await?
        .ok_or_else(|| {
            Invalid {
                reason: "missing round sources",
            }
            .build()
        })?;
    let by_id: HashMap<_, _> = unit
        .inputs
        .iter()
        .map(|f| (f.meta_ref().file_id, f.meta_ref()))
        .collect();
    let (index, batch) = batch_window::batch_for_unit(&ids, *WIDTH, &by_id.keys().collect())
        .ok_or_else(|| {
            Invalid {
                reason: "unit does not own exactly one complete batch",
            }
            .build()
        })?;
    let root = batch_root(&root, ids.len(), index);
    let ids = batch;
    let inputs: Vec<_> = ids.iter().map(|k| by_id[k].clone()).collect();
    let result_key = format!("{root}/result");
    // No timer, sleep, or readiness wait runs inside the bounded executor.
    loop {
        if cancel.is_cancelled() {
            return crate::error::CompactionCancelledSnafu.fail();
        }
        if let Some(outputs) = read::<Outputs>(store, &result_key).await? {
            let files = outputs.get(&id).ok_or_else(|| {
                Invalid {
                    reason: "missing region result",
                }
                .build()
            })?;
            ensure!(
                files.iter().all(|f| f.region_id == id && f.level == 1),
                Invalid {
                    reason: "misrouted region result"
                }
            );
            return Ok(MergeOutput {
                files_to_add: files.clone(),
                files_to_remove: inputs,
                compaction_time_window: Some(DAY),
                ..Default::default()
            });
        }
        ensure!(
            id.region_number() == owner,
            Invalid {
                reason: "result not ready at dispatch"
            }
        );
        let ready = ready_regions(store, &root, region_count(id))
            .await?
            .ok_or_else(|| {
                Invalid {
                    reason: "source owner dispatched before readiness",
                }
                .build()
            })?;
        let mut targets = BTreeMap::new();
        let mut expressions = Vec::new();
        for (n, r) in &ready {
            ensure!(
                r.metadata.region_id == RegionId::new(id.table_id(), *n)
                    && r.inputs.iter().map(|f| f.file_id).eq(ids.iter().copied()),
                Invalid {
                    reason: "conflicting reservation"
                }
            );
            let expr = crate::region::parse_partition_expr(r.metadata.partition_expr.as_deref())?
                .ok_or_else(|| {
                Invalid {
                    reason: "missing partition expression",
                }
                .build()
            })?;
            expressions.push(expr);
            targets.insert(
                r.metadata.region_id,
                MultiRegionTarget {
                    metadata: Arc::new(r.metadata.clone()),
                    options: r.options.clone(),
                    inputs: r.inputs.iter().map(|f| (f.file_id(), f.clone())).collect(),
                },
            );
        }
        let mut columns = std::collections::HashSet::new();
        for expression in &expressions {
            expression.collect_column_names(&mut columns);
        }
        ensure!(
            columns.len() == 1,
            Invalid {
                reason: "expected one partition tag"
            }
        );
        let rule = MultiDimPartitionRule::try_new(
            columns.into_iter().collect(),
            (0..region_count(id)).collect(),
            expressions,
            true,
        )
        .context(crate::error::RouteMultiRegionSnafu)?;
        let plan = MultiRegionCompactionPlan {
            partition_rule: Arc::new(rule),
            sources: inputs.clone(),
            targets,
            memory_limit: std::env::var("TESTBED_SHARED_COMPACTION_MEMORY_MIB")
                .unwrap_or_else(|_| "8192".into())
                .parse::<usize>()
                .map_err(|_| {
                    Invalid {
                        reason: "invalid shared memory MiB",
                    }
                    .build()
                })?
                .checked_mul(1 << 20)
                .ok_or_else(|| {
                    Invalid {
                        reason: "shared memory MiB overflow",
                    }
                    .build()
                })?,
            max_active_workers: 2,
            spill_limit: 0,
            max_output_file_size: 256 << 20,
            window_seconds: DAY,
        };
        let context = MultiRegionCompactionContext {
            access_layer: ctx.access_layer.clone(),
            cache: ctx.cache_manager.clone(),
            config: ctx.engine_config.clone(),
            protection: Arc::new(Guard {
                store: store.clone(),
                root: root.clone(),
            }),
            cancellation: cancel.clone(),
            spill_directory: Default::default(),
        };
        common_telemetry::info!(
            "table merge start table={} owner={} round={} batch={} memory_limit_bytes={} table_batch_limit={}",
            id.table_id(),
            owner,
            round,
            index,
            plan.memory_limit,
            batch_limit()
        );
        let execution_start = Instant::now();
        let output = compact_regions(plan, context).await?;
        let execution = execution_start.elapsed();
        ensure!(
            output.stats.source_files == *WIDTH as u64
                && output.stats.source_rows == output.stats.output_rows,
            Invalid {
                reason: "table scan accounting mismatch"
            }
        );
        common_telemetry::info!(
            "testbed shared compaction table={} owner={} round={} batch={} region_ready_wait_ms={} execution_ms={} stats={:?}",
            id.table_id(),
            owner,
            round,
            index,
            0,
            execution.as_millis(),
            output.stats
        );
        create(store, &result_key, &output.files).await?;
        continue;
    }
}

/// Persist the completion marker after the manifest edit, before the worker
/// makes the new L1s selectable. This also leaves the old-layout replay intact.
pub(crate) async fn record_publication(ctx: &CompactionContext, inputs: &[FileMeta]) -> Result<()> {
    let owner = source_owner(inputs.iter())?.region_number();
    let root = prefix(ctx.region_id, owner, cursor(ctx.region_id, owner));
    let store = ctx.access_layer.object_store();
    let ids: Vec<FileId> = read(store, &format!("{root}/sources"))
        .await?
        .ok_or_else(|| {
            Invalid {
                reason: "missing shared publication roster",
            }
            .build()
        })?;
    let input_ids = inputs.iter().map(|f| &f.file_id).collect();
    let (index, _) = batch_window::batch_for_unit(&ids, *WIDTH, &input_ids).ok_or_else(|| {
        Invalid {
            reason: "missing shared publication batch",
        }
        .build()
    })?;
    let batch = batch_root(&root, ids.len(), index);
    create(
        store,
        &format!("{batch}/applied-{}", ctx.region_id.region_number()),
        &true,
    )
    .await
}

#[cfg(test)]
mod readiness_tests {
    use super::*;
    use crate::compaction::test_util::{new_file_handle, primary_key_metadata_for_test};
    use crate::sst::file_purger::NoopFilePurger;
    use crate::sst::version::SstVersion;

    #[test]
    fn only_a_full_registered_shared_batch_needs_a_retry() {
        let mut ssts = SstVersion::new(primary_key_metadata_for_test());
        assert!(!has_pending_shared_batch(&ssts));
        let mut files = Vec::new();
        for _ in 0..*WIDTH {
            let mut file = new_file_handle(FileId::random(), 0, 100, 0)
                .meta_ref()
                .clone();
            file.region_id = RegionId::new(42, 0);
            file.partition_expr = None;
            file.sequence = std::num::NonZeroU64::new(1);
            files.push(file);
        }
        let last = files.pop().unwrap();
        ssts.add_files(Arc::new(NoopFilePurger), files.into_iter());
        assert!(!has_pending_shared_batch(&ssts));
        let mut l1 = last.clone();
        l1.file_id = FileId::random();
        l1.level = 1;
        let mut unregistered = last.clone();
        unregistered.file_id = FileId::random();
        unregistered.sequence = None;
        ssts.add_files(Arc::new(NoopFilePurger), [l1, unregistered].into_iter());
        assert!(!has_pending_shared_batch(&ssts));
        ssts.add_files(Arc::new(NoopFilePurger), [last].into_iter());
        assert!(has_pending_shared_batch(&ssts));
    }
}

// Check immutable readiness markers without reparsing wide Region schemas.
async fn all_ready(store: &ObjectStore, root: &str, count: u32) -> Result<bool> {
    for n in 0..count {
        if !store
            .exists(&format!("{root}/ready-{n}"))
            .await
            .context(OpenDalSnafu)?
        {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
mod owner_tests {
    use super::*;
    use crate::compaction::test_util::{new_file_handle, primary_key_metadata_for_test};
    use crate::sst::file_purger::NoopFilePurger;
    use crate::sst::version::SstVersion;

    fn input(owner: u32) -> FileMeta {
        let mut f = new_file_handle(FileId::random(), 0, 100, 0)
            .meta_ref()
            .clone();
        f.region_id = RegionId::new(42, owner);
        f.partition_expr = None;
        f.sequence = std::num::NonZeroU64::new(1);
        f
    }

    #[test]
    fn source_owner_is_not_a_partition_filter() {
        for n in 0..12 {
            let a = input(n);
            let b = input(n);
            assert!(is_shared(&a));
            assert_eq!(
                source_owner([&a, &b].into_iter()).unwrap(),
                RegionId::new(42, n)
            );
            let other = input((n + 1) % 12);
            assert!(source_owner([&a, &other].into_iter()).is_err());
            let mut routed = b;
            routed.level = 1;
            assert!(!is_shared(&routed));
        }
        assert!(source_owner(std::iter::empty()).is_err());
    }

    #[test]
    fn small_tails_from_different_owners_do_not_form_one_batch() {
        let mut ssts = SstVersion::new(primary_key_metadata_for_test());
        let mut files = Vec::new();
        for owner in 0..12 {
            files.extend((0..(*WIDTH - 1)).map(|_| input(owner)));
        }
        ssts.add_files(Arc::new(NoopFilePurger), files.into_iter());
        assert!(!has_pending_shared_batch(&ssts));
        ssts.add_files(Arc::new(NoopFilePurger), [input(7)].into_iter());
        assert!(has_pending_shared_batch(&ssts));
    }

    #[test]
    fn owners_and_followers_have_independent_round_cursors() {
        let id = RegionId::new(43, 2);
        CURSORS.lock().unwrap().insert((id, 1), 3);
        assert_eq!(cursor(id, 1), 3);
        assert_eq!(cursor(id, 0), 0);
        assert_eq!(cursor(RegionId::new(43, 3), 1), 0);
        assert_eq!(prefix(id, 0, 3), "_testbed_table_jobs/43/3");
        assert_eq!(prefix(id, 1, 3), "_testbed_table_jobs/43/owners/1/3");
    }

    #[tokio::test]
    async fn readiness_requires_every_target_and_does_not_wait() {
        let store =
            object_store::ObjectStore::new(object_store::services::Memory::default()).unwrap();
        for n in [2, 0] {
            store
                .write(format!("job/ready-{n}").as_str(), "ready")
                .await
                .unwrap();
            assert!(!all_ready(&store, "job", 3).await.unwrap());
        }
        store.write("job/ready-1", "ready").await.unwrap();
        assert!(all_ready(&store, "job", 3).await.unwrap());
        // A different owner's batch cannot borrow these readiness markers.
        assert!(!all_ready(&store, "other-job", 3).await.unwrap());
    }

    #[tokio::test]
    async fn picker_only_dispatches_owner_scan_or_finished_result() {
        let mut regions = Vec::new();
        let mut files = Vec::new();
        for owner in [3, 5] {
            files.extend((0..*WIDTH).map(|_| {
                let mut f = input(owner);
                f.region_id = RegionId::new(98765, owner);
                f
            }));
        }
        let mut prototype = super::super::new_test_compaction_region().await;
        prototype.access_layer = Arc::new(crate::access_layer::AccessLayer::new(
            "",
            PathType::Data,
            object_store::ObjectStore::new(object_store::services::Memory::default()).unwrap(),
            prototype.access_layer.puffin_manager_factory().clone(),
            prototype.access_layer.intermediate_manager().clone(),
        ));
        for n in 0..region_count(RegionId::new(98765, 0)) {
            let mut r = super::super::new_test_compaction_region().await;
            r.region_id = RegionId::new(98765, n);
            r.access_layer = prototype.access_layer.clone();
            let mut metadata = (*r.region_metadata).clone();
            metadata.region_id = r.region_id;
            r.region_metadata = Arc::new(metadata);
            let mut ssts = SstVersion::new(r.region_metadata.clone());
            ssts.add_files(Arc::new(NoopFilePurger), files.clone().into_iter());
            r.current_version.ssts = Arc::new(ssts);
            regions.push(r);
        }
        assert!(pick(&regions[3]).await.unwrap().is_none());
        assert!(pick(&regions[5]).await.unwrap().is_none());
        for r in &regions {
            if ![3, 5].contains(&r.region_id.region_number()) {
                assert!(pick(r).await.unwrap().is_none());
            }
        }
        // Each owner must also join the other's roster, without running it.
        pick(&regions[3]).await.unwrap();
        pick(&regions[5]).await.unwrap();
        for owner in [3usize, 5] {
            let plan = pick(&regions[owner]).await.unwrap().unwrap();
            assert_eq!(plan.outputs.len(), 1);
            assert_eq!(
                source_owner(plan.outputs[0].inputs.iter().map(|f| f.meta_ref())).unwrap(),
                regions[owner].region_id
            );
        }
        assert!(pick(&regions[0]).await.unwrap().is_none());
        let root = prefix(regions[0].region_id, 3, 0);
        let outputs: Outputs = regions.iter().map(|r| (r.region_id, Vec::new())).collect();
        create(
            prototype.access_layer.object_store(),
            &format!("{root}/result"),
            &outputs,
        )
        .await
        .unwrap();
        let publish = pick(&regions[0]).await.unwrap().unwrap();
        assert_eq!(publish.outputs.len(), 1);
        assert_eq!(
            source_owner(publish.outputs[0].inputs.iter().map(|f| f.meta_ref())).unwrap(),
            regions[3].region_id
        );
    }
}
