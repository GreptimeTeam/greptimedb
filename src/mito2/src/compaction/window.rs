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

use std::collections::BTreeMap;
use std::fmt::Debug;

use api::v1::region::compact_type::Ty;
use api::v1::region::CompactType;
use common_telemetry::info;
use common_time::range::TimestampRange;
use common_time::timestamp::TimeUnit;
use common_time::timestamp_millis::BucketAligned;
use common_time::Timestamp;
use store_api::storage::RegionId;

use crate::compaction::buckets::infer_time_bucket;
use crate::compaction::picker::{CompactionTask, Picker};
use crate::compaction::task::CompactionTaskImpl;
use crate::compaction::{get_expired_ssts, CompactionOutput, CompactionRequest};
use crate::region::version::VersionRef;
use crate::sst::file::{FileHandle, FileId};

/// Compaction picker that splits the time range of all involved files to windows, and merges
/// the data segments intersects with those windows of files together so that the output files
/// never overlaps.
#[derive(Debug)]
pub struct WindowedCompactionPicker {
    compaction_time_window_seconds: Option<i64>,
}

impl WindowedCompactionPicker {
    pub fn new(window_seconds: Option<i64>) -> Self {
        Self {
            compaction_time_window_seconds: window_seconds,
        }
    }

    // Computes compaction time window. First we respect user specified parameter, then
    // use persisted window. If persist window is not present, we check the time window
    // provided while creating table. If all of those are absent, we infer the window
    // from files in level0.
    fn calculate_time_window(
        &self,
        region_id: RegionId,
        compact_type: CompactType,
        current_version: &VersionRef,
    ) -> i64 {
        let window = if let Some(Ty::StrictWindow(w)) = &compact_type.ty {
            if w.window != 0 {
                // 0 means window is not provided.
                Some(w.window)
            } else {
                None
            }
        } else {
            unreachable!()
        };
        window
            .or(current_version
                .compaction_time_window
                .map(|t| t.as_secs() as i64))
            .or(self.compaction_time_window_seconds)
            .unwrap_or_else(|| {
                let levels = current_version.ssts.levels();
                let inferred = infer_time_bucket(levels[0].files());
                info!(
                    "Compaction window for region {} is not present, inferring from files: {:?}",
                    region_id, inferred
                );
                inferred
            })
    }
}

impl Picker for WindowedCompactionPicker {
    fn pick(&self, req: CompactionRequest) -> Option<Box<dyn CompactionTask>> {
        let region_id = req.region_id();
        let CompactionRequest {
            engine_config,
            current_version,
            access_layer,
            compact_type,
            request_sender,
            waiters,
            file_purger,
            start_time,
            cache_manager,
            manifest_ctx,
            version_control,
            listener,
        } = req;

        let time_window = self.calculate_time_window(region_id, compact_type, &current_version);
        info!(
            "Compaction window for region: {} is {} seconds",
            region_id, time_window
        );

        let expired_ssts = get_expired_ssts(
            current_version.ssts.levels(),
            current_version.options.ttl,
            Timestamp::current_millis(),
        );
        if !expired_ssts.is_empty() {
            info!("Expired SSTs in region {}: {:?}", region_id, expired_ssts);
            // here we mark expired SSTs as compacting to avoid them being picked.
            expired_ssts.iter().for_each(|f| f.set_compacting(true));
        }

        let windows = calculate_time_buckets(
            time_window,
            current_version
                .ssts
                .levels()
                .iter()
                .flat_map(|level| level.files.values()),
        );

        let outputs = build_output(windows, time_window);

        let task = CompactionTaskImpl {
            engine_config: engine_config.clone(),
            region_id,
            metadata: current_version.metadata.clone().clone(),
            sst_layer: access_layer.clone(),
            outputs,
            expired_ssts,
            compaction_time_window: Some(time_window),
            request_sender,
            waiters,
            file_purger,
            start_time,
            cache_manager,
            storage: current_version.options.storage.clone(),
            index_options: current_version.options.index_options.clone(),
            append_mode: current_version.options.append_mode,
            manifest_ctx,
            version_control,
            listener,
        };
        Some(Box::new(task))
    }
}

fn build_output(
    windows: BTreeMap<i64, Vec<FileHandle>>,
    window_size: i64,
) -> Vec<CompactionOutput> {
    let mut outputs = Vec::with_capacity(windows.len());
    for (window_lower_bound, files) in windows {
        // safety: the upper bound must > lower bound.
        let output_time_range = Some(
            TimestampRange::new(
                Timestamp::new_second(window_lower_bound),
                Timestamp::new_second(window_lower_bound + window_size),
            )
            .unwrap(),
        );

        let output = CompactionOutput {
            output_file_id: FileId::random(),
            output_level: 1,
            inputs: files,
            filter_deleted: false,
            output_time_range,
        };
        outputs.push(output);
    }

    outputs
}

/// Calculates buckets for files. If file does not contain a time range in metadata, it will be
/// assigned to a special bucket `i64::MAX` (normally no timestamp can be aligned to this bucket)
/// so that all files without timestamp can be compacted together.
fn calculate_time_buckets<'a>(
    bucket_sec: i64,
    files: impl Iterator<Item = &'a FileHandle>,
) -> BTreeMap<i64, Vec<FileHandle>> {
    let mut buckets = BTreeMap::new();

    for file in files {
        let (start, end) = file.time_range();
        let bounds = file_time_bucket_span(
            start.convert_to(TimeUnit::Second).unwrap().value(),
            end.convert_to(TimeUnit::Second).unwrap().value(),
            bucket_sec,
        );
        for bound in bounds {
            buckets
                .entry(bound)
                .or_insert_with(Vec::new)
                .push(file.clone());
        }
    }
    buckets
}

/// Calculates timestamp span between start and end timestamp.
fn file_time_bucket_span(start_sec: i64, end_sec: i64, bucket_sec: i64) -> Vec<i64> {
    assert!(start_sec <= end_sec);

    // if timestamp is between `[i64::MIN, i64::MIN.align_by_bucket(bucket)]`, which cannot
    // be aligned to a valid i64 bound, simply return `i64::MIN` rather than just underflow.
    let mut start_aligned = start_sec.align_by_bucket(bucket_sec).unwrap_or(i64::MIN);
    let end_aligned = end_sec.align_by_bucket(bucket_sec).unwrap_or(i64::MIN);

    let mut res = Vec::with_capacity(((end_aligned - start_aligned) / bucket_sec + 1) as usize);
    while start_aligned < end_aligned {
        res.push(start_aligned);
        start_aligned += bucket_sec;
    }
    res.push(end_aligned);
    res
}
