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

//! Immutable index snapshots and aggregate series-file handles.

use crate::series_index::catalog::SeriesIndexEntry;
use crate::sst::file::{FileHandle, RegionFileId};
use std::sync::Arc;

/// Legacy codec coverage descriptor derived from immutable narthex metadata.
/// The region manifest carries logical coverage separately from this descriptor.
pub(crate) fn narthex_series_entry(
    file: &crate::sst::file::FileMeta,
    region_dir: &str,
) -> crate::error::Result<SeriesIndexEntry> {
    use crate::series_index::artifact::{ArtifactFormat, ArtifactMetadata};
    let crate::sst::file::FileFormat::NarthexSeriesIndex(_) = &file.format else {
        return Err(crate::series_index::columnar::invalid(
            "expected narthex series metadata",
        ));
    };
    Ok(SeriesIndexEntry {
        artifact: Some(ArtifactMetadata {
            format: ArtifactFormat::ColumnarSeries,
            generation: file.file_id,
            objects: file
                .narthex_objects(region_dir)
                .expect("matched Narthex series index"),
            file_size: file.file_size,
        }),
        file_size: file.file_size,
        index_uuid: file.file_id,
        bucket_start: file.time_range.0,
        bucket_end: file.time_range.1,
        source_file_ids: None,
        min_file_sequence: 0,
        max_file_sequence: 0,
        compaction_window_secs: 0,
        window_sequences: Default::default(),
    })
}

/// The handle pins the immutable region artifact for the entire reader lifetime.
#[derive(Clone, Debug)]
pub(crate) struct SeriesIndexFileHandle(Arc<SeriesFile>);

#[derive(Debug)]
struct SeriesFile {
    file: FileHandle,
    entry: SeriesIndexEntry,
    manifest_path: String,
}

impl SeriesIndexFileHandle {
    #[cfg(test)]
    pub(crate) fn new(region_id: store_api::storage::RegionId, entry: SeriesIndexEntry) -> Self {
        let file = FileHandle::new(
            crate::sst::file::FileMeta {
                region_id,
                file_id: entry.index_uuid,
                ..Default::default()
            },
            crate::test_util::new_noop_file_purger(),
        );
        let manifest_path = format!(
            "{}/series/{}/manifest.json",
            region_id.as_u64(),
            entry.index_uuid
        );
        Self(Arc::new(SeriesFile {
            file,
            entry,
            manifest_path,
        }))
    }

    pub(crate) fn from_narthex(file: FileHandle, region_dir: &str) -> crate::error::Result<Self> {
        let entry = narthex_series_entry(file.meta_ref(), region_dir)?;
        let manifest_path = format!(
            "{}manifest.json",
            crate::sst::location::narthex_file_dir(region_dir, file.meta_ref().file_id)
        );
        Ok(Self(Arc::new(SeriesFile {
            file,
            entry,
            manifest_path,
        })))
    }
    pub(crate) fn manifest_path(&self) -> String {
        self.0.manifest_path.clone()
    }
    pub(crate) fn file_id(&self) -> RegionFileId {
        self.0.file.file_id()
    }
    pub(crate) fn entry(&self) -> &SeriesIndexEntry {
        &self.0.entry
    }
}
