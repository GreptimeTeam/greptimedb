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

//! Scrape-time counts of registered SST metadata values.

use std::sync::{Arc, Mutex, Weak};

use lazy_static::lazy_static;
use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;
use prometheus::{IntGaugeVec, Opts};

use crate::metrics::TYPE_LABEL;
use crate::sst::file_metadata::FileMetadataInternerState;

lazy_static! {
    pub(crate) static ref FILE_METADATA_COLLECTOR: FileMetadataCollector = {
        let collector = FileMetadataCollector::default();
        prometheus::register(Box::new(collector.clone())).unwrap();
        collector
    };
}

/// Aggregates live interners without retaining them or adding work to intern/drop.
/// Labels have fixed cardinality; entries are distinct values, not SST view counts.
#[derive(Clone)]
pub(crate) struct FileMetadataCollector {
    sources: Arc<Mutex<Vec<Weak<FileMetadataInternerState>>>>,
    entries: IntGaugeVec,
}

impl Default for FileMetadataCollector {
    fn default() -> Self {
        Self {
            sources: Arc::new(Mutex::new(Vec::new())),
            entries: IntGaugeVec::new(
                Opts::new(
                    "greptime_mito_sst_metadata_interner_entries",
                    "Registered values in live SST metadata interners",
                ),
                &[TYPE_LABEL],
            )
            .unwrap(),
        }
    }
}

impl FileMetadataCollector {
    pub(crate) fn register(&self, source: &Arc<FileMetadataInternerState>) {
        let mut sources = self.sources.lock().unwrap();
        sources.retain(|source| source.strong_count() > 0);
        sources.push(Arc::downgrade(source));
    }
}

impl Collector for FileMetadataCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.entries.desc()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        // Serialize gathers so concurrent scrapes cannot overwrite each other's gauges.
        // Registration never holds a shard lock; intern/drop never take this mutex.
        let mut sources = self.sources.lock().unwrap();
        let mut files = 0;
        let mut expressions = 0;
        sources.retain(|source| {
            let Some(source) = source.upgrade() else {
                return false;
            };
            let counts = source.entry_counts();
            files += counts.0;
            expressions += counts.1;
            true
        });
        self.entries.with_label_values(&["file"]).set(files as i64);
        self.entries
            .with_label_values(&["partition_expr"])
            .set(expressions as i64);
        self.entries.collect()
    }
}
