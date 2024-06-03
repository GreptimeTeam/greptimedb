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

use std::fmt::Debug;
use std::sync::Arc;

use api::v1::region::compact_request;

use crate::compaction::twcs::TwcsPicker;
use crate::compaction::window::WindowedCompactionPicker;
use crate::compaction::CompactionOutput;
use crate::region::options::CompactionOptions;
use crate::region::version::VersionRef;
use crate::sst::file::FileHandle;

#[async_trait::async_trait]
pub trait CompactionTask: Debug + Send + Sync + 'static {
    async fn run(&mut self);
}

/// Picker picks input SST files and builds the compaction task.
/// Different compaction strategy may implement different pickers.
pub trait Picker: Debug + Send + Sync + 'static {
    fn pick(&self, current_version: VersionRef) -> Option<PickerOutput>;
}

#[derive(Clone)]
pub struct PickerOutput {
    pub outputs: Vec<CompactionOutput>,
    pub expired_ssts: Vec<FileHandle>,
    pub time_window_size: i64,
}

pub(crate) fn new_picker(
    compact_request_options: compact_request::Options,
    compaction_options: &CompactionOptions,
) -> Arc<dyn Picker> {
    if let compact_request::Options::StrictWindow(window) = &compact_request_options {
        let window = if window.window_seconds == 0 {
            None
        } else {
            Some(window.window_seconds)
        };
        Arc::new(WindowedCompactionPicker::new(window)) as Arc<_>
    } else {
        match compaction_options {
            CompactionOptions::Twcs(twcs_opts) => Arc::new(TwcsPicker::new(
                twcs_opts.max_active_window_files,
                twcs_opts.max_inactive_window_files,
                twcs_opts.time_window_seconds(),
            )) as Arc<_>,
        }
    }
}
