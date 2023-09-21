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

use std::any::Any;

use common_error::ext::ErrorExt;
use common_runtime::error::Error as RuntimeError;
use snafu::{Location, Snafu};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to start log store gc task"))]
    StartGcTask {
        location: Location,
        source: RuntimeError,
    },

    #[snafu(display("Failed to stop log store gc task"))]
    StopGcTask {
        location: Location,
        source: RuntimeError,
    },

    #[snafu(display("Failed to add entry to LogBatch"))]
    AddEntryLogBatch {
        source: raft_engine::Error,
        location: Location,
    },

    #[snafu(display("Failed to perform raft-engine operation"))]
    RaftEngine {
        source: raft_engine::Error,
        location: Location,
    },

    #[snafu(display("Log store not started yet"))]
    IllegalState { location: Location },

    #[snafu(display("Namespace is illegal: {}", ns))]
    IllegalNamespace { ns: u64, location: Location },

    #[snafu(display(
        "Failed to fetch entries from namespace: {}, start: {}, end: {}, max size: {}",
        ns,
        start,
        end,
        max_size,
    ))]
    FetchEntry {
        ns: u64,
        start: u64,
        end: u64,
        max_size: usize,
        source: raft_engine::Error,
        location: Location,
    },

    #[snafu(display(
        "Cannot override compacted entry, namespace: {}, first index: {}, attempt index: {}",
        namespace,
        first_index,
        attempt_index
    ))]
    OverrideCompactedEntry {
        namespace: u64,
        first_index: u64,
        attempt_index: u64,
        location: Location,
    },
}

impl ErrorExt for Error {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub type Result<T> = std::result::Result<T, Error>;
