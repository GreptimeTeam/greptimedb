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

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct PipelineOptions {
    /// Default for the HTTP log ingestion and pipeline dry-run `ignore_errors` parameter.
    /// Explicit request values override this setting. Malformed input may be discarded,
    /// and invalid custom timestamps in identity ingestion fall back to server time.
    /// Pipeline execution errors are controlled separately by `skip_error`.
    /// Defaults to false. Changes require a restart.
    pub ignore_errors: bool,
}
