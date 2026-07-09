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

const DEFAULT_TRACE_INGEST_CHUNK_SIZE: usize = 64;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct OtlpOptions {
    pub enable: bool,
    /// Maximum spans per trace ingest chunk. Set to 0 to disable splitting.
    pub trace_ingest_chunk_size: usize,
}

impl Default for OtlpOptions {
    fn default() -> Self {
        Self {
            enable: true,
            trace_ingest_chunk_size: DEFAULT_TRACE_INGEST_CHUNK_SIZE,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_otlp_options() {
        let default = OtlpOptions::default();
        assert!(default.enable);
        assert_eq!(
            default.trace_ingest_chunk_size,
            DEFAULT_TRACE_INGEST_CHUNK_SIZE
        );

        let options: OtlpOptions = toml::from_str("enable = false").unwrap();
        assert!(!options.enable);
        assert_eq!(
            options.trace_ingest_chunk_size,
            DEFAULT_TRACE_INGEST_CHUNK_SIZE
        );

        let options: OtlpOptions = toml::from_str("trace_ingest_chunk_size = 0").unwrap();
        assert!(options.enable);
        assert_eq!(options.trace_ingest_chunk_size, 0);
    }
}
