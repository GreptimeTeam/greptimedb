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

use common_base::readable_size::ReadableSize;
use common_stat::get_total_memory_readable;
use serde::{Deserialize, Serialize};

const DEFAULT_TRACE_INGEST_CHUNK_SIZE: usize = 512;
const MIN_TRACE_AUX_CACHE_SIZE: ReadableSize = ReadableSize::mb(32);

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct OtlpOptions {
    pub enable: bool,
    /// Maximum spans per trace ingest chunk. Set to 0 to disable splitting.
    pub trace_ingest_chunk_size: usize,
    /// Estimated memory budget for cached trace service/operation keys per frontend,
    /// shared across all catalogs, schemas, and trace tables. Set to 0 to disable caching.
    /// Defaults to 1/128 of the host or pod memory limit, with a minimum of 32MiB.
    /// Uses 32MiB if memory detection is unavailable.
    /// Excludes cache and allocator overhead; shared table names are charged per entry.
    pub trace_aux_cache_size: ReadableSize,
    /// Whether to synthesize the `greptime_otel_resource_info` descriptor
    /// table from the resource attributes of OTLP metrics, so metrics-only
    /// services reach the semantic graph. Off by default: it creates and
    /// writes a table the user did not send.
    pub experimental_enable_resource_info: bool,
}

impl Default for OtlpOptions {
    fn default() -> Self {
        Self {
            enable: true,
            trace_ingest_chunk_size: DEFAULT_TRACE_INGEST_CHUNK_SIZE,
            trace_aux_cache_size: default_trace_aux_cache_size(get_total_memory_readable()),
            experimental_enable_resource_info: false,
        }
    }
}

fn default_trace_aux_cache_size(memory: Option<ReadableSize>) -> ReadableSize {
    memory
        .map(|size| size / 128)
        .unwrap_or_default()
        .max(MIN_TRACE_AUX_CACHE_SIZE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_trace_aux_cache_size() {
        for (memory, expected) in [
            (None, ReadableSize::mb(32)),
            (Some(ReadableSize(0)), ReadableSize::mb(32)),
            (Some(ReadableSize::gb(1)), ReadableSize::mb(32)),
            (Some(ReadableSize::gb(4)), ReadableSize::mb(32)),
            (Some(ReadableSize::gb(8)), ReadableSize::mb(64)),
            (Some(ReadableSize::gb(64)), ReadableSize::mb(512)),
        ] {
            assert_eq!(default_trace_aux_cache_size(memory), expected);
        }
    }

    #[test]
    fn test_otlp_options() {
        let default = OtlpOptions::default();
        assert!(default.enable);
        assert_eq!(default.trace_ingest_chunk_size, 512);
        assert_eq!(
            default.trace_aux_cache_size,
            default_trace_aux_cache_size(get_total_memory_readable())
        );
        assert!(!default.experimental_enable_resource_info);

        let options: OtlpOptions = toml::from_str("enable = false").unwrap();
        assert!(!options.enable);
        assert_eq!(options.trace_aux_cache_size, default.trace_aux_cache_size);
        assert_eq!(
            options.trace_ingest_chunk_size,
            DEFAULT_TRACE_INGEST_CHUNK_SIZE
        );

        let options: OtlpOptions = toml::from_str("trace_ingest_chunk_size = 0").unwrap();
        assert!(options.enable);
        assert_eq!(options.trace_ingest_chunk_size, 0);

        let serialized = toml::to_string(&options).unwrap();
        assert_eq!(toml::from_str::<OtlpOptions>(&serialized).unwrap(), options);

        for (value, size) in [("0", ReadableSize(0)), ("\"1MiB\"", ReadableSize::mb(1))] {
            let options: OtlpOptions =
                toml::from_str(&format!("trace_aux_cache_size = {value}")).unwrap();
            assert_eq!(options.trace_aux_cache_size, size);
            let serialized = toml::to_string(&options).unwrap();
            assert_eq!(toml::from_str::<OtlpOptions>(&serialized).unwrap(), options);
        }
    }
}
