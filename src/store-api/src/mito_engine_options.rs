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

//! Option keys for the mito engine.
//! We define them in this mod so the create parser can use it to validate table options.

use common_wal::options::WAL_OPTIONS_KEY;

/// Returns true if the `key` is a valid option key for the mito engine.
pub fn is_mito_engine_option_key(key: &str) -> bool {
    [
        "ttl",
        "compaction.type",
        "compaction.twcs.max_active_window_files",
        "compaction.twcs.max_inactive_window_files",
        "compaction.twcs.time_window",
        "storage",
        "index.inverted_index.ignore_column_ids",
        "index.inverted_index.segment_row_count",
        WAL_OPTIONS_KEY,
        "memtable.type",
        "memtable.experimental.index_max_keys_per_shard",
        "memtable.experimental.data_freeze_threshold",
        "memtable.experimental.fork_dictionary_bytes",
    ]
    .contains(&key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_mito_engine_option_key() {
        assert!(is_mito_engine_option_key("ttl"));
        assert!(is_mito_engine_option_key("compaction.type"));
        assert!(is_mito_engine_option_key(
            "compaction.twcs.max_active_window_files"
        ));
        assert!(is_mito_engine_option_key(
            "compaction.twcs.max_inactive_window_files"
        ));
        assert!(is_mito_engine_option_key("compaction.twcs.time_window"));
        assert!(is_mito_engine_option_key("storage"));
        assert!(is_mito_engine_option_key(
            "index.inverted_index.ignore_column_ids"
        ));
        assert!(is_mito_engine_option_key(
            "index.inverted_index.segment_row_count"
        ));
        assert!(is_mito_engine_option_key("wal_options"));
        assert!(is_mito_engine_option_key("memtable.type"));
        assert!(is_mito_engine_option_key(
            "memtable.experimental.index_max_keys_per_shard"
        ));
        assert!(is_mito_engine_option_key(
            "memtable.experimental.data_freeze_threshold"
        ));
        assert!(is_mito_engine_option_key(
            "memtable.experimental.fork_dictionary_bytes"
        ));
        assert!(!is_mito_engine_option_key("foo"));
    }
}
