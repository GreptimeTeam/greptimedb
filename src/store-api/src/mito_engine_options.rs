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

/// Option key for all WAL options.
pub use common_wal::options::WAL_OPTIONS_KEY;
/// Option key for append mode.
pub const APPEND_MODE_KEY: &str = "append_mode";
/// Option key for merge mode.
pub const MERGE_MODE_KEY: &str = "merge_mode";
/// Option key for TTL(time-to-live)
pub const TTL_KEY: &str = "ttl";
/// Option key for snapshot read.
pub const SNAPSHOT_READ: &str = "snapshot_read";
/// Option key for compaction type.
pub const COMPACTION_TYPE: &str = "compaction.type";
/// TWCS compaction strategy.
pub const COMPACTION_TYPE_TWCS: &str = "twcs";
/// Option key for twcs min file num to trigger a compaction.
pub const TWCS_TRIGGER_FILE_NUM: &str = "compaction.twcs.trigger_file_num";
/// Option key for twcs max output file size.
pub const TWCS_MAX_OUTPUT_FILE_SIZE: &str = "compaction.twcs.max_output_file_size";
/// Option key for twcs time window.
pub const TWCS_TIME_WINDOW: &str = "compaction.twcs.time_window";
/// Option key for twcs remote compaction.
pub const TWCS_REMOTE_COMPACTION: &str = "compaction.twcs.remote_compaction";
/// Option key for twcs fallback to local.
pub const TWCS_FALLBACK_TO_LOCAL: &str = "compaction.twcs.fallback_to_local";
/// Option key for memtable type.
pub const MEMTABLE_TYPE: &str = "memtable.type";
/// Option key for memtable partition tree index max keys per shard.
pub const MEMTABLE_PARTITION_TREE_INDEX_MAX_KEYS_PER_SHARD: &str =
    "memtable.partition_tree.index_max_keys_per_shard";
/// Option key for memtable partition tree data freeze threshold.
pub const MEMTABLE_PARTITION_TREE_DATA_FREEZE_THRESHOLD: &str =
    "memtable.partition_tree.data_freeze_threshold";
/// Option key for memtable partition tree fork dictionary bytes.
pub const MEMTABLE_PARTITION_TREE_FORK_DICTIONARY_BYTES: &str =
    "memtable.partition_tree.fork_dictionary_bytes";
/// Option key for skipping WAL.
pub const SKIP_WAL_KEY: &str = "skip_wal";
/// Option key for sst format.
pub const SST_FORMAT_KEY: &str = "sst_format";
// Note: Adding new options here should also check if this option should be removed in [metric_engine::engine::create::region_options_for_metadata_region].

/// Returns true if the `key` is a valid option key for the mito engine.
pub fn is_mito_engine_option_key(key: &str) -> bool {
    [
        "ttl",
        COMPACTION_TYPE,
        TWCS_TRIGGER_FILE_NUM,
        TWCS_MAX_OUTPUT_FILE_SIZE,
        TWCS_TIME_WINDOW,
        TWCS_REMOTE_COMPACTION,
        TWCS_FALLBACK_TO_LOCAL,
        "storage",
        "index.inverted_index.ignore_column_ids",
        "index.inverted_index.segment_row_count",
        WAL_OPTIONS_KEY,
        MEMTABLE_TYPE,
        MEMTABLE_PARTITION_TREE_INDEX_MAX_KEYS_PER_SHARD,
        MEMTABLE_PARTITION_TREE_DATA_FREEZE_THRESHOLD,
        MEMTABLE_PARTITION_TREE_FORK_DICTIONARY_BYTES,
        // We don't allow to create a mito table with sparse primary key encoding directly.
        APPEND_MODE_KEY,
        MERGE_MODE_KEY,
        SST_FORMAT_KEY,
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
            "compaction.twcs.trigger_file_num"
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
            "memtable.partition_tree.index_max_keys_per_shard"
        ));
        assert!(is_mito_engine_option_key(
            "memtable.partition_tree.data_freeze_threshold"
        ));
        assert!(is_mito_engine_option_key(
            "memtable.partition_tree.fork_dictionary_bytes"
        ));
        assert!(is_mito_engine_option_key("append_mode"));
        assert!(!is_mito_engine_option_key("foo"));
    }
}
