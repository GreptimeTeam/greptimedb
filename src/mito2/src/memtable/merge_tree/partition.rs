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

//! Partition of a merge tree.
//!
//! We only support partitioning the tree by pre-defined internal columns.

use std::sync::{Arc, RwLock};

use crate::memtable::merge_tree::shard::Shard;
use crate::memtable::merge_tree::shard_builder::ShardBuilder;
use crate::memtable::merge_tree::ShardId;

/// Key of a partition.
pub type PartitionKey = u32;

/// A tree partition.
pub struct Partition {
    inner: RwLock<Inner>,
}

pub type PartitionRef = Arc<Partition>;

/// Inner struct of the partition.
struct Inner {
    /// Shard whose dictionary is active.
    shard_builder: ShardBuilder,
    next_shard_id: ShardId,
    /// Shards with frozon dictionary.
    shards: Vec<Shard>,
}
