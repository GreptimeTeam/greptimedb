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

//! Builder of a shard.

use crate::memtable::merge_tree::data::DataBuffer;
use crate::memtable::merge_tree::dict::KeyDictBuilder;

/// Builder to write keys and data to a shard that the key dictionary
/// is still active.
pub struct ShardBuilder {
    /// Builder for the key dictionary.
    dict_builder: KeyDictBuilder,
    /// Buffer to store data.
    data_buffer: DataBuffer,
}
