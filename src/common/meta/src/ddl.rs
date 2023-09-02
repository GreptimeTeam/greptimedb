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

use crate::cache_invalidator::CacheInvalidatorRef;
use crate::datanode_manager::DatanodeManagerRef;
use crate::key::TableMetadataManagerRef;

pub mod alter_table;
pub mod create_table;
pub mod drop_table;
pub mod utils;

#[derive(Clone)]
pub struct DdlContext {
    pub datanode_manager: DatanodeManagerRef,
    pub cache_invalidator: CacheInvalidatorRef,
    pub table_metadata_manager: TableMetadataManagerRef,
}
