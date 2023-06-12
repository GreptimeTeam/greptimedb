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

// ! mito table engine metrics

/// Elapsed time of creating tables
pub const MITO_CREATE_TABLE_ELAPSED: &str = "datanode.mito.create_table";
/// Elapsed time of creating single region when creating tables.
pub const MITO_CREATE_REGION_ELAPSED: &str = "datanode.mito.create_table.create_region";
/// Elapsed time of updating table manifest when creating tables.
pub const MITO_CREATE_TABLE_UPDATE_MANIFEST_ELAPSED: &str =
    "datanode.mito.create_table.update_manifest";
/// Elapsed time of opening tables
pub const MITO_OPEN_TABLE_ELAPSED: &str = "datanode.mito.open_table";
/// Elapsed time of altering tables
pub const MITO_ALTER_TABLE_ELAPSED: &str = "datanode.mito.alter_table";
/// Elapsed time of insertion
pub const MITO_INSERT_ELAPSED: &str = "datanode.mito.insert";
/// Insert batch size.
pub const MITO_INSERT_BATCH_SIZE: &str = "datanode.mito.insert_batch_size";
