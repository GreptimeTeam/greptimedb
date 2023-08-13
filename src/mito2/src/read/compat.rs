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

//! Utilities to read data with different schema revision.

use crate::metadata::ColumnMetadata;

/// Compatibility adapter for data with different schema.
pub struct SchemaCompat {
    /// Metadata of columns expected to read.
    columns_to_read: Vec<ColumnMetadata>,
}

// 1. batch: full pk and values
// for new pk column, need to add default value.
// for columns to read, need to add default vector.

// parquet columns to read
// - all primary key columns in old meta
// - other columns to read in old meta
//
// needs
// - old region metadata
// - column ids to read
