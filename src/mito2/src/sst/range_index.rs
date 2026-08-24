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

//! Per-SST series row-range index.

mod writer;

pub use writer::{
    SstRangeIndexWriter, SstRangeIndexWriterMetrics, SstRangeIndexWriterOptions, range_index_schema,
};

const ROW_GROUP_ID_COLUMN: &str = "row_group_id";
const TABLE_ID_COLUMN: &str = "__table_id";
const TSID_COLUMN: &str = "__tsid";
const START_COLUMN: &str = "start";
const END_COLUMN: &str = "end";
