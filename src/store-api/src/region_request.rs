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

use std::collections::HashMap;

use datatypes::arrow::row::Rows;

use crate::storage::{OpType, RegionId, ScanRequest};

#[derive(Debug)]
pub enum RegionRequest {
    Write(RegionWriteRequest),
    Read(RegionReadRequest),
    Delete(RegionDeleteRequest),
    Create(RegionCreateRequest),
    Drop(RegionDropRequest),
    Open(RegionOpenRequest),
    Close(RegionCloseRequest),
    Alter(RegionAlterRequest),
    Flush(RegionFlushRequest),
    Compact(RegionCompactRequest),
}

/// Request to write a region.
#[derive(Debug)]
pub struct RegionWriteRequest {
    /// Region to write.
    pub region_id: RegionId,
    /// Type of the write request.
    pub op_type: OpType,
    /// Rows to write.
    pub rows: Rows,
    /// Map column name to column index in `rows`.
    pub name_to_index: HashMap<String, usize>,
}

#[derive(Debug)]
pub struct RegionReadRequest {
    pub req: ScanRequest,
}

#[derive(Debug)]
pub struct RegionDeleteRequest {
    // todo
}

#[derive(Debug)]
pub struct RegionCreateRequest {
    /// Region engine name
    pub engine: String,
}

#[derive(Debug)]
pub struct RegionDropRequest {
    /// Region engine name
    pub engine: String,
}

/// Open region request.
#[derive(Debug)]
pub struct RegionOpenRequest {
    /// Region engine name
    pub engine: String,
    /// Region to open.
    pub region_id: RegionId,
    /// Data directory of the region.
    pub region_dir: String,
    /// Options of the created region.
    pub options: HashMap<String, String>,
}

/// Close region request.
#[derive(Debug)]
pub struct RegionCloseRequest {
    /// Region engine name
    pub engine: String,
    /// Region to close.
    pub region_id: RegionId,
}

#[derive(Debug)]
pub struct RegionAlterRequest {
    // todo
}

#[derive(Debug)]
pub struct RegionFlushRequest {
    // todo
}

#[derive(Debug)]
pub struct RegionCompactRequest {
    // todo
}
