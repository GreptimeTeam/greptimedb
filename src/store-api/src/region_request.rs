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

use api::v1::region::region_request;
use api::v1::Rows;

use crate::metadata::ColumnMetadata;
use crate::storage::{AlterRequest, ColumnId, RegionId, ScanRequest};

#[derive(Debug)]
pub enum RegionRequest {
    // TODO: rename to InsertRequest
    Put(RegionPutRequest),
    Delete(RegionDeleteRequest),
    Create(RegionCreateRequest),
    Drop(RegionDropRequest),
    Open(RegionOpenRequest),
    Close(RegionCloseRequest),
    Alter(RegionAlterRequest),
    Flush(RegionFlushRequest),
    Compact(RegionCompactRequest),
}

impl RegionRequest {
    /// Convert [Body](region_request::Body) to a group of [RegionRequest] with region id.
    /// Inserts/Deletes request might become multiple requests. Others are one-to-one.
    pub fn from_request_body(body: region_request::Body) -> Vec<(RegionId, Self)> {
        match body {
            region_request::Body::Inserts(inserts) => inserts
                .requests
                .into_iter()
                .map(|r| {
                    (
                        r.region_id.into(),
                        Self::Put(RegionPutRequest { rows: todo!() }),
                    )
                })
                .collect(),
            region_request::Body::Deletes(deletes) => todo!(),
            region_request::Body::Create(create) => {
                vec![(
                    create.region_id.into(),
                    Self::Create(RegionCreateRequest {
                        engine: create.engine,
                        column_metadatas: todo!(),
                        primary_key: create.primary_key,
                        create_if_not_exists: create.create_if_not_exists,
                        options: create.options,
                        region_dir: create.region_dir,
                    }),
                )]
            }
            region_request::Body::Drop(drop) => {
                vec![(drop.region_id.into(), Self::Drop(RegionDropRequest {}))]
            }
            region_request::Body::Open(open) => {
                vec![(
                    open.region_id.into(),
                    Self::Open(RegionOpenRequest {
                        engine: open.engine,
                        region_dir: open.region_dir,
                        options: open.options,
                    }),
                )]
            }
            region_request::Body::Close(close) => {
                vec![(close.region_id.into(), Self::Close(RegionCloseRequest {}))]
            }
            region_request::Body::Alter(alter) => {
                vec![(
                    alter.region_id.into(),
                    Self::Alter(RegionAlterRequest { request: todo!() }),
                )]
            }
            region_request::Body::Flush(flush) => {
                vec![(flush.region_id.into(), Self::Flush(RegionFlushRequest {}))]
            }
            region_request::Body::Compact(compact) => {
                vec![(
                    compact.region_id.into(),
                    Self::Compact(RegionCompactRequest {}),
                )]
            }
        }
    }
}

/// Request to put data into a region.
#[derive(Debug)]
pub struct RegionPutRequest {
    /// Rows to put.
    pub rows: Rows,
}

#[derive(Debug)]
pub struct RegionReadRequest {
    pub request: ScanRequest,
}

/// Request to delete data from a region.
#[derive(Debug)]
pub struct RegionDeleteRequest {
    /// Keys to rows to delete.
    ///
    /// Each row only contains primary key columns and a time index column.
    pub rows: Rows,
}

#[derive(Debug)]
pub struct RegionCreateRequest {
    /// Region engine name
    pub engine: String,
    /// Columns in this region.
    pub column_metadatas: Vec<ColumnMetadata>,
    /// Columns in the primary key.
    pub primary_key: Vec<ColumnId>,
    /// Create region if not exists.
    pub create_if_not_exists: bool,
    /// Options of the created region.
    pub options: HashMap<String, String>,
    /// Directory for region's data home. Usually is composed by catalog and table id
    pub region_dir: String,
}

#[derive(Debug)]
pub struct RegionDropRequest {}

/// Open region request.
#[derive(Debug)]
pub struct RegionOpenRequest {
    /// Region engine name
    pub engine: String,
    /// Data directory of the region.
    pub region_dir: String,
    /// Options of the opened region.
    pub options: HashMap<String, String>,
}

/// Close region request.
#[derive(Debug)]
pub struct RegionCloseRequest {}

#[derive(Debug)]
pub struct RegionAlterRequest {
    pub request: AlterRequest,
}

#[derive(Debug)]
pub struct RegionFlushRequest {}

#[derive(Debug)]
pub struct RegionCompactRequest {}
