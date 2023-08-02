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

//! Worker requests.

use std::time::Duration;

use common_base::readable_size::ReadableSize;
use greptime_proto::v1::Rows;
use store_api::storage::{ColumnId, CompactionStrategy, OpType, RegionId};
use tokio::sync::oneshot::{self, Receiver, Sender};

use crate::config::DEFAULT_WRITE_BUFFER_SIZE;
use crate::error::Result;
use crate::metadata::ColumnMetadata;

/// Options that affect the entire region.
///
/// Users need to specify the options while creating/opening a region.
#[derive(Debug)]
pub struct RegionOptions {
    /// Region memtable max size in bytes.
    pub write_buffer_size: Option<ReadableSize>,
    /// Region SST files TTL.
    pub ttl: Option<Duration>,
    /// Compaction strategy.
    pub compaction_strategy: CompactionStrategy,
}

impl Default for RegionOptions {
    fn default() -> Self {
        RegionOptions {
            write_buffer_size: Some(DEFAULT_WRITE_BUFFER_SIZE),
            ttl: None,
            compaction_strategy: CompactionStrategy::LeveledTimeWindow,
        }
    }
}

/// Create region request.
#[derive(Debug)]
pub struct CreateRequest {
    /// Region to create.
    pub region_id: RegionId,
    /// Data directory of the region.
    pub region_dir: String,
    /// Columns in this region.
    pub column_metadatas: Vec<ColumnMetadata>,
    /// Columns in the primary key.
    pub primary_key: Vec<ColumnId>,
    /// Create region if not exists.
    pub create_if_not_exists: bool,
    /// Options of the created region.
    pub options: RegionOptions,
}

/// Open region request.
#[derive(Debug)]
pub struct OpenRequest {
    /// Region to open.
    pub region_id: RegionId,
    /// Data directory of the region.
    pub region_dir: String,
    /// Options of the created region.
    pub options: RegionOptions,
}

/// Close region request.
#[derive(Debug)]
pub struct CloseRequest {
    /// Region to close.
    pub region_id: RegionId,
}

/// Request to write a region.
#[derive(Debug)]
pub struct WriteRequest {
    /// Region to write.
    pub region_id: RegionId,
    /// Type of the write request.
    pub op_type: OpType,
    /// Rows to write.
    pub rows: Rows,
}

impl WriteRequest {
    /// Validate the request.
    pub(crate) fn validate(&self) -> Result<()> {
        // - checks whether the request is too large.
        // - checks whether each row in rows has the same schema.
        // - checks rows don't have duplicate columns.
        unimplemented!()
    }
}

/// Sender and write request.
pub(crate) struct SenderWriteRequest {
    /// Result sender.
    pub(crate) sender: Option<Sender<Result<()>>>,
    pub(crate) request: WriteRequest,
}

/// Request sent to a worker
pub(crate) enum WorkerRequest {
    /// Region request.
    Region(RegionRequest),

    /// Notify a worker to stop.
    Stop,
}

/// Request to modify a region.
#[derive(Debug)]
pub(crate) struct RegionRequest {
    /// Sender to send result.
    ///
    /// Now the result is a `Result<()>`, but we could replace the empty tuple
    /// with an enum if we need to carry more information.
    pub(crate) sender: Option<Sender<Result<()>>>,
    /// Request body.
    pub(crate) body: RequestBody,
}

impl RegionRequest {
    /// Creates a [RegionRequest] and a receiver from `body`.
    pub(crate) fn from_body(body: RequestBody) -> (RegionRequest, Receiver<Result<()>>) {
        let (sender, receiver) = oneshot::channel();
        (
            RegionRequest {
                sender: Some(sender),
                body,
            },
            receiver,
        )
    }
}

/// Body to carry actual region request.
#[derive(Debug)]
pub(crate) enum RequestBody {
    /// Write to a region.
    Write(WriteRequest),

    // DDL:
    /// Creates a new region.
    Create(CreateRequest),
    /// Opens an existing region.
    Open(OpenRequest),
    /// Closes a region.
    Close(CloseRequest),
}

impl RequestBody {
    /// Region id of this request.
    pub(crate) fn region_id(&self) -> RegionId {
        match self {
            RequestBody::Write(req) => req.region_id,
            RequestBody::Create(req) => req.region_id,
            RequestBody::Open(req) => req.region_id,
            RequestBody::Close(req) => req.region_id,
        }
    }

    /// Returns whether the request is a write request.
    pub(crate) fn is_write(&self) -> bool {
        matches!(self, RequestBody::Write(_))
    }

    /// Converts the request into a [WriteRequest].
    ///
    /// # Panics
    /// Panics if it isn't a [WriteRequest].
    pub(crate) fn into_write_request(self) -> WriteRequest {
        match self {
            RequestBody::Write(req) => req,
            other => panic!("expect write request, found {other:?}"),
        }
    }
}
