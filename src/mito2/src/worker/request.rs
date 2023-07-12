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

use datatypes::schema::RawSchema;
use store_api::storage::{CompactionStrategy, RegionId};
use tokio::sync::oneshot::{self, Receiver, Sender};

use crate::error::Result;

/// Create region request.
#[derive(Debug)]
pub struct CreateRequest {
    /// Region to create.
    pub region_id: RegionId,
    /// Schema of the table that this region belongs to.
    pub schema: RawSchema,
    /// Indices of columns in the primary key.
    pub primary_key_indices: Vec<usize>,
    /// Create region if not exists.
    pub create_if_not_exists: bool,

    // Options:
    /// Region memtable max size in bytes
    pub write_buffer_size: Option<usize>,
    /// Region SST files TTL
    pub ttl: Option<Duration>,
    /// Compaction strategy
    pub compaction_strategy: CompactionStrategy,
}

impl CreateRequest {
    /// Validate the request.
    fn validate(&self) -> Result<()> {
        unimplemented!()
    }
}

/// Open region request.
#[derive(Debug)]
pub struct OpenRequest {
    /// Region to open.
    pub region_id: RegionId,
    /// Region memtable max size in bytes
    pub write_buffer_size: Option<usize>,
    /// Region SST files TTL
    pub ttl: Option<Duration>,
    /// Compaction strategy
    pub compaction_strategy: CompactionStrategy,
}

/// Request to write a region.
#[derive(Debug)]
pub(crate) struct WriteRequest {
    /// Region to write.
    pub region_id: RegionId,
}

/// Request handled by workers.
#[derive(Debug)]
pub(crate) struct WorkerRequest {
    /// Sender to send result.
    pub(crate) sender: Option<Sender<Result<()>>>,
    /// Request body.
    pub(crate) body: RequestBody,
}

impl WorkerRequest {
    /// Creates a [WorkerRequest] and a receiver from `body`.
    pub(crate) fn from_body(body: RequestBody) -> (WorkerRequest, Receiver<Result<()>>) {
        let (sender, receiver) = oneshot::channel();
        (
            WorkerRequest {
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
    // DML:
    /// Write to a region.
    Write(WriteRequest),

    // DDL:
    /// Creates a new region.
    Create(CreateRequest),
    /// Opens an existing region.
    Open(OpenRequest),
}

impl RequestBody {
    /// Region id of this request.
    pub(crate) fn region_id(&self) -> RegionId {
        match self {
            RequestBody::Write(req) => req.region_id,
            RequestBody::Create(req) => req.region_id,
            RequestBody::Open(req) => req.region_id,
        }
    }

    /// Returns whether the request is a DDL (e.g. CREATE/OPEN/ALTER).
    pub(crate) fn is_ddl(&self) -> bool {
        match self {
            RequestBody::Write(_) => false,
            RequestBody::Create(_) => true,
            RequestBody::Open(_) => true,
        }
    }
}
