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

//! Worker requests and channels to batch requests.

use std::mem;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datatypes::schema::RawSchema;
use store_api::storage::{CompactionStrategy, RegionId};
use tokio::sync::Notify;
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

/// Request handled by workers.
#[derive(Debug)]
pub(crate) enum WorkerRequest {
    /// Write a region.
    Write(WriteRequest),

    /// Control a region.
    Control(ControlRequest),
}

/// Request to write a region.
#[derive(Debug)]
pub(crate) struct WriteRequest {
    /// Region to write.
    pub region_id: RegionId,
}

/// Request to control (alter) a region.
#[derive(Debug)]
pub(crate) enum ControlRequest {
    /// Creates a new region.
    Create(CreateRequest),

    /// Opens an existing region.
    Open(OpenRequest),
}

// Region request sender.
#[derive(Debug)]
pub(crate) struct Sender {
    channel: Arc<RequestChan>,
}

impl Sender {
    /// Sends a `request` to the channel.
    pub(crate) fn send(&self, request: WorkerRequest) {
        self.channel.push_back(request);
    }

    /// Notify the channel receiver without sending a request.
    pub(crate) fn notify(&self) {
        self.channel.notify_one();
    }
}

// Receivers **should not support Clone** since we can't wake up multiple receivers.
/// Region request receiver.
#[derive(Debug)]
pub(crate) struct Receiver {
    channel: Arc<RequestChan>,
}

impl Receiver {
    /// Receives all requests buffered in the channel in FIFO order.
    ///
    /// Waits for next request if the channel is empty.
    pub(crate) async fn receive_all(&self, buffer: &mut RequestQueue) {
        self.channel.notified().await;
        self.channel.take(buffer);
    }
}

/// Creates an unbounded new request channel to buffer requests.
///
/// Returns a pair of [Sender] and [Receiver].
pub(crate) fn request_channel() -> (Sender, Receiver) {
    let channel = Arc::new(RequestChan::default());
    let sender = Sender {
        channel: channel.clone(),
    };
    let receiver = Receiver { channel };

    (sender, receiver)
}

/// Request queue grouped by request type.
#[derive(Debug, Default)]
pub(crate) struct RequestQueue {
    /// Queued write requests.
    pub(crate) write_requests: Vec<WriteRequest>,
    /// Queued control requests.
    pub(crate) control_requests: Vec<ControlRequest>,
}

impl RequestQueue {
    /// Clear the queue.
    pub(crate) fn clear(&mut self) {
        self.write_requests.clear();
        self.control_requests.clear();
    }

    /// Push request to a specific queue.
    fn push_back(&mut self, request: WorkerRequest) {
        match request {
            WorkerRequest::Write(req) => self.write_requests.push(req),
            WorkerRequest::Control(req) => self.control_requests.push(req),
        }
    }

    /// Returns true if the queue is empty.
    fn is_empty(&self) -> bool {
        self.write_requests.is_empty() && self.control_requests.is_empty()
    }
}

/// A multi-producer, single-consumer channel to batch region requests.
#[derive(Debug, Default)]
struct RequestChan {
    /// Requests in FIFO order.
    channel: Mutex<RequestQueue>,
    /// Receiver notify.
    notify: Notify,
}

impl RequestChan {
    /// Push a new `request` to the end of the channel.
    fn push_back(&self, request: WorkerRequest) {
        let mut channel = self.channel.lock().unwrap();
        let wake = channel.is_empty();
        channel.push_back(request);
        if wake {
            // Only notify waker when this is the first request
            // in the channel.
            self.notify.notify_one();
        }
    }

    /// Take all requests from the channel to the `buffer`.
    ///
    /// Requests in the buffer have the same order as what they have in the channel.
    fn take(&self, buffer: &mut RequestQueue) {
        let mut channel = self.channel.lock().unwrap();
        mem::swap(&mut *channel, buffer);
    }

    /// Waits for requests.
    ///
    /// Only single waiter should wait on this method.
    async fn notified(&self) {
        self.notify.notified().await
    }

    /// Notify the receiver without sending a request.
    fn notify_one(&self) {
        self.notify.notify_one();
    }
}
