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

//! Channel to batch requests.

use std::mem;
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use crate::worker::request::WorkerRequest;

/// Region request sender.
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
    pub(crate) async fn receive_all(&self, buffer: &mut RequestBuffer) {
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
pub(crate) type RequestBuffer = Vec<WorkerRequest>;

/// A multi-producer, single-consumer channel to batch region requests.
#[derive(Debug, Default)]
struct RequestChan {
    /// Requests in FIFO order.
    channel: Mutex<RequestBuffer>,
    /// Receiver notify.
    notify: Notify,
}

impl RequestChan {
    /// Push a new `request` to the end of the channel.
    fn push_back(&self, request: WorkerRequest) {
        let mut channel = self.channel.lock().unwrap();
        let wake = channel.is_empty();
        channel.push(request);
        if wake {
            // Only notify waker when this is the first request
            // in the channel.
            self.notify.notify_one();
        }
    }

    /// Take all requests from the channel to the `buffer`.
    ///
    /// Requests in the buffer have the same order as what they have in the channel.
    fn take(&self, buffer: &mut RequestBuffer) {
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
