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

//! [CancellationHandle] is used to compose with manual implementation of [futures::future::Future]
//! or [futures::stream::Stream] to facilitate cancellation.
//! See example in [frontend::stream_wrapper::CancellableStreamWrapper].

use std::sync::atomic::{AtomicBool, Ordering};

use futures::task::AtomicWaker;

pub struct CancellationHandle {
    pub waker: AtomicWaker,
    cancelled: AtomicBool,
}

impl CancellationHandle {
    pub fn new() -> Self {
        Self {
            waker: AtomicWaker::new(),
            cancelled: AtomicBool::new(false),
        }
    }

    /// Cancels a future or stream.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
        self.waker.wake();
    }

    /// Is this handle cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }
}
