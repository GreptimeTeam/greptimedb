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

use std::future::Future;
use std::sync::Arc;

use tokio::runtime::Handle;
pub use tokio::task::JoinHandle;

use crate::runtime::{Dropper, RuntimeTrait};
use crate::Builder;

/// A runtime to run future tasks
#[derive(Clone, Debug)]
pub struct DefaultRuntime {
    name: String,
    handle: Handle,
    // Used to receive a drop signal when dropper is dropped, inspired by databend
    _dropper: Arc<Dropper>,
}

impl DefaultRuntime {
    pub(crate) fn new(name: &str, handle: Handle, dropper: Arc<Dropper>) -> Self {
        Self {
            name: name.to_string(),
            handle,
            _dropper: dropper,
        }
    }
}

impl RuntimeTrait for DefaultRuntime {
    fn builder() -> Builder {
        Builder::default()
    }

    /// Spawn a future and execute it in this thread pool
    ///
    /// Similar to tokio::runtime::Runtime::spawn()
    fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.handle.spawn(future)
    }

    /// Run the provided function on an executor dedicated to blocking
    /// operations.
    fn spawn_blocking<F, R>(&self, func: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.handle.spawn_blocking(func)
    }

    /// Run a future to complete, this is the runtime's entry point
    fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.handle.block_on(future)
    }

    fn name(&self) -> &str {
        &self.name
    }
}
