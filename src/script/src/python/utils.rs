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

use common_runtime::JoinHandle;
use futures::Future;

/// just like [`tokio::task::spawn_blocking`] but using a dedicated runtime(runtime `bg`) using by `scripts` crate
pub fn spawn_blocking_script<F, R>(f: F) -> JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    common_runtime::spawn_blocking_bg(f)
}

/// Please only use this method because you are calling from (optionally first as async) to sync then to a async
/// a terrible hack to call async from sync by:
///
/// TODO(discord9): find a better way
/// 1. using a cached runtime
/// 2. block on that runtime
pub fn block_on_async<T, F>(f: F) -> std::thread::Result<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let rt = common_runtime::bg_runtime();
    // spawn a thread to block on the runtime, also should prevent `start a runtime inside of runtime` error
    // it's ok to block here, assume calling from async to sync is using a `spawn_blocking_*` call
    std::thread::spawn(move || rt.block_on(f)).join()
}
