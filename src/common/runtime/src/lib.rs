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

pub mod error;
mod global;
mod metrics;
mod repeated_task;
pub mod runtime;

pub use global::{
    bg_runtime, block_on_bg, block_on_read, block_on_write, create_runtime, init_global_runtimes,
    read_runtime, spawn_bg, spawn_blocking_bg, spawn_blocking_read, spawn_blocking_write,
    spawn_read, spawn_write, write_runtime,
};

pub use crate::repeated_task::{BoxedTaskFunction, RepeatedTask, TaskFunction};
pub use crate::runtime::{Builder, JoinError, JoinHandle, Runtime};
