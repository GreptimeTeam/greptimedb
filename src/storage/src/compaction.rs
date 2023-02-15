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

mod dedup_deque;
pub mod noop;
mod picker;
mod rate_limit;
mod scheduler;
mod strategy;
mod task;
mod writer;

use std::sync::Arc;

pub use picker::{Picker, PickerContext, SimplePicker};
pub use scheduler::{
    CompactionRequest, CompactionRequestImpl, CompactionScheduler, CompactionSchedulerConfig,
    LocalCompactionScheduler,
};
pub use task::{CompactionTask, CompactionTaskImpl};

pub type CompactionSchedulerRef<S> =
    Arc<dyn CompactionScheduler<CompactionRequestImpl<S>> + Send + Sync>;
