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

//! Abstraction to get current time.

use std::sync::Arc;
use std::time::Duration;

use common_time::util::current_time_millis;

/// Trait to get current time and deal with durations.
///
/// We define the trait to simplify time related tests.
pub trait TimeProvider: std::fmt::Debug + Send + Sync {
    /// Returns current time in millis.
    fn current_time_millis(&self) -> i64;

    /// Returns millis elapsed since specify time.
    fn elapsed_since(&self, current_millis: i64) -> i64;

    /// Computes the actual duration to wait from an expected one.
    fn wait_duration(&self, duration: Duration) -> Duration {
        duration
    }
}

pub type TimeProviderRef = Arc<dyn TimeProvider>;

/// Default implementation of the time provider based on std.
#[derive(Debug)]
pub struct StdTimeProvider;

impl TimeProvider for StdTimeProvider {
    fn current_time_millis(&self) -> i64 {
        current_time_millis()
    }

    fn elapsed_since(&self, current_millis: i64) -> i64 {
        current_time_millis() - current_millis
    }
}
