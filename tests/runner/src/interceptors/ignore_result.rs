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

use sqlness::SqlnessError;
use sqlness::interceptor::{Interceptor, InterceptorFactory, InterceptorRef};

pub const PREFIX: &str = "IGNORE_RESULT";

/// Interceptor that ignores result matching for a query.
///
/// Usage: `-- SQLNESS IGNORE_RESULT`
///
/// When this interceptor is used, the test runner will only check that the query
/// executes successfully, without comparing the actual output to the expected result.
/// This is useful for operations where the exact output may vary (e.g., timestamps,
/// auto-generated IDs) or when testing that a feature doesn't crash.
pub struct IgnoreResultInterceptor;

impl Interceptor for IgnoreResultInterceptor {
    fn after_execute(&self, result: &mut String) {
        *result = "-- IGNORE_RESULT: Query executed successfully".to_string();
    }
}

pub struct IgnoreResultInterceptorFactory;

impl InterceptorFactory for IgnoreResultInterceptorFactory {
    fn try_new(&self, _ctx: &str) -> Result<InterceptorRef, SqlnessError> {
        Ok(Box::new(IgnoreResultInterceptor))
    }
}
