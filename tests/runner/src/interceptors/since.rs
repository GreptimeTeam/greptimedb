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

use crate::version::Version;

pub const PREFIX: &str = "SINCE";

/// Interceptor that skips tests if the target version is less than the specified version.
///
/// Usage: `-- SQLNESS SINCE 0.15.0`
///
/// The test will be skipped if `target_version < since_version`.
pub struct SinceInterceptor {
    since_version: Version,
    target_version: Version,
}

impl SinceInterceptor {
    pub fn new(since_version: Version, target_version: Version) -> Self {
        Self {
            since_version,
            target_version,
        }
    }
}

impl Interceptor for SinceInterceptor {
    fn before_execute(&self, sql: &mut Vec<String>, _ctx: &mut sqlness::QueryContext) {
        // Skip execution if target version is less than since version
        if self.target_version < self.since_version {
            sql.clear();
        }
    }

    fn after_execute(&self, result: &mut String) {
        result.clear();
        *result = format!(
            "{} target version {} < {}",
            sqlness::SKIP_MARKER_PREFIX,
            self.target_version,
            self.since_version
        );
    }
}

pub struct SinceInterceptorFactory {
    target_version: Version,
}

impl SinceInterceptorFactory {
    pub fn new(target_version: Version) -> Self {
        Self { target_version }
    }
}

impl InterceptorFactory for SinceInterceptorFactory {
    fn try_new(&self, ctx: &str) -> Result<InterceptorRef, SqlnessError> {
        let since_version = Version::parse(ctx).map_err(|e| SqlnessError::InvalidContext {
            prefix: PREFIX.to_string(),
            msg: format!("Failed to parse version '{}': {:?}", ctx, e),
        })?;

        Ok(Box::new(SinceInterceptor::new(
            since_version,
            self.target_version.clone(),
        )))
    }
}
