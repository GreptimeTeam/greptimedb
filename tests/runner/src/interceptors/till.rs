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

pub const PREFIX: &str = "TILL";

/// Interceptor that skips tests if the target version is greater than the specified version.
///
/// Usage: `-- SQLNESS TILL 0.15.0`
///
/// The test will be skipped if `target_version > till_version`.
pub struct TillInterceptor {
    till_version: Version,
    target_version: Version,
}

impl TillInterceptor {
    pub fn new(till_version: Version, target_version: Version) -> Self {
        Self {
            till_version,
            target_version,
        }
    }
}

impl Interceptor for TillInterceptor {
    fn before_execute(&self, sql: &mut Vec<String>, _ctx: &mut sqlness::QueryContext) {
        if self.target_version > self.till_version {
            sql.clear();
        }
    }
}

pub struct TillInterceptorFactory {
    target_version: Version,
}

impl TillInterceptorFactory {
    pub fn new(target_version: Version) -> Self {
        Self { target_version }
    }
}

impl InterceptorFactory for TillInterceptorFactory {
    fn try_new(&self, ctx: &str) -> Result<InterceptorRef, SqlnessError> {
        let till_version = Version::parse(ctx).map_err(|e| SqlnessError::InvalidContext {
            prefix: PREFIX.to_string(),
            msg: format!("Failed to parse version '{}': {:?}", ctx, e),
        })?;

        Ok(Box::new(TillInterceptor::new(
            till_version,
            self.target_version.clone(),
        )))
    }
}
