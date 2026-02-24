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

use sqlness::interceptor::{Interceptor, InterceptorFactory, InterceptorRef};
use sqlness::{SKIP_MARKER_PREFIX, SqlnessError};

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

    fn maybe_rewrite_to_skip_sql(&self, sql: &mut Vec<String>) {
        if self.target_version < self.since_version {
            let skip_marker = format!("{} {}", SKIP_MARKER_PREFIX, self.skip_reason());
            sql.clear();
            sql.push(format!("SELECT '{}';", skip_marker));
        }
    }

    fn skip_reason(&self) -> String {
        format!(
            "target version {} < {}",
            self.target_version, self.since_version
        )
    }

    fn normalize_skip_result(&self, result: &mut String) {
        if result.contains(SKIP_MARKER_PREFIX) {
            *result = format!("{} {}", SKIP_MARKER_PREFIX, self.skip_reason());
        }
    }
}

impl Interceptor for SinceInterceptor {
    fn before_execute(&self, sql: &mut Vec<String>, _ctx: &mut sqlness::QueryContext) {
        self.maybe_rewrite_to_skip_sql(sql);
    }

    fn after_execute(&self, result: &mut String) {
        self.normalize_skip_result(result);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_before_execute_keeps_sql_when_not_skipped() {
        let interceptor = SinceInterceptor::new(
            Version::parse("0.15.0").unwrap(),
            Version::parse("0.15.0").unwrap(),
        );
        let mut sql = vec!["SELECT 1;".to_string()];

        interceptor.maybe_rewrite_to_skip_sql(&mut sql);

        assert_eq!(sql, vec!["SELECT 1;"]);
    }

    #[test]
    fn test_before_execute_rewrites_sql_when_skipped() {
        let interceptor = SinceInterceptor::new(
            Version::parse("0.16.0").unwrap(),
            Version::parse("0.15.0").unwrap(),
        );
        let mut sql = vec!["SELECT 1;".to_string()];

        interceptor.maybe_rewrite_to_skip_sql(&mut sql);

        assert_eq!(sql.len(), 1);
        assert!(sql[0].contains(SKIP_MARKER_PREFIX));
        assert!(sql[0].contains("target version 0.15.0 < 0.16.0"));
    }

    #[test]
    fn test_after_execute_normalizes_skip_result() {
        let interceptor = SinceInterceptor::new(
            Version::parse("0.16.0").unwrap(),
            Version::parse("0.15.0").unwrap(),
        );
        let mut result = format!(
            "+----------------+\n| {} target version 0.15.0 < 0.16.0 |\n+----------------+",
            SKIP_MARKER_PREFIX
        );

        interceptor.normalize_skip_result(&mut result);

        assert_eq!(
            result,
            format!("{} target version 0.15.0 < 0.16.0", SKIP_MARKER_PREFIX)
        );
    }

    #[test]
    fn test_after_execute_keeps_non_skip_result() {
        let interceptor = SinceInterceptor::new(
            Version::parse("0.16.0").unwrap(),
            Version::parse("0.15.0").unwrap(),
        );
        let mut result = "Affected Rows: 1".to_string();

        interceptor.normalize_skip_result(&mut result);

        assert_eq!(result, "Affected Rows: 1");
    }
}
