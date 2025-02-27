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

use common_time::Timestamp;
use datafusion_expr::{col, lit, Expr};
use datatypes::timestamp::TimestampNanosecond;

use crate::error::{InvalidPipelineVersionSnafu, Result};
use crate::table::{
    PIPELINE_TABLE_CREATED_AT_COLUMN_NAME, PIPELINE_TABLE_PIPELINE_NAME_COLUMN_NAME,
    PIPELINE_TABLE_PIPELINE_SCHEMA_COLUMN_NAME,
};
use crate::PipelineVersion;

pub fn to_pipeline_version(version_str: Option<&str>) -> Result<PipelineVersion> {
    match version_str {
        Some(version) => {
            let ts = Timestamp::from_str_utc(version)
                .map_err(|_| InvalidPipelineVersionSnafu { version }.build())?;
            Ok(Some(TimestampNanosecond(ts)))
        }
        None => Ok(None),
    }
}

pub(crate) fn prepare_dataframe_conditions(
    schema: &str,
    name: &str,
    version: PipelineVersion,
) -> Expr {
    let mut conditions = vec![
        col(PIPELINE_TABLE_PIPELINE_NAME_COLUMN_NAME).eq(lit(name)),
        col(PIPELINE_TABLE_PIPELINE_SCHEMA_COLUMN_NAME).eq(lit(schema)),
    ];

    if let Some(v) = version {
        conditions
            .push(col(PIPELINE_TABLE_CREATED_AT_COLUMN_NAME).eq(lit(v.0.to_iso8601_string())));
    }

    conditions.into_iter().reduce(Expr::and).unwrap()
}

pub(crate) fn generate_pipeline_cache_key(
    schema: &str,
    name: &str,
    version: PipelineVersion,
) -> String {
    match version {
        Some(version) => format!("{}/{}/{}", schema, name, i64::from(version)),
        None => format!("{}/{}/latest", schema, name),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_pipeline_version() {
        let none_result = to_pipeline_version(None);
        assert!(none_result.is_ok());
        assert!(none_result.unwrap().is_none());

        let some_result = to_pipeline_version(Some("2023-01-01 00:00:00Z"));
        assert!(some_result.is_ok());
        assert_eq!(
            some_result.unwrap(),
            Some(TimestampNanosecond::new(1672531200000000000))
        );

        let invalid = to_pipeline_version(Some("invalid"));
        assert!(invalid.is_err());
    }

    #[test]
    fn test_generate_pipeline_cache_key() {
        let schema = "test_schema";
        let name = "test_name";
        let latest = generate_pipeline_cache_key(schema, name, None);
        assert_eq!(latest, "test_schema/test_name/latest");

        let versioned = generate_pipeline_cache_key(
            schema,
            name,
            Some(TimestampNanosecond::new(1672531200000000000)),
        );
        assert_eq!(versioned, "test_schema/test_name/1672531200000000000");
    }
}
