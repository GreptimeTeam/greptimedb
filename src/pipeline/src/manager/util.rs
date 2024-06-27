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
use datafusion_expr::{and, col, lit, Expr};
use datatypes::timestamp::TimestampNanosecond;

use crate::error::{InvalidPipelineVersionSnafu, Result};
use crate::table::{
    PIPELINE_TABLE_CREATED_AT_COLUMN_NAME, PIPELINE_TABLE_PIPELINE_NAME_COLUMN_NAME,
    PIPELINE_TABLE_PIPELINE_SCHEMA_COLUMN_NAME,
};
use crate::PipelineVersion;

pub fn to_pipeline_version(version_str: Option<String>) -> Result<PipelineVersion> {
    match version_str {
        Some(version) => {
            let ts = Timestamp::from_str_utc(&version)
                .map_err(|_| InvalidPipelineVersionSnafu { version }.build())?;
            Ok(Some(TimestampNanosecond(ts)))
        }
        None => Ok(None),
    }
}

pub(crate) fn build_plan_filter(schema: &str, name: &str, version: PipelineVersion) -> Expr {
    let schema_and_name_filter = and(
        col(PIPELINE_TABLE_PIPELINE_SCHEMA_COLUMN_NAME).eq(lit(schema)),
        col(PIPELINE_TABLE_PIPELINE_NAME_COLUMN_NAME).eq(lit(name)),
    );
    if let Some(v) = version {
        and(
            schema_and_name_filter,
            col(PIPELINE_TABLE_CREATED_AT_COLUMN_NAME).eq(lit(v.0.to_iso8601_string())),
        )
    } else {
        schema_and_name_filter
    }
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
