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

use common_time::Timezone;
use session::context::{QueryContextBuilder, QueryContextRef};
use snafu::ResultExt;

use crate::error::{Error, InvalidTimezoneSnafu};

pub fn to_meta_query_context(
    query_context: QueryContextRef,
) -> common_meta::rpc::ddl::QueryContext {
    common_meta::rpc::ddl::QueryContext {
        current_catalog: query_context.current_catalog().to_string(),
        current_schema: query_context.current_schema().clone(),
        timezone: query_context.timezone().to_string(),
        extensions: query_context.extensions(),
        channel: query_context.channel() as u8,
    }
}

pub fn try_to_session_query_context(
    value: common_meta::rpc::ddl::QueryContext,
) -> Result<session::context::QueryContext, Error> {
    Ok(QueryContextBuilder::default()
        .current_catalog(value.current_catalog)
        .current_schema(value.current_schema)
        .timezone(
            Timezone::from_tz_string(&value.timezone).context(InvalidTimezoneSnafu {
                timezone: value.timezone,
            })?,
        )
        .extensions(value.extensions)
        .channel((value.channel as u32).into())
        .build())
}
