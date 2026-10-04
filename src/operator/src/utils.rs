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

use std::sync::{Arc, RwLock};

use common_event_recorder::ProcedureEventInput;
use common_meta::procedure_executor::ExecutorContext;
use common_meta::rpc::ddl::{ORIGIN_FRONTEND_ADDR_EXTENSION_KEY, TriggerReason};
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
        snapshot_seqs: query_context.snapshots(),
        sst_min_sequences: query_context.sst_min_sequences(),
    }
}

/// Builds the execution envelope for a frontend-originated procedure.
pub(crate) fn to_executor_context(
    query_context: QueryContextRef,
    trigger_reason: TriggerReason,
) -> ExecutorContext {
    let actor = query_context.current_user().username().to_string();
    ExecutorContext {
        query_context: Some(to_meta_query_context(query_context)),
        actor: Some(actor),
        event_input: Some(ProcedureEventInput::new(trigger_reason)),
        ..Default::default()
    }
}

/// Builds a frontend-originated procedure envelope with the trusted frontend origin.
pub(crate) fn to_executor_context_with_origin_frontend(
    query_context: QueryContextRef,
    origin_frontend_addr: &str,
    trigger_reason: TriggerReason,
) -> ExecutorContext {
    let mut executor_context = to_executor_context(query_context, trigger_reason);
    if let Some(query_context) = &mut executor_context.query_context {
        query_context.extensions.insert(
            ORIGIN_FRONTEND_ADDR_EXTENSION_KEY.to_string(),
            origin_frontend_addr.to_string(),
        );
    }
    executor_context
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
        .snapshot_seqs(Arc::new(RwLock::new(value.snapshot_seqs)))
        .sst_min_sequences(Arc::new(RwLock::new(value.sst_min_sequences)))
        .build())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};

    use common_meta::rpc::ddl::{ORIGIN_FRONTEND_ADDR_EXTENSION_KEY, TriggerReason};
    use common_time::Timezone;
    use session::context::QueryContextBuilder;

    use super::{
        to_executor_context_with_origin_frontend, to_meta_query_context,
        try_to_session_query_context,
    };

    #[test]
    fn test_query_context_meta_roundtrip_with_sequences() {
        let session_ctx = Arc::new(
            QueryContextBuilder::default()
                .current_catalog("c1".to_string())
                .current_schema("s1".to_string())
                .timezone(Timezone::from_tz_string("UTC").unwrap())
                .set_extension("flow.return_region_seq".to_string(), "true".to_string())
                .snapshot_seqs(Arc::new(RwLock::new(HashMap::from([(10, 100)]))))
                .sst_min_sequences(Arc::new(RwLock::new(HashMap::from([(10, 90)]))))
                .build(),
        );

        let meta_ctx = to_meta_query_context(session_ctx);
        let roundtrip = try_to_session_query_context(meta_ctx).unwrap();

        assert_eq!(roundtrip.current_catalog(), "c1");
        assert_eq!(roundtrip.current_schema(), "s1");
        assert_eq!(roundtrip.snapshots(), HashMap::from([(10, 100)]));
        assert_eq!(roundtrip.sst_min_sequences(), HashMap::from([(10, 90)]));
        assert_eq!(roundtrip.extension("flow.return_region_seq"), Some("true"));
    }

    #[test]
    fn test_executor_context_with_origin_frontend_overrides_reserved_key() {
        let session_ctx = Arc::new(
            QueryContextBuilder::default()
                .set_extension(
                    ORIGIN_FRONTEND_ADDR_EXTENSION_KEY.to_string(),
                    "spoofed".to_string(),
                )
                .build(),
        );

        let executor_context = to_executor_context_with_origin_frontend(
            session_ctx,
            "127.0.0.1:4000",
            TriggerReason::Manual,
        );
        let meta_ctx = executor_context.query_context.as_ref().unwrap();

        assert_eq!(
            meta_ctx
                .extensions
                .get(ORIGIN_FRONTEND_ADDR_EXTENSION_KEY)
                .map(String::as_str),
            Some("127.0.0.1:4000")
        );
        assert_eq!(
            executor_context.event_input.unwrap().reason,
            TriggerReason::Manual
        );
    }
}
