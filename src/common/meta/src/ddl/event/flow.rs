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

use std::any::Any;

use api::v1::value::ValueData;
use api::v1::{ColumnSchema, Row};
use common_event_recorder::Event;
use common_event_recorder::error::{Result, SerializeEventSnafu};
use common_event_recorder::event_table::{
    CATALOG_NAME_COLUMN, FLOW_ID_COLUMN, FLOW_NAME_COLUMN, SCHEMA_NAME_COLUMN, column_schemas,
    nullable_string, nullable_value,
};
use serde::Serialize;
use snafu::ResultExt;

pub(crate) const CREATE_FLOW_EVENT_TYPE: &str = "create_flow";
pub(crate) const DROP_FLOW_EVENT_TYPE: &str = "drop_flow";

const PAYLOAD_VERSION: u8 = 1;

/// The bounded Create Flow intent allowed in a submitted event payload.
#[derive(Debug)]
pub(crate) struct CreateFlowEventIntent {
    pub(crate) or_replace: bool,
    pub(crate) create_if_not_exists: bool,
    pub(crate) expire_after: Option<i64>,
    pub(crate) eval_interval_secs: Option<i64>,
}

#[derive(Debug, Serialize)]
struct CreateFlowPayload {
    version: u8,
    or_replace: bool,
    create_if_not_exists: bool,
    expire_after: Option<i64>,
    eval_interval_secs: Option<i64>,
}

#[derive(Debug, Serialize)]
struct DropFlowPayload {
    version: u8,
    drop_if_exists: bool,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum FlowDdlPayload {
    Create(CreateFlowPayload),
    Drop(DropFlowPayload),
}

/// A Flow DDL event with a schema shared by every lifecycle trigger.
#[derive(Debug)]
pub(crate) struct FlowDdlEvent {
    event_type: &'static str,
    catalog_name: Option<String>,
    schema_name: Option<String>,
    flow_name: Option<String>,
    flow_id: Option<u32>,
    payload: Option<FlowDdlPayload>,
}

impl FlowDdlEvent {
    /// Builds the bounded event emitted when creating a Flow is submitted.
    pub(crate) fn create_submitted(
        catalog_name: &str,
        schema_name: &str,
        flow_name: &str,
        intent: CreateFlowEventIntent,
    ) -> Self {
        Self {
            event_type: CREATE_FLOW_EVENT_TYPE,
            catalog_name: Some(catalog_name.to_string()),
            schema_name: Some(schema_name.to_string()),
            flow_name: Some(flow_name.to_string()),
            flow_id: None,
            payload: Some(FlowDdlPayload::Create(CreateFlowPayload {
                version: PAYLOAD_VERSION,
                or_replace: intent.or_replace,
                create_if_not_exists: intent.create_if_not_exists,
                expire_after: intent.expire_after,
                eval_interval_secs: intent.eval_interval_secs,
            })),
        }
    }

    /// Builds the bounded event emitted when dropping a Flow is submitted.
    pub(crate) fn drop_submitted(
        catalog_name: &str,
        flow_name: &str,
        flow_id: u32,
        drop_if_exists: bool,
    ) -> Self {
        Self {
            event_type: DROP_FLOW_EVENT_TYPE,
            catalog_name: Some(catalog_name.to_string()),
            schema_name: None,
            flow_name: Some(flow_name.to_string()),
            flow_id: Some(flow_id),
            payload: Some(FlowDdlPayload::Drop(DropFlowPayload {
                version: PAYLOAD_VERSION,
                drop_if_exists,
            })),
        }
    }

    /// Builds a lightweight Create Flow lifecycle event.
    pub(crate) fn create_lifecycle() -> Self {
        Self::lifecycle(CREATE_FLOW_EVENT_TYPE)
    }

    /// Builds a successful Create Flow event containing only a resolved ID.
    pub(crate) fn create_succeeded(flow_id: Option<u32>) -> Self {
        Self {
            flow_id,
            ..Self::lifecycle(CREATE_FLOW_EVENT_TYPE)
        }
    }

    /// Builds a lightweight Drop Flow lifecycle event.
    pub(crate) fn drop_lifecycle() -> Self {
        Self::lifecycle(DROP_FLOW_EVENT_TYPE)
    }

    fn lifecycle(event_type: &'static str) -> Self {
        Self {
            event_type,
            catalog_name: None,
            schema_name: None,
            flow_name: None,
            flow_id: None,
            payload: None,
        }
    }
}

impl Event for FlowDdlEvent {
    fn event_type(&self) -> &str {
        self.event_type
    }

    fn json_payload(&self) -> Result<serde_json::Value> {
        match &self.payload {
            Some(payload) => serde_json::to_value(payload).context(SerializeEventSnafu),
            None => Ok(serde_json::Value::Null),
        }
    }

    fn extra_schema(&self) -> Vec<ColumnSchema> {
        column_schemas([
            &CATALOG_NAME_COLUMN,
            &SCHEMA_NAME_COLUMN,
            &FLOW_NAME_COLUMN,
            &FLOW_ID_COLUMN,
        ])
    }

    fn extra_rows(&self) -> Result<Vec<Row>> {
        Ok(vec![Row {
            values: vec![
                nullable_string(self.catalog_name.as_deref()),
                nullable_string(self.schema_name.as_deref()),
                nullable_string(self.flow_name.as_deref()),
                nullable_value(self.flow_id.map(ValueData::U32Value)),
            ],
        }])
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
