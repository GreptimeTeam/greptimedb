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
    CATALOG_NAME_COLUMN, EVENT_CONTEXT_COLUMN, SCHEMA_NAME_COLUMN, VIEW_ID_COLUMN,
    VIEW_NAME_COLUMN, column_schemas, nullable_json, nullable_string, nullable_value,
};
use serde::Serialize;
use snafu::ResultExt;

use crate::rpc::ddl::EventContext;

pub(crate) const CREATE_VIEW_EVENT_TYPE: &str = "create_view";
pub(crate) const DROP_VIEW_EVENT_TYPE: &str = "drop_view";

const PAYLOAD_VERSION: u8 = 1;

/// The bounded Create View intent allowed in a submitted event payload.
#[derive(Debug)]
pub(crate) struct CreateViewEventIntent {
    pub(crate) or_replace: bool,
    pub(crate) create_if_not_exists: bool,
    pub(crate) referenced_table_count: usize,
    pub(crate) column_count: usize,
}

#[derive(Debug, Serialize)]
struct CreateViewPayload {
    version: u8,
    or_replace: bool,
    create_if_not_exists: bool,
    referenced_table_count: usize,
    column_count: usize,
}

#[derive(Debug, Serialize)]
struct DropViewPayload {
    version: u8,
    drop_if_exists: bool,
}

#[derive(Debug)]
pub(crate) struct ViewDdlEvent {
    event_type: &'static str,
    catalog_name: Option<String>,
    schema_name: Option<String>,
    view_name: Option<String>,
    view_id: Option<u32>,
    payload: Option<ViewDdlPayload>,
    event_context: Option<EventContext>,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum ViewDdlPayload {
    Create(CreateViewPayload),
    Drop(DropViewPayload),
}

impl ViewDdlEvent {
    pub(crate) fn create_submitted(
        catalog_name: &str,
        schema_name: &str,
        view_name: &str,
        intent: CreateViewEventIntent,
        event_context: EventContext,
    ) -> Self {
        Self::submitted(
            CREATE_VIEW_EVENT_TYPE,
            catalog_name,
            schema_name,
            view_name,
            None,
            ViewDdlPayload::Create(CreateViewPayload {
                version: PAYLOAD_VERSION,
                or_replace: intent.or_replace,
                create_if_not_exists: intent.create_if_not_exists,
                referenced_table_count: intent.referenced_table_count,
                column_count: intent.column_count,
            }),
            event_context,
        )
    }

    pub(crate) fn drop_submitted(
        catalog_name: &str,
        schema_name: &str,
        view_name: &str,
        view_id: u32,
        drop_if_exists: bool,
        event_context: EventContext,
    ) -> Self {
        Self::submitted(
            DROP_VIEW_EVENT_TYPE,
            catalog_name,
            schema_name,
            view_name,
            Some(view_id),
            ViewDdlPayload::Drop(DropViewPayload {
                version: PAYLOAD_VERSION,
                drop_if_exists,
            }),
            event_context,
        )
    }

    /// Builds a lightweight create-view lifecycle event with no locator data.
    pub(crate) fn create_lifecycle() -> Self {
        Self::lifecycle(CREATE_VIEW_EVENT_TYPE)
    }

    /// Builds the successful create-view row that carries only the allocated id.
    pub(crate) fn create_succeeded(view_id: u32) -> Self {
        Self::succeeded(CREATE_VIEW_EVENT_TYPE, view_id)
    }

    /// Builds a lightweight drop-view lifecycle event with no locator data.
    pub(crate) fn drop_lifecycle() -> Self {
        Self::lifecycle(DROP_VIEW_EVENT_TYPE)
    }

    fn submitted(
        event_type: &'static str,
        catalog_name: &str,
        schema_name: &str,
        view_name: &str,
        view_id: Option<u32>,
        payload: ViewDdlPayload,
        event_context: EventContext,
    ) -> Self {
        Self {
            event_type,
            catalog_name: Some(catalog_name.to_string()),
            schema_name: Some(schema_name.to_string()),
            view_name: Some(view_name.to_string()),
            view_id,
            payload: Some(payload),
            event_context: Some(event_context),
        }
    }

    fn lifecycle(event_type: &'static str) -> Self {
        Self {
            event_type,
            catalog_name: None,
            schema_name: None,
            view_name: None,
            view_id: None,
            payload: None,
            event_context: None,
        }
    }

    fn succeeded(event_type: &'static str, view_id: u32) -> Self {
        Self {
            event_type,
            catalog_name: None,
            schema_name: None,
            view_name: None,
            view_id: Some(view_id),
            payload: None,
            event_context: None,
        }
    }
}

impl Event for ViewDdlEvent {
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
            &VIEW_NAME_COLUMN,
            &VIEW_ID_COLUMN,
            &EVENT_CONTEXT_COLUMN,
        ])
    }

    fn extra_rows(&self) -> Result<Vec<Row>> {
        let event_context = self
            .event_context
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .context(SerializeEventSnafu)?;
        Ok(vec![Row {
            values: vec![
                nullable_string(self.catalog_name.as_deref()),
                nullable_string(self.schema_name.as_deref()),
                nullable_string(self.view_name.as_deref()),
                nullable_value(self.view_id.map(ValueData::U32Value)),
                nullable_json(event_context.as_ref()),
            ],
        }])
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
