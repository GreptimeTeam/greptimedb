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
use std::collections::HashMap;

use api::v1::{ColumnSchema, Row};
use common_event_recorder::Event;
use common_event_recorder::error::{Result, SerializeEventSnafu};
use common_event_recorder::event_table::{
    CATALOG_NAME_COLUMN as EVENT_TABLE_CATALOG_NAME_COLUMN,
    SCHEMA_NAME_COLUMN as EVENT_TABLE_SCHEMA_NAME_COLUMN, column_schemas, nullable_string,
};
use serde::Serialize;
use snafu::ResultExt;

use crate::rpc::ddl::{AlterDatabaseKind, SetDatabaseOption, UnsetDatabaseOption};

pub(crate) const CREATE_DATABASE_EVENT_TYPE: &str = "create_database";
pub(crate) const ALTER_DATABASE_EVENT_TYPE: &str = "alter_database";
pub(crate) const DROP_DATABASE_EVENT_TYPE: &str = "drop_database";
const PAYLOAD_VERSION: u8 = 1;
const TTL_OPTION_NAME: &str = "ttl";

#[derive(Debug)]
pub(crate) struct DatabaseDdlEvent {
    event_type: &'static str,
    catalog_name: Option<String>,
    schema_name: Option<String>,
    payload: Option<DatabaseDdlPayload>,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum DatabaseDdlPayload {
    Create(CreateDatabasePayload),
    Alter(AlterDatabasePayload),
    Drop(DropDatabasePayload),
}

#[derive(Debug, Serialize)]
struct CreateDatabasePayload {
    version: u8,
    create_if_not_exists: bool,
    options: Vec<DatabaseOptionIntent>,
}

#[derive(Debug, Serialize)]
struct AlterDatabasePayload {
    version: u8,
    #[serde(flatten)]
    intent: AlterDatabaseIntent,
}

#[derive(Debug, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum AlterDatabaseIntent {
    Set { options: Vec<DatabaseOptionIntent> },
    Unset { options: Vec<String> },
}

#[derive(Debug, Serialize)]
struct DropDatabasePayload {
    version: u8,
    drop_if_exists: bool,
}

#[derive(Debug, Serialize)]
struct DatabaseOptionIntent {
    key: String,
    value: String,
}

impl DatabaseDdlEvent {
    pub(crate) fn create_submitted(
        catalog_name: &str,
        schema_name: &str,
        create_if_not_exists: bool,
        options: &HashMap<String, String>,
    ) -> Self {
        let mut options = options.iter().collect::<Vec<_>>();
        options.sort_unstable_by_key(|(left, _)| *left);
        let options = options
            .into_iter()
            .map(|(key, value)| DatabaseOptionIntent {
                key: key.clone(),
                value: value.clone(),
            })
            .collect();
        Self::submitted(
            CREATE_DATABASE_EVENT_TYPE,
            catalog_name,
            schema_name,
            DatabaseDdlPayload::Create(CreateDatabasePayload {
                version: PAYLOAD_VERSION,
                create_if_not_exists,
                options,
            }),
        )
    }

    pub(crate) fn alter_submitted(
        catalog_name: &str,
        schema_name: &str,
        kind: &AlterDatabaseKind,
    ) -> Self {
        let intent = match kind {
            AlterDatabaseKind::SetDatabaseOptions(options) => AlterDatabaseIntent::Set {
                options: options
                    .0
                    .iter()
                    .map(|option| match option {
                        SetDatabaseOption::Ttl(ttl) => DatabaseOptionIntent {
                            key: TTL_OPTION_NAME.to_string(),
                            value: ttl.to_string(),
                        },
                        SetDatabaseOption::Other(key, value) => DatabaseOptionIntent {
                            key: key.clone(),
                            value: value.clone(),
                        },
                    })
                    .collect(),
            },
            AlterDatabaseKind::UnsetDatabaseOptions(options) => {
                let options = options
                    .0
                    .iter()
                    .map(|option| match option {
                        UnsetDatabaseOption::Ttl => TTL_OPTION_NAME.to_string(),
                        UnsetDatabaseOption::Other(key) => key.clone(),
                    })
                    .collect();
                AlterDatabaseIntent::Unset { options }
            }
        };
        Self::submitted(
            ALTER_DATABASE_EVENT_TYPE,
            catalog_name,
            schema_name,
            DatabaseDdlPayload::Alter(AlterDatabasePayload {
                version: PAYLOAD_VERSION,
                intent,
            }),
        )
    }

    pub(crate) fn drop_submitted(
        catalog_name: &str,
        schema_name: &str,
        drop_if_exists: bool,
    ) -> Self {
        Self::submitted(
            DROP_DATABASE_EVENT_TYPE,
            catalog_name,
            schema_name,
            DatabaseDdlPayload::Drop(DropDatabasePayload {
                version: PAYLOAD_VERSION,
                drop_if_exists,
            }),
        )
    }

    pub(crate) fn create_lifecycle() -> Self {
        Self::lifecycle(CREATE_DATABASE_EVENT_TYPE)
    }

    pub(crate) fn alter_lifecycle() -> Self {
        Self::lifecycle(ALTER_DATABASE_EVENT_TYPE)
    }

    pub(crate) fn drop_lifecycle() -> Self {
        Self::lifecycle(DROP_DATABASE_EVENT_TYPE)
    }

    fn submitted(
        event_type: &'static str,
        catalog_name: &str,
        schema_name: &str,
        payload: DatabaseDdlPayload,
    ) -> Self {
        Self {
            event_type,
            catalog_name: Some(catalog_name.to_string()),
            schema_name: Some(schema_name.to_string()),
            payload: Some(payload),
        }
    }

    fn lifecycle(event_type: &'static str) -> Self {
        Self {
            event_type,
            catalog_name: None,
            schema_name: None,
            payload: None,
        }
    }
}

impl Event for DatabaseDdlEvent {
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
            &EVENT_TABLE_CATALOG_NAME_COLUMN,
            &EVENT_TABLE_SCHEMA_NAME_COLUMN,
        ])
    }

    fn extra_rows(&self) -> Result<Vec<Row>> {
        Ok(vec![Row {
            values: vec![
                nullable_string(self.catalog_name.as_deref()),
                nullable_string(self.schema_name.as_deref()),
            ],
        }])
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
