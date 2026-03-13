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
use api::v1::{ColumnDataType, ColumnSchema, Row, SemanticType};
use common_event_recorder::Event;
use common_event_recorder::error::Result;
use serde::Serialize;

pub const SLOW_QUERY_TABLE_NAME: &str = "slow_queries";
pub const SLOW_QUERY_TABLE_COST_COLUMN_NAME: &str = "cost";
pub const SLOW_QUERY_TABLE_THRESHOLD_COLUMN_NAME: &str = "threshold";
pub const SLOW_QUERY_TABLE_QUERY_COLUMN_NAME: &str = "query";
pub const SLOW_QUERY_TABLE_TIMESTAMP_COLUMN_NAME: &str = "timestamp";
pub const SLOW_QUERY_TABLE_IS_PROMQL_COLUMN_NAME: &str = "is_promql";
pub const SLOW_QUERY_TABLE_PROMQL_START_COLUMN_NAME: &str = "promql_start";
pub const SLOW_QUERY_TABLE_PROMQL_END_COLUMN_NAME: &str = "promql_end";
pub const SLOW_QUERY_TABLE_PROMQL_RANGE_COLUMN_NAME: &str = "promql_range";
pub const SLOW_QUERY_TABLE_PROMQL_STEP_COLUMN_NAME: &str = "promql_step";
pub const SLOW_QUERY_EVENT_TYPE: &str = "slow_query";

/// SlowQueryEvent is the event of slow query.
#[derive(Debug, Serialize)]
pub struct SlowQueryEvent {
    pub cost: u64,
    pub threshold: u64,
    pub query: String,
    pub is_promql: bool,
    pub promql_range: Option<u64>,
    pub promql_step: Option<u64>,
    pub promql_start: Option<i64>,
    pub promql_end: Option<i64>,
}

impl Event for SlowQueryEvent {
    fn table_name(&self) -> &str {
        SLOW_QUERY_TABLE_NAME
    }

    fn event_type(&self) -> &str {
        SLOW_QUERY_EVENT_TYPE
    }

    fn extra_schema(&self) -> Vec<ColumnSchema> {
        vec![
            ColumnSchema {
                column_name: SLOW_QUERY_TABLE_COST_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::Uint64.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
            ColumnSchema {
                column_name: SLOW_QUERY_TABLE_THRESHOLD_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::Uint64.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
            ColumnSchema {
                column_name: SLOW_QUERY_TABLE_QUERY_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::String.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
            ColumnSchema {
                column_name: SLOW_QUERY_TABLE_IS_PROMQL_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::Boolean.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
            ColumnSchema {
                column_name: SLOW_QUERY_TABLE_PROMQL_RANGE_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::Uint64.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
            ColumnSchema {
                column_name: SLOW_QUERY_TABLE_PROMQL_STEP_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::Uint64.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
            ColumnSchema {
                column_name: SLOW_QUERY_TABLE_PROMQL_START_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::TimestampMillisecond.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
            ColumnSchema {
                column_name: SLOW_QUERY_TABLE_PROMQL_END_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::TimestampMillisecond.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
        ]
    }

    fn extra_rows(&self) -> Result<Vec<Row>> {
        Ok(vec![Row {
            values: vec![
                ValueData::U64Value(self.cost).into(),
                ValueData::U64Value(self.threshold).into(),
                ValueData::StringValue(self.query.to_string()).into(),
                ValueData::BoolValue(self.is_promql).into(),
                ValueData::U64Value(self.promql_range.unwrap_or(0)).into(),
                ValueData::U64Value(self.promql_step.unwrap_or(0)).into(),
                ValueData::TimestampMillisecondValue(self.promql_start.unwrap_or(0)).into(),
                ValueData::TimestampMillisecondValue(self.promql_end.unwrap_or(0)).into(),
            ],
        }])
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
