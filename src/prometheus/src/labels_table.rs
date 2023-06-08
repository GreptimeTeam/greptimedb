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

use std::sync::Arc;

use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema};
use once_cell::sync::Lazy;

/// The proemtheus metadata table for labels
pub const TABLE_NAME: &str = "prometheus_labels";
/// The metric column
pub const METRIC_COLUMN: &str = "metric";
/// The lablel column
pub const LABEL_COLUMN: &str = "label";
/// Implict timestamp column
pub const TS_COLUMN: &str = "ts";

static SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        ColumnSchema::new(METRIC_COLUMN, ConcreteDataType::string_datatype(), false),
        ColumnSchema::new(LABEL_COLUMN, ConcreteDataType::string_datatype(), false),
        ColumnSchema::new(
            TS_COLUMN,
            ConcreteDataType::timestamp_second_datatype(),
            true,
        )
        .with_time_index(true),
    ]))
});

pub fn schema() -> Arc<Schema> {
    SCHEMA.clone()
}

/// Return the SQL to load all prometheus metrics and labels.
#[inline]
pub fn load_table_sql() -> String {
    format!("select {METRIC_COLUMN}, {LABEL_COLUMN} from {TABLE_NAME} order by {METRIC_COLUMN}")
}
