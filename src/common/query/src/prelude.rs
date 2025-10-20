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

pub use datafusion_common::ScalarValue;
use once_cell::sync::OnceCell;

pub use crate::columnar_value::ColumnarValue;

/// Default timestamp column name.
static GREPTIME_TIMESTAMP_CELL: OnceCell<String> = OnceCell::new();

/// Get the default timestamp column name.
/// Returns the configured value, or `greptime_timestamp` if not set.
pub fn greptime_timestamp() -> &'static str {
    GREPTIME_TIMESTAMP_CELL.get_or_init(|| GREPTIME_TIMESTAMP.to_string())
}

/// Set the default timestamp column name.
/// This should be called once during application startup.
/// Returns Ok(()) if successful, or Err with the attempted value if already set.
pub fn set_greptime_timestamp(name: Option<&str>) {
    let ts = match name {
        None | Some("") => GREPTIME_TIMESTAMP,
        Some(ts) => ts,
    };
    GREPTIME_TIMESTAMP_CELL.get_or_init(|| ts.to_string());
}

/// Default timestamp column name constant for backward compatibility.
const GREPTIME_TIMESTAMP: &str = "greptime_timestamp";
/// Default value column name for Prometheus metrics.
pub const GREPTIME_VALUE: &str = "greptime_value";
/// Default counter column name for OTLP metrics.
pub const GREPTIME_COUNT: &str = "greptime_count";
/// Default physical table name
pub const GREPTIME_PHYSICAL_TABLE: &str = "greptime_physical_table";
