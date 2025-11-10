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

use common_base::regex_pattern::NAME_PATTERN_REG;
pub use datafusion_common::ScalarValue;
use once_cell::sync::OnceCell;
use snafu::ensure;

pub use crate::columnar_value::ColumnarValue;
use crate::error::{InvalidColumnPrefixSnafu, Result};

/// Default time index column name.
static GREPTIME_TIMESTAMP_CELL: OnceCell<String> = OnceCell::new();

/// Default value column name.
static GREPTIME_VALUE_CELL: OnceCell<String> = OnceCell::new();

pub fn set_default_prefix(prefix: Option<&str>) -> Result<()> {
    match prefix {
        None => {
            // use default greptime prefix
            GREPTIME_TIMESTAMP_CELL.get_or_init(|| GREPTIME_TIMESTAMP.to_string());
            GREPTIME_VALUE_CELL.get_or_init(|| GREPTIME_VALUE.to_string());
        }
        Some(s) if s.trim().is_empty() => {
            // use "" to disable prefix
            GREPTIME_TIMESTAMP_CELL.get_or_init(|| "timestamp".to_string());
            GREPTIME_VALUE_CELL.get_or_init(|| "value".to_string());
        }
        Some(x) => {
            ensure!(
                NAME_PATTERN_REG.is_match(x),
                InvalidColumnPrefixSnafu { prefix: x }
            );
            GREPTIME_TIMESTAMP_CELL.get_or_init(|| format!("{}_timestamp", x));
            GREPTIME_VALUE_CELL.get_or_init(|| format!("{}_value", x));
        }
    }
    Ok(())
}

/// Get the default timestamp column name.
/// Returns the configured value, or `greptime_timestamp` if not set.
pub fn greptime_timestamp() -> &'static str {
    GREPTIME_TIMESTAMP_CELL.get_or_init(|| GREPTIME_TIMESTAMP.to_string())
}

/// Get the default value column name.
/// Returns the configured value, or `greptime_value` if not set.
pub fn greptime_value() -> &'static str {
    GREPTIME_VALUE_CELL.get_or_init(|| GREPTIME_VALUE.to_string())
}

/// Default timestamp column name constant for backward compatibility.
const GREPTIME_TIMESTAMP: &str = "greptime_timestamp";
/// Default value column name constant for backward compatibility.
const GREPTIME_VALUE: &str = "greptime_value";
/// Default counter column name for OTLP metrics (legacy mode).
pub const GREPTIME_COUNT: &str = "greptime_count";
/// Default physical table name
pub const GREPTIME_PHYSICAL_TABLE: &str = "greptime_physical_table";
