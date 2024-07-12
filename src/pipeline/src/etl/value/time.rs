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

use common_telemetry::error;

#[derive(Debug, Clone, PartialEq)]
pub struct Time {
    pub value: String,
    pub nanosecond: i64,
    pub format: Option<Arc<String>>,
    pub timezone: Option<Arc<String>>,
    // TODO(yuanbohan): support locale
    // pub locale: Option<String>,
}

impl Time {
    pub(crate) fn new(value: impl Into<String>, nanosecond: i64) -> Self {
        let value = value.into();
        Time {
            value,
            nanosecond,
            format: None,
            timezone: None,
        }
    }

    pub(crate) fn with_format(&mut self, format: Option<Arc<String>>) {
        self.format = format;
    }

    pub(crate) fn with_timezone(&mut self, timezone: Option<Arc<String>>) {
        self.timezone = timezone;
    }

    pub(crate) fn timestamp_nanos(&self) -> i64 {
        self.nanosecond
    }

    pub(crate) fn timestamp_micros(&self) -> i64 {
        self.nanosecond / 1_000
    }

    pub(crate) fn timestamp_millis(&self) -> i64 {
        self.nanosecond / 1_000_000
    }

    pub(crate) fn timestamp(&self) -> i64 {
        self.nanosecond / 1_000_000_000
    }
}

impl Default for Time {
    fn default() -> Self {
        let dt = chrono::Utc::now();
        let v = dt.to_rfc3339();
        let ns = match dt.timestamp_nanos_opt() {
            Some(ns) => ns,
            None => {
                error!("failed to get nanosecond from timestamp, use 0 instead");
                0
            }
        };
        Time::new(v, ns)
    }
}

impl std::fmt::Display for Time {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let format = if let Some(format) = &self.format {
            format!(", format: {}", format)
        } else {
            "".to_string()
        };

        let timezone = if let Some(timezone) = &self.timezone {
            format!(", timezone: {}", timezone)
        } else {
            "".to_string()
        };

        write!(f, "{}, format: {}{}", self.value, format, timezone)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Epoch {
    Nanosecond(i64),
    Microsecond(i64),
    Millisecond(i64),
    Second(i64),
}

pub(crate) const NANOSECOND_RESOLUTION: &str = "nanosecond";
pub(crate) const NANO_RESOLUTION: &str = "nano";
pub(crate) const NS_RESOLUTION: &str = "ns";
pub(crate) const MICROSECOND_RESOLUTION: &str = "microsecond";
pub(crate) const MICRO_RESOLUTION: &str = "micro";
pub(crate) const US_RESOLUTION: &str = "us";
pub(crate) const MILLISECOND_RESOLUTION: &str = "millisecond";
pub(crate) const MILLI_RESOLUTION: &str = "milli";
pub(crate) const MS_RESOLUTION: &str = "ms";
pub(crate) const SECOND_RESOLUTION: &str = "second";
pub(crate) const SEC_RESOLUTION: &str = "sec";
pub(crate) const S_RESOLUTION: &str = "s";

pub(crate) const VALID_RESOLUTIONS: [&str; 12] = [
    NANOSECOND_RESOLUTION,
    NANO_RESOLUTION,
    NS_RESOLUTION,
    MICROSECOND_RESOLUTION,
    MICRO_RESOLUTION,
    US_RESOLUTION,
    MILLISECOND_RESOLUTION,
    MILLI_RESOLUTION,
    MS_RESOLUTION,
    SECOND_RESOLUTION,
    SEC_RESOLUTION,
    S_RESOLUTION,
];

impl Epoch {
    pub(crate) fn timestamp_nanos(&self) -> i64 {
        match self {
            Epoch::Nanosecond(v) => *v,
            Epoch::Microsecond(v) => *v * 1_000,
            Epoch::Millisecond(v) => *v * 1_000_000,
            Epoch::Second(v) => *v * 1_000_000_000,
        }
    }

    pub(crate) fn timestamp_micros(&self) -> i64 {
        match self {
            Epoch::Nanosecond(v) => *v / 1_000,
            Epoch::Microsecond(v) => *v,
            Epoch::Millisecond(v) => *v * 1_000,
            Epoch::Second(v) => *v * 1_000_000,
        }
    }

    pub(crate) fn timestamp_millis(&self) -> i64 {
        match self {
            Epoch::Nanosecond(v) => *v / 1_000_000,
            Epoch::Microsecond(v) => *v / 1_000,
            Epoch::Millisecond(v) => *v,
            Epoch::Second(v) => *v * 1_000,
        }
    }

    pub(crate) fn timestamp(&self) -> i64 {
        match self {
            Epoch::Nanosecond(v) => *v / 1_000_000_000,
            Epoch::Microsecond(v) => *v / 1_000_000,
            Epoch::Millisecond(v) => *v / 1_000,
            Epoch::Second(v) => *v,
        }
    }
}

impl Default for Epoch {
    fn default() -> Self {
        Epoch::Nanosecond(chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0))
    }
}

impl std::fmt::Display for Epoch {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let (value, resolution) = match self {
            Epoch::Nanosecond(v) => (v, NANOSECOND_RESOLUTION),
            Epoch::Microsecond(v) => (v, MICROSECOND_RESOLUTION),
            Epoch::Millisecond(v) => (v, MILLISECOND_RESOLUTION),
            Epoch::Second(v) => (v, SECOND_RESOLUTION),
        };

        write!(f, "{}, resolution: {}", value, resolution)
    }
}
