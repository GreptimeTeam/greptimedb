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

use common_telemetry::error;

#[derive(Debug, Clone, PartialEq)]
pub struct Timestamp {
    pub nanosecond: i64,
}

impl Timestamp {
    pub(crate) fn new(nanosecond: i64) -> Self {
        Timestamp { nanosecond }
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

impl Default for Timestamp {
    fn default() -> Self {
        let dt = chrono::Utc::now();
        let ns = match dt.timestamp_nanos_opt() {
            Some(ns) => ns,
            None => {
                error!("failed to get nanosecond from timestamp, use 0 instead");
                0
            }
        };
        Timestamp::new(ns)
    }
}

impl std::fmt::Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ns: {}", self.nanosecond)
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
