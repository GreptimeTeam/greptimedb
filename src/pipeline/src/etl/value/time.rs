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

#[derive(Debug, Clone, PartialEq)]
pub enum Timestamp {
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

impl Timestamp {
    pub(crate) fn timestamp_nanos(&self) -> i64 {
        match self {
            Timestamp::Nanosecond(v) => *v,
            Timestamp::Microsecond(v) => *v * 1_000,
            Timestamp::Millisecond(v) => *v * 1_000_000,
            Timestamp::Second(v) => *v * 1_000_000_000,
        }
    }

    pub(crate) fn timestamp_micros(&self) -> i64 {
        match self {
            Timestamp::Nanosecond(v) => *v / 1_000,
            Timestamp::Microsecond(v) => *v,
            Timestamp::Millisecond(v) => *v * 1_000,
            Timestamp::Second(v) => *v * 1_000_000,
        }
    }

    pub(crate) fn timestamp_millis(&self) -> i64 {
        match self {
            Timestamp::Nanosecond(v) => *v / 1_000_000,
            Timestamp::Microsecond(v) => *v / 1_000,
            Timestamp::Millisecond(v) => *v,
            Timestamp::Second(v) => *v * 1_000,
        }
    }

    pub(crate) fn timestamp(&self) -> i64 {
        match self {
            Timestamp::Nanosecond(v) => *v / 1_000_000_000,
            Timestamp::Microsecond(v) => *v / 1_000_000,
            Timestamp::Millisecond(v) => *v / 1_000,
            Timestamp::Second(v) => *v,
        }
    }
}

impl Default for Timestamp {
    fn default() -> Self {
        Timestamp::Nanosecond(chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0))
    }
}

impl std::fmt::Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let (value, resolution) = match self {
            Timestamp::Nanosecond(v) => (v, NANOSECOND_RESOLUTION),
            Timestamp::Microsecond(v) => (v, MICROSECOND_RESOLUTION),
            Timestamp::Millisecond(v) => (v, MILLISECOND_RESOLUTION),
            Timestamp::Second(v) => (v, SECOND_RESOLUTION),
        };

        write!(f, "{}, resolution: {}", value, resolution)
    }
}
