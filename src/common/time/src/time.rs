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
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

use chrono::offset::Local;
use chrono::{NaiveDateTime, NaiveTime, TimeZone as ChronoTimeZone, Utc};
use serde::{Deserialize, Serialize};

use crate::timestamp::TimeUnit;
use crate::timezone::TimeZone;

/// Time value, represents the elapsed time since midnight in the unit of `TimeUnit`.
#[derive(Debug, Clone, Default, Copy, Serialize, Deserialize)]
pub struct Time {
    value: i64,
    unit: TimeUnit,
}

impl Time {
    /// Creates the time by value and `TimeUnit`.
    pub fn new(value: i64, unit: TimeUnit) -> Self {
        Self { value, unit }
    }

    /// Creates the time in nanosecond.
    pub fn new_nanosecond(value: i64) -> Self {
        Self {
            value,
            unit: TimeUnit::Nanosecond,
        }
    }

    /// Creates the time in second.
    pub fn new_second(value: i64) -> Self {
        Self {
            value,
            unit: TimeUnit::Second,
        }
    }

    /// Creates the time in millissecond.
    pub fn new_millisecond(value: i64) -> Self {
        Self {
            value,
            unit: TimeUnit::Millisecond,
        }
    }

    /// Creates the time in microsecond.
    pub fn new_microsecond(value: i64) -> Self {
        Self {
            value,
            unit: TimeUnit::Microsecond,
        }
    }

    /// Returns the `TimeUnit` of the time.
    pub fn unit(&self) -> &TimeUnit {
        &self.unit
    }

    /// Returns the value of the time.
    pub fn value(&self) -> i64 {
        self.value
    }

    /// Split a [Time] into seconds part and nanoseconds part.
    /// Notice the seconds part of split result is always rounded down to floor.
    fn split(&self) -> (u32, u32) {
        let sec_mul = (TimeUnit::Second.factor() / self.unit.factor()) as i64;
        let nsec_mul = (self.unit.factor() / TimeUnit::Nanosecond.factor()) as i64;

        let sec_div = u32::try_from(self.value.div_euclid(sec_mul)).unwrap();
        let sec_mod = self.value.rem_euclid(sec_mul);
        // safety:  the max possible value of `sec_mod` is 999,999,999
        let nsec = u32::try_from(sec_mod * nsec_mul).unwrap();
        (sec_div, nsec)
    }

    /// Format Time to ISO8601 string. If the time exceeds what chrono time can
    /// represent, this function simply print the time unit and value in plain string.
    pub fn to_iso8601_string(&self) -> String {
        self.as_formatted_string("%H:%M:%S%.f%z", None)
    }

    /// Format Time for local timezone.
    pub fn to_local_string(&self) -> String {
        self.as_formatted_string("%H:%M:%S%.f", None)
    }

    /// Format Time for given timezone.
    /// When timezone is None, using local time by default.
    pub fn to_timezone_aware_string(&self, tz: Option<TimeZone>) -> String {
        self.as_formatted_string("%H:%M:%S%.f", tz)
    }

    fn as_formatted_string(self, pattern: &str, timezone: Option<TimeZone>) -> String {
        if let Some(time) = self.to_chrono_time() {
            let date = Utc::now().date_naive();
            let datetime = NaiveDateTime::new(date, time);

            match timezone {
                Some(TimeZone::Offset(offset)) => {
                    format!("{}", offset.from_utc_datetime(&datetime).format(pattern))
                }
                Some(TimeZone::Named(tz)) => {
                    format!("{}", tz.from_utc_datetime(&datetime).format(pattern))
                }
                None => {
                    let local = Local {};
                    format!("{}", local.from_utc_datetime(&datetime).format(pattern))
                }
            }
        } else {
            format!("[Time{}: {}]", self.unit, self.value)
        }
    }

    /// Cast the [Time] into chrono NaiveDateTime
    pub fn to_chrono_time(&self) -> Option<NaiveTime> {
        let (sec, nsec) = self.split();
        NaiveTime::from_num_seconds_from_midnight_opt(sec, nsec)
    }
}

impl From<i64> for Time {
    fn from(v: i64) -> Self {
        Self {
            value: v,
            unit: TimeUnit::Millisecond,
        }
    }
}

impl From<Time> for i64 {
    fn from(t: Time) -> Self {
        t.value
    }
}

impl From<Time> for i32 {
    fn from(t: Time) -> Self {
        t.value as i32
    }
}

impl From<Time> for serde_json::Value {
    fn from(d: Time) -> Self {
        serde_json::Value::String(d.to_iso8601_string())
    }
}

impl PartialOrd for Time {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Time {
    fn cmp(&self, other: &Self) -> Ordering {
        // fast path: most comparisons use the same unit.
        if self.unit == other.unit {
            return self.value.cmp(&other.value);
        }

        let (s_sec, s_nsec) = self.split();
        let (o_sec, o_nsec) = other.split();
        match s_sec.cmp(&o_sec) {
            Ordering::Less => Ordering::Less,
            Ordering::Greater => Ordering::Greater,
            Ordering::Equal => s_nsec.cmp(&o_nsec),
        }
    }
}

impl PartialEq for Time {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Time {}

impl Hash for Time {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let (sec, nsec) = self.split();
        state.write_u32(sec);
        state.write_u32(nsec);
    }
}
