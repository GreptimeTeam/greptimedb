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

use std::str::FromStr;

use chrono::{LocalResult, NaiveDateTime, TimeZone};
use chrono_tz::Tz;

use crate::Timezone;
use crate::timezone::get_timezone;

pub fn format_utc_datetime(utc: &NaiveDateTime, pattern: &str) -> String {
    match get_timezone(None) {
        crate::Timezone::Offset(offset) => {
            offset.from_utc_datetime(utc).format(pattern).to_string()
        }
        crate::Timezone::Named(tz) => tz.from_utc_datetime(utc).format(pattern).to_string(),
    }
}

/// Cast a [`NaiveDateTime`] with the given timezone.
pub fn datetime_to_utc(
    datetime: &NaiveDateTime,
    timezone: &Timezone,
) -> LocalResult<NaiveDateTime> {
    match timezone {
        crate::Timezone::Offset(offset) => {
            offset.from_local_datetime(datetime).map(|x| x.naive_utc())
        }
        crate::Timezone::Named(tz) => tz.from_local_datetime(datetime).map(|x| x.naive_utc()),
    }
}

pub fn find_tz_from_env() -> Option<Tz> {
    // Windows does not support "TZ" env variable, which is used in the `Local` timezone under Unix.
    // However, we are used to set "TZ" env as the default timezone without actually providing a
    // timezone argument (especially in tests), and it's very convenient to do so, we decide to make
    // it work under Windows as well.
    std::env::var("TZ")
        .ok()
        .and_then(|tz| Tz::from_str(&tz).ok())
}

/// A trait for types that provide the current system time.
pub trait SystemTimer {
    /// Returns the time duration since UNIX_EPOCH in milliseconds.
    fn current_time_millis(&self) -> i64;

    /// Returns the current time in rfc3339 format.
    fn current_time_rfc3339(&self) -> String;
}

/// Default implementation of [`SystemTimer`]
#[derive(Debug, Default, Clone, Copy)]
pub struct DefaultSystemTimer;

impl SystemTimer for DefaultSystemTimer {
    fn current_time_millis(&self) -> i64 {
        current_time_millis()
    }

    fn current_time_rfc3339(&self) -> String {
        current_time_rfc3339()
    }
}

/// Returns the time duration since UNIX_EPOCH in milliseconds.
pub fn current_time_millis() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// Returns the current time in rfc3339 format.
pub fn current_time_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339()
}

/// Returns the yesterday time in rfc3339 format.
pub fn yesterday_rfc3339() -> String {
    let now = chrono::Utc::now();
    let day_before = now
        - chrono::Duration::try_days(1).unwrap_or_else(|| {
            panic!("now time ('{now}') is too early to calculate the day before")
        });
    day_before.to_rfc3339()
}

/// Port of rust unstable features `int_roundings`.
pub(crate) fn div_ceil(this: i64, rhs: i64) -> i64 {
    let d = this / rhs;
    let r = this % rhs;
    if r > 0 && rhs > 0 { d + 1 } else { d }
}

/// Formats nanoseconds into human-readable time with dynamic unit selection.
///
/// This function automatically chooses the most appropriate unit (seconds, milliseconds,
/// microseconds, or nanoseconds) to display the time in a readable format.
///
/// # Examples
///
/// ```
/// use common_time::util::format_nanoseconds_human_readable;
///
/// assert_eq!("1.23s", format_nanoseconds_human_readable(1_234_567_890));
/// assert_eq!("456ms", format_nanoseconds_human_readable(456_000_000));
/// assert_eq!("789us", format_nanoseconds_human_readable(789_000));
/// assert_eq!("123ns", format_nanoseconds_human_readable(123));
/// ```
pub fn format_nanoseconds_human_readable(nanos: usize) -> String {
    if nanos == 0 {
        return "0ns".to_string();
    }

    let nanos_i64 = nanos as i64;

    // Try seconds first (if >= 1 second)
    if nanos_i64 >= 1_000_000_000 {
        let secs = nanos_i64 as f64 / 1_000_000_000.0;
        if secs >= 10.0 {
            return format!("{:.1}s", secs);
        } else {
            return format!("{:.2}s", secs);
        }
    }

    // Try milliseconds (if >= 1 millisecond)
    if nanos_i64 >= 1_000_000 {
        let millis = nanos_i64 as f64 / 1_000_000.0;
        if millis >= 10.0 {
            return format!("{:.0}ms", millis);
        } else {
            return format!("{:.1}ms", millis);
        }
    }

    // Try microseconds (if >= 1 microsecond)
    if nanos_i64 >= 1_000 {
        let micros = nanos_i64 as f64 / 1_000.0;
        if micros >= 10.0 {
            return format!("{:.0}us", micros);
        } else {
            return format!("{:.1}us", micros);
        }
    }

    // Less than 1 microsecond, display as nanoseconds
    format!("{}ns", nanos_i64)
}

#[cfg(test)]
mod tests {
    use std::time::{self, SystemTime};

    use chrono::{Datelike, TimeZone, Timelike};

    use super::*;

    #[test]
    fn test_current_time_millis() {
        let now = current_time_millis();

        let millis_from_std = SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let datetime_now = chrono::Utc.timestamp_millis_opt(now).unwrap();
        let datetime_std = chrono::Utc.timestamp_millis_opt(millis_from_std).unwrap();

        assert_eq!(datetime_std.year(), datetime_now.year());
        assert_eq!(datetime_std.month(), datetime_now.month());
        assert_eq!(datetime_std.day(), datetime_now.day());
        assert_eq!(datetime_std.hour(), datetime_now.hour());
        assert_eq!(datetime_std.minute(), datetime_now.minute());
    }

    #[test]
    fn test_div_ceil() {
        let v0 = 9223372036854676001;
        assert_eq!(9223372036854677, div_ceil(v0, 1000));
    }

    #[test]
    fn test_format_nanoseconds_human_readable() {
        // Test zero
        assert_eq!("0ns", format_nanoseconds_human_readable(0));

        // Test nanoseconds (< 1 microsecond)
        assert_eq!("1ns", format_nanoseconds_human_readable(1));
        assert_eq!("123ns", format_nanoseconds_human_readable(123));
        assert_eq!("999ns", format_nanoseconds_human_readable(999));

        // Test microseconds (>= 1 microsecond, < 1 millisecond)
        assert_eq!("1.0us", format_nanoseconds_human_readable(1_000));
        assert_eq!("1.5us", format_nanoseconds_human_readable(1_500));
        assert_eq!("10us", format_nanoseconds_human_readable(10_000));
        assert_eq!("123us", format_nanoseconds_human_readable(123_000));
        assert_eq!("999us", format_nanoseconds_human_readable(999_000));

        // Test milliseconds (>= 1 millisecond, < 1 second)
        assert_eq!("1.0ms", format_nanoseconds_human_readable(1_000_000));
        assert_eq!("1.5ms", format_nanoseconds_human_readable(1_500_000));
        assert_eq!("10ms", format_nanoseconds_human_readable(10_000_000));
        assert_eq!("123ms", format_nanoseconds_human_readable(123_000_000));
        assert_eq!("999ms", format_nanoseconds_human_readable(999_000_000));

        // Test seconds (>= 1 second)
        assert_eq!("1.00s", format_nanoseconds_human_readable(1_000_000_000));
        assert_eq!("1.23s", format_nanoseconds_human_readable(1_234_567_890));
        assert_eq!("10.0s", format_nanoseconds_human_readable(10_000_000_000));
        assert_eq!("123.5s", format_nanoseconds_human_readable(123_456_789_012));

        // Test large values
        assert_eq!(
            "1234.6s",
            format_nanoseconds_human_readable(1_234_567_890_123)
        );
    }
}
