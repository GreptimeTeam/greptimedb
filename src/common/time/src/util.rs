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
    let day_before = now - chrono::Duration::days(1);
    day_before.to_rfc3339()
}

/// Port of rust unstable features `int_roundings`.
pub(crate) fn div_ceil(this: i64, rhs: i64) -> i64 {
    let d = this / rhs;
    let r = this % rhs;
    if r > 0 && rhs > 0 {
        d + 1
    } else {
        d
    }
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
}
