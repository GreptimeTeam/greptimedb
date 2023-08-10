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

use std::fmt::{Display, Formatter};
use std::str::FromStr;

use chrono::{LocalResult, NaiveDateTime};
use serde::{Deserialize, Serialize};

use crate::error::{Error, InvalidDateStrSnafu, Result};
use crate::util::{format_utc_datetime, local_datetime_to_utc};

const DATETIME_FORMAT: &str = "%F %T";
const DATETIME_FORMAT_WITH_TZ: &str = "%F %T%z";

/// [DateTime] represents the **seconds elapsed since "1970-01-01 00:00:00 UTC" (UNIX Epoch)**.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub struct DateTime(i64);

impl Display for DateTime {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(abs_time) = NaiveDateTime::from_timestamp_opt(self.0, 0) {
            write!(
                f,
                "{}",
                format_utc_datetime(&abs_time, DATETIME_FORMAT_WITH_TZ)
            )
        } else {
            write!(f, "DateTime({})", self.0)
        }
    }
}

impl From<DateTime> for serde_json::Value {
    fn from(d: DateTime) -> Self {
        serde_json::Value::String(d.to_string())
    }
}

impl FromStr for DateTime {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let s = s.trim();
        let timestamp = if let Ok(d) = NaiveDateTime::parse_from_str(s, DATETIME_FORMAT) {
            match local_datetime_to_utc(&d) {
                LocalResult::None => {
                    return InvalidDateStrSnafu { raw: s }.fail();
                }
                LocalResult::Single(d) | LocalResult::Ambiguous(d, _) => d.timestamp(),
            }
        } else if let Ok(v) = chrono::DateTime::parse_from_str(s, DATETIME_FORMAT_WITH_TZ) {
            v.timestamp()
        } else {
            return InvalidDateStrSnafu { raw: s }.fail();
        };

        Ok(Self(timestamp))
    }
}

impl From<i64> for DateTime {
    fn from(v: i64) -> Self {
        Self(v)
    }
}

impl DateTime {
    pub fn new(val: i64) -> Self {
        Self(val)
    }

    pub fn val(&self) -> i64 {
        self.0
    }

    pub fn to_chrono_datetime(&self) -> Option<NaiveDateTime> {
        NaiveDateTime::from_timestamp_millis(self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_new_date_time() {
        std::env::set_var("TZ", "Asia/Shanghai");
        assert_eq!("1970-01-01 08:00:00+0800", DateTime::new(0).to_string());
        assert_eq!("1970-01-01 08:00:01+0800", DateTime::new(1).to_string());
        assert_eq!("1970-01-01 07:59:59+0800", DateTime::new(-1).to_string());
    }

    #[test]
    pub fn test_parse_from_string() {
        std::env::set_var("TZ", "Asia/Shanghai");
        let time = "1970-01-01 00:00:00+0800";
        let dt = DateTime::from_str(time).unwrap();
        assert_eq!(time, &dt.to_string());
        let dt = DateTime::from_str("      1970-01-01       00:00:00+0800       ").unwrap();
        assert_eq!(time, &dt.to_string());
    }

    #[test]
    pub fn test_from() {
        let d: DateTime = 42.into();
        assert_eq!(42, d.val());
    }

    #[test]
    fn test_parse_local_date_time() {
        std::env::set_var("TZ", "Asia/Shanghai");
        assert_eq!(
            -28800,
            DateTime::from_str("1970-01-01 00:00:00").unwrap().val()
        );
        assert_eq!(0, DateTime::from_str("1970-01-01 08:00:00").unwrap().val());
    }

    #[test]
    fn test_parse_local_date_time_with_tz() {
        let ts = DateTime::from_str("1970-01-01 08:00:00+0000")
            .unwrap()
            .val();
        assert_eq!(28800, ts);
    }
}
