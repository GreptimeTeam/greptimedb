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

use chrono::{Datelike, Days, Months, NaiveDate};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::ResultExt;

use crate::error::{Error, ParseDateStrSnafu, Result};
use crate::interval::Interval;

const UNIX_EPOCH_FROM_CE: i32 = 719_163;

/// ISO 8601 [Date] values. The inner representation is a signed 32 bit integer that represents the
/// **days since "1970-01-01 00:00:00 UTC" (UNIX Epoch)**.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Deserialize, Serialize,
)]
pub struct Date(i32);

impl From<Date> for Value {
    fn from(d: Date) -> Self {
        Value::String(d.to_string())
    }
}

impl FromStr for Date {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let s = s.trim();
        let date = NaiveDate::parse_from_str(s, "%F").context(ParseDateStrSnafu { raw: s })?;
        Ok(Self(date.num_days_from_ce() - UNIX_EPOCH_FROM_CE))
    }
}

impl From<i32> for Date {
    fn from(v: i32) -> Self {
        Self(v)
    }
}

impl From<NaiveDate> for Date {
    fn from(date: NaiveDate) -> Self {
        Self(date.num_days_from_ce() - UNIX_EPOCH_FROM_CE)
    }
}

impl Display for Date {
    /// [Date] is formatted according to ISO-8601 standard.
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(abs_date) = NaiveDate::from_num_days_from_ce_opt(UNIX_EPOCH_FROM_CE + self.0) {
            write!(f, "{}", abs_date.format("%F"))
        } else {
            write!(f, "Date({})", self.0)
        }
    }
}

impl Date {
    pub fn new(val: i32) -> Self {
        Self(val)
    }

    pub fn val(&self) -> i32 {
        self.0
    }

    pub fn to_chrono_date(&self) -> Option<NaiveDate> {
        NaiveDate::from_num_days_from_ce_opt(UNIX_EPOCH_FROM_CE + self.0)
    }

    pub fn to_secs(&self) -> i64 {
        (self.0 as i64) * 24 * 3600
    }

    /// Adds given Interval to the current date.
    /// Returns None if the resulting date would be out of range.
    pub fn add_interval(&self, interval: Interval) -> Option<Date> {
        let naive_date = self.to_chrono_date()?;

        let (months, days, _) = interval.to_month_day_nano();

        naive_date
            .checked_add_months(Months::new(months as u32))?
            .checked_add_days(Days::new(days as u64))
            .map(Into::into)
    }

    /// Subtracts given Interval to the current date.
    /// Returns None if the resulting date would be out of range.
    pub fn sub_interval(&self, interval: Interval) -> Option<Date> {
        let naive_date = self.to_chrono_date()?;

        let (months, days, _) = interval.to_month_day_nano();

        naive_date
            .checked_sub_months(Months::new(months as u32))?
            .checked_sub_days(Days::new(days as u64))
            .map(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;

    #[test]
    pub fn test_print_date2() {
        assert_eq!("1969-12-31", Date::new(-1).to_string());
        assert_eq!("1970-01-01", Date::new(0).to_string());
        assert_eq!("1970-02-12", Date::new(42).to_string());
    }

    #[test]
    pub fn test_date_parse() {
        assert_eq!(
            "1970-01-01",
            Date::from_str("1970-01-01").unwrap().to_string()
        );

        assert_eq!(
            "1969-01-01",
            Date::from_str("1969-01-01").unwrap().to_string()
        );

        assert_eq!(
            "1969-01-01",
            Date::from_str("     1969-01-01       ")
                .unwrap()
                .to_string()
        );

        let now = Utc::now().date_naive().format("%F").to_string();
        assert_eq!(now, Date::from_str(&now).unwrap().to_string());
    }

    #[test]
    fn test_add_sub_interval() {
        let date = Date::new(1000);

        let interval = Interval::from_year_month(3);

        let new_date = date.add_interval(interval).unwrap();
        assert_eq!(new_date.val(), 1091);

        assert_eq!(date, new_date.sub_interval(interval).unwrap());
    }

    #[test]
    pub fn test_min_max() {
        let mut date = Date::from_str("9999-12-31").unwrap();
        date.0 += 1000;
        assert_eq!(date, Date::from_str(&date.to_string()).unwrap());
    }

    #[test]
    pub fn test_from() {
        let d: Date = 42.into();
        assert_eq!(42, d.val());
    }

    #[test]
    fn test_to_secs() {
        let d = Date::from_str("1970-01-01").unwrap();
        assert_eq!(d.to_secs(), 0);
        let d = Date::from_str("1970-01-02").unwrap();
        assert_eq!(d.to_secs(), 24 * 3600);
        let d = Date::from_str("1970-01-03").unwrap();
        assert_eq!(d.to_secs(), 2 * 24 * 3600);
    }
}
