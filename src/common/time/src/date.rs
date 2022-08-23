use std::fmt::{Display, Formatter};
use std::str::FromStr;

use chrono::{Datelike, NaiveDate};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::{ensure, ResultExt};

use crate::error::Result;
use crate::error::{DateOverflowSnafu, Error, ParseDateStrSnafu};

const UNIX_EPOCH_FROM_CE: i32 = 719_163;

/// ISO 8601 [Date] values. The inner representation is a signed 32 bit integer that represents the
/// **days since "1970-01-01 00:00:00 UTC" (UNIX Epoch)**.
///
/// [Date] value ranges between "0000-01-01" to "9999-12-31".
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
        let date = NaiveDate::parse_from_str(s, "%F").context(ParseDateStrSnafu { raw: s })?;
        Ok(Self(date.num_days_from_ce() - UNIX_EPOCH_FROM_CE))
    }
}

impl Display for Date {
    /// [Date] is formatted according to ISO-8601 standard.
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let abs_date = NaiveDate::from_num_days_from_ce(UNIX_EPOCH_FROM_CE + self.0);
        f.write_str(&abs_date.format("%F").to_string())
    }
}

impl Date {
    pub fn try_new(val: i32) -> Result<Self> {
        ensure!(
            val >= Self::MIN.0 && val <= Self::MAX.0,
            DateOverflowSnafu { value: val }
        );

        Ok(Self(val))
    }

    pub fn val(&self) -> i32 {
        self.0
    }

    /// Max valid Date value: "9999-12-31"
    pub const MAX: Date = Date(2932896);
    /// Min valid Date value: "1000-01-01"
    pub const MIN: Date = Date(-354285);
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;

    #[test]
    pub fn test_print_date2() {
        assert_eq!("1969-12-31", Date::try_new(-1).unwrap().to_string());
        assert_eq!("1970-01-01", Date::try_new(0).unwrap().to_string());
        assert_eq!("1970-02-12", Date::try_new(42).unwrap().to_string());
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

        let now = Utc::now().date().format("%F").to_string();
        assert_eq!(now, Date::from_str(&now).unwrap().to_string());
    }

    #[test]
    pub fn test_illegal_date_values() {
        assert!(Date::try_new(Date::MAX.0 + 1).is_err());
        assert!(Date::try_new(Date::MIN.0 - 1).is_err());
    }

    #[test]
    pub fn test_edge_date_values() {
        let date = Date::from_str("9999-12-31").unwrap();
        assert_eq!(Date::MAX.0, date.0);
        assert_eq!(date, Date::try_new(date.0).unwrap());

        let date = Date::from_str("1000-01-01").unwrap();
        assert_eq!(Date::MIN.0, date.0);
        assert_eq!(date, Date::try_new(date.0).unwrap());
    }
}
