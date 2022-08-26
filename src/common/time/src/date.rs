use std::fmt::{Display, Formatter};
use std::str::FromStr;

use chrono::{Datelike, NaiveDate};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::ResultExt;

use crate::error::Result;
use crate::error::{Error, ParseDateStrSnafu};

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
    pub fn new(val: i32) -> Self {
        Self(val)
    }

    pub fn val(&self) -> i32 {
        self.0
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

        let now = Utc::now().date().format("%F").to_string();
        assert_eq!(now, Date::from_str(&now).unwrap().to_string());
    }

    #[test]
    pub fn test_min_max() {
        let mut date = Date::from_str("9999-12-31").unwrap();
        date.0 += 1000;
        assert_eq!(date, Date::from_str(&date.to_string()).unwrap());
    }
}
