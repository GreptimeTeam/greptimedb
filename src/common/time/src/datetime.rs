use std::fmt::{Display, Formatter};
use std::str::FromStr;

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};

use crate::error::{DateTimeOverflowSnafu, Error, ParseDateStrSnafu, Result};

const DATETIME_FORMAT: &str = "%F %T";

/// [DateTime] represents the **seconds elapsed since "1970-01-01 00:00:00 UTC" (UNIX Epoch)**.  
///
/// Valid [DateTime] value ranges from "0000-01-01 00:00:00" to "9999-12-31 23:59:59".
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub struct DateTime(i64);

impl Display for DateTime {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let abs_time = NaiveDateTime::from_timestamp(self.0, 0);
        write!(f, "{}", abs_time.format(DATETIME_FORMAT))
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
        let datetime = NaiveDateTime::parse_from_str(s, DATETIME_FORMAT)
            .context(ParseDateStrSnafu { raw: s })?;
        Ok(Self(datetime.timestamp()))
    }
}

impl DateTime {
    pub fn try_new(val: i64) -> Result<Self> {
        ensure!(
            val >= Self::MIN.0 && val <= Self::MAX.0,
            DateTimeOverflowSnafu { value: val }
        );

        Ok(Self(val))
    }

    pub fn val(&self) -> i64 {
        self.0
    }

    /// Max valid DateTime value: 9999-12-31 23:59:59
    pub const MAX: DateTime = DateTime(253402300799);
    /// Min valid DateTime value: 0000-01-01 00:00:00
    pub const MIN: DateTime = DateTime(-30610224000);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_new_date_time() {
        assert_eq!(
            "1970-01-01 00:00:00",
            DateTime::try_new(0).unwrap().to_string()
        );
        assert_eq!(
            "1970-01-01 00:00:01",
            DateTime::try_new(1).unwrap().to_string()
        );
        assert_eq!(
            "1969-12-31 23:59:59",
            DateTime::try_new(-1).unwrap().to_string()
        );
    }

    #[test]
    pub fn test_max_min() {
        assert_eq!("9999-12-31 23:59:59", DateTime::MAX.to_string());
        assert_eq!("1000-01-01 00:00:00", DateTime::MIN.to_string());
    }

    #[test]
    pub fn test_parse_from_string() {
        let time = "1970-01-01 00:00:00";
        let dt = DateTime::from_str(time).unwrap();
        assert_eq!(time, &dt.to_string());
    }
}
