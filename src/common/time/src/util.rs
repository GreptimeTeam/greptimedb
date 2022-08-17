/// Returns the time duration since UNIX_EPOCH in milliseconds.
pub fn current_time_millis() -> i64 {
    chrono::Utc::now().timestamp_millis()
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
        let datetime_now = chrono::Utc.timestamp_millis(now);
        let datetime_std = chrono::Utc.timestamp_millis(millis_from_std);

        assert_eq!(datetime_std.year(), datetime_now.year());
        assert_eq!(datetime_std.month(), datetime_now.month());
        assert_eq!(datetime_std.day(), datetime_now.day());
        assert_eq!(datetime_std.hour(), datetime_now.hour());
        assert_eq!(datetime_std.minute(), datetime_now.minute());
    }
}
