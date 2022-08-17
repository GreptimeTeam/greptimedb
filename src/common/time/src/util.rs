/// Returns the time duration since UNIX_EPOCH in milliseconds.
pub fn current_time_millis() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn test_current_time_millis() {
        let now = current_time_millis();
        let datetime = chrono::Utc.timestamp_millis(now);

        assert_eq!(now, datetime.timestamp_millis());
    }
}
