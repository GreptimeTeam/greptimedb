/// Calculates the time duration since UNIX_EPOCH in milliseconds.
pub fn current_timestamp() -> i64 {
    chrono::Utc::now().timestamp()
}
