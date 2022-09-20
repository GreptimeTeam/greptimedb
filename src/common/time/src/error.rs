use chrono::ParseError;
use snafu::{Backtrace, Snafu};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to parse string to date, raw: {}, source: {}", raw, source))]
    ParseDateStr { raw: String, source: ParseError },
    #[snafu(display("Failed to parse string to Timestamp, raw: {}", raw))]
    ParseTimestamp { raw: String, backtrace: Backtrace },
}

pub type Result<T> = std::result::Result<T, Error>;
