use chrono::ParseError;
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to parse string to date, raw: {}, source: {}", raw, source))]
    ParseDateStr { raw: String, source: ParseError },
    #[snafu(display("Failed to parse string to Timestamp, raw: {}", raw))]
    ParseTimestamp { raw: String },
    #[snafu(display("Invalid local timestamp representation, raw: {}", raw))]
    InvalidTimestampRepresentation { raw: String },
}

pub type Result<T> = std::result::Result<T, Error>;
