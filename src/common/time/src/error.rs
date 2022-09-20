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

#[cfg(test)]
mod tests {
    use chrono::NaiveDateTime;
    use snafu::ResultExt;

    use super::*;

    #[test]
    fn test_errors() {
        let raw = "2020-09-08T13:42:29.190855Z";
        let result = NaiveDateTime::parse_from_str(raw, "%F").context(ParseDateStrSnafu { raw });
        assert!(matches!(result.err().unwrap(), Error::ParseDateStr { .. }));

        assert_eq!(
            "Failed to parse string to Timestamp, raw: 2020-09-08T13:42:29.190855Z",
            ParseTimestampSnafu { raw }.build().to_string()
        );
    }
}
