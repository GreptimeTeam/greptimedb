use std::any::Any;

use common_error::prelude::{ErrorCompat, ErrorExt, Snafu, StatusCode};
use snafu::Backtrace;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to serialize data, source: {}", source))]
    Serialize {
        source: serde_json::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to convert datafusion type: {}", from))]
    Conversion { from: String, backtrace: Backtrace },
    #[snafu(display("Bad array access, {}", msg))]
    BadArrayAccess { msg: String, backtrace: Backtrace },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        // Inner serialization and deserialization error should not be exposed to users.
        StatusCode::Internal
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use snafu::ResultExt;

    use super::*;

    #[test]
    pub fn test_error() {
        let mut map = HashMap::new();
        map.insert(true, 1);
        map.insert(false, 2);

        let result = serde_json::to_string(&map).context(SerializeSnafu);
        assert!(result.is_err(), "serialize result is: {:?}", result);
        let err = serde_json::to_string(&map)
            .context(SerializeSnafu)
            .err()
            .unwrap();
        assert!(err.backtrace_opt().is_some());
        assert_eq!(StatusCode::Internal, err.status_code());
    }
}
