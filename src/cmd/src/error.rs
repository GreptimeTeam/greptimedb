use std::any::Any;

use common_error::prelude::*;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to start datanode, source: {}", source))]
    StartDatanode {
        #[snafu(backtrace)]
        source: datanode::error::Error,
    },

    #[snafu(display("Failed to read config file: {}, source: {}", path, source))]
    ReadConfig {
        source: std::io::Error,
        path: String,
    },

    #[snafu(display("Failed to parse config, source: {}", source))]
    ParseConfig { source: toml::de::Error },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::StartDatanode { source } => source.status_code(),
            Error::ReadConfig { .. } | Error::ParseConfig { .. } => StatusCode::InvalidArguments,
        }
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raise_read_config_error() -> std::result::Result<(), std::io::Error> {
        Err(std::io::ErrorKind::NotFound.into())
    }

    #[test]
    fn test_error() {
        let e = raise_read_config_error()
            .context(ReadConfigSnafu { path: "test" })
            .err()
            .unwrap();

        assert!(e.backtrace_opt().is_none());
        assert_eq!(e.status_code(), StatusCode::InvalidArguments);
    }
}
