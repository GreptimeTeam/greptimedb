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
    #[snafu(display("Failed to serialize config, source: {}", source))]
    SerializeConfig { source: toml::ser::Error },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::StartDatanode { source } => source.status_code(),
            Error::SerializeConfig { .. } => StatusCode::Unexpected,
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
