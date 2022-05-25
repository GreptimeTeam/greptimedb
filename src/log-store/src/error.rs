use std::any::Any;

use common_error::ext::BoxedError;
use common_error::prelude::{ErrorExt, Snafu};
use snafu::{Backtrace, ErrorCompat};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to encode entry"))]
    Encode { source: BoxedError },

    #[snafu(display("Failed to decode entry"))]
    Decode { backtrace: Backtrace },

    #[snafu(display("Entry corrupted, msg: {}", msg))]
    Corrupted { msg: String, backtrace: Backtrace },

    #[snafu(display("IO error, source: {}", source))]
    Io {
        source: std::io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to open log file {}, source: {}", file_name, source))]
    OpenLog {
        file_name: String,
        source: std::io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("File name {} illegal", file_name))]
    FileNameIllegal {
        file_name: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Internal error, msg: {}", msg))]
    Internal { msg: String, backtrace: Backtrace },

    #[snafu(display("End of LogFile"))]
    Eof,

    #[snafu(display("File duplicate on start: {}", msg))]
    DuplicateFile { msg: String },

    #[snafu(display("Log file suffix is illegal: {}", suffix))]
    SuffixIllegal { suffix: String },
}

impl ErrorExt for Error {
    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub type Result<T> = std::result::Result<T, Error>;
