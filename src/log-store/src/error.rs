use std::any::Any;

use common_error::prelude::{ErrorExt, Snafu};
use snafu::{Backtrace, ErrorCompat};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to deserialize entry"))]
    Deserialization,

    #[snafu(display("Entry corrupted, msg:{}", msg))]
    Corrupted { msg: String },

    #[snafu(display("IO error, source: {}", source))]
    IO { source: std::io::Error },

    #[snafu(display("File name {} illegal", file_name))]
    FileNameIllegal { file_name: String },

    #[snafu(display("Internal error, msg: {}", msg))]
    Internal { msg: String },

    #[snafu(display("End of LogFile"))]
    Eof,
}

impl ErrorExt for Error {
    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
