use std::any::Any;

use common_error::ext::ErrorExt;
use common_error::prelude::{Snafu, StatusCode};
use snafu::{Backtrace, ErrorCompat};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to open scripts table, source: {}", source))]
    OpenScriptsTable {
        #[snafu(backtrace)]
        source: table::error::Error,
    },

    #[snafu(display("Failed to create scripts table, source: {}", source))]
    CreateScriptsTable {
        #[snafu(backtrace)]
        source: table::error::Error,
    },
    #[snafu(display("Failed to insert script to scripts table, source: {}", source))]
    InsertScript {
        #[snafu(backtrace)]
        source: table::error::Error,
    },

    #[snafu(display("Failed to compile python script, source: {}", source))]
    CompilePython {
        #[snafu(backtrace)]
        source: crate::python::error::Error,
    },

    #[snafu(display("Failed to execute python script {}, source: {}", name, source))]
    ExecutePython {
        name: String,
        #[snafu(backtrace)]
        source: crate::python::error::Error,
    },
    #[snafu(display("Script not found, name: {}", name))]
    ScriptNotFound { backtrace: Backtrace, name: String },

    #[snafu(display("Failed to find script by name: {}", name))]
    FindScript {
        name: String,
        #[snafu(backtrace)]
        source: query::error::Error,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;
        match self {
            OpenScriptsTable { .. } | CreateScriptsTable { .. } => StatusCode::Unexpected,

            InsertScript { source } => source.status_code(),

            CompilePython { source } | ExecutePython { source, .. } => source.status_code(),

            FindScript { source, .. } => source.status_code(),

            ScriptNotFound { .. } => StatusCode::InvalidArguments,
        }
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
