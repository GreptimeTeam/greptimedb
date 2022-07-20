use arrow::error::ArrowError;
// use common_error::prelude::ErrorExt;
use rustpython_compiler_core::error::CompileError as CoreCompileError;
use rustpython_parser::{ast::Location, error::ParseError};
use rustpython_vm::builtins::PyBaseExceptionRef;
pub use snafu::ensure;
use snafu::prelude::Snafu;

pub type Result<T> = std::result::Result<T, Error>;
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    TypeCast {
        error: datatypes::error::Error,
    },
    PyParse {
        error: ParseError,
    },
    PyCompile {
        error: CoreCompileError,
    },
    PyRuntime {
        error: PyBaseExceptionRef,
    },
    Arrow {
        error: ArrowError,
    },
    /// errors in coprocessors' parse check for types and etc.
    CoprParse {
        reason: String,
        // location is option because maybe errors can't give a clear location?
        loc: Option<Location>,
    },
    /// Other types of error that isn't any of above
    Other {
        reason: String,
    },
}

// impl from for those error so one can use question mark and implictly cast into `CoprError`
impl From<ArrowError> for Error {
    fn from(error: ArrowError) -> Self {
        Self::Arrow { error }
    }
}
impl From<PyBaseExceptionRef> for Error {
    fn from(err: PyBaseExceptionRef) -> Self {
        Self::PyRuntime { error: err }
    }
}
impl From<ParseError> for Error {
    fn from(e: ParseError) -> Self {
        Self::PyParse { error: e }
    }
}
impl From<datatypes::error::Error> for Error {
    fn from(e: datatypes::error::Error) -> Self {
        Self::TypeCast { error: e }
    }
}

impl From<CoreCompileError> for Error {
    fn from(err: rustpython_compiler_core::error::CompileError) -> Self {
        Self::PyCompile { error: err }
    }
}
