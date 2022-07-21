use arrow::error::ArrowError;
use common_error::prelude::{ErrorExt, StatusCode};
use rustpython_compiler_core::error::CompileError as CoreCompileError;
use rustpython_parser::{ast::Location, error::ParseError};
pub use snafu::ensure;
use snafu::prelude::Snafu;

common_error::define_opaque_error!(Error);
pub type Result<T> = std::result::Result<T, InnerError>;

// TODO: rewrite Error
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum InnerError {
    TypeCast {
        error: datatypes::error::Error,
    },
    PyParse {
        error: ParseError,
    },
    PyCompile {
        error: CoreCompileError,
    },
    /// TODO: maybe use [`rustpython_vm::exceptions::SerializeException`] instead of print out exception chain
    PyRuntime {
        py_exception: String,
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
impl From<InnerError> for Error {
    fn from(err: InnerError) -> Self {
        Self::new(err)
    }
}
impl ErrorExt for InnerError {
    fn status_code(&self) -> common_error::prelude::StatusCode {
        StatusCode::Unknown
    }
    fn backtrace_opt(&self) -> Option<&common_error::snafu::Backtrace> {
        None
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
// impl from for those error so one can use question mark and implictly cast into `CoprError`
impl From<ArrowError> for InnerError {
    fn from(error: ArrowError) -> Self {
        Self::Arrow { error }
    }
}
impl From<String> for InnerError {
    fn from(err: String) -> Self {
        Self::PyRuntime { py_exception: err }
    }
}
impl From<ParseError> for InnerError {
    fn from(e: ParseError) -> Self {
        Self::PyParse { error: e }
    }
}
impl From<datatypes::error::Error> for InnerError {
    fn from(e: datatypes::error::Error) -> Self {
        Self::TypeCast { error: e }
    }
}

impl From<CoreCompileError> for InnerError {
    fn from(err: rustpython_compiler_core::error::CompileError) -> Self {
        Self::PyCompile { error: err }
    }
}
