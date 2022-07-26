use arrow::error::ArrowError;
use common_error::prelude::{ErrorExt, StatusCode};
use datatypes::error::Error as DataTypeError;
use rustpython_compiler_core::error::CompileError as CoreCompileError;
use rustpython_parser::{ast::Location, error::ParseError};
pub use snafu::ensure;
use snafu::{prelude::Snafu, Backtrace};

common_error::define_opaque_error!(Error);
pub type Result<T> = std::result::Result<T, Error>;

/// for now it's just a String containing Exception info print by `write_exceptions`
///
/// TODO: maybe use [`rustpython_vm::exceptions::SerializeException`] instead of print out exception chain
#[derive(Debug, Snafu)]
pub struct PyExceptionSerde {
    pub output: String,
}

// TODO: rewrite Error
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum InnerError {
    #[snafu(display("Datatype error: {}", source))]
    TypeCast {
        #[snafu(backtrace)]
        source: DataTypeError,
    },
    /// these Python Errors already have very clear error hint, so no backtraces needed for them
    #[snafu(display("Python Parsing error: {}", source))]
    PyParse {
        backtrace: Backtrace,
        source: ParseError,
    },
    #[snafu(display("Python Compile error: {}", source))]
    PyCompile {
        backtrace: Backtrace,
        source: CoreCompileError,
    },
    /// rustpython problem, using python virtual machines' backtrace instead
    #[snafu(display("Python Runtime error: {}", source.output))]
    PyRuntime {
        backtrace: Backtrace,
        source: PyExceptionSerde,
    },
    #[snafu(display("Arrow error: {}", source))]
    Arrow {
        backtrace: Backtrace,
        source: ArrowError,
    },
    /// errors in coprocessors' parse check for types and etc.
    #[snafu(display("Coprocessor error: {} at {}.", reason, 
    if let Some(loc) = loc{
        format!("{loc}")
    }else{
        "Unknown location".into()
    }))]
    CoprParse {
        backtrace: Backtrace,
        reason: String,
        // location is option because maybe errors can't give a clear location?
        loc: Option<Location>,
    },
    /// Other types of error that isn't any of above
    #[snafu(display("Coprocessor's Internal types of error: {}", reason))]
    Other {
        backtrace: Backtrace,
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
        match self {
            Self::TypeCast { source } => source.backtrace_opt(),
            Self::Arrow {
                backtrace,
                source: _,
            } => Some(backtrace),
            _ => None,
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
// impl from for those error so one can use question mark and implictly cast into `CoprError`
impl From<DataTypeError> for InnerError {
    fn from(e: DataTypeError) -> Self {
        Self::TypeCast { source: e }
    }
}
