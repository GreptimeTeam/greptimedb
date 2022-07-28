use arrow::error::ArrowError;
use common_error::prelude::{ErrorExt, StatusCode};
use datatypes::error::Error as DataTypeError;
use rustpython_compiler_core::error::CompileError as CoreCompileError;
use rustpython_parser::{ast::Location, error::ParseError};
pub use snafu::ensure;
use snafu::{prelude::Snafu, Backtrace};
use console::{style, Style};
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
pub enum Error {
    #[snafu(display("Datatype error: {}", source))]
    TypeCast {
        #[snafu(backtrace)]
        source: DataTypeError,
    },
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

impl ErrorExt for Error {
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
impl From<DataTypeError> for Error {
    fn from(e: DataTypeError) -> Self {
        Self::TypeCast { source: e }
    }
}

/// pretty print [`Error`] in given script,
/// basically print a arrow which point to where error occurs(if possible to get a location)
pub fn pretty_print_error_in_src(
    script: &str,
    err: &Error,
    ln_offset: usize,
    filename: &str,
) -> String {
    // get a location if possible
    let loc: Option<Location> = match err {
        Error::PyParse {
            backtrace: _,
            source,
        } => Some(source.location),
        Error::PyCompile {
            backtrace: _,
            source,
        } => Some(source.location),
        Error::CoprParse {
            backtrace: _,
            reason: _,
            loc,
        } => loc.to_owned(),
        _ => None,
    };
    if let Some(loc) = loc {
        let reason = get_error_reason(err);
        return visualize_loc(
            script,
            &loc,
            &err.to_string(),
            &reason,
            ln_offset,
            filename,
        );
    }

    todo!()
}

/// pretty print a location in script with desc.
///
/// `ln_offset` is line offset number that added to `loc`'s `row`, `filename` is the file's name display with it's row and columns info.
pub fn visualize_loc(
    script: &str,
    loc: &Location,
    err_ty: &str,
    desc: &str,
    ln_offset: usize,
    filename: &str,
) -> String {
    let lines: Vec<&str> = script.split('\n').collect();
    let loc = Location::new(ln_offset + loc.row(), loc.column());
    let (row, col) = (loc.row(), loc.column());
    let red_bold = Style::new().red().bold();
    let blue_bold = Style::new().blue().bold();
    let indicate = format!(
        "
{error}: {err_ty}
{r_arrow} {filename}:{row}:{col}
{prow:2}{ln_pad} {line}
{prow:2}{ln_pad} {arrow:>pad$} {desc}
",
        error = red_bold.apply_to("error"),
        err_ty = style(err_ty).bold(),
        r_arrow = blue_bold.apply_to(" -->"),
        filename = filename,
        row = row,
        col = col,
        line = lines[loc.row() - 1],
        pad = loc.column(),
        arrow = red_bold.apply_to("^"),
        desc = red_bold.apply_to(desc),
        ln_pad = blue_bold.apply_to("|"),
        prow = blue_bold.apply_to(row),
    );
    indicate
}

/// extract a reason for [`Error`]
pub fn get_error_reason(err: &Error) -> String {
    match err {
        Error::CoprParse {
            backtrace: _,
            reason,
            loc: _,
        } => reason.clone(),
        Error::Other {
            backtrace: _,
            reason,
        } => reason.clone(),
        Error::PyRuntime {
            backtrace: _,
            source,
        } => source.output.clone(),
        Error::PyParse {
            backtrace: _,
            source,
        } => format!("{}", source.error),
        _ => format!("Unknown error: {:?}", err)
    }
}
