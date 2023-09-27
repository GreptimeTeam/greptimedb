// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use console::{style, Style};
use datafusion::error::DataFusionError;
use datatypes::arrow::error::ArrowError;
use datatypes::error::Error as DataTypeError;
use query::error::Error as QueryError;
use rustpython_codegen::error::CodegenError;
use rustpython_parser::ast::Location;
use rustpython_parser::ParseError;
pub use snafu::ensure;
use snafu::prelude::Snafu;
use snafu::Location as SnafuLocation;
pub type Result<T> = std::result::Result<T, Error>;

pub(crate) fn ret_other_error_with(reason: String) -> OtherSnafu<String> {
    OtherSnafu { reason }
}

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Datatype error"))]
    TypeCast {
        location: SnafuLocation,
        source: DataTypeError,
    },

    #[snafu(display("Failed to query"))]
    DatabaseQuery {
        location: SnafuLocation,
        source: QueryError,
    },

    #[snafu(display("Failed to parse script"))]
    PyParse {
        location: SnafuLocation,
        #[snafu(source)]
        error: ParseError,
    },

    #[snafu(display("Failed to compile script"))]
    PyCompile {
        location: SnafuLocation,
        #[snafu(source)]
        error: CodegenError,
    },

    /// rustpython problem, using python virtual machines' backtrace instead
    #[snafu(display("Python Runtime error, error: {}", msg))]
    PyRuntime {
        msg: String,
        location: SnafuLocation,
    },

    #[snafu(display("Arrow error"))]
    Arrow {
        location: SnafuLocation,
        #[snafu(source)]
        error: ArrowError,
    },

    #[snafu(display("DataFusion error"))]
    DataFusion {
        location: SnafuLocation,
        #[snafu(source)]
        error: DataFusionError,
    },

    /// errors in coprocessors' parse check for types and etc.
    #[snafu(display("Coprocessor error: {} {}.", reason,
                    if let Some(loc) = loc{
                        format!("at {loc:?}")
                    }else{
                        "".into()
                    }))]
    CoprParse {
        location: SnafuLocation,
        reason: String,
        // location is option because maybe errors can't give a clear location?
        loc: Option<Location>,
    },

    /// Other types of error that isn't any of above
    #[snafu(display("Coprocessor's Internal error: {}", reason))]
    Other {
        location: SnafuLocation,
        reason: String,
    },

    #[snafu(display("Unsupported sql in coprocessor: {}", sql))]
    UnsupportedSql {
        sql: String,
        location: SnafuLocation,
    },

    #[snafu(display("Failed to retrieve record batches"))]
    RecordBatch {
        location: SnafuLocation,
        source: common_recordbatch::error::Error,
    },

    #[snafu(display("Failed to create record batch"))]
    NewRecordBatch {
        location: SnafuLocation,
        source: common_recordbatch::error::Error,
    },
    #[snafu(display("Failed to create tokio task"))]
    TokioJoin {
        #[snafu(source)]
        error: tokio::task::JoinError,
    },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::DataFusion { .. }
            | Error::Arrow { .. }
            | Error::PyRuntime { .. }
            | Error::TokioJoin { .. }
            | Error::Other { .. } => StatusCode::Internal,

            Error::RecordBatch { source, .. } | Error::NewRecordBatch { source, .. } => {
                source.status_code()
            }
            Error::DatabaseQuery { source, .. } => source.status_code(),
            Error::TypeCast { source, .. } => source.status_code(),

            Error::PyParse { .. }
            | Error::PyCompile { .. }
            | Error::CoprParse { .. }
            | Error::UnsupportedSql { .. } => StatusCode::InvalidArguments,
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
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
    let (reason, loc) = get_error_reason_loc(err);
    if let Some(loc) = loc {
        visualize_loc(script, &loc, &err.to_string(), &reason, ln_offset, filename)
    } else {
        // No location provide
        format!("\n{}: {}", style("error").red().bold(), err)
    }
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
    let (row, col) = (loc.row(), loc.column());
    let red_bold = Style::new().red().bold();
    let blue_bold = Style::new().blue().bold();
    let col_space = (ln_offset + row).to_string().len().max(1);
    let space: String = " ".repeat(col_space - 1);
    let indicate = format!(
        "
{error}: {err_ty}
{space}{r_arrow}{filename}:{row}:{col}
{prow:col_space$}{ln_pad} {line}
{space} {ln_pad} {arrow:>pad$} {desc}
",
        error = red_bold.apply_to("error"),
        err_ty = style(err_ty).bold(),
        r_arrow = blue_bold.apply_to("-->"),
        filename = filename,
        row = ln_offset + row,
        col = col,
        line = lines[loc.row() - 1],
        pad = loc.column(),
        arrow = red_bold.apply_to("^"),
        desc = red_bold.apply_to(desc),
        ln_pad = blue_bold.apply_to("|"),
        prow = blue_bold.apply_to(ln_offset + row),
        space = space
    );
    indicate
}

/// extract a reason for [`Error`] in string format, also return a location if possible
pub fn get_error_reason_loc(err: &Error) -> (String, Option<Location>) {
    match err {
        Error::CoprParse { reason, loc, .. } => (reason.clone(), *loc),
        Error::Other { reason, .. } => (reason.clone(), None),
        Error::PyRuntime { msg, .. } => (msg.clone(), None),
        Error::PyParse { error, .. } => (error.error.to_string(), Some(error.location)),
        Error::PyCompile { error, .. } => (error.error.to_string(), Some(error.location)),
        _ => (format!("Unknown error: {err:?}"), None),
    }
}
