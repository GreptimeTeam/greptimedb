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

use std::any::Any;

use common_error::ext::ErrorExt;
use common_error::prelude::{Snafu, StatusCode};
use snafu::{Backtrace, ErrorCompat, Location};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to find scripts table, source: {}", source))]
    FindScriptsTable {
        #[snafu(backtrace)]
        source: catalog::error::Error,
    },

    #[snafu(display("Failed to register scripts table, source: {}", source))]
    RegisterScriptsTable {
        #[snafu(backtrace)]
        source: catalog::error::Error,
    },

    #[snafu(display("Scripts table not found"))]
    ScriptsTableNotFound { location: Location },

    #[snafu(display(
        "Failed to insert script to scripts table, name: {}, source: {}",
        name,
        source
    ))]
    InsertScript {
        name: String,
        #[snafu(backtrace)]
        source: table::error::Error,
    },

    #[snafu(display("Failed to compile python script, name: {}, source: {}", name, source))]
    CompilePython {
        name: String,
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
    ScriptNotFound { location: Location, name: String },

    #[snafu(display("Failed to find script by name: {}", name))]
    FindScript {
        name: String,
        #[snafu(backtrace)]
        source: query::error::Error,
    },

    #[snafu(display("Failed to collect record batch, source: {}", source))]
    CollectRecords {
        #[snafu(backtrace)]
        source: common_recordbatch::error::Error,
    },

    #[snafu(display("Failed to cast type, msg: {}", msg))]
    CastType { msg: String, location: Location },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;
        match self {
            CastType { .. } => StatusCode::Unexpected,
            ScriptsTableNotFound { .. } => StatusCode::TableNotFound,
            RegisterScriptsTable { source } | FindScriptsTable { source } => source.status_code(),
            InsertScript { source, .. } => source.status_code(),
            CompilePython { source, .. } | ExecutePython { source, .. } => source.status_code(),
            FindScript { source, .. } => source.status_code(),
            CollectRecords { source } => source.status_code(),
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

#[cfg(test)]
mod tests {
    use snafu::ResultExt;

    use super::*;

    fn throw_catalog_error() -> catalog::error::Result<()> {
        catalog::error::IllegalManagerStateSnafu { msg: "test" }.fail()
    }

    fn throw_python_error() -> crate::python::error::Result<()> {
        crate::python::error::CoprParseSnafu {
            reason: "test",
            loc: None,
        }
        .fail()
    }

    #[test]
    fn test_error() {
        let err = throw_catalog_error()
            .context(FindScriptsTableSnafu)
            .unwrap_err();
        assert_eq!(StatusCode::Unexpected, err.status_code());
        assert!(err.backtrace_opt().is_some());

        let err = throw_python_error()
            .context(ExecutePythonSnafu { name: "test" })
            .unwrap_err();
        assert_eq!(StatusCode::InvalidArguments, err.status_code());
        assert!(err.backtrace_opt().is_some());
    }
}
