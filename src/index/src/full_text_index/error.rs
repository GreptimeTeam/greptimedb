// Copyright 2024 Greptime Team
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

use common_macro::stack_trace_debug;
use snafu::{Location, Snafu};
use tantivy::directory::error::OpenDirectoryError;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Tantivy error"))]
    Tantivy {
        #[snafu(source)]
        error: tantivy::TantivyError,
        location: Location,
    },

    #[snafu(display("Failed to open directory"))]
    OpenDirectory {
        #[snafu(source)]
        error: OpenDirectoryError,
        location: Location,
    },

    #[snafu(display("Failed to parse tantivy query"))]
    ParseQuery {
        #[snafu(source)]
        error: tantivy::query::QueryParserError,
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;
