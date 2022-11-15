// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;

use common_error::ext::ErrorExt;
use common_error::prelude::StatusCode;
use datatypes::prelude::ConcreteDataType;
use snafu::prelude::*;
use snafu::{Backtrace, ErrorCompat};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Unknown proto column datatype: {}", datatype))]
    UnknownColumnDataType { datatype: i32, backtrace: Backtrace },

    #[snafu(display("Failed to create column datatype from {:?}", from))]
    IntoColumnDataType {
        from: ConcreteDataType,
        backtrace: Backtrace,
    },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::UnknownColumnDataType { .. } => StatusCode::InvalidArguments,
            Error::IntoColumnDataType { .. } => StatusCode::Unexpected,
        }
    }
    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
