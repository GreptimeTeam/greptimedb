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
use common_macro::stack_trace_debug;
use snafu::Snafu;

use crate::TimeFilter;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Invalid time filter: {filter:?}"))]
    InvalidTimeFilter { filter: TimeFilter },

    #[snafu(display("Invalid date format: {input}"))]
    InvalidDateFormat { input: String },

    #[snafu(display("Invalid span format: {input}"))]
    InvalidSpanFormat { input: String },

    #[snafu(display("End time {end} is before start time {start}"))]
    EndBeforeStart { start: String, end: String },
}

impl ErrorExt for Error {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub type Result<T> = std::result::Result<T, Error>;
