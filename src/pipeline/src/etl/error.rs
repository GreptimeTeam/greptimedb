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
use datatypes::timestamp::TimestampNanosecond;
use snafu::{Location, Snafu};

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Empty input field"))]
    EmptyInputField {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Missing input field"))]
    MissingInputField {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Processor must be a map"))]
    ProcessorMustBeMap {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("{processor} processor: missing field: {field}"))]
    ProcessorMissingField {
        processor: String,
        field: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("{processor} processor: expect string value, but got {v:?}"))]
    ProcessorExpectString {
        processor: String,
        v: crate::etl::Value,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("processor key must be a string"))]
    ProcessorKeyMustBeString {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("{kind} processor: failed to parse {value}"))]
    ProcessorFailedToParseString {
        kind: String,
        value: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("processor must have a string key"))]
    ProcessorMustHaveStringKey {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("unsupported {processor} processor"))]
    UnsupportedProcessor {
        processor: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Field {field} must be a {ty}"))]
    FieldMustBeType {
        field: String,
        ty: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Field parse from string failed: {field}"))]
    FaildParseFieldFromString {
        error: Box<dyn std::error::Error>,
        field: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("failed to parse {key} as int: {value}"))]
    FailedToParseIntKey {
        key: String,
        value: String,
        error: std::num::ParseIntError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("failed to parse {key} as float: {value}"))]
    FailedToParseFloatKey {
        key: String,
        value: String,
        error: std::num::ParseFloatError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("{kind} processor.{key} not found in intermediate keys"))]
    IntermediateKeyIndexError {
        kind: String,
        key: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("{k} missing value in {s}"))]
    CmcdMissingValue {
        k: String,
        s: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("{part} missing key in {s}"))]
    CmcdMissingKey {
        part: String,
        s: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("key must be a string, but got {k:?}"))]
    KeyMustBeString {
        k: yaml_rust::Yaml,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("csv read error"))]
    CsvReadError {
        #[snafu(implicit)]
        location: Location,
        error: csv::Error,
    },
    #[snafu(display("expected at least one record from csv format, but got none"))]
    CsvNoRecord {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("'{separator}' must be a single character, but got '{value}'"))]
    CsvSeparatorName {
        separator: &'static str,
        value: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("'{quote}' must be a single character, but got '{value}'"))]
    CsvQuoteName {
        quote: &'static str,
        value: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Parse date timezone error {value}"))]
    DateParseTimezoneError {
        value: String,
        error: chrono_tz::ParseError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Parse date error {value}"))]
    DateParseError {
        value: String,
        error: chrono::ParseError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("failed to get local timezone"))]
    DateFailedToGetLocalTimezone {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("failed to get timestamp"))]
    DateFailedToGetTimestamp {
        #[snafu(implicit)]
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;
