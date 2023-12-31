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
use std::io::Error as IoError;

use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use snafu::{Location, Snafu};

use crate::inverted_index::search::predicate::Predicate;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Failed to seek"))]
    Seek {
        #[snafu(source)]
        error: IoError,
        location: Location,
    },

    #[snafu(display("Failed to read"))]
    Read {
        #[snafu(source)]
        error: IoError,
        location: Location,
    },

    #[snafu(display("Failed to write"))]
    Write {
        #[snafu(source)]
        error: IoError,
        location: Location,
    },

    #[snafu(display("Failed to flush"))]
    Flush {
        #[snafu(source)]
        error: IoError,
        location: Location,
    },

    #[snafu(display("Failed to close"))]
    Close {
        #[snafu(source)]
        error: IoError,
        location: Location,
    },

    #[snafu(display(
        "Unexpected inverted index blob size, min: {min_blob_size}, actual: {actual_blob_size}"
    ))]
    UnexpectedBlobSize {
        min_blob_size: u64,
        actual_blob_size: u64,
        location: Location,
    },

    #[snafu(display("Unexpected inverted index footer payload size, max: {max_payload_size}, actual: {actual_payload_size}"))]
    UnexpectedFooterPayloadSize {
        max_payload_size: u64,
        actual_payload_size: u64,
        location: Location,
    },

    #[snafu(display("Unexpected inverted index offset size, offset: {offset}, size: {size}, blob_size: {blob_size}, payload_size: {payload_size}"))]
    UnexpectedOffsetSize {
        offset: u64,
        size: u64,
        blob_size: u64,
        payload_size: u64,
    },

    #[snafu(display("Unexpected zero segment row count"))]
    UnexpectedZeroSegmentRowCount { location: Location },

    #[snafu(display("Failed to decode fst"))]
    DecodeFst {
        #[snafu(source)]
        error: fst::Error,
        location: Location,
    },

    #[snafu(display("Failed to decode protobuf"))]
    DecodeProto {
        #[snafu(source)]
        error: prost::DecodeError,
        location: Location,
    },

    #[snafu(display("Failed to parse regex pattern: {pattern}"))]
    ParseRegex {
        #[snafu(source)]
        error: regex::Error,
        pattern: String,
        location: Location,
    },

    #[snafu(display("Failed to parse regex DFA"))]
    ParseDFA {
        #[snafu(source)]
        error: Box<regex_automata::dfa::Error>,
        location: Location,
    },

    #[snafu(display("Unexpected empty predicates to construct fst applier"))]
    EmptyPredicates { location: Location },

    #[snafu(display("Failed to construct intersection fst applier with InList predicate"))]
    IntersectionApplierWithInList { location: Location },

    #[snafu(display("Failed to construct keys fst applier without InList predicate"))]
    KeysApplierWithoutInList { location: Location },

    #[snafu(display(
        "Failed to construct keys fst applier with unexpected predicates: {predicates:?}"
    ))]
    KeysApplierUnexpectedPredicates {
        location: Location,
        predicates: Vec<Predicate>,
    },

    #[snafu(display("index not found, name: {name}"))]
    IndexNotFound { name: String, location: Location },

    #[snafu(display("Failed to insert value to FST"))]
    FstInsert {
        #[snafu(source)]
        error: fst::Error,
        location: Location,
    },

    #[snafu(display("Failed to compile FST"))]
    FstCompile {
        #[snafu(source)]
        error: fst::Error,
        location: Location,
    },

    #[snafu(display("Failed to perform IO operation"))]
    CommonIoError {
        #[snafu(source)]
        error: IoError,
        location: Location,
    },

    #[snafu(display("Unknown intermediate codec magic: {magic:?}"))]
    UnknownIntermediateCodecMagic { magic: [u8; 4], location: Location },

    #[snafu(display("Inconsistent row count, index_name: {index_name}, total_row_count: {total_row_count}, expected: {expected_row_count}"))]
    InconsistentRowCount {
        index_name: String,
        total_row_count: usize,
        expected_row_count: usize,
    },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;
        match self {
            Seek { .. }
            | Read { .. }
            | Write { .. }
            | Flush { .. }
            | Close { .. }
            | UnexpectedFooterPayloadSize { .. }
            | UnexpectedZeroSegmentRowCount { .. }
            | UnexpectedOffsetSize { .. }
            | UnexpectedBlobSize { .. }
            | DecodeProto { .. }
            | DecodeFst { .. }
            | KeysApplierUnexpectedPredicates { .. }
            | CommonIoError { .. }
            | UnknownIntermediateCodecMagic { .. }
            | FstCompile { .. } => StatusCode::Unexpected,

            ParseRegex { .. }
            | ParseDFA { .. }
            | KeysApplierWithoutInList { .. }
            | IntersectionApplierWithInList { .. }
            | EmptyPredicates { .. }
            | FstInsert { .. }
            | InconsistentRowCount { .. }
            | IndexNotFound { .. } => StatusCode::InvalidArguments,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub type Result<T> = std::result::Result<T, Error>;
