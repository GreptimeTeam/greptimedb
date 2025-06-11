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

use datatypes::prelude::ConcreteDataType;
use snafu::{Location, Snafu};

/// Error definitions for mito encoding.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Row value mismatches field data type"))]
    FieldTypeMismatch { source: datatypes::error::Error },

    #[snafu(display("Failed to serialize field"))]
    SerializeField {
        #[snafu(source)]
        error: memcomparable::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Data type: {} does not support serialization/deserialization",
        data_type,
    ))]
    NotSupportedField {
        data_type: ConcreteDataType,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to deserialize field"))]
    DeserializeField {
        #[snafu(source)]
        error: memcomparable::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Operation not supported: {}", err_msg))]
    UnsupportedOperation {
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
