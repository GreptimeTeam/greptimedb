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

use common_error::prelude::*;
pub use common_query::error::{Error, Result};
use datatypes::error::Error as DataTypeError;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum InnerError {
    #[snafu(display("Fail to get scalar vector, {}", source))]
    GetScalarVector {
        source: DataTypeError,
        backtrace: Backtrace,
    },
}

impl ErrorExt for InnerError {
    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl From<InnerError> for Error {
    fn from(err: InnerError) -> Self {
        Self::new(err)
    }
}

#[cfg(test)]
mod tests {
    use snafu::GenerateImplicitData;

    use super::*;

    fn raise_datatype_error() -> std::result::Result<(), DataTypeError> {
        Err(DataTypeError::Conversion {
            from: "test".to_string(),
            backtrace: Backtrace::generate(),
        })
    }

    #[test]
    fn test_get_scalar_vector_error() {
        let err: Error = raise_datatype_error()
            .context(GetScalarVectorSnafu)
            .err()
            .unwrap()
            .into();
        assert!(err.backtrace_opt().is_some());
    }
}
