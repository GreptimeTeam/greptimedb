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

use common_error::prelude::*;
use common_recordbatch::error::Error as RecordBatchError;
use datafusion::error::DataFusionError;
use datatypes::arrow::error::ArrowError;

common_error::define_opaque_error!(Error);

pub type Result<T> = std::result::Result<T, Error>;

impl From<Error> for DataFusionError {
    fn from(e: Error) -> Self {
        Self::External(Box::new(e))
    }
}

/// Default error implementation of table.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum InnerError {
    #[snafu(display("Datafusion error: {}", source))]
    Datafusion {
        source: DataFusionError,
        backtrace: Backtrace,
    },

    #[snafu(display("Poll stream failed, source: {}", source))]
    PollStream {
        source: ArrowError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to convert Arrow schema, source: {}", source))]
    SchemaConversion {
        source: datatypes::error::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Table projection error, source: {}", source))]
    TableProjection {
        source: ArrowError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to create record batch for Tables, source: {}", source))]
    TablesRecordBatch {
        #[snafu(backtrace)]
        source: BoxedError,
    },

    #[snafu(display("Column {} already exists in table {}", column_name, table_name))]
    ColumnExists {
        column_name: String,
        table_name: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to build schema, msg: {}, source: {}", msg, source))]
    SchemaBuild {
        #[snafu(backtrace)]
        source: datatypes::error::Error,
        msg: String,
    },

    #[snafu(display("Column {} not exists in table {}", column_name, table_name))]
    ColumnNotExists {
        column_name: String,
        table_name: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Not allowed to remove index column {} from table {}",
        column_name,
        table_name
    ))]
    RemoveColumnInIndex {
        column_name: String,
        table_name: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to build column descriptor for table: {}, column: {}, source: {}",
        table_name,
        column_name,
        source,
    ))]
    BuildColumnDescriptor {
        source: store_api::storage::ColumnDescriptorBuilderError,
        table_name: String,
        column_name: String,
        backtrace: Backtrace,
    },
}

impl ErrorExt for InnerError {
    fn status_code(&self) -> StatusCode {
        match self {
            InnerError::Datafusion { .. }
            | InnerError::PollStream { .. }
            | InnerError::SchemaConversion { .. }
            | InnerError::TableProjection { .. } => StatusCode::EngineExecuteQuery,
            InnerError::RemoveColumnInIndex { .. } | InnerError::BuildColumnDescriptor { .. } => {
                StatusCode::InvalidArguments
            }
            InnerError::TablesRecordBatch { .. } => StatusCode::Unexpected,
            InnerError::ColumnExists { .. } => StatusCode::TableColumnExists,
            InnerError::SchemaBuild { source, .. } => source.status_code(),
            InnerError::ColumnNotExists { .. } => StatusCode::TableColumnNotFound,
        }
    }

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

impl From<InnerError> for DataFusionError {
    fn from(e: InnerError) -> DataFusionError {
        DataFusionError::External(Box::new(e))
    }
}

impl From<InnerError> for RecordBatchError {
    fn from(e: InnerError) -> RecordBatchError {
        RecordBatchError::External {
            source: BoxedError::new(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn throw_df_error() -> Result<()> {
        Err(DataFusionError::NotImplemented("table test".to_string())).context(DatafusionSnafu)?
    }

    fn throw_column_exists_inner() -> std::result::Result<(), InnerError> {
        ColumnExistsSnafu {
            column_name: "col",
            table_name: "test",
        }
        .fail()
    }

    fn throw_missing_column() -> Result<()> {
        Ok(throw_column_exists_inner()?)
    }

    fn throw_arrow() -> Result<()> {
        Err(ArrowError::ComputeError("Overflow".to_string())).context(PollStreamSnafu)?
    }

    #[test]
    fn test_error() {
        let err = throw_df_error().err().unwrap();
        assert!(err.backtrace_opt().is_some());
        assert_eq!(StatusCode::EngineExecuteQuery, err.status_code());

        let err = throw_missing_column().err().unwrap();
        assert!(err.backtrace_opt().is_some());
        assert_eq!(StatusCode::TableColumnExists, err.status_code());

        let err = throw_arrow().err().unwrap();
        assert!(err.backtrace_opt().is_some());
        assert_eq!(StatusCode::EngineExecuteQuery, err.status_code());
    }

    #[test]
    fn test_into_record_batch_error() {
        let err = throw_column_exists_inner().err().unwrap();
        let err: RecordBatchError = err.into();
        assert!(err.backtrace_opt().is_some());
        assert_eq!(StatusCode::TableColumnExists, err.status_code());
    }

    #[test]
    fn test_into_df_error() {
        let err = throw_column_exists_inner().err().unwrap();
        let err: DataFusionError = err.into();
        assert!(matches!(err, DataFusionError::External(_)));
    }
}
