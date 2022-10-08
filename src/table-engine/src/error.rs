use std::any::Any;

use common_error::ext::BoxedError;
use common_error::prelude::*;
use table::metadata::{TableInfoBuilderError, TableMetaBuilderError};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to create region, source: {}", source))]
    CreateRegion {
        #[snafu(backtrace)]
        source: BoxedError,
    },

    #[snafu(display("Failed to open region, region: {}, source: {}", region_name, source))]
    OpenRegion {
        region_name: String,
        #[snafu(backtrace)]
        source: BoxedError,
    },

    #[snafu(display(
        "Failed to build table meta for table: {}, source: {}",
        table_name,
        source
    ))]
    BuildTableMeta {
        source: TableMetaBuilderError,
        table_name: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to build table info for table: {}, source: {}",
        table_name,
        source
    ))]
    BuildTableInfo {
        source: TableInfoBuilderError,
        table_name: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Missing timestamp index for table: {}", table_name))]
    MissingTimestampIndex {
        table_name: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to build row key descriptor for table: {}, source: {}",
        table_name,
        source
    ))]
    BuildRowKeyDescriptor {
        source: store_api::storage::RowKeyDescriptorBuilderError,
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

    #[snafu(display(
        "Failed to build column family descriptor for table: {}, source: {}",
        table_name,
        source
    ))]
    BuildColumnFamilyDescriptor {
        source: store_api::storage::ColumnFamilyDescriptorBuilderError,
        table_name: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to build region descriptor for table: {}, region: {}, source: {}",
        table_name,
        region_name,
        source,
    ))]
    BuildRegionDescriptor {
        source: store_api::storage::RegionDescriptorBuilderError,
        table_name: String,
        region_name: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to update table metadata to manifest,  table: {}, source: {}",
        table_name,
        source,
    ))]
    UpdateTableManifest {
        #[snafu(backtrace)]
        source: storage::error::Error,
        table_name: String,
    },

    #[snafu(display(
        "Failed to scan table metadata from manifest,  table: {}, source: {}",
        table_name,
        source,
    ))]
    ScanTableManifest {
        #[snafu(backtrace)]
        source: storage::error::Error,
        table_name: String,
    },

    #[snafu(display("Table info not found in manifest, table: {}", table_name))]
    TableInfoNotFound {
        backtrace: Backtrace,
        table_name: String,
    },

    #[snafu(display("Table already exists: {}", table_name))]
    TableExists {
        backtrace: Backtrace,
        table_name: String,
    },

    #[snafu(display("Table not found: {}", table_name))]
    TableNotFound {
        backtrace: Backtrace,
        table_name: String,
    },

    #[snafu(display("Column {} already exists in table {}", column_name, table_name))]
    ColumnExists {
        backtrace: Backtrace,
        column_name: String,
        table_name: String,
    },

    #[snafu(display("Columns {} not exist in table {}", column_names.join(","), table_name))]
    ColumnsNotExist {
        backtrace: Backtrace,
        column_names: Vec<String>,
        table_name: String,
    },

    #[snafu(display("Failed to build schema, msg: {}, source: {}", msg, source))]
    SchemaBuild {
        #[snafu(backtrace)]
        source: datatypes::error::Error,
        msg: String,
    },

    #[snafu(display("Failed to alter table {}, source: {}", table_name, source))]
    AlterTable {
        table_name: String,
        #[snafu(backtrace)]
        source: table::error::Error,
    },

    #[snafu(display(
        "Projected columnd not found in region, column: {}",
        column_qualified_name
    ))]
    ProjectedColumnNotFound {
        backtrace: Backtrace,
        column_qualified_name: String,
    },

    #[snafu(display("Unsupported column default constraint, source: {}", source))]
    UnsupportedDefaultConstraint {
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },
}

impl From<Error> for table::error::Error {
    fn from(e: Error) -> Self {
        table::error::Error::new(e)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;

        match self {
            CreateRegion { source, .. } | OpenRegion { source, .. } => source.status_code(),

            AlterTable { source, .. } => source.status_code(),

            SchemaBuild { source, .. } => source.status_code(),

            BuildRowKeyDescriptor { .. }
            | BuildColumnDescriptor { .. }
            | BuildColumnFamilyDescriptor { .. }
            | BuildTableMeta { .. }
            | BuildTableInfo { .. }
            | BuildRegionDescriptor { .. }
            | TableExists { .. }
            | ProjectedColumnNotFound { .. }
            | MissingTimestampIndex { .. }
            | UnsupportedDefaultConstraint { .. }
            | TableNotFound { .. } => StatusCode::InvalidArguments,

            ColumnExists { .. } => StatusCode::TableColumnExists,

            ColumnsNotExist { .. } => StatusCode::TableColumnNotFound,

            TableInfoNotFound { .. } => StatusCode::Unexpected,

            ScanTableManifest { .. } | UpdateTableManifest { .. } => StatusCode::StorageUnavailable,
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
    use common_error::ext::BoxedError;
    use common_error::mock::MockError;

    use super::*;

    fn throw_create_table(code: StatusCode) -> Result<()> {
        let mock_err = MockError::with_backtrace(code);
        Err(BoxedError::new(mock_err)).context(CreateRegionSnafu)
    }

    #[test]
    fn test_error() {
        let err = throw_create_table(StatusCode::InvalidArguments)
            .err()
            .unwrap();
        assert_eq!(StatusCode::InvalidArguments, err.status_code());
        assert!(err.backtrace_opt().is_some());
    }

    #[test]
    pub fn test_opaque_error() {
        let error = throw_create_table(StatusCode::InvalidSyntax).err().unwrap();
        let table_engine_error: table::error::Error = error.into();
        assert!(table_engine_error.backtrace_opt().is_some());
        assert_eq!(StatusCode::InvalidSyntax, table_engine_error.status_code());
    }
}
