use std::any::Any;

use common_error::ext::BoxedError;
use common_error::prelude::*;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Fail to create region, source: {}", source))]
    CreateRegion {
        #[snafu(backtrace)]
        source: BoxedError,
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
        match self {
            Error::CreateRegion { source, .. } => source.status_code(),
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
