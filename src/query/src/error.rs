use datafusion::error::DataFusionError;

common_error::define_opaque_error!(Error);

pub type Result<T> = std::result::Result<T, Error>;

impl From<Error> for DataFusionError {
    fn from(e: Error) -> DataFusionError {
        DataFusionError::External(Box::new(e))
    }
}

impl From<catalog::error::Error> for Error {
    fn from(e: catalog::error::Error) -> Self {
        Error::new(e)
    }
}
