use datafusion::error::DataFusionError;
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Datafusion error: {}", source))]
    Datafusion { source: DataFusionError },
    #[snafu(display("Not expected to run ExecutionPlan more than once."))]
    ExecuteRepeatedly,
}
pub type Result<T> = std::result::Result<T, Error>;

impl Into<DataFusionError> for Error {
    fn into(self) -> DataFusionError {
        DataFusionError::External(Box::new(self))
    }
}
