use datafusion::error::DataFusionError;
use snafu::Snafu;

/// business error of query engine
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Datafusion query engine error: {}", source))]
    Datafusion { source: DataFusionError },
    #[snafu(display("PhysicalPlan downcast_ref failed"))]
    PhysicalPlanDowncast,
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<Error> for DataFusionError {
    fn from(e: Error) -> DataFusionError {
        DataFusionError::External(Box::new(e))
    }
}
