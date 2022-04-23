use datafusion::error::DataFusionError;
use snafu::Snafu;

/// business error of query engine
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Datafusion query engine error: {}", source), visibility(pub))]
    Datafusion { source: DataFusionError },
}

pub type Result<T> = std::result::Result<T, Error>;
