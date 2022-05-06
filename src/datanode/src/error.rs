use hyper::Error as HyperError;
use query::error::Error as QueryError;
use snafu::Snafu;

/// business error of datanode.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Query error: {}", source))]
    Query { source: QueryError },
    #[snafu(display("Http error: {}", source))]
    Hyper { source: HyperError },
}

pub type Result<T> = std::result::Result<T, Error>;
