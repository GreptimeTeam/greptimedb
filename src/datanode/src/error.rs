use snafu::Snafu;

/// business error of datanode.
#[derive(Debug, Snafu)]
#[snafu(display("DataNode error"))]
pub struct Error;

pub type Result<T> = std::result::Result<T, Error>;
