use common_error::prelude::*;
use tonic::{Code, Status};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Error stream request next is None"))]
    StreamNone { backtrace: Backtrace },
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<Error> for Status {
    fn from(err: Error) -> Self {
        Status::new(Code::Internal, err.to_string())
    }
}
