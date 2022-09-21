use common_error::prelude::*;
use tonic::{Code, Status};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Error stream request next is None"))]
    StreamNone { backtrace: Backtrace },

    #[snafu(display("Empty key is not allowed"))]
    EmptyKey { backtrace: Backtrace },

    #[snafu(display("Failed to execute via Etcd, source: {}", source))]
    EtcdFailed { source: etcd_client::Error },

    #[snafu(display("Failed to connect to Etcd, source: {}", source))]
    ConnectEtcd { source: etcd_client::Error },
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<Error> for Status {
    fn from(err: Error) -> Self {
        Status::new(Code::Internal, err.to_string())
    }
}
