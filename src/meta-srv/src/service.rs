use std::pin::Pin;

use futures::Stream;
use tonic::Response;
use tonic::Status;

pub mod admin;
mod heartbeat;
pub mod route;
pub mod store;

pub const PROTOCOL_VERSION: u64 = 1;

pub type GrpcResult<T> = std::result::Result<Response<T>, Status>;
pub type GrpcStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + Sync + 'static>>;
