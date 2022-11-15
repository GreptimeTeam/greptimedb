use std::pin::Pin;

use futures::Stream;
use tonic::{Response, Status};

pub mod admin;
mod heartbeat;
pub mod router;
pub mod store;

pub type GrpcResult<T> = std::result::Result<Response<T>, Status>;
pub type GrpcStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + Sync + 'static>>;
