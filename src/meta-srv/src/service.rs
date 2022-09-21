use std::pin::Pin;

use futures::Stream;
use tonic::{Response, Status};

use self::{route::RouteRef, store::kv::KvStoreRef};

mod admin;
mod heartbeat;
mod route;
pub mod store;

pub const PROTOCOL_VERSION: u64 = 1;

pub type GrpcResult<T> = std::result::Result<Response<T>, Status>;
pub type GrpcStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + Sync + 'static>>;

#[derive(Clone)]
pub struct MetaServer {
    kv_store: KvStoreRef,
    _route: RouteRef,
}
