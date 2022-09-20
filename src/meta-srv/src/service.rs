use std::pin::Pin;

use futures::Stream;
use tonic::{Response, Status};

use self::{admin::AdminRef, route::RouteRef, store::StoreRef};

mod admin;
mod heartbeat;
mod route;
mod store;

pub const PROTOCOL_VERSION: u64 = 1;

pub type GrpcResult<T> = std::result::Result<Response<T>, Status>;
pub type GrpcStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + Sync + 'static>>;

#[derive(Clone)]
struct MetaServer {
    _admin: AdminRef,
    _route: RouteRef,
    _store: StoreRef,
}
