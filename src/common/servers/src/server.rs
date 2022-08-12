use std::net::SocketAddr;

use async_trait::async_trait;

use crate::error::Result;

#[async_trait]
pub trait Server: Send {
    async fn shutdown(&mut self) -> Result<()>;
    async fn start(&mut self, listening: SocketAddr) -> Result<SocketAddr>;
}
