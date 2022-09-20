#![feature(assert_matches)]

pub mod error;
pub mod grpc;
pub mod http;
pub mod mysql;
#[cfg(feature = "opentsdb")]
pub mod opentsdb;
#[cfg(feature = "postgres")]
pub mod postgres;
pub mod query_handler;
pub mod server;
mod shutdown;
