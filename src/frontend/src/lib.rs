#![feature(assert_matches)]

pub mod error;
pub mod frontend;
pub mod grpc;
pub mod influxdb;
pub mod instance;
pub mod mysql;
pub mod opentsdb;
pub mod partition;
pub mod postgres;
pub mod prometheus;
mod server;
pub mod spliter;
#[cfg(test)]
mod tests;
