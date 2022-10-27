#![feature(assert_matches)]

pub mod error;
pub mod frontend;
pub mod influxdb;
pub mod instance;
pub(crate) mod mock;
pub mod mysql;
pub mod opentsdb;
mod partition;
pub mod postgres;
mod server;
mod spliter;
mod sql;
mod table;
#[cfg(test)]
mod tests;
