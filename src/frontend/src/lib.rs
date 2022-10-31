#![feature(assert_matches)]

mod catalog;
pub mod error;
pub mod frontend;
pub mod influxdb;
pub mod instance;
pub(crate) mod mock;
pub mod mysql;
pub mod opentsdb;
pub mod partition;
pub mod postgres;
pub mod prometheus;
mod server;
pub mod spliter;
mod sql;
mod table;
#[cfg(test)]
mod tests;
