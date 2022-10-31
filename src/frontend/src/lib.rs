#![feature(assert_matches)]

pub mod error;
pub mod frontend;
pub mod influxdb;
pub mod instance;
pub mod mysql;
pub mod opentsdb;
pub mod partitioning;
pub mod postgres;
pub mod prometheus;
mod server;
pub mod spliter;
#[cfg(test)]
mod tests;
