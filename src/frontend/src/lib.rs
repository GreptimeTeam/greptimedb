#![feature(assert_matches)]

pub mod error;
pub mod frontend;
pub mod influxdb;
pub mod instance;
pub mod mysql;
pub mod opentsdb;
pub mod postgres;
mod server;
pub mod spliter;
pub mod partition;
#[cfg(test)]
mod tests;
