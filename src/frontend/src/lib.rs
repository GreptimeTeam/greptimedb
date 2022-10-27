#![feature(assert_matches)]

pub mod error;
pub mod frontend;
pub mod influxdb;
pub mod instance;
pub mod mysql;
pub mod opentsdb;
pub mod postgres;
pub mod prometheus;
mod server;
#[cfg(test)]
mod tests;
