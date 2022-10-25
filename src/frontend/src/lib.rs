#![feature(assert_matches)]

pub mod error;
pub mod frontend;
pub mod influxdb;
pub mod instance;
pub(crate) mod mock;
pub mod mysql;
pub mod opentsdb;
pub mod postgres;
mod server;
mod table;
#[cfg(test)]
mod tests;
