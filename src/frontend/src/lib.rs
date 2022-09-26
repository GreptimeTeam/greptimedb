#![feature(assert_matches)]

pub mod error;
pub mod frontend;
pub mod instance;
pub mod mysql;
pub mod opentsdb;
pub mod postgres;
mod server;
#[cfg(test)]
mod tests;
