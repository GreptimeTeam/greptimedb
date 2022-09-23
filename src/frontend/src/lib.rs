#![feature(assert_matches)]

pub mod error;
pub mod frontend;
pub mod instance;
pub(crate) mod opentsdb;
pub mod postgres;
mod server;
#[cfg(test)]
mod tests;
