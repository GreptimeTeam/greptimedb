#![feature(assert_matches)]

pub mod error;
pub mod frontend;
pub mod instance;
#[cfg(feature = "opentsdb")]
pub(crate) mod opentsdb;
#[cfg(feature = "postgres")]
pub mod postgres;
mod server;
#[cfg(test)]
mod tests;
