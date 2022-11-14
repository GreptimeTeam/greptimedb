#![feature(btree_drain_filter)]
pub mod bootstrap;
pub mod error;
pub mod handler;
mod keys;
pub mod lease;
pub mod metasrv;
#[cfg(feature = "mock")]
pub mod mocks;
pub mod selector;
mod sequence;
pub mod service;
mod util;

pub use crate::error::Result;
