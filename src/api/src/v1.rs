#![allow(clippy::derive_partial_eq_without_eq)]
tonic::include_proto!("greptime.v1");

pub mod codec {
    tonic::include_proto!("greptime.v1.codec");
}

pub mod meta {
    tonic::include_proto!("greptime.v1.meta");
}
