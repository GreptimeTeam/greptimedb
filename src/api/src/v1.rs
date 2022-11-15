#![allow(clippy::derive_partial_eq_without_eq)]
tonic::include_proto!("greptime.v1");

pub const GREPTIME_FD_SET: &[u8] = tonic::include_file_descriptor_set!("greptime_fd");

pub mod codec {
    tonic::include_proto!("greptime.v1.codec");
}

pub mod meta;
