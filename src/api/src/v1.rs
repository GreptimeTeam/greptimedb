#![allow(clippy::all)]
tonic::include_proto!("greptime.v1");

pub mod codec {
    #![allow(clippy::all)]
    tonic::include_proto!("greptime.v1.codec");
}
