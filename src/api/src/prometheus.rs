#![allow(clippy::derive_partial_eq_without_eq)]

pub mod remote {
    tonic::include_proto!("prometheus");
}
