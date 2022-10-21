#![allow(clippy::all)]
tonic::include_proto!("greptime.storage.wal.v1");

use crate::write_batch::{Mutation, WriteBatch};

pub fn gen_mutation_types(write_batch: &WriteBatch) -> Vec<i32> {
    write_batch
        .iter()
        .map(|m| match m {
            Mutation::Put(_) => MutationType::Put.into(),
        })
        .collect::<Vec<_>>()
}

impl WalHeader {
    pub fn with_last_manifest_version(last_manifest_version: u64) -> Self {
        Self {
            last_manifest_version,
            ..Default::default()
        }
    }
}
