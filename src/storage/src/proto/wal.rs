#![allow(clippy::all)]
tonic::include_proto!("greptime.storage.wal.v1");

use crate::write_batch::{Mutation, WriteBatch};

pub fn gen_mutation_extras(write_batch: &WriteBatch) -> Vec<MutationExtra> {
    write_batch
        .iter()
        .map(|m| match m {
            Mutation::Put(_) => MutationExtra {
                mutation_type: MutationType::Put.into(),
            },
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
