#![allow(clippy::all)]
tonic::include_proto!("greptime.storage.wal.v1");

use crate::{
    bit_vec,
    write_batch::{Mutation, WriteBatch},
};

pub fn gen_mutation_extras(write_batch: &WriteBatch) -> Vec<MutationExtra> {
    let column_schemas = write_batch.schema().column_schemas();
    write_batch
        .iter()
        .map(|m| match m {
            Mutation::Put(put) => {
                if put.num_columns() == column_schemas.len() {
                    MutationExtra {
                        mutation_type: MutationType::Put.into(),
                        column_null_mask: Default::default(),
                    }
                } else {
                    let mut column_null_mask = bit_vec::BitVec::repeat(false, column_schemas.len());
                    for (i, cs) in column_schemas.iter().enumerate() {
                        if put.column_by_name(&cs.name).is_none() {
                            column_null_mask.set(i, true);
                        }
                    }
                    MutationExtra {
                        mutation_type: MutationType::Put.into(),
                        column_null_mask: column_null_mask.into_vec(),
                    }
                }
            }
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
