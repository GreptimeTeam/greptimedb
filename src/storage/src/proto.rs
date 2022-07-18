#![allow(clippy::all)]

tonic::include_proto!("greptime.storage.wal.v1");

use crate::proto::wal_header::{mutation_extra::MutationType, MutationExtra};
use crate::write_batch::{Mutation, WriteBatch};

pub fn gen_mutation_extras(write_batch: &WriteBatch) -> Vec<MutationExtra> {
    let column_schemas = write_batch.schema().column_schemas();
    write_batch
        .iter()
        .map(|m| match m {
            Mutation::Put(put) => {
                if put.num_columns() == column_schemas.len() {
                    MutationExtra {
                        mutation_type: MutationType::Put as i32,
                        null_mask: Default::default(),
                    }
                } else {
                    let mut null_mask = bit_vec::BitVec::from_elem(column_schemas.len(), false);
                    for (i, cs) in column_schemas.iter().enumerate() {
                        if put.column_by_name(&cs.name).is_none() {
                            null_mask.set(i, true);
                        }
                    }
                    MutationExtra {
                        mutation_type: MutationType::Put as i32,
                        null_mask: null_mask.to_bytes(),
                    }
                }
            }
        })
        .collect::<Vec<_>>()
}
