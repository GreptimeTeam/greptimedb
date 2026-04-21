// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::pin::Pin;
use std::task::{Context, Poll};

use datatypes::arrow::array::new_null_array;
use datatypes::arrow::datatypes::SchemaRef;
use datatypes::arrow::record_batch::RecordBatch;
use futures::Stream;
use parquet::arrow::async_reader::ParquetRecordBatchStream;
use snafu::{ResultExt, ensure};

use crate::error::{NewRecordBatchSnafu, Result, UnexpectedSnafu};
use crate::sst::parquet::async_reader::SstAsyncFileReader;

pub struct MissingColFiller {
    inner: ParquetRecordBatchStream<SstAsyncFileReader>,
    output_schema: SchemaRef,
    projected_root_matches: Vec<bool>,
    all_matched: bool,
}

pub(crate) type ProjectedRecordBatchStream = MissingColFiller;

impl MissingColFiller {
    pub fn new(
        inner: ParquetRecordBatchStream<SstAsyncFileReader>,
        projected_root_matches: Vec<bool>,
        output_schema: SchemaRef,
    ) -> Result<MissingColFiller> {
        ensure!(
            projected_root_matches.len() == output_schema.fields().len(),
            UnexpectedSnafu {
                reason: format!(
                    "MissingColFiller projected root matches len {} does not match output schema columns {}",
                    projected_root_matches.len(),
                    output_schema.fields().len()
                ),
            }
        );

        let all_matched = projected_root_matches.iter().all(|&m| m);

        Ok(MissingColFiller {
            inner,
            output_schema,
            projected_root_matches,
            all_matched,
        })
    }
}

impl Stream for MissingColFiller {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(rb))) => {
                let output_schema = &this.output_schema;
                let rb = if this.all_matched {
                    fill_missing_cols(rb, output_schema, &this.projected_root_matches)?
                } else {
                    rb
                };
                Poll::Ready(Some(Ok(rb)))
            }
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(
                UnexpectedSnafu {
                    reason: format!("failed to poll missing col filler: {err}"),
                }
                .fail(),
            )),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

fn fill_missing_cols(
    rb: RecordBatch,
    output_schema: &SchemaRef,
    projected_root_matches: &[bool],
) -> Result<RecordBatch> {
    let expected_input_cols = projected_root_matches
        .iter()
        .filter(|matched| **matched)
        .count();

    ensure!(
        rb.columns().len() == expected_input_cols,
        UnexpectedSnafu {
            reason: format!(
                "MissingColFiller expected {} input columns but got {}",
                expected_input_cols,
                rb.columns().len()
            ),
        }
    );

    let mut cols = Vec::with_capacity(projected_root_matches.len());
    let mut idx = 0;

    for (field, matched) in output_schema.fields().iter().zip(projected_root_matches) {
        if *matched {
            cols.push(rb.column(idx).clone());
            idx += 1;
        } else {
            cols.push(new_null_array(field.data_type(), rb.num_rows()));
        }
    }

    RecordBatch::try_new(output_schema.clone(), cols).context(NewRecordBatchSnafu)
}
