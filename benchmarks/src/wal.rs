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

use std::mem::size_of;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use api::v1::value::ValueData;
use api::v1::{ColumnDataType, ColumnSchema, Mutation, OpType, Row, Rows, Value, WalEntry};
use common_wal::options::WalOptions;
use futures::StreamExt;
use mito2::wal::{Wal, WalWriter};
use rand::distributions::{Alphanumeric, DistString, Uniform};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use store_api::logstore::LogStore;
use store_api::storage::RegionId;

use crate::metrics;

/// The region used for wal benchmarker.
pub struct Region {
    id: RegionId,
    schema: Vec<ColumnSchema>,
    wal_options: WalOptions,
    next_sequence: AtomicU64,
    next_entry_id: AtomicU64,
    next_timestamp: AtomicI64,
    rng: Mutex<Option<SmallRng>>,
    rows_factor: usize,
}

impl Region {
    /// Creates a new region.
    pub fn new(
        id: RegionId,
        schema: Vec<ColumnSchema>,
        wal_options: WalOptions,
        rows_factor: usize,
        rng_seed: u64,
    ) -> Self {
        Self {
            id,
            schema,
            wal_options,
            next_sequence: AtomicU64::new(1),
            next_entry_id: AtomicU64::new(1),
            next_timestamp: AtomicI64::new(1655276557000),
            rng: Mutex::new(Some(SmallRng::seed_from_u64(rng_seed))),
            rows_factor,
        }
    }

    /// Scrapes the region and adds the generated entry to wal.
    pub fn add_wal_entry<S: LogStore>(&self, wal_writer: &mut WalWriter<S>) {
        let num_rows = self.rows_factor * 5;
        let mutation = Mutation {
            op_type: OpType::Put as i32,
            sequence: self
                .next_sequence
                .fetch_add(num_rows as u64, Ordering::Relaxed),
            rows: Some(self.build_rows(num_rows)),
        };
        let entry = WalEntry {
            mutations: vec![mutation],
        };
        metrics::METRIC_WAL_WRITE_BYTES_TOTAL.inc_by(Self::entry_estimated_size(&entry) as u64);

        wal_writer
            .add_entry(
                self.id,
                self.next_entry_id.fetch_add(1, Ordering::Relaxed),
                &entry,
                &self.wal_options,
            )
            .unwrap();
    }

    /// Replays the region.
    pub async fn replay<S: LogStore>(&self, wal: &Arc<Wal<S>>) {
        let mut wal_stream = wal.scan(self.id, 0, &self.wal_options).unwrap();
        while let Some(res) = wal_stream.next().await {
            let (_, entry) = res.unwrap();
            metrics::METRIC_WAL_READ_BYTES_TOTAL.inc_by(Self::entry_estimated_size(&entry) as u64);
        }
    }

    /// Computes the estimated size in bytes of the entry.
    pub fn entry_estimated_size(entry: &WalEntry) -> usize {
        let wrapper_size = size_of::<WalEntry>()
            + entry.mutations.capacity() * size_of::<Mutation>()
            + size_of::<Rows>();

        let rows = entry.mutations[0].rows.as_ref().unwrap();

        let schema_size = rows.schema.capacity() * size_of::<ColumnSchema>()
            + rows
                .schema
                .iter()
                .map(|s| s.column_name.capacity())
                .sum::<usize>();
        let values_size = (rows.rows.capacity() * size_of::<Row>())
            + rows
                .rows
                .iter()
                .map(|r| r.values.capacity() * size_of::<Value>())
                .sum::<usize>();

        wrapper_size + schema_size + values_size
    }

    fn build_rows(&self, num_rows: usize) -> Rows {
        let cols = self
            .schema
            .iter()
            .map(|col_schema| {
                let col_data_type = ColumnDataType::try_from(col_schema.datatype).unwrap();
                self.build_col(&col_data_type, num_rows)
            })
            .collect::<Vec<_>>();

        let rows = (0..num_rows)
            .map(|i| {
                let values = cols.iter().map(|col| col[i].clone()).collect();
                Row { values }
            })
            .collect();

        Rows {
            schema: self.schema.clone(),
            rows,
        }
    }

    fn build_col(&self, col_data_type: &ColumnDataType, num_rows: usize) -> Vec<Value> {
        let mut rng_guard = self.rng.lock().unwrap();
        let rng = rng_guard.as_mut().unwrap();
        match col_data_type {
            ColumnDataType::TimestampMillisecond => (0..num_rows)
                .map(|_| {
                    let ts = self.next_timestamp.fetch_add(1000, Ordering::Relaxed);
                    Value {
                        value_data: Some(ValueData::TimestampMillisecondValue(ts)),
                    }
                })
                .collect(),
            ColumnDataType::Int32 => (0..num_rows)
                .map(|_| {
                    let v = rng.sample(Uniform::new(0, 10_000));
                    Value {
                        value_data: Some(ValueData::I32Value(v)),
                    }
                })
                .collect(),
            ColumnDataType::Float32 => (0..num_rows)
                .map(|_| {
                    let v = rng.sample(Uniform::new(0.0, 5000.0));
                    Value {
                        value_data: Some(ValueData::F32Value(v)),
                    }
                })
                .collect(),
            ColumnDataType::String => (0..num_rows)
                .map(|_| {
                    let v = Alphanumeric.sample_string(rng, 10);
                    Value {
                        value_data: Some(ValueData::StringValue(v)),
                    }
                })
                .collect(),
            _ => unreachable!(),
        }
    }
}
