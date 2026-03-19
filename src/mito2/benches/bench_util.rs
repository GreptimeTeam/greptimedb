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

//! Shared utilities for mito2 benchmarks.
//!
//! Provides a TSBS cpu-like data generator ([`CpuDataGenerator`]) and schema
//! ([`cpu_metadata`]) used by multiple benchmark binaries in this directory.

#![allow(dead_code)]

use api::v1::value::ValueData;
use api::v1::{Row, Rows, SemanticType};
use datafusion_common::Column;
use datafusion_expr::{Expr, lit};
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use mito2::memtable::KeyValues;
use mito2::test_util::memtable_util::region_metadata_to_row_schema;
use rand::Rng;
use rand::rngs::ThreadRng;
use rand::seq::IndexedRandom;
use store_api::metadata::{
    ColumnMetadata, RegionMetadata, RegionMetadataBuilder, RegionMetadataRef,
};
use store_api::storage::RegionId;
use table::predicate::Predicate;

pub struct Host {
    pub hostname: String,
    pub region: String,
    pub datacenter: String,
    pub rack: String,
    pub os: String,
    pub arch: String,
    pub team: String,
    pub service: String,
    pub service_version: String,
    pub service_environment: String,
}

impl Host {
    pub fn random_with_id(id: usize) -> Host {
        let mut rng = rand::rng();
        let region = format!("ap-southeast-{}", rng.random_range(0..10));
        let datacenter = format!(
            "{}{}",
            region,
            ['a', 'b', 'c', 'd', 'e'].choose(&mut rng).unwrap()
        );
        Host {
            hostname: format!("host_{id}"),
            region,
            datacenter,
            rack: rng.random_range(0..100).to_string(),
            os: "Ubuntu16.04LTS".to_string(),
            arch: "x86".to_string(),
            team: "CHI".to_string(),
            service: rng.random_range(0..100).to_string(),
            service_version: rng.random_range(0..10).to_string(),
            service_environment: "test".to_string(),
        }
    }

    pub fn fill_values(&self, values: &mut Vec<api::v1::Value>) {
        let tags = [
            api::v1::Value {
                value_data: Some(ValueData::StringValue(self.hostname.clone())),
            },
            api::v1::Value {
                value_data: Some(ValueData::StringValue(self.region.clone())),
            },
            api::v1::Value {
                value_data: Some(ValueData::StringValue(self.datacenter.clone())),
            },
            api::v1::Value {
                value_data: Some(ValueData::StringValue(self.rack.clone())),
            },
            api::v1::Value {
                value_data: Some(ValueData::StringValue(self.os.clone())),
            },
            api::v1::Value {
                value_data: Some(ValueData::StringValue(self.arch.clone())),
            },
            api::v1::Value {
                value_data: Some(ValueData::StringValue(self.team.clone())),
            },
            api::v1::Value {
                value_data: Some(ValueData::StringValue(self.service.clone())),
            },
            api::v1::Value {
                value_data: Some(ValueData::StringValue(self.service_version.clone())),
            },
            api::v1::Value {
                value_data: Some(ValueData::StringValue(self.service_environment.clone())),
            },
        ];
        for tag in tags {
            values.push(tag);
        }
    }
}

pub struct CpuDataGenerator {
    pub metadata: RegionMetadataRef,
    column_schemas: Vec<api::v1::ColumnSchema>,
    hosts: Vec<Host>,
    start_sec: i64,
    end_sec: i64,
}

impl CpuDataGenerator {
    pub fn new(
        metadata: RegionMetadataRef,
        num_hosts: usize,
        start_sec: i64,
        end_sec: i64,
    ) -> Self {
        let column_schemas = region_metadata_to_row_schema(&metadata);
        Self {
            metadata,
            column_schemas,
            hosts: Self::generate_hosts(num_hosts),
            start_sec,
            end_sec,
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = KeyValues> + '_ {
        // point per 10s.
        (self.start_sec..self.end_sec)
            .step_by(10)
            .enumerate()
            .map(|(seq, ts)| self.build_key_values(seq, ts))
    }

    pub fn build_key_values(&self, seq: usize, current_sec: i64) -> KeyValues {
        let rows = self
            .hosts
            .iter()
            .map(|host| {
                let mut rng = rand::rng();
                let mut values = Vec::with_capacity(21);
                values.push(api::v1::Value {
                    value_data: Some(ValueData::TimestampMillisecondValue(current_sec * 1000)),
                });
                host.fill_values(&mut values);
                for _ in 0..10 {
                    values.push(api::v1::Value {
                        value_data: Some(ValueData::F64Value(Self::random_f64(&mut rng))),
                    });
                }
                Row { values }
            })
            .collect();
        let mutation = api::v1::Mutation {
            op_type: api::v1::OpType::Put as i32,
            sequence: seq as u64,
            rows: Some(Rows {
                schema: self.column_schemas.clone(),
                rows,
            }),
            write_hint: None,
        };

        KeyValues::new(&self.metadata, mutation).unwrap()
    }

    pub fn random_host_filter(&self) -> Predicate {
        let host = self.random_hostname();
        let expr = Expr::Column(Column::from_name("hostname")).eq(lit(host));
        Predicate::new(vec![expr])
    }

    pub fn random_host_filter_exprs(&self) -> Vec<Expr> {
        let host = self.random_hostname();
        vec![Expr::Column(Column::from_name("hostname")).eq(lit(host))]
    }

    pub fn random_hostname(&self) -> String {
        let mut rng = rand::rng();
        self.hosts.choose(&mut rng).unwrap().hostname.clone()
    }

    pub fn random_f64(rng: &mut ThreadRng) -> f64 {
        let base: u32 = rng.random_range(30..95);
        base as f64
    }

    pub fn generate_hosts(num_hosts: usize) -> Vec<Host> {
        (0..num_hosts).map(Host::random_with_id).collect()
    }
}

/// Creates a metadata for TSBS cpu-like table.
pub fn cpu_metadata() -> RegionMetadata {
    let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
    builder.push_column_metadata(ColumnMetadata {
        column_schema: ColumnSchema::new(
            "ts",
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        ),
        semantic_type: SemanticType::Timestamp,
        column_id: 0,
    });
    let mut column_id = 1;
    let tags = [
        "hostname",
        "region",
        "datacenter",
        "rack",
        "os",
        "arch",
        "team",
        "service",
        "service_version",
        "service_environment",
    ];
    for tag in tags {
        builder.push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(tag, ConcreteDataType::string_datatype(), true),
            semantic_type: SemanticType::Tag,
            column_id,
        });
        column_id += 1;
    }
    let fields = [
        "usage_user",
        "usage_system",
        "usage_idle",
        "usage_nice",
        "usage_iowait",
        "usage_irq",
        "usage_softirq",
        "usage_steal",
        "usage_guest",
        "usage_guest_nice",
    ];
    for field in fields {
        builder.push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(field, ConcreteDataType::float64_datatype(), true),
            semantic_type: SemanticType::Field,
            column_id,
        });
        column_id += 1;
    }
    builder.primary_key(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    builder.build().unwrap()
}
