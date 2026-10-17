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

use std::sync::Arc;

use datatypes::series::samples::{FieldSpec, SeriesSchema};
use datatypes::series::time::{EvalGrid, GridSpec};
use datatypes::series::{Label, MetricName};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use super::GridSeriesBuilder;
use super::reference::{assert_series_eq, random_dense};

fn grid_schema(fields: usize, grid_len: u64) -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: (0..fields)
            .map(|index| FieldSpec {
                name: Arc::from(format!("f{index}")),
            })
            .collect::<Vec<_>>()
            .into(),
        eval_grid: Some(Arc::new(
            EvalGrid::try_new(GridSpec::try_new(-5_000, 1_000, grid_len).unwrap()).unwrap(),
        )),
    })
}

fn metric(host: usize) -> MetricName {
    MetricName::try_new(
        Some(Arc::from("cpu")),
        vec![Label {
            name: Arc::from("host"),
            value: Arc::from(format!("h{host}")),
        }],
    )
    .unwrap()
}

#[test]
fn builder_matches_dense_reference() {
    for seed in 0..200 {
        let mut rng = StdRng::seed_from_u64(seed);
        let fields = rng.random_range(1..=3);
        let grid_len = rng.random_range(1..64);
        let mut builder = GridSeriesBuilder::try_new(grid_schema(fields, grid_len)).unwrap();
        // One builder serves several series, as it does inside an operator.
        for host in 0..3 {
            let expected = random_dense(&mut rng, grid_len as usize, fields);
            for (index, row) in expected.iter().enumerate() {
                // Missing rows may be pushed as all-None or skipped entirely.
                if row.iter().any(Option::is_some) || rng.random_bool(0.5) {
                    builder.push(index, row).unwrap();
                }
            }
            let actual = builder.finish(metric(host)).unwrap();
            assert_series_eq(actual.as_ref(), &expected, &metric(host));
        }
    }
}

#[test]
fn builder_rejects_invalid_rows() {
    let mut builder = GridSeriesBuilder::try_new(grid_schema(2, 4)).unwrap();
    builder.push(1, &[Some(1.0), None]).unwrap();
    assert!(builder.push(1, &[Some(2.0), None]).is_err());
    assert!(builder.push(0, &[Some(2.0), None]).is_err());
    assert!(builder.push(4, &[Some(2.0), None]).is_err());
    assert!(builder.push(2, &[Some(2.0)]).is_err());

    let mut schema = grid_schema(1, 4).as_ref().clone();
    schema.eval_grid = None;
    assert!(GridSeriesBuilder::try_new(Arc::new(schema)).is_err());
}
