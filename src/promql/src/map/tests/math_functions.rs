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

use super::*;

#[test]
fn clamp_follows_go_min_max_and_empties_reversed_bounds() {
    let schema = schema(&["value"]);
    let input = series(
        &schema,
        7,
        vec![floats(&[
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            Some(-0.0),
            Some(0.0),
            Some(f64::NAN),
            None,
        ])],
    );
    let output = |op| {
        map_math(&schema, &input, op)
            .unwrap()
            .map(|series| series.fields.get(0).unwrap().clone())
    };
    // Go's Max lets +Inf win over NaN and Min lets -Inf win; +0/-0 are ordered.
    let infinite = output(MathOp::ClampMin { min: f64::NAN }).unwrap();
    assert_floats(
        &infinite,
        &[
            Some(f64::INFINITY),
            Some(f64::NAN),
            Some(f64::NAN),
            Some(f64::NAN),
            Some(f64::NAN),
            None,
        ],
    );
    let infinite = output(MathOp::ClampMax { max: f64::NAN }).unwrap();
    assert_floats(
        &infinite,
        &[
            Some(f64::NAN),
            Some(f64::NEG_INFINITY),
            Some(f64::NAN),
            Some(f64::NAN),
            Some(f64::NAN),
            None,
        ],
    );
    let zero = output(MathOp::Clamp { min: 0.0, max: 0.0 }).unwrap();
    assert_floats(
        &zero,
        &[
            Some(0.0),
            Some(0.0),
            Some(0.0),
            Some(0.0),
            Some(f64::NAN),
            None,
        ],
    );
    let zero = output(MathOp::Clamp {
        min: -0.0,
        max: -0.0,
    })
    .unwrap();
    assert_floats(
        &zero,
        &[
            Some(-0.0),
            Some(-0.0),
            Some(-0.0),
            Some(-0.0),
            Some(f64::NAN),
            None,
        ],
    );
    // Either zero can win in Go's Max; the sign shows through a later `1 / x`.
    let zero = output(MathOp::ClampMin { min: -0.0 }).unwrap();
    assert_floats(
        &zero,
        &[
            Some(f64::INFINITY),
            Some(-0.0),
            Some(-0.0),
            Some(0.0),
            Some(f64::NAN),
            None,
        ],
    );
    assert!(output(MathOp::Clamp { min: 2.0, max: 1.0 }).is_none());
    assert!(
        output(MathOp::Clamp {
            min: f64::NAN,
            max: 1.0
        })
        .is_some()
    );
}

#[test]
fn sgn_deg_and_rad_keep_prometheus_operation_order() {
    let schema = schema(&["value"]);
    let input = series(
        &schema,
        0,
        vec![floats(&[Some(-0.0), Some(f64::NAN), Some(11.0), Some(3.0)])],
    );
    let field = |op| {
        map_math(&schema, &input, op)
            .unwrap()
            .unwrap()
            .fields
            .get(0)
            .unwrap()
            .clone()
    };
    assert_floats(
        &field(MathOp::Sgn),
        &[Some(-0.0), Some(f64::NAN), Some(1.0), Some(1.0)],
    );
    // Bits from Go on amd64; 11 and 3 round differently under to_degrees/to_radians.
    assert_floats(
        &field(MathOp::Deg),
        &[
            Some(-0.0),
            Some(f64::NAN),
            Some(f64::from_bits(0x4083b2075224955b)),
            Some(f64::from_bits(0x40657c6513cad17a)),
        ],
    );
    assert_ne!(11.0 * 180.0 / PI, 11.0_f64.to_degrees());
    assert_floats(
        &field(MathOp::Rad),
        &[
            Some(-0.0),
            Some(f64::NAN),
            Some(f64::from_bits(0x3fc893011f31982e)),
            Some(f64::from_bits(0x3faacee9f37bebd5)),
        ],
    );
    assert_ne!(3.0 * PI / 180.0, 3.0_f64.to_radians());
}

#[test]
fn pi_produces_an_unlabelled_scalar_only_on_the_requested_range() {
    let schema = schema(&["value"]);
    for len in [0, 1, 7] {
        let range = GridRange::try_new(schema.eval_grid.clone().unwrap(), 13, len).unwrap();
        let output = map_pi(&schema, range.clone()).unwrap();
        assert!(matches!(&output.time, TimeAxis::Grid(actual) if actual == &range));
        assert!(output.metric.name().is_none());
        assert!(output.metric.tags().is_empty());
        assert_floats(output.fields.get(0).unwrap(), &vec![Some(PI); len]);
    }
    let invalid = super::schema(&["a", "b"]);
    let range = GridRange::try_new(invalid.eval_grid.clone().unwrap(), 0, 1).unwrap();
    assert!(map_pi(&invalid, range).is_err());
}
