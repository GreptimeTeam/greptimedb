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

use datatypes::arrow::array::{Array, Float64Array};

#[derive(Debug, Clone)]
pub enum VisibleSeriesValues {
    Dense(Float64Array),
    Integer(ExactIntegerSeries),
}

/// Every reconstructed value has been checked against the original floating-point bits.
#[derive(Debug, Clone)]
pub struct ExactIntegerSeries {
    first: i64,
    step: i64,
    count: usize,
}

impl ExactIntegerSeries {
    fn detect(values: &Float64Array) -> Option<Self> {
        if values.len() < 4 || values.null_count() != 0 {
            return None;
        }
        let integer = |value: f64| {
            if !value.is_finite() || value.abs() > (1u64 << 53) as f64 {
                return None;
            }
            let integer = value as i64;
            ((integer as f64).to_bits() == value.to_bits()).then_some(integer)
        };
        let first = integer(values.value(0))?;
        let step = integer(values.value(1))? - first;
        let last = i128::from(first) + i128::from(step) * (values.len() - 1) as i128;
        if last.abs() > (1i128 << 53) {
            return None;
        }
        for (index, &value) in values.values().iter().enumerate().skip(2) {
            let expected = (i128::from(first) + i128::from(step) * index as i128) as i64;
            if (expected as f64).to_bits() != value.to_bits() {
                return None;
            }
        }
        Some(Self {
            first,
            step,
            count: values.len(),
        })
    }

    pub fn step(&self) -> i64 {
        self.step
    }

    pub fn value(&self, index: usize) -> f64 {
        assert!(index < self.count);
        // Detection bounds both endpoints by 2^53, so every in-bounds
        // affine offset fits i64 even when it crosses zero.
        (self.first + self.step * index as i64) as f64
    }
}

impl From<Float64Array> for VisibleSeriesValues {
    fn from(values: Float64Array) -> Self {
        Self::Dense(values)
    }
}

impl VisibleSeriesValues {
    /// Constructs a lossless affine representation whose every integer is exactly representable.
    pub fn from_exact_integer(first: i64, step: i64, count: usize) -> Option<Self> {
        let last = i128::from(first)
            .checked_add(i128::from(step).checked_mul(count.checked_sub(1)? as i128)?)?;
        if i128::from(first).abs() > (1i128 << 53) || last.abs() > (1i128 << 53) {
            return None;
        }
        Some(Self::Integer(ExactIntegerSeries { first, step, count }))
    }

    pub fn compact(values: Float64Array) -> Self {
        ExactIntegerSeries::detect(&values).map_or(Self::Dense(values), Self::Integer)
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Dense(values) => values.len(),
            Self::Integer(values) => values.count,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn null_count(&self) -> usize {
        match self {
            Self::Dense(values) => values.null_count(),
            Self::Integer(_) => 0,
        }
    }

    pub fn value(&self, index: usize) -> f64 {
        match self {
            Self::Dense(values) => values.value(index),
            Self::Integer(values) => values.value(index),
        }
    }

    pub fn iter_values(&self) -> impl Iterator<Item = f64> + '_ {
        (0..self.len()).map(|index| self.value(index))
    }

    pub fn slice(&self, offset: usize, count: usize) -> Self {
        assert!(offset <= self.len() && count <= self.len() - offset);
        match self {
            Self::Dense(values) => Self::Dense(values.slice(offset, count)),
            Self::Integer(values) => Self::Integer(ExactIntegerSeries {
                first: if count == 0 {
                    values.first
                } else {
                    (i128::from(values.first) + i128::from(values.step) * offset as i128) as i64
                },
                step: values.step,
                count,
            }),
        }
    }

    pub fn materialize(&self) -> Float64Array {
        match self {
            Self::Dense(values) => values.clone(),
            Self::Integer(_) => Float64Array::from_iter_values(self.iter_values()),
        }
    }

    pub fn retained_bytes(&self) -> usize {
        size_of::<Self>()
            + match self {
                Self::Dense(values) => values.get_buffer_memory_size(),
                Self::Integer(_) => 0,
            }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_integer_constructor_checks_endpoints_and_empty_sequences() {
        for (first, step, count) in [
            (0, 0, 0),
            (1i64 << 54, 0, 1),
            (1i64 << 53, 1, 2),
            (0, i64::MAX, usize::MAX),
        ] {
            assert!(VisibleSeriesValues::from_exact_integer(first, step, count).is_none());
        }
        for (first, step, count) in [
            (-(1i64 << 53), 1i64 << 51, 9),
            (1i64 << 53, -(1i64 << 51), 9),
            (7, 0, 1),
        ] {
            let series = VisibleSeriesValues::from_exact_integer(first, step, count).unwrap();
            for index in 0..count {
                let expected =
                    ((i128::from(first) + i128::from(step) * index as i128) as f64).to_bits();
                assert_eq!(series.value(index).to_bits(), expected);
            }
            assert_eq!(
                series.slice(count - 1, 1).value(0).to_bits(),
                series.value(count - 1).to_bits()
            );
        }
    }

    #[test]
    fn exact_integer_blocks_preserve_bits_and_slices() {
        for (first, step) in [
            (0, 10),
            (100, -3),
            (-100, 0),
            ((1i64 << 53) - 63, 1),
            (-(1i64 << 53), 1),
            (-(1i64 << 53), 1i64 << 48),
            ((1i64 << 53) - (1i64 << 48), -(1i64 << 48)),
        ] {
            let original: Vec<f64> = (0..64).map(|i| (first + step * i) as f64).collect();
            let block = VisibleSeriesValues::compact(Float64Array::from(original.clone()));
            assert!(matches!(block, VisibleSeriesValues::Integer(_)));
            for (offset, count) in [(0, 64), (7, 31), (63, 1), (64, 0)] {
                let slice = block.slice(offset, count);
                assert_eq!(
                    slice.iter_values().map(f64::to_bits).collect::<Vec<_>>(),
                    original[offset..offset + count]
                        .iter()
                        .map(|v| v.to_bits())
                        .collect::<Vec<_>>()
                );
            }
            assert!(block.retained_bytes() < original.len() * 8);
        }
    }

    #[test]
    fn compact_values_retain_nulls_and_nonaffine_bits() {
        let cases = [
            vec![0.0, 1.0, 2.0, -0.0],
            vec![-0.0, 0.0, 0.0, 0.0],
            vec![0.5, 1.5, 2.5, 3.5],
            vec![0.0, 1.0, 2.0, f64::from_bits(0x7ff0000000000002)],
            vec![0.0, 1.0, f64::NAN, 3.0],
            vec![f64::INFINITY; 4],
            vec![(1u64 << 54) as f64; 4],
            vec![1.0, 2.0, 3.0, 5.0],
        ];
        for values in cases {
            let block = VisibleSeriesValues::compact(Float64Array::from(values.clone()));
            assert!(matches!(block, VisibleSeriesValues::Dense(_)));
            assert_eq!(
                block.iter_values().map(f64::to_bits).collect::<Vec<_>>(),
                values.iter().map(|v| v.to_bits()).collect::<Vec<_>>()
            );
        }
        let block = VisibleSeriesValues::compact(Float64Array::from(vec![
            Some(0.0),
            Some(1.0),
            None,
            Some(3.0),
        ]));
        assert_eq!(block.null_count(), 1);
        assert!(block.materialize().is_null(2));
    }
}
