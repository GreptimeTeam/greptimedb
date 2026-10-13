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

const STALE: u64 = 0x7ff0_0000_0000_0002;

#[derive(Clone, Copy)]
pub(crate) enum Mode {
    Instant,
    DisjointMax,
}

/// Produces evaluation points from validated samples across arbitrary block boundaries.
pub(crate) struct BlockEvaluator<'a> {
    instants: &'a [i64],
    window: i64,
    offset: i64,
    mode: Mode,
    next: usize,
    latest: Option<(i128, f64)>,
    maximum: Option<f64>,
    output: Vec<Option<f64>>,
}

impl<'a> BlockEvaluator<'a> {
    pub(crate) fn new(
        instants: &'a [i64],
        window: i64,
        offset: i64,
        mode: Mode,
    ) -> Result<Self, &'static str> {
        if instants.is_empty() || window < 0 || instants.windows(2).any(|p| p[0] >= p[1]) {
            return Err("Invalid block evaluation grid");
        }
        if matches!(mode, Mode::DisjointMax)
            && (window == 0
                || instants
                    .windows(2)
                    .any(|p| i128::from(p[1]) - i128::from(p[0]) < i128::from(window)))
        {
            return Err("Max block evaluation requires disjoint windows");
        }
        Ok(Self {
            instants,
            window,
            offset,
            mode,
            next: 0,
            latest: None,
            maximum: None,
            output: Vec::with_capacity(instants.len()),
        })
    }

    pub(crate) fn push(&mut self, timestamp: i64, value: f64) -> Result<(), &'static str> {
        let shifted = match self.mode {
            Mode::Instant => i128::from(timestamp) + i128::from(self.offset),
            Mode::DisjointMax => {
                // The existing range adapter removes stale values before checking offset overflow.
                if value.to_bits() == STALE {
                    return Ok(());
                }
                i128::from(
                    timestamp
                        .checked_add(self.offset)
                        .ok_or("Range UDF timestamp shift overflow")?,
                )
            }
        };
        while self.next < self.instants.len() && shifted > i128::from(self.instants[self.next]) {
            self.emit();
        }
        if self.next == self.instants.len() {
            return Ok(());
        }
        match self.mode {
            Mode::Instant => self.latest = Some((shifted, value)),
            Mode::DisjointMax => {
                if shifted > i128::from(self.instants[self.next]) - i128::from(self.window)
                    && self
                        .maximum
                        .is_none_or(|current| current.is_nan() || value > current)
                {
                    self.maximum = Some(value);
                }
            }
        }
        Ok(())
    }

    fn emit(&mut self) {
        let end = i128::from(self.instants[self.next]);
        let value = match self.mode {
            Mode::Instant => self.latest.and_then(|(timestamp, value)| {
                ((timestamp == end || timestamp > end - i128::from(self.window))
                    && value.to_bits() != STALE)
                    .then_some(value)
            }),
            Mode::DisjointMax => self.maximum.take(),
        };
        self.output.push(value);
        self.next += 1;
    }

    pub(crate) fn finish(mut self) -> Vec<Option<f64>> {
        while self.next < self.instants.len() {
            self.emit();
        }
        self.output
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bits(values: Vec<Option<f64>>) -> Vec<Option<u64>> {
        values
            .into_iter()
            .map(|value| value.map(f64::to_bits))
            .collect()
    }

    fn oracle(
        samples: &[(i64, f64)],
        instants: &[i64],
        window: i64,
        offset: i64,
        mode: Mode,
    ) -> Vec<Option<f64>> {
        instants
            .iter()
            .map(|&end| {
                let end = i128::from(end) - i128::from(offset);
                match mode {
                    Mode::Instant => samples
                        .iter()
                        .rev()
                        .find(|(t, _)| i128::from(*t) <= end)
                        .and_then(|&(t, value)| {
                            ((i128::from(t) == end || i128::from(t) > end - i128::from(window))
                                && value.to_bits() != STALE)
                                .then_some(value)
                        }),
                    Mode::DisjointMax => samples
                        .iter()
                        .filter(|(t, value)| {
                            i128::from(*t) > end - i128::from(window)
                                && i128::from(*t) <= end
                                && value.to_bits() != STALE
                        })
                        .fold(None, |maximum, &(_, value)| match maximum {
                            None => Some(value),
                            Some(current) if current.is_nan() || value > current => Some(value),
                            _ => maximum,
                        }),
                }
            })
            .collect()
    }

    #[test]
    fn irregular_blocks_match_independent_window_scans() {
        let special = [
            0,
            1 << 63,
            STALE,
            0x7ff8000000000001,
            0x7ff8000000000007,
            f64::INFINITY.to_bits(),
            f64::NEG_INFINITY.to_bits(),
            3.0f64.to_bits(),
        ];
        let mut seed = 17u64;
        let mut random = || {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            seed >> 32
        };
        for _ in 0..100 {
            let mut t = -300;
            let samples: Vec<_> = (0..80)
                .map(|_| {
                    t += 1 + (random() % 17) as i64;
                    (
                        t,
                        f64::from_bits(special[random() as usize % special.len()]),
                    )
                })
                .collect();
            let step = 1 + (random() % 53) as i64;
            let window = 1 + (random() % step as u64) as i64;
            let offset = (random() % 41) as i64 - 20;
            let instants: Vec<_> = (0..50).map(|i| -400 + i * step).collect();
            for mode in [Mode::Instant, Mode::DisjointMax] {
                let expected = bits(oracle(&samples, &instants, window, offset, mode));
                for size in [1, 7, 32, 80] {
                    let mut evaluator =
                        BlockEvaluator::new(&instants, window, offset, mode).unwrap();
                    for block in samples.chunks(size) {
                        for &(t, value) in block {
                            evaluator.push(t, value).unwrap();
                        }
                    }
                    assert_eq!(bits(evaluator.finish()), expected);
                }
            }
        }
    }

    #[test]
    fn stale_instant_does_not_resurrect_previous_value() {
        let mut evaluator = BlockEvaluator::new(&[10, 20, 30], 20, 0, Mode::Instant).unwrap();
        evaluator.push(5, 4.0).unwrap();
        evaluator.push(10, f64::from_bits(STALE)).unwrap();
        evaluator.push(25, -0.0).unwrap();
        assert_eq!(
            bits(evaluator.finish()),
            [None, None, Some((-0.0f64).to_bits())]
        );
    }

    #[test]
    fn endpoints_and_nan_bits_are_preserved() {
        let samples = [
            (0, 99.0),
            (1, -0.0),
            (10, 0.0),
            (11, f64::from_bits(0x7ff8000000000001)),
            (20, f64::from_bits(0x7ff8000000000007)),
        ];
        let mut evaluator = BlockEvaluator::new(&[10, 20, 30], 10, 0, Mode::DisjointMax).unwrap();
        for (t, value) in samples {
            evaluator.push(t, value).unwrap();
        }
        assert_eq!(
            bits(evaluator.finish()),
            [Some((-0.0f64).to_bits()), Some(0x7ff8000000000007), None]
        );
    }

    #[test]
    fn offset_overflow_matches_range_adapter() {
        let mut evaluator = BlockEvaluator::new(&[i64::MAX], 1, 1, Mode::DisjointMax).unwrap();
        evaluator.push(i64::MAX, f64::from_bits(STALE)).unwrap();
        assert!(evaluator.push(i64::MAX, 1.0).is_err());
        assert!(BlockEvaluator::new(&[10, 15], 10, 0, Mode::DisjointMax).is_err());
        let mut instant = BlockEvaluator::new(&[i64::MAX], 0, 1, Mode::Instant).unwrap();
        instant.push(i64::MAX - 1, -0.0).unwrap();
        assert_eq!(bits(instant.finish()), [Some((-0.0f64).to_bits())]);
    }
}
