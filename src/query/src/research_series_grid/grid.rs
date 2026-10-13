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

/// Millisecond evaluation instants shared by every series in one query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TimeGrid {
    pub instants: Vec<i64>,
}

impl TimeGrid {
    pub fn new(start: i64, end: i64, step: i64) -> Result<Self, &'static str> {
        if step <= 0 || end < start {
            return Err("invalid evaluation grid");
        }
        let count = usize::try_from((i128::from(end) - i128::from(start)) / i128::from(step) + 1)
            .map_err(|_| "evaluation grid exceeds addressable memory")?;
        let mut instants = Vec::new();
        instants
            .try_reserve_exact(count)
            .map_err(|_| "cannot allocate evaluation grid")?;
        for index in 0..count {
            instants.push((i128::from(start) + index as i128 * i128::from(step)) as i64);
        }
        Ok(Self { instants })
    }

    /// Matches GT's left-open lookback, retaining stale markers until selection.
    /// Inputs are the strictly ordered, unique timestamps validated by the snapshot reader.
    pub fn instant_indices(
        &self,
        timestamps: &[i64],
        values: &[f64],
        lookback: i64,
        offset: i64,
    ) -> Result<Vec<Option<usize>>, &'static str> {
        if timestamps.len() != values.len() || lookback < 0 {
            return Err("invalid instant selector input");
        }
        let mut cursor = 0;
        Ok(self
            .instants
            .iter()
            .map(|&instant| {
                let end = i128::from(instant) - i128::from(offset);
                while cursor < timestamps.len() && i128::from(timestamps[cursor]) <= end {
                    cursor += 1;
                }
                let index = cursor.checked_sub(1)?;
                let timestamp = i128::from(timestamps[index]);
                if (timestamp == end || timestamp > end - i128::from(lookback))
                    && values[index].to_bits() != 0x7ff0_0000_0000_0002
                {
                    Some(index)
                } else {
                    None
                }
            })
            .collect())
    }

    /// Produces RangeArray selections without expanding samples into evaluation rows.
    /// Range input has already removed stale markers, but retains ordinary NaNs.
    pub fn ranges(
        &self,
        timestamps: &[i64],
        window: i64,
        offset: i64,
    ) -> Result<Vec<(u32, u32)>, &'static str> {
        if window <= 0 || timestamps.len() > u32::MAX as usize {
            return Err("invalid range selector input");
        }
        let (mut left, mut right) = (0, 0);
        Ok(self
            .instants
            .iter()
            .map(|&instant| {
                let end = i128::from(instant) - i128::from(offset);
                let start = end - i128::from(window);
                while right < timestamps.len() && i128::from(timestamps[right]) <= end {
                    right += 1;
                }
                while left < right && i128::from(timestamps[left]) <= start {
                    left += 1;
                }
                (left as u32, (right - left) as u32)
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grid_keeps_unaligned_end_and_extreme_timestamps() {
        assert_eq!(TimeGrid::new(-5, 6, 4).unwrap().instants, [-5, -1, 3]);
        assert_eq!(
            TimeGrid::new(i64::MAX - 1, i64::MAX, 2).unwrap().instants,
            [i64::MAX - 1]
        );
        assert_eq!(
            TimeGrid::new(i64::MIN, i64::MIN + 1, 1).unwrap().instants,
            [i64::MIN, i64::MIN + 1]
        );
        assert!(TimeGrid::new(0, 1, 0).is_err());
        assert!(TimeGrid::new(1, 0, 1).is_err());
    }

    #[test]
    fn stale_suppresses_instant_without_falling_back() {
        let grid = TimeGrid::new(0, 50, 10).unwrap();
        let timestamps = [0, 10, 30];
        let values = [1.0, f64::from_bits(0x7ff0_0000_0000_0002), f64::NAN];
        assert_eq!(
            grid.instant_indices(&timestamps, &values, 20, 0).unwrap(),
            [Some(0), None, None, Some(2), Some(2), None]
        );
        assert_eq!(
            grid.instant_indices(&timestamps, &values, 0, 0).unwrap(),
            [Some(0), None, None, Some(2), None, None]
        );
    }

    #[test]
    fn cursors_match_independent_window_search() {
        let timestamps = [-100, -20, -1, 0, 1, 2, 16, 45, 90, 91, 500];
        let values = [1.0; 11];
        for step in [1, 7, 30, 100] {
            let grid = TimeGrid::new(-140, 600, step).unwrap();
            for offset in [-200, -1, 0, 1, 200] {
                for window in [1, 17, 100, 1000] {
                    let ranges = grid.ranges(&timestamps, window, offset).unwrap();
                    let instants = grid
                        .instant_indices(&timestamps, &values, window, offset)
                        .unwrap();
                    for (i, &instant) in grid.instants.iter().enumerate() {
                        let end = i128::from(instant) - i128::from(offset);
                        let start = end - i128::from(window);
                        let expected: Vec<_> = timestamps
                            .iter()
                            .enumerate()
                            .filter(|(_, ts)| i128::from(**ts) > start && i128::from(**ts) <= end)
                            .map(|(index, _)| index)
                            .collect();
                        let (first, count) = ranges[i];
                        assert_eq!(
                            (first as usize..(first + count) as usize).collect::<Vec<_>>(),
                            expected
                        );
                        assert_eq!(instants[i], expected.last().copied());
                    }
                }
            }
        }
    }

    #[test]
    fn offset_arithmetic_does_not_wrap() {
        let grid = TimeGrid::new(i64::MAX - 1, i64::MAX, 1).unwrap();
        let timestamps = [i64::MIN, 0, i64::MAX];
        assert_eq!(grid.ranges(&timestamps, 10, -10).unwrap(), [(2, 1), (3, 0)]);
        assert_eq!(
            grid.instant_indices(&timestamps, &[1.0; 3], 10, -10)
                .unwrap(),
            [Some(2), None]
        );
        let grid = TimeGrid::new(i64::MIN, i64::MIN + 1, 1).unwrap();
        assert_eq!(grid.ranges(&timestamps, 10, 10).unwrap(), [(0, 0), (0, 0)]);
    }
}
