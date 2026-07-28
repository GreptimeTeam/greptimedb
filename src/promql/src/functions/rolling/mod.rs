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

pub(super) mod count;

use std::ops::Range;

use datafusion::common::DataFusionError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Window {
    pub(super) left: usize,
    pub(super) right: usize,
}

impl Window {
    pub(super) fn len(self) -> usize {
        self.right - self.left
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Layout {
    Sliding,
    Arbitrary,
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct Transition {
    pub(super) removed: Range<usize>,
    pub(super) added: Range<usize>,
}

pub(super) fn window_from_raw(
    offset: u32,
    len: u32,
    backing_len: usize,
    index: usize,
    name: &str,
) -> Result<Window, DataFusionError> {
    let left = usize::try_from(offset).map_err(|_| invalid_range_error(index, name))?;
    let len = usize::try_from(len).map_err(|_| invalid_range_error(index, name))?;
    let right = left
        .checked_add(len)
        .filter(|right| *right <= backing_len)
        .ok_or_else(|| invalid_range_error(index, name))?;

    Ok(Window { left, right })
}

pub(super) fn classify(windows: &[Window]) -> Layout {
    if windows
        .windows(2)
        .all(|pair| pair[1].left >= pair[0].left && pair[1].right >= pair[0].right)
    {
        Layout::Sliding
    } else {
        Layout::Arbitrary
    }
}

pub(super) fn transition(old: Window, new: Window) -> Transition {
    Transition {
        removed: old.left..new.left.min(old.right),
        added: old.right.max(new.left)..new.right,
    }
}

fn invalid_range_error(index: usize, name: &str) -> DataFusionError {
    DataFusionError::Execution(format!(
        "RangeArray's element {index} has an invalid range in PromQL function {name}"
    ))
}

#[cfg(test)]
mod test {
    use super::*;

    fn window(left: usize, right: usize) -> Window {
        Window { left, right }
    }

    #[test]
    fn classifies_dense_repeated_disjoint_and_arbitrary_layouts() {
        assert_eq!(classify(&[]), Layout::Sliding);
        assert_eq!(
            classify(&[window(1, 3), window(2, 4), window(2, 4), window(5, 7)]),
            Layout::Sliding
        );
        assert_eq!(
            classify(&[window(1, 5), window(2, 4)]),
            Layout::Arbitrary,
            "a decreasing right edge falls back for the whole invocation"
        );
        assert_eq!(
            classify(&[window(2, 4), window(1, 5)]),
            Layout::Arbitrary,
            "a compatible prefix does not create a partial sliding run"
        );
        assert_eq!(classify(&[window(1, 5), window(2, 5)]), Layout::Sliding);
    }

    #[test]
    fn computes_exact_monotone_transition_differences() {
        assert_eq!(
            transition(window(1, 5), window(2, 6)),
            Transition {
                removed: 1..2,
                added: 5..6,
            }
        );
        assert_eq!(
            transition(window(1, 3), window(1, 5)),
            Transition {
                removed: 1..1,
                added: 3..5,
            }
        );
        assert_eq!(
            transition(window(1, 5), window(3, 5)),
            Transition {
                removed: 1..3,
                added: 5..5,
            }
        );
        assert_eq!(
            transition(window(1, 5), window(1, 5)),
            Transition {
                removed: 1..1,
                added: 5..5,
            }
        );
        assert_eq!(
            transition(window(1, 3), window(5, 7)),
            Transition {
                removed: 1..3,
                added: 5..7,
            }
        );
        assert_eq!(
            transition(window(2, 5), window(5, 5)),
            Transition {
                removed: 2..5,
                added: 5..5,
            }
        );
        assert_eq!(
            transition(window(5, 5), window(5, 7)),
            Transition {
                removed: 5..5,
                added: 5..7,
            }
        );
    }

    #[test]
    fn rejects_checked_end_failures_with_the_frozen_error() {
        assert_eq!(
            window_from_raw(1, 3, 4, 0, "prom_count_over_time").unwrap(),
            window(1, 4)
        );

        let error = window_from_raw(1, 1, 1, 0, "prom_count_over_time").unwrap_err();
        match error {
            DataFusionError::Execution(message) => assert_eq!(
                message,
                "RangeArray's element 0 has an invalid range in PromQL function prom_count_over_time"
            ),
            other => panic!("expected execution error, got {other:?}"),
        }
    }
}
