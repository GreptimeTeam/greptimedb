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

/// Canonical Prometheus stale-marker NaN bit pattern.
pub const PROMETHEUS_STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;

/// Returns whether `value` is the canonical Prometheus stale-marker NaN.
#[inline]
pub fn is_prometheus_stale_nan(value: f64) -> bool {
    value.to_bits() == PROMETHEUS_STALE_NAN_BITS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_only_the_canonical_stale_marker() {
        assert!(is_prometheus_stale_nan(f64::from_bits(
            0x7ff0_0000_0000_0002
        )));
        assert!(!is_prometheus_stale_nan(f64::from_bits(
            0x7ff8_0000_0000_0000
        )));
    }
}
