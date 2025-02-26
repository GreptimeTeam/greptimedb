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

use nalgebra::DVectorView;

/// Calculates the squared L2 distance between two vectors.
///
/// **Note:** Must ensure that the length of the two vectors are the same.
pub fn l2sq(lhs: &[f32], rhs: &[f32]) -> f32 {
    let lhs = DVectorView::from_slice(lhs, lhs.len());
    let rhs = DVectorView::from_slice(rhs, rhs.len());

    (lhs - rhs).norm_squared()
}

#[cfg(test)]
mod tests {
    use approx::assert_relative_eq;

    use super::*;

    #[test]
    fn test_l2sq_scalar() {
        let lhs = vec![1.0, 2.0, 3.0];
        let rhs = vec![1.0, 2.0, 3.0];
        assert_relative_eq!(l2sq(&lhs, &rhs), 0.0, epsilon = 1e-2);

        let lhs = vec![1.0, 2.0, 3.0];
        let rhs = vec![4.0, 5.0, 6.0];
        assert_relative_eq!(l2sq(&lhs, &rhs), 27.0, epsilon = 1e-2);

        let lhs = vec![1.0, 2.0, 3.0];
        let rhs = vec![7.0, 8.0, 9.0];
        assert_relative_eq!(l2sq(&lhs, &rhs), 108.0, epsilon = 1e-2);

        let lhs = vec![0.0, 0.0, 0.0];
        let rhs = vec![1.0, 2.0, 3.0];
        assert_relative_eq!(l2sq(&lhs, &rhs), 14.0, epsilon = 1e-2);

        let lhs = vec![0.0, 0.0, 0.0];
        let rhs = vec![4.0, 5.0, 6.0];
        assert_relative_eq!(l2sq(&lhs, &rhs), 77.0, epsilon = 1e-2);

        let lhs = vec![0.0, 0.0, 0.0];
        let rhs = vec![7.0, 8.0, 9.0];
        assert_relative_eq!(l2sq(&lhs, &rhs), 194.0, epsilon = 1e-2);

        let lhs = vec![7.0, 8.0, 9.0];
        let rhs = vec![1.0, 2.0, 3.0];
        assert_relative_eq!(l2sq(&lhs, &rhs), 108.0, epsilon = 1e-2);

        let lhs = vec![7.0, 8.0, 9.0];
        let rhs = vec![4.0, 5.0, 6.0];
        assert_relative_eq!(l2sq(&lhs, &rhs), 27.0, epsilon = 1e-2);

        let lhs = vec![7.0, 8.0, 9.0];
        let rhs = vec![7.0, 8.0, 9.0];
        assert_relative_eq!(l2sq(&lhs, &rhs), 0.0, epsilon = 1e-2);
    }
}
