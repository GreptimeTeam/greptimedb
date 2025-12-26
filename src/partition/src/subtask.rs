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

use std::collections::VecDeque;

use crate::error::Result;
use crate::expr::PartitionExpr;
use crate::overlap::associate_from_to;

/// Indices are into the original input arrays (array of [`PartitionExpr`]). A connected component.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepartitionSubtask {
    pub from_expr_indices: Vec<usize>,
    pub to_expr_indices: Vec<usize>,
    /// For each `from_expr_indices[k]`, the corresponding vector contains global
    /// `to_expr_indices` that overlap with it (indices into the original `to_exprs`).
    pub transition_map: Vec<Vec<usize>>,
}

/// Create independent subtasks out of given FROM/TO partition expressions.
pub fn create_subtasks(
    from_exprs: &[PartitionExpr],
    to_exprs: &[PartitionExpr],
) -> Result<Vec<RepartitionSubtask>> {
    // FROM -> TO
    let assoc = associate_from_to(from_exprs, to_exprs)?;
    if !assoc.iter().any(|v| !v.is_empty()) {
        return Ok(vec![]);
    }

    // TO -> FROM
    let mut rev = vec![Vec::new(); to_exprs.len()];
    for (li, rights) in assoc.iter().enumerate() {
        for &r in rights {
            rev[r].push(li);
        }
    }

    // FROM(left), TO(right). Undirected
    let mut visited_left = vec![false; from_exprs.len()];
    let mut visited_right = vec![false; to_exprs.len()];
    let mut subtasks = Vec::new();

    for li in 0..from_exprs.len() {
        if assoc[li].is_empty() || visited_left[li] {
            continue;
        }

        #[derive(Copy, Clone)]
        enum Node {
            Left(usize),
            Right(usize),
        }
        let mut left_set = Vec::new();
        let mut right_set = Vec::new();
        let mut queue = VecDeque::new();

        visited_left[li] = true;
        queue.push_back(Node::Left(li));

        while let Some(node) = queue.pop_front() {
            match node {
                Node::Left(left) => {
                    left_set.push(left);
                    for &r in &assoc[left] {
                        if !visited_right[r] {
                            visited_right[r] = true;
                            queue.push_back(Node::Right(r));
                        }
                    }
                }
                Node::Right(right) => {
                    right_set.push(right);
                    for &l in &rev[right] {
                        if !visited_left[l] {
                            visited_left[l] = true;
                            queue.push_back(Node::Left(l));
                        }
                    }
                }
            }
        }

        left_set.sort_unstable();
        right_set.sort_unstable();

        let transition_map = left_set
            .iter()
            .map(|&i| assoc[i].clone())
            .collect::<Vec<_>>();

        subtasks.push(RepartitionSubtask {
            from_expr_indices: left_set,
            to_expr_indices: right_set,
            transition_map,
        });
    }

    Ok(subtasks)
}

#[cfg(test)]
mod tests {
    use datatypes::value::Value;

    use super::*;
    use crate::expr::col;
    #[test]
    fn test_split_one_to_two() {
        // Left: [0, 40)
        let from = vec![
            col("u")
                .gt_eq(Value::Int64(0))
                .and(col("u").lt(Value::Int64(20))),
        ];

        // Right: [0, 10), [10, 20)
        let to = vec![
            col("u")
                .gt_eq(Value::Int64(0))
                .and(col("u").lt(Value::Int64(10))),
            col("u")
                .gt_eq(Value::Int64(10))
                .and(col("u").lt(Value::Int64(20))),
        ];

        let subtasks = create_subtasks(&from, &to).unwrap();
        assert_eq!(subtasks.len(), 1);
        assert_eq!(subtasks[0].from_expr_indices, vec![0]);
        assert_eq!(subtasks[0].to_expr_indices, vec![0, 1]);
        assert_eq!(subtasks[0].transition_map[0], vec![0, 1]);
    }

    #[test]
    fn test_merge_two_to_one() {
        // Left: [0, 10), [10, 20)
        let from = vec![
            col("u")
                .gt_eq(Value::Int64(0))
                .and(col("u").lt(Value::Int64(10))),
            col("u")
                .gt_eq(Value::Int64(10))
                .and(col("u").lt(Value::Int64(20))),
        ];
        // Right: [0, 40)
        let to = vec![
            col("u")
                .gt_eq(Value::Int64(0))
                .and(col("u").lt(Value::Int64(20))),
        ];

        let subtasks = create_subtasks(&from, &to).unwrap();
        assert_eq!(subtasks.len(), 1);
        assert_eq!(subtasks[0].from_expr_indices, vec![0, 1]);
        assert_eq!(subtasks[0].to_expr_indices, vec![0]);
        assert_eq!(subtasks[0].transition_map[0], vec![0]);
        assert_eq!(subtasks[0].transition_map[1], vec![0]);
    }

    #[test]
    fn test_create_subtasks_disconnected() {
        // Left: A:[0,10), B:[20,30)
        let from = vec![
            col("x")
                .gt_eq(Value::Int64(0))
                .and(col("x").lt(Value::Int64(10))),
            col("x")
                .gt_eq(Value::Int64(20))
                .and(col("x").lt(Value::Int64(30))),
        ];
        // Right: C:[5,15), D:[40,50)
        let to = vec![
            col("x")
                .gt_eq(Value::Int64(5))
                .and(col("x").lt(Value::Int64(15))),
            col("x")
                .gt_eq(Value::Int64(40))
                .and(col("x").lt(Value::Int64(50))),
        ];

        let subtasks = create_subtasks(&from, &to).unwrap();

        // Expect two components: {A,C} and {B} has no edges so filtered out
        // Note: nodes with no edges are excluded by construction
        assert_eq!(subtasks.len(), 1);
        assert_eq!(subtasks[0].from_expr_indices, vec![0]);
        assert_eq!(subtasks[0].to_expr_indices, vec![0]);
        assert_eq!(subtasks[0].transition_map, vec![vec![0]]);
    }

    #[test]
    fn test_create_subtasks_multi() {
        // Left: [0,100), [100,200)
        let from = vec![
            col("u")
                .gt_eq(Value::Int64(0))
                .and(col("u").lt(Value::Int64(100))),
            col("u")
                .gt_eq(Value::Int64(100))
                .and(col("u").lt(Value::Int64(200))),
        ];
        // Right: [0,50), [50,150), [150,250)
        let to = vec![
            col("u")
                .gt_eq(Value::Int64(0))
                .and(col("u").lt(Value::Int64(50))),
            col("u")
                .gt_eq(Value::Int64(50))
                .and(col("u").lt(Value::Int64(150))),
            col("u")
                .gt_eq(Value::Int64(150))
                .and(col("u").lt(Value::Int64(250))),
        ];

        let subtasks = create_subtasks(&from, &to).unwrap();
        // All connected into a single component
        assert_eq!(subtasks.len(), 1);
        assert_eq!(subtasks[0].from_expr_indices, vec![0, 1]);
        assert_eq!(subtasks[0].to_expr_indices, vec![0, 1, 2]);
        // [0,100) -> [0,50), [50,150)
        // [100,200) -> [50,150), [150,250)
        assert_eq!(subtasks[0].transition_map[0], vec![0, 1]);
        assert_eq!(subtasks[0].transition_map[1], vec![1, 2]);
    }

    #[test]
    fn test_two_components() {
        // Left: A:[0,10), B:[20,30)
        let from = vec![
            col("x")
                .gt_eq(Value::Int64(0))
                .and(col("x").lt(Value::Int64(10))),
            col("x")
                .gt_eq(Value::Int64(20))
                .and(col("x").lt(Value::Int64(30))),
        ];
        // Right: C:[5,7), D:[22,28)
        let to = vec![
            col("x")
                .gt_eq(Value::Int64(5))
                .and(col("x").lt(Value::Int64(7))),
            col("x")
                .gt_eq(Value::Int64(22))
                .and(col("x").lt(Value::Int64(28))),
        ];
        let mut subtasks = create_subtasks(&from, &to).unwrap();
        // Deterministic order: left indices sorted, so components may appear in order of discovery.
        assert_eq!(subtasks.len(), 2);
        // Sort for stable assertion by smallest left index
        subtasks.sort_by_key(|s| s.from_expr_indices[0]);
        assert_eq!(subtasks[0].from_expr_indices, vec![0]);
        assert_eq!(subtasks[0].to_expr_indices, vec![0]);
        assert_eq!(subtasks[0].transition_map, vec![vec![0]]);
        assert_eq!(subtasks[1].from_expr_indices, vec![1]);
        assert_eq!(subtasks[1].to_expr_indices, vec![1]);
        assert_eq!(subtasks[1].transition_map, vec![vec![1]]);
    }

    #[test]
    fn test_bridge_single_component() {
        // Left: [0,10), [10,20)
        let from = vec![
            col("u")
                .gt_eq(Value::Int64(0))
                .and(col("u").lt(Value::Int64(10))),
            col("u")
                .gt_eq(Value::Int64(10))
                .and(col("u").lt(Value::Int64(20))),
        ];
        // Right: [5,15), [15,25)
        let to = vec![
            col("u")
                .gt_eq(Value::Int64(5))
                .and(col("u").lt(Value::Int64(15))),
            col("u")
                .gt_eq(Value::Int64(15))
                .and(col("u").lt(Value::Int64(25))),
        ];
        let subtasks = create_subtasks(&from, &to).unwrap();
        assert_eq!(subtasks.len(), 1);
        assert_eq!(subtasks[0].from_expr_indices, vec![0, 1]);
        assert_eq!(subtasks[0].to_expr_indices, vec![0, 1]);
        assert_eq!(subtasks[0].transition_map[0], vec![0]);
        assert_eq!(subtasks[0].transition_map[1], vec![0, 1]);
    }

    #[test]
    fn test_all_isolated_no_subtasks() {
        // No edges at all
        let from = vec![col("k").lt(Value::Int64(10))];
        let to = vec![col("k").gt_eq(Value::Int64(10))];
        let subtasks = create_subtasks(&from, &to).unwrap();
        assert!(subtasks.is_empty());
    }

    #[test]
    fn test_three_components() {
        // Left: A:[0,10), B:[20,30), C:[40,50)
        let from = vec![
            col("x")
                .gt_eq(Value::Int64(0))
                .and(col("x").lt(Value::Int64(10))),
            col("x")
                .gt_eq(Value::Int64(20))
                .and(col("x").lt(Value::Int64(30))),
            col("x")
                .gt_eq(Value::Int64(40))
                .and(col("x").lt(Value::Int64(50))),
        ];
        // Right: A:[0,10), B:[20,30), C:[40,60)
        let to = vec![
            col("x")
                .gt_eq(Value::Int64(0))
                .and(col("x").lt(Value::Int64(10))),
            col("x")
                .gt_eq(Value::Int64(20))
                .and(col("x").lt(Value::Int64(30))),
            col("x")
                .gt_eq(Value::Int64(40))
                .and(col("x").lt(Value::Int64(60))),
        ];
        let subtasks = create_subtasks(&from, &to).unwrap();
        assert_eq!(subtasks.len(), 3);
        assert_eq!(subtasks[0].from_expr_indices, vec![0]);
        assert_eq!(subtasks[0].to_expr_indices, vec![0]);
        assert_eq!(subtasks[0].transition_map, vec![vec![0]]);
        assert_eq!(subtasks[1].from_expr_indices, vec![1]);
        assert_eq!(subtasks[1].to_expr_indices, vec![1]);
        assert_eq!(subtasks[1].transition_map, vec![vec![1]]);
        assert_eq!(subtasks[2].from_expr_indices, vec![2]);
        assert_eq!(subtasks[2].to_expr_indices, vec![2]);
        assert_eq!(subtasks[2].transition_map, vec![vec![2]]);
    }
}
