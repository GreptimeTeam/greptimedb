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

use rand::distributions::WeightedIndex;
use rand::prelude::Distribution;
use rand::thread_rng;
use snafu::{ensure, ResultExt};

use crate::error;
use crate::error::Result;

/// A common trait for weighted balance algorithm.
pub trait WeightedChoose<Item>: Send + Sync {
    /// The method will re-set weight array.
    ///
    /// Note:
    /// 1. make sure weight_array is not empty.
    /// 2. the total weight is greater than 0.
    /// Otherwise an error will be returned.
    fn set_weight_array(&mut self, weight_array: Vec<WeightedItem<Item>>) -> Result<()>;

    /// The method will choose one item.
    ///
    /// If not set weight_array before, an error will be returned.
    fn choose_one(&mut self) -> Result<Item>;

    /// The method will reverse choose one item.
    ///
    /// If not set weight_array before, an error will be returned.
    fn reverse_choose_one(&mut self) -> Result<Item>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WeightedItem<Item> {
    pub item: Item,
    pub weight: usize,
    pub reverse_weight: usize,
}

/// A implementation of weighted balance: random weighted choose.
///
/// The algorithm is as follows:
///
/// ```text
///           random value
/// ─────────────────────────────────▶
///                                  │
///                                  ▼
/// ┌─────────────────┬─────────┬──────────────────────┬─────┬─────────────────┐
/// │element_0        │element_1│element_2             │...  │element_n        │
/// └─────────────────┴─────────┴──────────────────────┴─────┴─────────────────┘
/// ```
pub struct RandomWeightedChoose<Item> {
    items: Vec<WeightedItem<Item>>,
    weighted_index: Option<WeightedIndex<usize>>,
    reverse_weighted_index: Option<WeightedIndex<usize>>,
}

impl<Item> Default for RandomWeightedChoose<Item> {
    fn default() -> Self {
        Self {
            items: Vec::default(),
            weighted_index: None,
            reverse_weighted_index: None,
        }
    }
}

impl<Item> WeightedChoose<Item> for RandomWeightedChoose<Item>
where
    Item: Clone + Send + Sync,
{
    fn set_weight_array(&mut self, weight_array: Vec<WeightedItem<Item>>) -> Result<()> {
        self.weighted_index = Some(
            WeightedIndex::new(weight_array.iter().map(|item| item.weight))
                .context(error::WeightArraySnafu)?,
        );

        self.reverse_weighted_index = Some(
            WeightedIndex::new(weight_array.iter().map(|item| item.reverse_weight))
                .context(error::WeightArraySnafu)?,
        );

        self.items = weight_array;

        Ok(())
    }

    fn choose_one(&mut self) -> Result<Item> {
        ensure!(
            !self.items.is_empty() && self.weighted_index.is_some(),
            error::NotSetWeightArraySnafu
        );

        // unwrap safety: whether weighted_index is none has been checked before.
        let weighted_index = self.weighted_index.as_ref().unwrap();

        Ok(self.items[weighted_index.sample(&mut thread_rng())]
            .item
            .clone())
    }

    fn reverse_choose_one(&mut self) -> Result<Item> {
        ensure!(
            !self.items.is_empty() && self.reverse_weighted_index.is_some(),
            error::NotSetWeightArraySnafu
        );

        // unwrap safety: whether reverse_weighted_index is none has been checked before.
        let reverse_weighted_index = self.reverse_weighted_index.as_ref().unwrap();

        Ok(self.items[reverse_weighted_index.sample(&mut thread_rng())]
            .item
            .clone())
    }
}

#[cfg(test)]
mod tests {
    use super::{RandomWeightedChoose, WeightedChoose, WeightedItem};

    #[test]
    fn test_random_weighted_choose() {
        let mut choose = RandomWeightedChoose::default();
        choose
            .set_weight_array(vec![
                WeightedItem {
                    item: 1,
                    weight: 100,
                    reverse_weight: 0,
                },
                WeightedItem {
                    item: 2,
                    weight: 0,
                    reverse_weight: 100,
                },
            ])
            .unwrap();
        for _ in 0..100 {
            let ret = choose.choose_one().unwrap();
            assert_eq!(1, ret);
        }

        for _ in 0..100 {
            let ret = choose.reverse_choose_one().unwrap();
            assert_eq!(2, ret);
        }
    }

    #[test]
    #[should_panic]
    fn test_random_weighted_choose_should_panic() {
        let mut choose: RandomWeightedChoose<u32> = RandomWeightedChoose::default();
        choose.set_weight_array(vec![]).unwrap();
        let _ = choose.choose_one().unwrap();
    }

    #[test]
    #[should_panic]
    fn test_random_reverse_weighted_choose_should_panic() {
        let mut choose: RandomWeightedChoose<u32> = RandomWeightedChoose::default();
        choose.set_weight_array(vec![]).unwrap();
        let _ = choose.reverse_choose_one().unwrap();
    }
}
