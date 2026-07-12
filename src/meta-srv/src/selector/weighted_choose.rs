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

use rand::rng;
use rand::seq::IndexedRandom;
use snafu::ResultExt;

use crate::error;
use crate::error::Result;

/// A common trait for weighted balance algorithm.
pub trait WeightedChoose<Item>: Send + Sync {
    /// The method will choose one item.
    fn choose_one(&mut self) -> Result<Item>;

    /// The method will choose multiple items.
    ///
    /// ## Note
    ///
    /// - Returns less than `amount` items if the weight_array is not enough.
    /// - The returned items cannot be duplicated.
    fn choose_multiple(&mut self, amount: usize) -> Result<Vec<Item>>;

    /// Returns the length of the weight_array.
    fn len(&self) -> usize;

    /// Returns whether the weight_array is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// The struct represents a weighted item.
#[derive(Debug, Clone, PartialEq)]
pub struct WeightedItem<Item> {
    pub item: Item,
    pub weight: f64,
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
}

impl<Item> RandomWeightedChoose<Item> {
    pub fn new(items: Vec<WeightedItem<Item>>) -> Self {
        Self { items }
    }
}

impl<Item> Default for RandomWeightedChoose<Item> {
    fn default() -> Self {
        Self {
            items: Vec::default(),
        }
    }
}

impl<Item> WeightedChoose<Item> for RandomWeightedChoose<Item>
where
    Item: Clone + Send + Sync,
{
    fn choose_one(&mut self) -> Result<Item> {
        // unwrap safety: whether weighted_index is none has been checked before.
        let item = self
            .items
            .choose_weighted(&mut rng(), |item| item.weight)
            .context(error::ChooseItemsSnafu)?
            .item
            .clone();
        Ok(item)
    }

    fn choose_multiple(&mut self, amount: usize) -> Result<Vec<Item>> {
        let amount = amount.min(self.items.iter().filter(|item| item.weight > 0.0).count());

        Ok(self
            .items
            .choose_multiple_weighted(&mut rng(), amount, |item| item.weight)
            .context(error::ChooseItemsSnafu)?
            .cloned()
            .map(|item| item.item)
            .collect::<Vec<_>>())
    }

    fn len(&self) -> usize {
        self.items.len()
    }
}

#[cfg(test)]
mod tests {
    use super::{RandomWeightedChoose, WeightedChoose, WeightedItem};

    #[test]
    fn test_random_weighted_choose() {
        let mut choose = RandomWeightedChoose::new(vec![
            WeightedItem {
                item: 1,
                weight: 100.0,
            },
            WeightedItem {
                item: 2,
                weight: 0.0,
            },
        ]);

        for _ in 0..100 {
            let ret = choose.choose_one().unwrap();
            assert_eq!(1, ret);
        }

        for _ in 0..100 {
            let ret = choose.choose_multiple(3).unwrap();
            assert_eq!(vec![1], ret);
        }
    }
}
