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

use std::collections::HashSet;

use common_meta::peer::Peer;
use snafu::ensure;

use super::weighted_choose::{WeightedChoose, WeightedItem};
use crate::error;
use crate::error::Result;
use crate::selector::SelectorOptions;

/// According to the `opts`, choose peers from the `weight_array` through `weighted_choose`.
pub fn choose_peers<W>(
    mut weight_array: Vec<WeightedItem<Peer>>,
    opts: &SelectorOptions,
    weighted_choose: &mut W,
) -> Result<Vec<Peer>>
where
    W: WeightedChoose<Peer>,
{
    let min_required_items = opts.min_required_items;
    ensure!(
        !weight_array.is_empty(),
        error::NoEnoughAvailableDatanodeSnafu {
            required: min_required_items,
            available: 0_usize,
        }
    );

    if opts.allow_duplication {
        weighted_choose.set_weight_array(weight_array)?;
        (0..min_required_items)
            .map(|_| weighted_choose.choose_one())
            .collect::<Result<_>>()
    } else {
        let weight_array_len = weight_array.len();

        // When opts.allow_duplication is false, we need to check that the length of the weighted array is greater than
        // or equal to min_required_items, otherwise it may cause an infinite loop.
        ensure!(
            weight_array_len >= min_required_items,
            error::NoEnoughAvailableDatanodeSnafu {
                required: min_required_items,
                available: weight_array_len,
            }
        );

        if weight_array_len == min_required_items {
            return Ok(weight_array.into_iter().map(|item| item.item).collect());
        }

        weighted_choose.set_weight_array(weight_array.clone())?;

        // Assume min_required_items is 3, weight_array_len is 100, then we can choose 3 items from the weight array
        // and return. But assume min_required_items is 99, weight_array_len is 100. It's not cheap to choose 99 items
        // from the weight array. So we can reverse choose 1 item from the weight array, and return the remaining 99
        // items.
        if min_required_items * 2 > weight_array_len {
            let select_num = weight_array_len - min_required_items;
            let mut selected = HashSet::with_capacity(select_num);
            while selected.len() < select_num {
                let item = weighted_choose.reverse_choose_one()?;
                selected.insert(item);
            }
            weight_array.retain(|item| !selected.contains(&item.item));
            Ok(weight_array.into_iter().map(|item| item.item).collect())
        } else {
            let mut selected = HashSet::with_capacity(min_required_items);
            while selected.len() < min_required_items {
                let item = weighted_choose.choose_one()?;
                selected.insert(item);
            }
            Ok(selected.into_iter().collect())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use common_meta::peer::Peer;

    use crate::selector::common::choose_peers;
    use crate::selector::weighted_choose::{RandomWeightedChoose, WeightedItem};
    use crate::selector::SelectorOptions;

    #[test]
    fn test_choose_peers() {
        let weight_array = vec![
            WeightedItem {
                item: Peer {
                    id: 1,
                    addr: "127.0.0.1:3001".to_string(),
                },
                weight: 1,
                reverse_weight: 1,
            },
            WeightedItem {
                item: Peer {
                    id: 2,
                    addr: "127.0.0.1:3001".to_string(),
                },
                weight: 1,
                reverse_weight: 1,
            },
            WeightedItem {
                item: Peer {
                    id: 3,
                    addr: "127.0.0.1:3001".to_string(),
                },
                weight: 1,
                reverse_weight: 1,
            },
            WeightedItem {
                item: Peer {
                    id: 4,
                    addr: "127.0.0.1:3001".to_string(),
                },
                weight: 1,
                reverse_weight: 1,
            },
            WeightedItem {
                item: Peer {
                    id: 5,
                    addr: "127.0.0.1:3001".to_string(),
                },
                weight: 1,
                reverse_weight: 1,
            },
        ];

        for i in 1..=5 {
            let opts = SelectorOptions {
                min_required_items: i,
                allow_duplication: false,
            };

            let selected_peers: HashSet<_> = choose_peers(
                weight_array.clone(),
                &opts,
                &mut RandomWeightedChoose::default(),
            )
            .unwrap()
            .into_iter()
            .collect();

            assert_eq!(i, selected_peers.len());
        }

        let opts = SelectorOptions {
            min_required_items: 6,
            allow_duplication: false,
        };

        let selected_result = choose_peers(
            weight_array.clone(),
            &opts,
            &mut RandomWeightedChoose::default(),
        );
        assert!(selected_result.is_err());

        for i in 1..=50 {
            let opts = SelectorOptions {
                min_required_items: i,
                allow_duplication: true,
            };

            let selected_peers = choose_peers(
                weight_array.clone(),
                &opts,
                &mut RandomWeightedChoose::default(),
            )
            .unwrap();

            assert_eq!(i, selected_peers.len());
        }
    }
}
