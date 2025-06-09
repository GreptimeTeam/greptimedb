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

use crate::error;
use crate::error::Result;
use crate::metasrv::SelectTarget;
use crate::selector::weighted_choose::{WeightedChoose, WeightedItem};
use crate::selector::SelectorOptions;

/// Filter out the excluded peers from the `weight_array`.
pub fn filter_out_excluded_peers(
    weight_array: &mut Vec<WeightedItem<Peer>>,
    exclude_peer_ids: &HashSet<u64>,
) {
    weight_array.retain(|peer| !exclude_peer_ids.contains(&peer.item.id));
}

/// According to the `opts`, choose peers from the `weight_array` through `weighted_choose`.
pub fn choose_items<W>(opts: &SelectorOptions, weighted_choose: &mut W) -> Result<Vec<Peer>>
where
    W: WeightedChoose<Peer>,
{
    let min_required_items = opts.min_required_items;
    ensure!(
        !weighted_choose.is_empty(),
        error::NoEnoughAvailableNodeSnafu {
            required: min_required_items,
            available: 0_usize,
            select_target: SelectTarget::Datanode
        }
    );

    if min_required_items == 1 {
        // fast path
        return Ok(vec![weighted_choose.choose_one()?]);
    }

    let available_count = weighted_choose.len();

    if opts.allow_duplication {
        // Calculate how many complete rounds of `available_count` items to select,
        // plus any additional items needed after complete rounds.
        let complete_batches = min_required_items / available_count;
        let leftover_items = min_required_items % available_count;
        if complete_batches == 0 {
            return weighted_choose.choose_multiple(leftover_items);
        }

        let mut result = Vec::with_capacity(min_required_items);
        for _ in 0..complete_batches {
            result.extend(weighted_choose.choose_multiple(available_count)?);
        }
        result.extend(weighted_choose.choose_multiple(leftover_items)?);

        Ok(result)
    } else {
        // Ensure the available items are sufficient when duplication is not allowed.
        ensure!(
            available_count >= min_required_items,
            error::NoEnoughAvailableNodeSnafu {
                required: min_required_items,
                available: available_count,
                select_target: SelectTarget::Datanode
            }
        );

        weighted_choose.choose_multiple(min_required_items)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use common_meta::peer::Peer;

    use crate::selector::common::{choose_items, filter_out_excluded_peers};
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
                weight: 1.0,
            },
            WeightedItem {
                item: Peer {
                    id: 2,
                    addr: "127.0.0.1:3001".to_string(),
                },
                weight: 1.0,
            },
            WeightedItem {
                item: Peer {
                    id: 3,
                    addr: "127.0.0.1:3001".to_string(),
                },
                weight: 1.0,
            },
            WeightedItem {
                item: Peer {
                    id: 4,
                    addr: "127.0.0.1:3001".to_string(),
                },
                weight: 1.0,
            },
            WeightedItem {
                item: Peer {
                    id: 5,
                    addr: "127.0.0.1:3001".to_string(),
                },
                weight: 1.0,
            },
        ];

        for i in 1..=5 {
            let opts = SelectorOptions {
                min_required_items: i,
                allow_duplication: false,
                exclude_peer_ids: HashSet::new(),
            };

            let selected_peers: HashSet<_> =
                choose_items(&opts, &mut RandomWeightedChoose::new(weight_array.clone()))
                    .unwrap()
                    .into_iter()
                    .collect();

            assert_eq!(i, selected_peers.len());
        }

        let opts = SelectorOptions {
            min_required_items: 6,
            allow_duplication: false,
            exclude_peer_ids: HashSet::new(),
        };

        let selected_result =
            choose_items(&opts, &mut RandomWeightedChoose::new(weight_array.clone()));
        assert!(selected_result.is_err());

        for i in 1..=50 {
            let opts = SelectorOptions {
                min_required_items: i,
                allow_duplication: true,
                exclude_peer_ids: HashSet::new(),
            };

            let selected_peers =
                choose_items(&opts, &mut RandomWeightedChoose::new(weight_array.clone())).unwrap();

            assert_eq!(i, selected_peers.len());
        }
    }

    #[test]
    fn test_filter_out_excluded_peers() {
        let mut weight_array = vec![
            WeightedItem {
                item: Peer {
                    id: 1,
                    addr: "127.0.0.1:3001".to_string(),
                },
                weight: 1.0,
            },
            WeightedItem {
                item: Peer {
                    id: 2,
                    addr: "127.0.0.1:3002".to_string(),
                },
                weight: 1.0,
            },
        ];

        let exclude_peer_ids = HashSet::from([1]);
        filter_out_excluded_peers(&mut weight_array, &exclude_peer_ids);

        assert_eq!(weight_array.len(), 1);
        assert_eq!(weight_array[0].item.id, 2);
    }
}
