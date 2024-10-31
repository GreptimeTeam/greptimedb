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

use common_meta::peer::Peer;
use snafu::ensure;

use super::weighted_choose::WeightedChoose;
use crate::error;
use crate::error::Result;
use crate::metasrv::SelectTarget;
use crate::selector::SelectorOptions;

/// According to the `opts`, choose peers from the `weight_array` through `weighted_choose`.
pub fn choose_peers<W>(opts: &SelectorOptions, weighted_choose: &mut W) -> Result<Vec<Peer>>
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

    if opts.allow_duplication {
        (0..min_required_items)
            .map(|_| weighted_choose.choose_one())
            .collect::<Result<_>>()
    } else {
        let weight_array_len = weighted_choose.len();

        // When opts.allow_duplication is false, we need to check that the length of the weighted array is greater than
        // or equal to min_required_items, otherwise it may cause an infinite loop.
        ensure!(
            weight_array_len >= min_required_items,
            error::NoEnoughAvailableNodeSnafu {
                required: min_required_items,
                available: weight_array_len,
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
            },
            WeightedItem {
                item: Peer {
                    id: 2,
                    addr: "127.0.0.1:3001".to_string(),
                },
                weight: 1,
            },
            WeightedItem {
                item: Peer {
                    id: 3,
                    addr: "127.0.0.1:3001".to_string(),
                },
                weight: 1,
            },
            WeightedItem {
                item: Peer {
                    id: 4,
                    addr: "127.0.0.1:3001".to_string(),
                },
                weight: 1,
            },
            WeightedItem {
                item: Peer {
                    id: 5,
                    addr: "127.0.0.1:3001".to_string(),
                },
                weight: 1,
            },
        ];

        for i in 1..=5 {
            let opts = SelectorOptions {
                min_required_items: i,
                allow_duplication: false,
            };

            let selected_peers: HashSet<_> =
                choose_peers(&opts, &mut RandomWeightedChoose::new(weight_array.clone()))
                    .unwrap()
                    .into_iter()
                    .collect();

            assert_eq!(i, selected_peers.len());
        }

        let opts = SelectorOptions {
            min_required_items: 6,
            allow_duplication: false,
        };

        let selected_result =
            choose_peers(&opts, &mut RandomWeightedChoose::new(weight_array.clone()));
        assert!(selected_result.is_err());

        for i in 1..=50 {
            let opts = SelectorOptions {
                min_required_items: i,
                allow_duplication: true,
            };

            let selected_peers =
                choose_peers(&opts, &mut RandomWeightedChoose::new(weight_array.clone())).unwrap();

            assert_eq!(i, selected_peers.len());
        }
    }
}
