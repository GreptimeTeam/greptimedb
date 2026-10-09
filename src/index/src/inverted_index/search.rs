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

pub mod fst_apply;
pub mod fst_values_mapper;
pub mod index_apply;
pub mod predicate;

/// Partitions `slice` in place so that elements matching `pred` come first,
/// preserving the original relative order within each group. Returns the
/// number of matching elements.
///
/// Stable replacement for the unstable `Iterator::partition_in_place`.
pub(crate) fn partition_in_place<T>(slice: &mut [T], mut pred: impl FnMut(&T) -> bool) -> usize {
    slice.sort_by_key(|x| !pred(x));
    slice.iter().filter(|x| pred(x)).count()
}
