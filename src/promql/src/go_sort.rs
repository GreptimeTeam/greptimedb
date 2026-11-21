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

//! A port of Go's `sort.Sort` / `slices.SortFunc` (pattern-defeating quicksort, Go 1.19+).
//!
//! Prometheus orders results with these, and with comparators that are not total orders
//! (NaN ranks first in `Less` against everything, `natsort.Compare` holds both ways for "1"
//! and "01"). The order of such elements is part of the result, and Rust's sorts may panic on
//! such comparators, so the exact algorithm is reproduced.

/// Sorts `v` the way Go's `sort.Sort` does with `less` as `Less`.
pub(crate) fn sort_by<T>(v: &mut [T], mut less: impl FnMut(&T, &T) -> bool) {
    let n = v.len();
    if n <= 1 {
        return;
    }
    let limit = bit_len(n);
    let mut sorter = Sorter { v, less: &mut less };
    sorter.pdqsort(0, n, limit);
}

fn bit_len(n: usize) -> usize {
    (usize::BITS - n.leading_zeros()) as usize
}

#[derive(PartialEq, Eq)]
enum Hint {
    Unknown,
    Increasing,
    Decreasing,
}

struct Sorter<'a, T, F> {
    v: &'a mut [T],
    less: &'a mut F,
}

impl<T, F: FnMut(&T, &T) -> bool> Sorter<'_, T, F> {
    fn less(&mut self, i: usize, j: usize) -> bool {
        (self.less)(&self.v[i], &self.v[j])
    }

    fn swap(&mut self, i: usize, j: usize) {
        self.v.swap(i, j);
    }

    fn insertion_sort(&mut self, a: usize, b: usize) {
        for i in a + 1..b {
            let mut j = i;
            while j > a && self.less(j, j - 1) {
                self.swap(j, j - 1);
                j -= 1;
            }
        }
    }

    fn sift_down(&mut self, lo: usize, hi: usize, first: usize) {
        let mut root = lo;
        loop {
            let mut child = 2 * root + 1;
            if child >= hi {
                break;
            }
            if child + 1 < hi && self.less(first + child, first + child + 1) {
                child += 1;
            }
            if !self.less(first + root, first + child) {
                return;
            }
            self.swap(first + root, first + child);
            root = child;
        }
    }

    fn heap_sort(&mut self, a: usize, b: usize) {
        let first = a;
        let hi = b - a;
        for i in (0..=(hi - 1) / 2).rev() {
            self.sift_down(i, hi, first);
        }
        for i in (0..hi).rev() {
            self.swap(first, first + i);
            self.sift_down(0, i, first);
        }
    }

    fn pdqsort(&mut self, mut a: usize, mut b: usize, mut limit: usize) {
        const MAX_INSERTION: usize = 12;
        let mut was_balanced = true;
        let mut was_partitioned = true;
        loop {
            let length = b - a;
            if length <= MAX_INSERTION {
                self.insertion_sort(a, b);
                return;
            }
            if limit == 0 {
                self.heap_sort(a, b);
                return;
            }
            if !was_balanced {
                self.break_patterns(a, b);
                limit -= 1;
            }

            let (mut pivot, mut hint) = self.choose_pivot(a, b);
            if hint == Hint::Decreasing {
                self.reverse_range(a, b);
                pivot = (b - 1) - (pivot - a);
                hint = Hint::Increasing;
            }
            if was_balanced
                && was_partitioned
                && hint == Hint::Increasing
                && self.partial_insertion_sort(a, b)
            {
                return;
            }
            if a > 0 && !self.less(a - 1, pivot) {
                a = self.partition_equal(a, b, pivot);
                continue;
            }

            let (mid, already_partitioned) = self.partition(a, b, pivot);
            was_partitioned = already_partitioned;
            let (left_len, right_len) = (mid - a, b - mid);
            let balance_threshold = length / 8;
            if left_len < right_len {
                was_balanced = left_len >= balance_threshold;
                self.pdqsort(a, mid, limit);
                a = mid + 1;
            } else {
                was_balanced = right_len >= balance_threshold;
                self.pdqsort(mid + 1, b, limit);
                b = mid;
            }
        }
    }

    // Indices stay signed here: `j` can step to `a` - 1 when nothing is below the pivot.
    fn partition(&mut self, a: usize, b: usize, pivot: usize) -> (usize, bool) {
        self.swap(a, pivot);
        let (mut i, mut j) = (a as isize + 1, b as isize - 1);
        let at = |k: isize| k as usize;
        while i <= j && self.less(at(i), a) {
            i += 1;
        }
        while i <= j && !self.less(at(j), a) {
            j -= 1;
        }
        if i > j {
            self.swap(at(j), a);
            return (at(j), true);
        }
        self.swap(at(i), at(j));
        i += 1;
        j -= 1;
        loop {
            while i <= j && self.less(at(i), a) {
                i += 1;
            }
            while i <= j && !self.less(at(j), a) {
                j -= 1;
            }
            if i > j {
                break;
            }
            self.swap(at(i), at(j));
            i += 1;
            j -= 1;
        }
        self.swap(at(j), a);
        (at(j), false)
    }

    fn partition_equal(&mut self, a: usize, b: usize, pivot: usize) -> usize {
        self.swap(a, pivot);
        let (mut i, mut j) = (a as isize + 1, b as isize - 1);
        let at = |k: isize| k as usize;
        loop {
            while i <= j && !self.less(a, at(i)) {
                i += 1;
            }
            while i <= j && self.less(a, at(j)) {
                j -= 1;
            }
            if i > j {
                break;
            }
            self.swap(at(i), at(j));
            i += 1;
            j -= 1;
        }
        at(i)
    }

    fn partial_insertion_sort(&mut self, a: usize, b: usize) -> bool {
        const MAX_STEPS: usize = 5;
        const SHORTEST_SHIFTING: usize = 50;
        let mut i = a + 1;
        for _ in 0..MAX_STEPS {
            while i < b && !self.less(i, i - 1) {
                i += 1;
            }
            if i == b {
                return true;
            }
            if b - a < SHORTEST_SHIFTING {
                return false;
            }
            self.swap(i, i - 1);
            if i - a >= 2 {
                let mut j = i - 1;
                while j >= 1 {
                    if !self.less(j, j - 1) {
                        break;
                    }
                    self.swap(j, j - 1);
                    j -= 1;
                }
            }
            if b - i >= 2 {
                for j in i + 1..b {
                    if !self.less(j, j - 1) {
                        break;
                    }
                    self.swap(j, j - 1);
                }
            }
        }
        false
    }

    fn break_patterns(&mut self, a: usize, b: usize) {
        let length = b - a;
        if length >= 8 {
            let mut random = length as u64;
            let modulus = 1usize << bit_len(length);
            for idx in a + (length / 4) * 2 - 1..=a + (length / 4) * 2 + 1 {
                random ^= random << 13;
                random ^= random >> 7;
                random ^= random << 17;
                let mut other = (random as usize) & (modulus - 1);
                if other >= length {
                    other -= length;
                }
                self.swap(idx, a + other);
            }
        }
    }

    fn choose_pivot(&mut self, a: usize, b: usize) -> (usize, Hint) {
        const SHORTEST_NINTHER: usize = 50;
        const MAX_SWAPS: usize = 4 * 3;
        let l = b - a;
        let mut swaps = 0;
        let mut i = a + l / 4;
        let mut j = a + l / 4 * 2;
        let mut k = a + l / 4 * 3;
        if l >= 8 {
            if l >= SHORTEST_NINTHER {
                i = self.median(i - 1, i, i + 1, &mut swaps);
                j = self.median(j - 1, j, j + 1, &mut swaps);
                k = self.median(k - 1, k, k + 1, &mut swaps);
            }
            j = self.median(i, j, k, &mut swaps);
        }
        match swaps {
            0 => (j, Hint::Increasing),
            MAX_SWAPS => (j, Hint::Decreasing),
            _ => (j, Hint::Unknown),
        }
    }

    fn order2(&mut self, a: usize, b: usize, swaps: &mut usize) -> (usize, usize) {
        if self.less(b, a) {
            *swaps += 1;
            return (b, a);
        }
        (a, b)
    }

    fn median(&mut self, a: usize, b: usize, c: usize, swaps: &mut usize) -> usize {
        let (a, b) = self.order2(a, b, swaps);
        let (b, _) = self.order2(b, c, swaps);
        let (_, b) = self.order2(a, b, swaps);
        b
    }

    fn reverse_range(&mut self, a: usize, b: usize) {
        let (mut i, mut j) = (a, b - 1);
        while i < j {
            self.swap(i, j);
            i += 1;
            j -= 1;
        }
    }
}

#[cfg(test)]
mod tests {
    /// Orders produced by Go's `sort.Sort` (and identically `slices.SortFunc`), generated by
    /// promql-compat `cmd/narthex-golden-gosort`; line format: `kind keys | ids`.
    #[test]
    fn matches_go_sort_on_comparators_that_are_not_total_orders() {
        let text = include_str!("go_sort_cases.txt");
        let mut cases = 0;
        for line in text.lines() {
            let (head, expected) = line.split_once(" | ").unwrap();
            let (kind, keys) = head.split_once(' ').unwrap();
            let kind: u8 = kind.parse().unwrap();
            let parse = |list: &str| -> Vec<usize> {
                list.split(',')
                    .filter(|item| !item.is_empty())
                    .map(|item| item.trim().parse().unwrap())
                    .collect()
            };
            let mut items: Vec<(usize, usize)> = parse(keys).into_iter().zip(0..).collect();
            super::sort_by(&mut items, |&(a, a_id), &(b, b_id)| match kind {
                0 => a < b,
                1 => a == 0 || a < b,
                2 => a % 3 < b % 3,
                _ => (a_id * 31 + b_id * 17) % 5 < 2,
            });
            let actual: Vec<usize> = items.iter().map(|&(_, id)| id).collect();
            assert_eq!(actual, parse(expected), "{line}");
            cases += 1;
        }
        assert!(cases >= 200);
    }
}
