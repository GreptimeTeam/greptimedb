---
Feature Name: Inverted Index for SST File
Tracking Issue: TBD
Date: 2023-11-03
Author: "Zhong Zhenchi <zhongzc_arch@outlook.com>"
---

# Summary
This RFC proposes an optimization towards the storage engine by introducing an inverted indexing methodology aimed at optimizing label selection queries specifically pertaining to Metrics with tag columns as the target for optimization.

# Introduction
In the current system setup, in the Mito Engine, the first column of Primary Keys has a Min-Max index, which significantly optimizes the outcome. However, there are limitations when it comes to other columns, primarily tags. This RFC suggests the implementation of an inverted index to provide enhanced filtering benefits to bridge these limitations and improve overall system performance.

# Design Detail

## Inverted Index

The primary aim of the proposed inverted index is to optimize tag columns in the SST Parquet Files within the Mito Engine. The mapping and construction of an inverted index, from Tag Values to Row Groups, enables efficient logical structures that provide faster and more flexible queries.

When scanning SST Files, pushed-down filters applied to a respective Tag's inverted index, determine the final Row Groups to be indexed and scanned, further bolstering the speed and efficiency of data retrieval processes.

## Index Format

The Inverted Index for each SST file follows the format shown below:

```
inverted_index₀ inverted_index₁ ... inverted_indexₙ footer
```

The structure inside each Inverted Index is as followed:

```
bitmap₀ bitmap₁ bitmap₂ ... bitmapₙ null_bitmap fst
```

The format is encapsulated by a footer:

```
footer_payload footer_payload_size
```

The `footer_payload` is presented in protobuf encoding of `InvertedIndexFooter`.

The complete format is containerized in [Puffin](https://iceberg.apache.org/puffin-spec/) with the type defined as `greptime-inverted-index-v1`.

## Protobuf Details

The `InvertedIndexFooter` is defined in the following protobuf structure:

```protobuf
message InvertedIndexFooter {
    repeated InvertedIndexMeta metas;
}

message InvertedIndexMeta {
    string name;
    uint64 row_count_in_group;
    uint64 fst_offset;
    uint64 fst_size;
    uint64 null_bitmap_offset;
    uint64 null_bitmap_size;
    InvertedIndexStats stats;
}

message InvertedIndexStats {
    uint64 null_count;
    uint64 distinct_count;
    bytes min_value;
    bytes max_value;
}
```

## Bitmap

Bitmaps are used to represent indices of fixed-size groups. Rows are divided into groups of a fixed size, defined in the `InvertedIndexMeta` as `row_count_in_group`.

For example, when `row_count_in_group` is `4096`, it means each group has `4096` rows. If there are a total of `10000` rows, there will be `3` groups in total. The first two groups will have `4096` rows each, and the last group will have `1808` rows. If the indexed values are found in row `200` and `9000`, they will correspond to groups `0` and `2`, respectively. Therefore, the bitmap should show `0` and `2`.

Bitmap is implemented using [BitVec](https://docs.rs/bitvec/latest/bitvec/), selected due to its efficient representation of dense data arrays typical of indices of groups.


## Finite State Transducer (FST)

[FST](https://docs.rs/fst/latest/fst/) is a highly efficient data structure ideal for in-memory indexing. It represents ordered sets or maps where the keys are bytes. The choice of the FST effectively balances the need for performance, space efficiency, and the ability to perform complex analyses such as regular expression matching.

The conventional usage of FST and `u64` values has been adapted to facilitate indirect indexing to row groups. As the row groups are represented as Bitmaps, we utilize the `u64` values split into bitmap's offset (higher 32 bits) and size (lower 32 bits) to represent the location of these Bitmaps. 

## API Design

Two APIs `InvertedIndexBuilder` for building indexes and  `InvertedIndexSearcher` for querying indexes are designed:

```rust
type Bytes = Vec<u8>;
type GroupId = u64;

trait InvertedIndexBuilder {
    fn add(&mut self, name: &str, value: Option<&Bytes>, group_id: GroupId) -> Result<()>;
    fn finish(&mut self) -> Result<()>;
}

enum Predicate {
    Gt(Bytes),
    GtEq(Bytes),
    Lt(Bytes),
    LtEq(Bytes),
    InList(Vec<Bytes>),
    RegexMatch(String),
}

trait InvertedIndexSearcher {
    fn search(&mut self, name: &str, predicates: &[Predicate]) -> Result<impl IntoIterator<GroupId>>;
}
```
