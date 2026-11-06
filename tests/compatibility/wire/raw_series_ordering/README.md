# Raw-series ordering schema metadata compatibility

`v2_metadata.json` is a hand-authored, frozen set of Arrow schema metadata maps
for the v2 series frame. It records the established layout/content/labels/time
metadata and mandatory ordering entry, for raw frames and for evaluated ones.
The Rust unit test parses these literal maps through `series_frame_ordering` and
through `SeriesFrameDeclaration::parse`; the fixture is not produced by the
schema builder. It covers the original `contiguous` and `read_key_then_time`
values, not the newer `interleaved` value.

Run the focused checks for the frame codec, streaming adapter, and fixture:

```shell
cargo nextest run -p datatypes -E 'test(series::frame)'
cargo nextest run -p common-query -E 'test(series_frame)'
```

The fixture proves that missing ordering metadata and unknown values are rejected,
the two original values decode, and that a whole declaration is accepted or
refused consistently with its layout and content: a `v3` layout, an unregistered
`greptime.series.*` key, a raw frame that declares a grid, and any content/time or
content/labels contradiction are refusals. Evaluated declarations use explicit
`contiguous` ordering; streaming round trips for evaluated frames are covered by
the crate's evaluated tests.

Because the declaration is cross-checked, a frame that names `evaluated` content
is only an evaluated frame when it also declares `explicit` labels, `grid_spans`
time, and a `greptime.series.grid`. `read_key_then_time` parses as a declaration
(a raw frame may legitimately claim it), but no writer produces one on an
evaluated frame: computed values have no storage key to be ordered by, so both the
schema builder and `unpack_evaluated_series` refuse it. The fixture keeps a
hand-written evaluated frame that states it and verifies that declaration
parsing refuses it.

Raw frames may also declare `greptime.series.ordering=interleaved`, using the full
spelling. The raw streaming adapter supports sequential independently closed
producer fragment chains under that declaration. A key may recur across fragments
with overlapping or reversed timestamps; `is_last` closes a producer fragment, not
necessarily the logical series. The adapter rejects switching keys while a fragment
is still open and an unfinished fragment at EOF, so it does not support arbitrary
multiplexing of open chains. Evaluated frames reject `interleaved`. The direct
codec tests and streaming pack/Arrow IPC/unpack tests cover this value; the frozen
fixture does not.

These tests do not prove interoperability with an old binary or test an end-to-end
client or service. Older readers that do not recognize `interleaved` reject it;
remote acceptance must not be assumed. The pack/IPC tests check declared stream and
batch schema agreement for all three raw orderings, including empty streams, and
check the same for evaluated frames.
