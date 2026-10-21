# Columnar series version 1 fixture

`part-0.series` and `manifest.json` are frozen compatibility inputs. The independent
Python generator uses explicit struct packing and an independent XXH64 implementation.
Run `python3 generate.py` from this directory only when
establishing a new fixture, not to accommodate a changed reader.

The uncompressed part has two leaves and an internal root. Its ordered rows are:

| Table | TSID | Row count | tag_0 | tag_1 |
| --- | --- | --- | --- | --- |
| 7 | 10 | 2 | null | null |
| 7 | 11 | 3 | empty string | null |
| 9 | 5 | 1 | é | null |

The tag_1 column is omitted from both blocks. Tests load the bytes through OpenDAL
and the public format boundary, validate exact values, exercise selective reads,
and ensure a stream pins the artifact after the reader is dropped.

Run `cargo nextest run -p mito2 series_index::columnar`.
