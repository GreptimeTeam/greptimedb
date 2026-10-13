# Object store WAL object fixtures

Frozen objects of the object store WAL, decoded and re-encoded by the format tests in
`src/log-store/src/object_store_wal/format.rs`, which own them:

```shell
cargo nextest run -p log-store -E 'test(test_format_matches_version_1_fixture) | test(test_format_matches_version_2_fixture)'
```

The bytes were derived from the layout described in the module documentation of
`src/log-store/src/object_store_wal.rs`, independently of the encoder, so a change to field
order, endianness or checksum coverage fails the tests even when the encoder and the decoder
change together. The tests never rewrite the files.

## `version_1.wal`

A version 1 object holding entries 1, 2 and 3 of region 1 and entries 10 and 11 of region 2;
279 bytes, SHA-256 `9af401eb6a4fb5c9d6ec69e79362a477ae10431e5e88644914e59aa2b27f8bc7`.

```text
# Header: magic, version 1, object sequence 7, epoch 3, predecessor
# sequence 6, predecessor epoch 2, reclaim boundary 5, header CRC32.
475457414c4f424a
0001
0000000000000007
0000000000000003
0000000000000006
0000000000000002
0000000000000005
118e773f
# Segment of region 1 at offset 54: region id, entry count, then
# (entry id, payload length, payload) per entry.
0000000100000001
00000003
0000000000000001
00000001
61
0000000000000002
00000002
6262
0000000000000003
00000003
636363
# Segment of region 2 at offset 108.
0000000200000001
00000002
000000000000000a
00000001
78
000000000000000b
00000002
7979
# Footer at offset 147: entry count, then per segment region id, min and
# max entry id, entry count, segment offset, length and CRC32.
00000002
0000000100000001
0000000000000001
0000000000000003
00000003
0000000000000036
0000000000000036
25ac0486
0000000200000001
000000000000000a
000000000000000b
00000002
000000000000006c
0000000000000027
65dc08ec
# Trailer: footer offset, footer length, footer CRC32, object CRC32, magic.
0000000000000093
0000000000000064
d80c9f07
65c91839
475457414c54524c
```

## `version_2.wal`

A version 2 object holding entries 1 and 2 of region 1, and the release records of region 1
through entry 2 and of region 2 in full; 213 bytes, SHA-256
`e71d1767147b322d5a645ec04fccc92cf6e8ef8610a2c234d141994890e82893`.

```text
# Header: magic, version 2, object sequence 9, epoch 3, predecessor
# sequence 8, predecessor epoch 3, reclaim boundary 6, header CRC32.
475457414c4f424a
0002
0000000000000009
0000000000000003
0000000000000008
0000000000000003
0000000000000006
e75b9e01
# Segment of region 1 at offset 54.
0000000100000001
00000002
0000000000000001
00000001
61
0000000000000002
00000002
6262
# Footer at offset 93: entry count and the entry of the segment as in
# version 1, then the release record count and, per record, the
# region id and the entry id the region is released through.
00000001
0000000100000001
0000000000000001
0000000000000002
00000002
0000000000000036
0000000000000027
b79527ef
00000002
0000000100000001
0000000000000002
0000000200000001
ffffffffffffffff
# Trailer: footer offset, footer length, footer CRC32, object CRC32, magic.
000000000000005d
0000000000000058
fab0fe5b
30f82508
475457414c54524c
```
