# Datanode epoch record

`value.json` is the value metasrv stores under `__datanode_epoch/{node_id}`: the highest
`node_epoch` (the process start time in milliseconds) the metasrv leader has accepted for that
datanode. It has no trailing newline.

The test checks that the frozen value still decodes and re-encodes byte for byte, and that the
key layout is unchanged:

```shell
cargo nextest run -p common-meta -E 'test(test_frozen_datanode_epoch_record)'
```
