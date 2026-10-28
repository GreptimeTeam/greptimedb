# FileMeta JSON path index compatibility

`legacy_file_meta.json` has the column-level index metadata written before
`json_path_indexes` was added. The `mito2` test
`test_file_meta_json_path_indexes_serde` checks that the missing field remains
unknown, while newly written empty and populated lists round-trip distinctly.

Run with `rtk cargo test -p mito2 test_file_meta_json_path_indexes_serde --lib`.
