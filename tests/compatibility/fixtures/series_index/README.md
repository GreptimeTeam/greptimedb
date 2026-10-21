# Series-index catalog compatibility

`legacy_catalog.json` is a synthetic fixture matching `SeriesIndexEntry` at
`3ad5f963285e51166fd8789920e4b3bf2cf60423`, before optional artifact metadata.
It is not captured from a released binary. Its entry must remain a reusable
Parquet index and serialize without adding an artifact field.

Run `cargo nextest run -p mito2 -E 'test(legacy_series_catalog_fixture)'`.
The owning test also checks the persisted artifact format names. Ordinary
index-reader tests exercise Parquet files without a full-primary-key column.
