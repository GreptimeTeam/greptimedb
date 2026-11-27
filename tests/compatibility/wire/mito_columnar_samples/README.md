# Mito columnar samples fixtures

`v1-manifest.json` and `v1-part.hex` archive the superseded per-part layout.
The shared samples index is documented in [mito-samples.md](../../../../docs/formats/mito-samples.md).

`legacy-catalog-entry.json` and `daily-catalog-entry.json` freeze the optional
source-coverage extension, exercised by `frozen_sample_catalog_coverage`. Empty
object lists isolate catalog decoding. Legacy entries cover one SST. Daily entries cover the entire
listed source set and become ineligible if any source disappears.

`sparse-daily-catalog-entry.json` freezes explicit populated-day coverage per source.
Older daily catalogs decode with empty coverage and are rebuilt before reuse.
The source Parquet files and sample payload encoding are unchanged.
