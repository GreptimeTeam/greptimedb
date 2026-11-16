# DROP DATABASE procedure fixtures

`legacy_executor.json` freezes the single-table executor layout from f8d8fe1071.
`common-meta::ddl::tests::drop_database::test_drop_database_legacy_state_fixture`
checks recovery and serialization without requiring new fields. Existing executors
retain their single-table behavior; new logical batches use a separate state tag.
Older binaries cannot resume the new batch state: drain these procedures before
rolling back metasrv. This fixture verifies upgrade recovery, not downgrade support.
