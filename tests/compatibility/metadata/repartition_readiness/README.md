# Repartition read-readiness route fixture

`route.json` freezes the existing JSON route envelope with the allocation-only
`greptime:repartition-pending-read` attribute. Its presence hides a newly allocated
target from query planning until metric logical-region synchronization completes.
Routes without the attribute remain readable; unknown values remain hidden.

The `common-meta` test `test_repartition_pending_read_route_roundtrip` checks this
fixture, old routes without the attribute, and unrelated-attribute preservation.
The query test `test_pending_repartition_targets_excluded_from_reads` also exercises
KV decoding and the route cache. SQL restart cases cannot pause this intermediate
procedure state reliably.

Older frontends preserve the attribute but do not honor it. Upgrade query frontends
before relying on this protection during concurrent reads and repartition.
