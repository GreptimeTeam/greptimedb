# Repartition read-readiness route fixture

`route.json` freezes the existing JSON route envelope with the allocation-only
`greptime:repartition-pending-read` attribute. Its presence rejects new query planning until every target manifest and the retained source have completed the handoff.
Routes without the attribute remain readable; unknown values also mark a pending handoff.

The `common-meta` test `test_repartition_pending_read_route_roundtrip` checks this
fixture, old routes without the attribute, and unrelated-attribute preservation.
The query test `test_repartition_transition_rejects_reads_until_handoff_complete` also exercises
KV decoding and the route cache. SQL restart cases cannot pause this intermediate
procedure state reliably.

Older frontends preserve the attribute but do not honor it. Upgrade both query frontends and datanodes before relying on this protection during concurrent reads and repartition. New frontends send the partition version captured during planning; new datanodes check it against the pinned scan snapshot. Legacy requests without that version keep their previous behavior.
