I hereby agree to the terms of the [GreptimeDB CLA](https://github.com/GreptimeTeam/.github/blob/main/CLA.md).

## Refer to a related PR or issue link (optional)

- RFC PR for remote dynamic filter propagation design

## What's changed and what's your intention?

This PR adds a new RFC for **remote dynamic filter propagation**.

The RFC explains why frontend-produced dynamic filters should be propagated to remote datanode scans in distributed queries, and proposes a minimal design that keeps remote dynamic filters as an **optimization only**, never a correctness dependency.

The document covers the main design points:

- end-to-end flow from join-produced alive dynamic filters to remote scan updates
- the identity model based on `query_id + filter_id`
- reusing the existing region unary RPC path for `update / unregister`
- a frontend query-engine runtime registry keyed by `query_id`
- registry lifecycle and cleanup semantics
- failure handling and safe degradation behavior
- alternatives that were considered and rejected

This is a documentation / design PR only. It does **not** implement the feature itself, and it does not change the current API, schema, or runtime behavior.

Current limitations described by the RFC:

- only the current minimal design is covered
- larger build-side membership propagation is left for later work
- some runtime policies, such as handling updates that arrive before scan registration, are still open questions

## PR Checklist
Please convert it to a draft if some of the following conditions are not met.

- [ ] I have written the necessary rustdoc comments.
- [ ] I have added the necessary unit tests and integration tests.
- [x] This PR requires documentation updates.
- [x] API changes are backward compatible.
- [x] Schema or data changes are backward compatible.
