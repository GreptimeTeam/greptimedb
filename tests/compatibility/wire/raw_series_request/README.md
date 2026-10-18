# Raw-series request control v1

`v1_request.json` is a hand-authored frozen JSON example consumed by the common-query request tests. The version-1 envelope and logical-tag objects reject unknown fields, and unsupported control versions are rejected. Physical metadata keeps its existing deserializer and compatibility rules. A legacy request without the reserved marker remains an ordinary request. A new client talking to an old server may receive an ordinary-schema response, which the raw unpacker rejects; this fixture does not claim cross-version raw execution support.
