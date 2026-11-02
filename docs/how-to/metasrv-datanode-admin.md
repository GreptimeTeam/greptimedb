# Datanode administration over HTTP

These endpoints are served under `/admin` on the metasrv HTTP server. Send requests
to the leader. Like the metasrv gRPC procedure service, they use
`Metasrv::is_leader()`; a single metasrv without election is the leader. A follower
returns HTTP 503 with the shared JSON error body. These endpoints are not added to
the deprecated admin service on the RPC port.

## Drain a datanode

```http
POST /admin/datanode/drain
Content-Type: application/json

{"node_id": 1, "target_node_id": 2}
```

Omit `target_node_id` to select destinations automatically. Both IDs are unsigned
64-bit integers. Source and destination must differ. This endpoint uses the drain
procedure defaults: parallelism 4, `TryAll`, and a total budget of 20 minutes.

The procedure marks the source unschedulable and migrates its leader regions.
Follower replicas remain on the source, following the existing drain procedure's
contract. HTTP 200 acknowledges submission, not completion:

```json
{"procedure_id": "00000000-0000-0000-0000-000000000001"}
```

While the drain is running, repeating the same request returns its procedure ID,
including after procedure recovery. A request with a different destination returns
HTTP 409. Retries do not extend the original deadline. Once the node is
unschedulable and has no remaining leader regions, repeating the request returns
`{"procedure_id": null}` without submitting another procedure. A failed drain can
be submitted again to process remaining regions.

## Query a procedure

```http
GET /admin/procedure/status?procedure_id=00000000-0000-0000-0000-000000000001
```

```json
{"procedure_id": "00000000-0000-0000-0000-000000000001", "status": "Done", "error": null}
```

Statuses follow the procedure state names: `Running`, `Retrying`,
`PrepareRollback`, `RollingBack`, `Done`, `Failed`, and `Poisoned`. An error state
includes its error message. Unknown or garbage-collected IDs return HTTP 404;
malformed IDs return HTTP 400. `/admin/procedure-manager/status` remains the
separate endpoint for the procedure manager's running/paused switch.

## Query or restore scheduling

```http
GET /admin/datanode/scheduling/status?node_id=1
```

```json
{"node_id": 1, "schedulable": false}
```

To allow new region placement after the drain finishes:

```http
POST /admin/datanode/mark-schedulable
Content-Type: application/json

{"node_id": 1}
```

The response is `{"node_id": 1, "schedulable": true}`. Repeated requests succeed.
Restoring scheduling while a drain owns the node returns HTTP 409. Scheduling
state is independent of liveness: a missing scheduling record means schedulable;
these endpoints do not register a datanode or establish that it is online.
