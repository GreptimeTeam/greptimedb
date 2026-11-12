# pipeline

ETL capability

## VRL host access

VRL host access is disabled by default. The server configuration option
`[pipeline].allow_vrl_host_access = true` restores the full VRL standard library
for trusted pipeline authors. It applies to both stored pipelines and dry-runs,
requires a server restart, and cannot be overridden in a pipeline definition.

With host access disabled, environment, hostname, system timezone, filesystem,
network, and logging functions are unavailable. This includes `get_env_var`,
`get_hostname`, `get_timezone_name`, `http_request`, `dns_lookup`, `reverse_dns`,
`parse_proto`, `encode_proto`, `parse_etld`, `validate_json_schema`, and `log`.
Grok parsing supports inline
aliases but rejects nonempty `alias_sources`. Timestamp parsing and formatting
use UTC by default; an explicit timezone must be a constant named timezone such
as `Asia/Shanghai`, rather than `local`, an empty string, or an event-derived
value. Ordinary time and random-value functions remain available.

Existing pipelines using restricted functions or arguments fail compilation
under the default policy, including when loaded from storage. This option
restricts VRL capabilities; it does not impose CPU or memory limits.

## processors

refer [elastic ingest processor][elastic-ingest-processor] for detail

### Example

Go to [pipeline](../../tests/pipeline.rs)

[elastic-ingest-processor]: https://www.elastic.co/guide/en/elasticsearch/reference/current/processors.html
