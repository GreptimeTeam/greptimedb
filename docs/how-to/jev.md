# Natural-language filtering with Jev (experimental MVP)

`jev(text, statement, threshold)` returns whether Jev judges the statement true
for the text with probability **greater than or equal to** the threshold.
It is an asynchronous SQL scalar function usable in `WHERE` and `SELECT`.

## Start GreptimeDB

Set these environment variables in the process that starts GreptimeDB:

```sh
export GREPTIMEDB_EXPERIMENTAL_JEV=true
export JEV_API_KEY='<your TypeSafe API key>'
cargo run -- standalone start
```

If your key is already exported in `~/.zshrc`, run `source ~/.zshrc` first.
The optional `JEV_MODEL` defaults to `jev-latest`; `JEV_ENDPOINT` defaults to
`https://api.typesafe.ai/v1/systemone` (the full evaluation endpoint URL).
The MVP uses environment variables rather than TOML configuration.

## Query

For a new example table, quote `service` and `message` in the DDL because the
current parser treats them as keywords:

```sql
CREATE TABLE events (
    occurred_at TIMESTAMP TIME INDEX,
    "service" STRING,
    "message" STRING,
    PRIMARY KEY ("service")
);
```

```sql
SELECT occurred_at, service, message
FROM events
WHERE occurred_at >= '2026-09-19T00:00:00Z'
  AND occurred_at <  '2026-09-20T00:00:00Z'
  AND service = 'payments'
  AND jev(
    (message),
    'The event reports that a payment still failed after retries.',
    0.8
  )
ORDER BY occurred_at;
```

The first two arguments are strings and the third is a number in `[0, 1]`.
`(message)` is an ordinary parenthesized string expression. For several columns,
combine them explicitly, for example `concat(service, ': ', message)`.
Any null argument produces SQL `NULL`, which `WHERE` excludes, without an API call.

## MVP behavior

- Each non-null row makes one HTTP request: the text is `state`, and the statement
  is a `noul` question's `instructions`. The returned `noul` probability is compared
  with the threshold locally; this is not the separate Choice/Score confidence.
- Up to eight requests run concurrently per evaluated batch, with a 30-second
  timeout per request. Errors (including rate limits and invalid responses) fail
  the query. There is no automatic retry or cross-query cache.
- Start with small, time-bounded queries. Ordinary SQL predicates can reduce the
  candidate set, but SQL does not guarantee left-to-right predicate evaluation or
  that `LIMIT` bounds the number of API calls.
- Text passed to the function is sent to the configured TypeSafe endpoint.
  The API key remains in the server environment, not in SQL.
- This MVP is intended for standalone use. A distributed deployment would need
  the environment on every node evaluating the function and separate validation.

API reference: <https://docs.typesafe.ai/api>

## Validation

The regular crate tests use a local HTTP server and need no API key:

```sh
cargo nextest run -p common-function
```

An opt-in test calls the real service on synthetic payment events:

```sh
source ~/.zshrc
GREPTIMEDB_EXPERIMENTAL_JEV=true cargo nextest run -p common-function \
  -E 'test(test_jev_live)' --run-ignored ignored-only
```
