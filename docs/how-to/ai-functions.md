# AI functions (experimental MVP)

Three asynchronous SQL scalar functions provide natural-language matching,
classification, and rating. The current backend is TypeSafe's Jev model:

| Function | Mode | Result |
| --- | --- | --- |
| `ai_match(text, prompt)` | Noul | `Float64` probability in **[0, 1]** that the prompt's statement is true |
| `ai_choose(text, prompt, criteria)` | Choice | `String` containing the selected option's name |
| `ai_score(text, prompt, criteria)` | Score | JSONB object containing `score`, `confidence`, and a `probabilities` array |

Use scalar results or extracted JSON fields in `SELECT`, comparisons in `WHERE`,
and `ORDER BY`.
All arguments are SQL strings. Any SQL NULL argument produces SQL `NULL`
without validating that row's criteria or calling the API.

## Start GreptimeDB

The functions are compiled and registered by the **default-enabled Cargo feature
`ai_functions`**. Enable the Jev backend separately in the server process:

```sh
export GREPTIMEDB_EXPERIMENTAL_JEV=true
export JEV_API_KEY='<your TypeSafe API key>'
cargo run -p cmd -- standalone start
```

If your key is already exported in `~/.zshrc`, run `source ~/.zshrc` first.
The optional `JEV_MODEL` defaults to `jev-latest`; `JEV_ENDPOINT` defaults to
`https://api.typesafe.ai/v1/systemone` (the full evaluation endpoint URL).
The MVP uses environment variables rather than TOML configuration.
The Cargo feature includes all three SQL functions in the build; the runtime environment
variables enable and configure its API calls. Default compilation does not turn
on external API calls. To enable the feature explicitly, use
`--features ai_functions`.

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

### Matching probability (Noul)

```sql
SELECT occurred_at, service, message
FROM events
WHERE occurred_at >= '2026-09-19T00:00:00Z'
  AND occurred_at <  '2026-09-20T00:00:00Z'
  AND service = 'payments'
  AND ai_match(
    (message),
    'The event reports that a payment still failed after retries.'
  ) >= 0.8
ORDER BY occurred_at;
```

To return the scores and rank matching events:

```sql
SELECT occurred_at, message,
       ai_match(message, 'The event reports that a payment still failed after retries.') AS score
FROM events
WHERE occurred_at >= '2026-09-19T00:00:00Z'
  AND occurred_at <  '2026-09-20T00:00:00Z'
  AND service = 'payments'
ORDER BY score DESC NULLS LAST;
```

Both arguments are strings; larger scores indicate a stronger match.
`(message)` is an ordinary parenthesized string expression. For several columns,
combine them explicitly, for example `concat(service, ': ', message)`.
Any null argument produces SQL `NULL`, which `WHERE` excludes, without an API call.

### Classification (Choice)

`criteria` is a JSON object with **1 to 255 options**. Each key is an option name;
its value is a description (string, object, or array), or JSON `null` if the name
is sufficient. The function returns the selected key, suitable for filtering
or grouping:

```sql
SELECT occurred_at, message,
       ai_choose(message, 'Which team should handle this event?',
                  '{"billing":"Payments, invoices, refunds","technical":"Bugs and outages","other":null}') AS team
FROM events
WHERE occurred_at >= '2026-09-19T00:00:00Z'
  AND occurred_at <  '2026-09-20T00:00:00Z';
```

### Rating (Score)

`criteria` is a JSON array of **2 to 10 ordered level descriptions**, from low
to high. Descriptions can be strings, objects, or arrays. Level numbers start
at zero. The function returns a JSONB object from one model evaluation:

```json
{
  "score": 1.05,
  "confidence": 0.92,
  "probabilities": [0.0, 0.95, 0.05]
}
```

- `score` is the provider's probability-weighted mean of the level numbers, in
  `[0, N − 1]` for N levels. It may fall between levels.
- `confidence` is the provider's confidence in `[0, 1]`, reflecting how concentrated
  the distribution is. It is not a guarantee that the answer is correct.
- `probabilities[i]` is the probability of `criteria[i]`, with exactly N entries in
  level order. These are individual level probabilities, not cumulative probabilities.

Different distributions can have the same score. `[0, 1, 0]` and `[0.5, 0, 0.5]`
both have score `1`: the first concentrates on the middle level, while the second
is split between the extremes. Inspect confidence and the distribution before
treating a mean score as a definite severity level.

For example, compute the rating once, extract numeric fields, and rank only rows
meeting a chosen confidence threshold:

```sql
SELECT occurred_at, message,
       json_get_float(rating, 'score') AS severity,
       json_get_float(rating, 'confidence') AS confidence
FROM (
    SELECT occurred_at, message,
           ai_score(message, 'How severe is this event?',
                    '["No impact to functionality","Degraded service with a workaround","Blocking issue with no workaround"]') AS rating
    FROM events
    WHERE occurred_at >= '2026-09-19T00:00:00Z'
      AND occurred_at <  '2026-09-20T00:00:00Z'
) AS rated
WHERE json_get_float(rating, 'confidence') >= 0.8
ORDER BY severity DESC NULLS LAST;
```

Use `json_to_string(rating)` to display the full object, or
`json_get_float(rating, 'probabilities[2]')` to read the probability of level 2.
The array positions correspond to the supplied criteria, which provide the level
descriptions.

## Reusing an evaluation

AI functions are marked volatile to prevent HTTP calls during planning. Identical
calls written separately in `SELECT` and `WHERE` are evaluated separately. If the
filter evaluates N non-null rows and M rows pass to the projection, this can cost
N + M requests, reaching 2N when all rows pass.

To return a result and filter by it, compute it once in a subquery and reference
its alias in the outer query:

```sql
SELECT occurred_at, message, score
FROM (
    SELECT occurred_at, message,
           ai_match(message, 'The event reports that a payment still failed after retries.') AS score
    FROM events
    WHERE occurred_at >= '2026-09-19T00:00:00Z'
      AND occurred_at <  '2026-09-20T00:00:00Z'
      AND service = 'payments'
) AS scored
WHERE score >= 0.8
ORDER BY score DESC;
```

This also applies to `ai_choose` and `ai_score`. Extract multiple JSON fields from
the aliased `ai_score` result, as in the rating example above, rather than calling
the model again for each field. Filter pushdown preserves the volatile projection
instead of duplicating it into the outer predicate.

## MVP behavior

- Each non-null row makes one HTTP request per function invocation: the text is
  `state`, and the prompt is the question's `instructions`. Choice and Score pass
  the parsed JSON as `criteria`. Calls to different functions are separate requests.
- Criteria for all non-null rows in a batch are validated before sending that
  batch's requests. Malformed JSON, invalid description types, or invalid option/
  level counts fail the query locally.
- Answers must have the requested question type. Noul probabilities outside
  `[0, 1]`, Choice labels not in the criteria, and Score values outside `[0, N − 1]`
  fail the query rather than being clamped or replaced with NULL. Score responses
  must also contain a numeric confidence in `[0, 1]` and probabilities for every
  level. Each probability must be numeric and in `[0, 1]`; their sum must be within
  `1e-6` of 1. Provider values are preserved without renormalizing the distribution
  or recomputing the score.
- Up to eight requests run concurrently per expression/batch invocation, not per
  query or process. Concurrent partitions and queries can exceed eight requests
  in total. Each request has a 30-second timeout. Errors (including rate limits
  and invalid responses) fail the query. There is no automatic retry or
  cross-query cache.
- Start with small, time-bounded queries. Ordinary SQL predicates can reduce the
  candidate set, but SQL does not guarantee left-to-right predicate evaluation or
  that `LIMIT` bounds the number of API calls.
- Text passed to the function is sent to the configured TypeSafe endpoint.
  The API key remains in the server environment, not in SQL.
- This MVP is intended for standalone use. A distributed deployment would need
  the environment on every node evaluating the function and separate validation.

API reference: <https://docs.typesafe.ai/api>

## Before stabilizing

The experimental MVP still needs the following controls before stabilization:

- A process-wide concurrency limit shared by AI function invocations, such as a semaphore.
- Bounded retries with exponential backoff for HTTP `429` and `529`, following
  TypeSafe's rate-limit guidance.
- Request budgets and metrics for API calls, latency, retries, and rate-limit errors.

These are follow-up work, not guarantees provided by the current implementation.

## Validation

Check both the default build (where all three functions are registered) and an isolated
`common-function` build without default features. The regular AI function tests use a local
HTTP server and need no API key:

```sh
cargo nextest run -p common-function
cargo nextest run -p common-function --no-default-features
cargo nextest run -p common-function --no-default-features --features ai_functions
```

The sqlness runner explicitly includes `ai_functions` when building its test binary:

```sh
cargo sqlness bare -t ai_functions
```

If using `--bins-dir`, provide a binary built with `ai_functions` (included by
default). CI covers the AI-enabled path through the regular unit tests and
sqlness tests; the feature-off checks above can be run locally when needed.

An opt-in test calls the real service on synthetic payment events:

```sh
source ~/.zshrc
GREPTIMEDB_EXPERIMENTAL_JEV=true cargo nextest run -p common-function \
  -E 'test(test_ai_match_live)' --run-ignored ignored-only
```
