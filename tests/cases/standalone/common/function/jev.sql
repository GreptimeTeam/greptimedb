-- No credentials or external service needed: NULL propagates without an API call.
SELECT jev(NULL, 'The event reports a failed payment.') AS score;

SELECT jev('message', NULL) AS score;

SELECT arrow_typeof(jev(NULL, 'condition')) AS score_type;

SELECT jev_choice(NULL, 'Route the ticket', '{"billing":null}') AS null_text,
       jev_choice('message', NULL, 'invalid JSON') AS null_prompt,
       jev_choice('message', 'Route the ticket', NULL) AS null_criteria;

SELECT jev_score(NULL, 'Rate severity', '["low","high"]') AS null_text,
       jev_score('message', NULL, 'invalid JSON') AS null_prompt,
       jev_score('message', 'Rate severity', NULL) AS null_criteria;

SELECT arrow_typeof(jev_choice(NULL, 'prompt', '{"billing":null}')) AS choice_type,
       arrow_typeof(jev_score(NULL, 'prompt', '["low","high"]')) AS score_type;

-- Validate SQL registration, coercion, and asynchronous filtering on a real table.
CREATE TABLE jev_events (
    occurred_at TIMESTAMP TIME INDEX,
    "service" STRING,
    "message" STRING,
    PRIMARY KEY ("service")
);

INSERT INTO jev_events VALUES
    ('2026-09-19T01:00:00Z', 'payments', NULL),
    ('2026-09-19T02:00:00Z', 'auth', NULL);

SELECT occurred_at, service, message FROM jev_events
WHERE occurred_at >= '2026-09-19T00:00:00Z'
  AND occurred_at < '2026-09-20T00:00:00Z'
  AND service = 'payments'
  AND jev((message), 'The event reports that a payment still failed after retries.') >= 0.8
ORDER BY occurred_at;

SELECT occurred_at,
       jev_choice(message, 'Route the ticket', '{"billing":"Payments","technical":"Errors"}') AS team,
       jev_score(message, 'Rate severity', '["low","medium","high"]') AS severity
FROM jev_events
ORDER BY occurred_at;

DROP TABLE jev_events;

-- The former three-argument Boolean signature is no longer accepted.
SELECT jev('message', 'condition', 0.8);

-- Invalid criteria fail locally before any HTTP request.
SELECT jev_choice('message', 'prompt', 'not json');

SELECT jev_choice('message', 'prompt', '{}');

SELECT jev_score('message', 'prompt', '["only one level"]');

-- Both additional modes require criteria.
SELECT jev_choice('message', 'prompt');

SELECT jev_score('message', 'prompt');
