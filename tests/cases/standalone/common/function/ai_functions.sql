-- No credentials or external service needed: NULL propagates without an API call.
SELECT ai_match(NULL, 'The event reports a failed payment.') AS score;

SELECT ai_match('message', NULL) AS score;

SELECT arrow_typeof(ai_match(NULL, 'condition')) AS score_type;

SELECT ai_choose(NULL, 'Route the ticket', '{"billing":null}') AS null_text,
       ai_choose('message', NULL, 'invalid JSON') AS null_prompt,
       ai_choose('message', 'Route the ticket', NULL) AS null_criteria;

SELECT ai_score(NULL, 'Rate severity', '["low","high"]') AS null_text,
       ai_score('message', NULL, 'invalid JSON') AS null_prompt,
       ai_score('message', 'Rate severity', NULL) AS null_criteria;

SELECT arrow_typeof(ai_choose(NULL, 'prompt', '{"billing":null}')) AS choice_type,
       arrow_typeof(ai_score(NULL, 'prompt', '["low","high"]')) AS score_type;

SELECT json_to_string(rating) AS rating,
       json_get_float(rating, 'score') AS score,
       json_get_float(rating, 'confidence') AS confidence,
       json_get_float(rating, 'probabilities[2]') AS high_probability
FROM (SELECT ai_score(NULL, 'prompt', '["low","medium","high"]') AS rating) AS rated;

-- Validate SQL registration, coercion, and asynchronous filtering on a real table.
CREATE TABLE ai_events (
    occurred_at TIMESTAMP TIME INDEX,
    "service" STRING,
    "message" STRING,
    PRIMARY KEY ("service")
);

INSERT INTO ai_events VALUES
    ('2026-09-19T01:00:00Z', 'payments', NULL),
    ('2026-09-19T02:00:00Z', 'auth', NULL);

SELECT occurred_at, service, message FROM ai_events
WHERE occurred_at >= '2026-09-19T00:00:00Z'
  AND occurred_at < '2026-09-20T00:00:00Z'
  AND service = 'payments'
  AND ai_match((message), 'The event reports that a payment still failed after retries.') >= 0.8
ORDER BY occurred_at;

SELECT occurred_at,
       ai_choose(message, 'Route the ticket', '{"billing":"Payments","technical":"Errors"}') AS team,
       ai_score(message, 'Rate severity', '["low","medium","high"]') AS rating
FROM ai_events
ORDER BY occurred_at;

DROP TABLE ai_events;

-- Matching uses two arguments; thresholds are expressed as SQL comparisons.
SELECT ai_match('message', 'condition', 0.8);

-- Invalid criteria fail locally before any HTTP request.
SELECT ai_choose('message', 'prompt', 'not json');

SELECT ai_choose('message', 'prompt', '{}');

SELECT ai_score('message', 'prompt', '["only one level"]');

-- Both additional modes require criteria.
SELECT ai_choose('message', 'prompt');

SELECT ai_score('message', 'prompt');
