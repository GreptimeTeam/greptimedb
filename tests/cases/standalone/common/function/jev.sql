-- No credentials or external service needed: NULL propagates without an API call.
SELECT jev(NULL, 'The event reports a failed payment.', 0.8) AS matched;

SELECT jev('message', NULL, 0.8) AS matched;

SELECT jev('message', 'The event reports a failed payment.', NULL) AS matched;

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
  AND jev((message), 'The event reports that a payment still failed after retries.', 0.8)
ORDER BY occurred_at;

DROP TABLE jev_events;

-- Invalid thresholds fail locally before any HTTP request.
SELECT jev('message', 'condition', -0.1);

SELECT jev('message', 'condition', 1.1);
