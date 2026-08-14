-- (1) direct uddsketch_calc of each persisted legacy row
SELECT grp, uddsketch_calc(0.5, state) AS p50
FROM uddsketch_states
ORDER BY grp;

-- (2) uddsketch_merge over the persisted legacy rows, then calc.
--    Succeeds only if every legacy row decodes to the exact declared
--    parameters (128, 0.01); the merge parameter check is the guard.
SELECT uddsketch_calc(0.5, uddsketch_merge(128, 0.01, state)) AS p50
FROM uddsketch_states;

-- (3) generate fresh canonical states with the new binary and merge mixed legacy + canonical rows
CREATE TABLE uddsketch_new_states (
  state BINARY,
  grp INT PRIMARY KEY,
  ts TIMESTAMP TIME INDEX DEFAULT now()
);

INSERT INTO uddsketch_new_states (state, grp)
SELECT uddsketch_state(128, 0.01, val), seq_id / 5 * 5 AS grp
FROM uddsketch_values
GROUP BY grp;

SELECT uddsketch_calc(0.5, uddsketch_merge(128, 0.01, state)) AS p50
FROM (
  SELECT state FROM uddsketch_states
  UNION ALL
  SELECT state FROM uddsketch_new_states
) AS all_states;

-- (4) canonical-only control
SELECT uddsketch_calc(0.5, uddsketch_merge(128, 0.01, state)) AS p50
FROM uddsketch_new_states;
