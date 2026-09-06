-- Direct calculation from each state persisted by the old binary.
SELECT grp, stddev_pop_calc(state) AS stddev
FROM stddev_states
ORDER BY grp;

-- Merge all persisted states with the new binary before calculating.
SELECT stddev_pop_calc(stddev_pop_merge(state)) AS stddev
FROM stddev_states;
