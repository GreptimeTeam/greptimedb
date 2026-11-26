SHOW CREATE TABLE repart_owned_range;

-- Every row survives the upgrade exactly once.
SELECT * FROM repart_owned_range ORDER BY ts;

-- Rows of the split partition are read through references that own a range.
SELECT * FROM repart_owned_range WHERE host >= 'h1' AND host < 'h2' ORDER BY ts;

-- Duplicated or dropped rows show up as counts other than one.
SELECT host, count(*) AS cnt FROM repart_owned_range GROUP BY host ORDER BY host;
