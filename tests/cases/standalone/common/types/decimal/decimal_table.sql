CREATE TABLE decimals(d DECIMAL(18,1) , ts timestamp time index);

INSERT INTO decimals VALUES (99000000000000000.0, 1000);

SELECT d + 1 FROM decimals;

SELECT d + 1000000000000000.0 FROM decimals;

SELECT -1 - d FROM decimals;

SELECT -1000000000000000.0 - d FROM decimals;

SELECT 1 * d FROM decimals;

SELECT 2 * d FROM decimals;

DROP TABLE decimals;
