-- ADD COLUMN must keep the column options that CREATE TABLE keeps; both SHOW CREATE TABLE
-- outputs below list the same column definitions. ADD COLUMN rejects inline indexes.
CREATE TABLE add_col_options (ts TIMESTAMP TIME INDEX, host STRING PRIMARY KEY);

ALTER TABLE add_col_options ADD COLUMN az STRING PRIMARY KEY COMMENT 'availability zone';

ALTER TABLE add_col_options ADD COLUMN body STRING NOT NULL DEFAULT '' COMMENT 'log body';

ALTER TABLE add_col_options ADD COLUMN prompt STRING COMPRESSION WITH (level = 5, page_size = '1MiB');

SHOW CREATE TABLE add_col_options;

INSERT INTO add_col_options (ts, host, az, prompt) VALUES (1, 'h1', 'az1', 'hello');

ADMIN flush_table('add_col_options');

SELECT * FROM add_col_options;

CREATE TABLE create_col_options (
    ts TIMESTAMP TIME INDEX,
    host STRING,
    az STRING COMMENT 'availability zone',
    body STRING NOT NULL DEFAULT '' COMMENT 'log body',
    prompt STRING COMPRESSION WITH (level = 5, page_size = '1MiB'),
    PRIMARY KEY (host, az),
);

SHOW CREATE TABLE create_col_options;

DROP TABLE add_col_options;

DROP TABLE create_col_options;
