CREATE TABLE log (
    ts TIMESTAMP TIME INDEX,
    msg STRING FULLTEXT,
);

SHOW CREATE TABLE log;

DROP TABLE log;


CREATE TABLE log_with_opts (
    ts TIMESTAMP TIME INDEX,
    msg TEXT FULLTEXT WITH (analyzer='English', case_sensitive='true'),
);

SHOW CREATE TABLE log_with_opts;

DROP TABLE log_with_opts;


CREATE TABLE log_multi_fulltext_cols (
    ts TIMESTAMP TIME INDEX,
    msg TINYTEXT FULLTEXT,
    msg2 VARCHAR FULLTEXT,
);

SHOW CREATE TABLE log_multi_fulltext_cols;

DROP TABLE log_multi_fulltext_cols;


CREATE TABLE log_dup_fulltext_opts (
    ts TIMESTAMP TIME INDEX,
    msg TEXT FULLTEXT FULLTEXT,
);

CREATE TABLE log_with_invalid_type (
    ts TIMESTAMP TIME INDEX,
    msg INT FULLTEXT,
);

CREATE TABLE log_with_invalid_option (
    ts TIMESTAMP TIME INDEX,
    msg TEXT FULLTEXT WITH (analyzer='English', invalid_option='true'),
);
