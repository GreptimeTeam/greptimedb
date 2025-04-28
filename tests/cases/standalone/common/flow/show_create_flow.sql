CREATE TABLE numbers_input_show (
    number INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(number),
    TIME INDEX(ts)
);
create table out_num_cnt_show (
    number INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP TIME INDEX,    
    PRIMARY KEY(number),
);


SELECT flow_name, table_catalog, flow_definition, source_table_names FROM INFORMATION_SCHEMA.FLOWS WHERE flow_name='filter_numbers_show';

SHOW FLOWS LIKE 'filter_numbers_show';

CREATE FLOW filter_numbers_show SINK TO out_num_cnt_show AS SELECT number FROM numbers_input_show where number > 10;

SHOW CREATE FLOW filter_numbers_show;

SELECT flow_name, table_catalog, flow_definition, source_table_names FROM INFORMATION_SCHEMA.FLOWS WHERE flow_name='filter_numbers_show';

SHOW FLOWS LIKE 'filter_numbers_show';

drop flow filter_numbers_show;

SELECT flow_name, table_catalog, flow_definition, source_table_names FROM INFORMATION_SCHEMA.FLOWS WHERE flow_name='filter_numbers_show';

SHOW FLOWS LIKE 'filter_numbers_show';

-- also test `CREATE OR REPLACE` and `IF NOT EXISTS`

-- (flow exists, replace, if not exists)=(false, false, false)
CREATE FLOW filter_numbers_show SINK TO out_num_cnt_show AS SELECT number, ts FROM numbers_input_show where number > 10;

SELECT flow_name, table_catalog, flow_definition, source_table_names FROM INFORMATION_SCHEMA.FLOWS WHERE flow_name='filter_numbers_show';

-- this one should error out
-- (flow exists, replace, if not exists)=(true, false, false)
CREATE FLOW filter_numbers_show SINK TO out_num_cnt_show AS SELECT number, ts FROM numbers_input_show where number > 15;

SELECT flow_name, table_catalog, flow_definition, source_table_names FROM INFORMATION_SCHEMA.FLOWS WHERE flow_name='filter_numbers_show';

-- makesure it's not replaced in flownode
INSERT INTO numbers_input_show VALUES (10, 0),(15, 1),(16, 2);

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('filter_numbers_show');

SELECT number, ts FROM out_num_cnt_show;

-- after this one, the flow SHOULD NOT be replaced
-- (flow exists, replace, if not exists)=(true, false, true)
CREATE FLOW IF NOT EXISTS filter_numbers_show SINK TO out_num_cnt_show AS SELECT number, ts FROM numbers_input_show where number > 5;

SELECT flow_name, table_catalog, flow_definition, source_table_names FROM INFORMATION_SCHEMA.FLOWS WHERE flow_name='filter_numbers_show';

-- makesure it's not replaced in flownode
INSERT INTO numbers_input_show VALUES (4,4),(5,4),(10, 3),(11, 4);

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('filter_numbers_show');

SELECT number, ts FROM out_num_cnt_show;

-- after this, the flow SHOULD be replaced
-- (flow exists, replace, if not exists)=(true, true, false)
CREATE OR REPLACE FLOW filter_numbers_show SINK TO out_num_cnt_show AS SELECT number, ts FROM numbers_input_show where number > 3;

SELECT flow_name, table_catalog, flow_definition, source_table_names FROM INFORMATION_SCHEMA.FLOWS WHERE flow_name='filter_numbers_show';

-- makesure it's replaced in flownode
INSERT INTO numbers_input_show VALUES (3, 1),(4, 2),(10, 3),(11, 4);

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('filter_numbers_show');

SELECT number, ts FROM out_num_cnt_show;

-- after this, the flow SHOULD error out since having both `replace` and `if not exists`
-- (flow exists, replace, if not exists)=(true, true, true)
CREATE OR REPLACE FLOW IF NOT EXISTS filter_numbers_show SINK TO out_num_cnt_show AS SELECT number, ts FROM numbers_input_show where number > 0;

SELECT flow_name, table_catalog, flow_definition, source_table_names FROM INFORMATION_SCHEMA.FLOWS WHERE flow_name='filter_numbers_show';

DROP FLOW filter_numbers_show;

-- (flow exists, replace, if not exists)=(false, true, true)
CREATE OR REPLACE FLOW IF NOT EXISTS filter_numbers_show SINK TO out_num_cnt_show AS SELECT number, ts FROM numbers_input_show where number > -1;

SELECT flow_name, table_catalog, flow_definition, source_table_names FROM INFORMATION_SCHEMA.FLOWS WHERE flow_name='filter_numbers_show';

DROP FLOW filter_numbers_show;

-- following always create since didn't exist
-- (flow exists, replace, if not exists)=(false, true, false)
CREATE OR REPLACE FLOW filter_numbers_show SINK TO out_num_cnt_show AS SELECT number, ts FROM numbers_input_show where number > -2;

SELECT flow_name, table_catalog, flow_definition, source_table_names FROM INFORMATION_SCHEMA.FLOWS WHERE flow_name='filter_numbers_show';

DROP FLOW filter_numbers_show;

-- (flow exists, replace, if not exists)=(false, false, true)
CREATE OR REPLACE FLOW filter_numbers_show SINK TO out_num_cnt_show AS SELECT number, ts FROM numbers_input_show where number > -3;

SELECT flow_name, table_catalog, flow_definition, source_table_names FROM INFORMATION_SCHEMA.FLOWS WHERE flow_name='filter_numbers_show';

-- makesure after recover should be the same
-- SQLNESS ARG restart=true
SELECT 1;

-- SQLNESS SLEEP 3s

SELECT flow_name, table_catalog, flow_definition, source_table_names FROM INFORMATION_SCHEMA.FLOWS WHERE flow_name='filter_numbers_show';

SELECT * FROM out_num_cnt_show;

INSERT INTO numbers_input_show VALUES(-4,0), (-3,1), (-2,2), (-1,3);

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('filter_numbers_show');

SELECT * FROM out_num_cnt_show;

DROP FLOW filter_numbers_show;

drop table out_num_cnt_show;

drop table numbers_input_show;

CREATE TABLE numbers_input_show (
    number INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(number),
    TIME INDEX(ts)
);
create table out_num_cnt_show (
    number INT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP TIME INDEX,    
    PRIMARY KEY(number),
);

CREATE FLOW filter_numbers_show SINK TO out_num_cnt_show AS SELECT number as n1 FROM numbers_input_show where number > 10;

INSERT INTO numbers_input_show VALUES (10, 0),(15, 1),(16, 2);

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('filter_numbers_show');

SELECT number FROM out_num_cnt_show;

-- should mismatch, hence the old flow remains
CREATE OR REPLACE FLOW filter_numbers_show SINK TO out_num_cnt_show AS SELECT number AS n1, number AS n2 FROM numbers_input_show where number > 15;

-- should mismatch, hence the old flow remains
CREATE OR REPLACE FLOW filter_numbers_show SINK TO out_num_cnt_show AS SELECT number AS n1, number AS n2, number AS n3 FROM numbers_input_show where number > 15;

SELECT flow_definition, source_table_names FROM INFORMATION_SCHEMA.FLOWS WHERE flow_name='filter_numbers_show';

INSERT INTO numbers_input_show VALUES (10, 6),(11, 8),(15, 7),(18, 3);

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('filter_numbers_show');

-- sink table shows new 11 since old flow remains
SELECT number FROM out_num_cnt_show;

DROP FLOW filter_numbers_show;

drop table out_num_cnt_show;

drop table numbers_input_show;
