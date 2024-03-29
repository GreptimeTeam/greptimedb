create table t0 (c0 varchar, t TIMESTAMP TIME INDEX);

insert into t0 values ('a', 1), (NULL,2), (NULL, 3), (NULL, 4), (NULL, 5), (NULL,6), (NULL,7);

-- SQLNESS SORT_RESULT 2 2
SELECT * FROM t0 ORDER BY t0.c0 DESC;

CREATE TABLE test0 (job VARCHAR, host VARCHAR, t TIMESTAMP TIME INDEX);

INSERT INTO test0 VALUES ('Shipping and Receiving Supervisor', 'Ackerman', 1), ('Shipping and Receiving Clerk', 'Berndt', 2), ('Shipping and Receiving Clerk', 'Kuppa', 3), ('Production Supervisor - WC60', 'Brown', 4), ('Production Supervisor - WC60', 'Campbell', 5), ('Production Supervisor - WC40', 'Dsa', 6);

SELECT * FROM test0 ORDER BY job, host;

SELECT * FROM test0 ORDER BY job DESC, host DESC;

CREATE TABLE test1 (s VARCHAR, t TIMESTAMP TIME INDEX);

INSERT INTO test1 VALUES ('2', 1), (NULL, 2), ('3555555555552', 3), ('1', 4), ('355555555556', 5), ('10', 6), ('3555555555553', 7), ('3555555555551', 8);

SELECT * FROM test1 ORDER BY s;

CREATE TABLE test4 (i INT, j INT, t TIMESTAMP TIME INDEX);

INSERT INTO test4 VALUES (3, 3, 1), (2, 3, 2), (2, 2, 3), (3, 2, 4);

SELECT * FROM test4 ORDER BY cast(i AS VARCHAR), j;

SELECT * FROM test4 ORDER BY i, cast(j AS VARCHAR);


SELECT * FROM test4 ORDER BY cast(i AS VARCHAR), cast(j AS VARCHAR);

CREATE TABLE tpch_q1_agg (l_returnflag VARCHAR, l_linestatus VARCHAR, sum_qty INT, sum_base_price DOUBLE, sum_disc_price DOUBLE, sum_charge DOUBLE, avg_qty DOUBLE, avg_price DOUBLE, avg_disc DOUBLE, count_order BIGINT, t TIMESTAMP TIME INDEX);

INSERT INTO tpch_q1_agg VALUES ('N', 'O', 7459297, 10512270008.90, 9986238338.3847, 10385578376.585467, 25.545537671232875, 36000.9246880137, 0.05009595890410959, 292000, 1), ('R', 'F', 3785523, 5337950526.47, 5071818532.9420, 5274405503.049367, 25.5259438574251, 35994.029214030925, 0.04998927856184382, 148301, 2), ('A', 'F', 3774200, 5320753880.69, 5054096266.6828, 5256751331.449234, 25.537587116854997, 36002.12382901414, 0.05014459706340077, 147790, 3), ('N', 'F', 95257, 133737795.84, 127132372.6512, 132286291.229445, 25.30066401062417, 35521.32691633466, 0.04939442231075697, 3765, 4);

SELECT * FROM tpch_q1_agg ORDER BY l_returnflag, l_linestatus;

create table test5 (i int, s varchar, t TIMESTAMP TIME INDEX);

CREATE TABLE test6 (i1 INT, s1 VARCHAR, i2 int, s2 VARCHAR, t TIMESTAMP TIME INDEX);

INSERT INTO test6 VALUES
(6, '0reallylongstring1', 3, '1reallylongstring8', 1),
(6, '0reallylongstring1', 3, '1reallylongstring7', 2),
(6, '0reallylongstring1', 4, '1reallylongstring8', 3),
(6, '0reallylongstring1', 4, '1reallylongstring7', 4),
(6, '0reallylongstring2', 3, '1reallylongstring8', 5),
(6, '0reallylongstring2', 3, '1reallylongstring7', 6),
(6, '0reallylongstring2', 4, '1reallylongstring8', 7),
(6, '0reallylongstring2', 4, '1reallylongstring7', 8),
(5, '0reallylongstring1', 3, '1reallylongstring8', 9),
(5, '0reallylongstring1', 3, '1reallylongstring7', 10),
(5, '0reallylongstring1', 4, '1reallylongstring8', 11),
(5, '0reallylongstring1', 4, '1reallylongstring7', 12),
(5, '0reallylongstring2', 3, '1reallylongstring8', 13),
(5, '0reallylongstring2', 3, '1reallylongstring7', 14),
(5, '0reallylongstring2', 4, '1reallylongstring8', 15),
(5, '0reallylongstring2', 4, '1reallylongstring7', 16);

SELECT i1, s1, i2, s2 FROM test6 ORDER BY i1, s1, i2, s2;

SELECT s1, i1, i2, s2 FROM test6 ORDER BY s1, i1, i2, s2;

SELECT s1, i1, s2, i2 FROM test6 ORDER BY s1, i1, s2, i2;

SELECT s1, s2, i1, i2 FROM test6 ORDER BY s1, s2, i1, i2;

SELECT i1, i2, s1, s2 FROM test6 ORDER BY i1, i2, s1, s2;

SELECT s1, s2, i1, i2 FROM test6 ORDER BY i2 DESC, s1, s2, i1;

create table test7 (p_brand VARCHAR, p_type VARCHAR, p_size INT, supplier_cnt BIGINT, t TIMESTAMP TIME INDEX);

insert into test7 values ('Brand#11', 'ECONOMY BRUSHED COPPER', 3, 4, 1), ('Brand#11', 'ECONOMY BRUSHED COPPER', 9, 4, 2), ('Brand#11', 'ECONOMY BRUSHED STEEL', 36, 4, 3), ('Brand#11', 'ECONOMY BRUSHED STEEL', 9, 4, 4), ('Brand#11', 'ECONOMY BURNISHED BRASS', 36, 4, 5), ('Brand#11', 'ECONOMY BURNISHED COPPER', 49, 4, 6), ('Brand#11', 'ECONOMY BURNISHED COPPER', 9, 4, 7), ('Brand#11', 'ECONOMY BURNISHED NICKEL', 14, 4,8), ('Brand#11', 'ECONOMY BURNISHED NICKEL', 49, 4, 9);

SELECT p_brand, p_type, p_size, supplier_cnt FROM test7 ORDER BY supplier_cnt DESC, p_brand, p_type, p_size;

create table test8 (i int, s varchar, t TIMESTAMP TIME INDEX);

insert into test8 values (3, 'aba', 1), (1, 'ccbcc', 2), (NULL, 'dbdbd', 3), (2, NULL, 4);

select i, split_part(s, 'b', 1) from test8 order by i;

CREATE TABLE "DirectReports"
(
    "EmployeeID" smallint,
    "Name" varchar NOT NULL,
    "Title" varchar NOT NULL,
    "EmployeeLevel" int NOT NULL,
    "Sort" varchar NOT NULL,
    "Timestamp" TIMESTAMP TIME INDEX,
);

INSERT INTO "DirectReports" VALUES
(1, 'Ken Sánchez', 'Chief Executive Officer', 1, 'Ken Sánchez', 1),
(273, '>Brian Welcker', 'Vice President of Sales', 2, 'Ken Sánchez>Brian Welcker', 2),
(274, '>>Stephen Jiang', 'North American Sales Manager', 3, 'Ken Sánchez>Brian Welcker>Stephen Jiang', 3),
(285, '>>Syed Abbas', 'Pacific Sales Manager', 3, 'Ken Sánchez>Brian Welcker>Syed Abbas', 4),
(16, '>>David Bradley', 'Marketing Manager', 3, 'Ken Sánchez>Brian Welcker>David Bradley', 5),
(275, '>>>Michael Blythe', 'Sales Representative', 4, 'Ken Sánchez>Brian Welcker>Stephen Jiang>Michael Blythe', 6),
(276, '>>>Linda Mitchell', 'Sales Representative', 4, 'Ken Sánchez>Brian Welcker>Stephen Jiang>Linda Mitchell', 7),
(286, '>>>Lynn Tsoflias', 'Sales Representative', 4, 'Ken Sánchez>Brian Welcker>Syed Abbas>Lynn Tsoflias', 8),
(23, '>>>Mary Gibson', 'Marketing Specialist', 4, 'Ken Sánchez>Brian Welcker>David Bradley>Mary Gibson', 9);

SELECT "EmployeeID", "Name", "Title", "EmployeeLevel"
FROM "DirectReports"
ORDER BY "Sort", "EmployeeID";

DROP TABLE t0;

DROP TABLE test0;

DROP TABLE test1;

DROP TABLE test4;

DROP TABLE tpch_q1_agg;

DROP TABLE test5;

DROP TABLE test6;

DROP table test7;

DROP table test8;

DROP TABLE "DirectReports";
