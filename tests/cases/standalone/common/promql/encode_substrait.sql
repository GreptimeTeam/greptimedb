create table count_total (
    ts timestamp time index,
    tag_a string,
    tag_b string,
    val double,
    primary key (tag_a, tag_b),
);

-- if `RangeManipulate` can be encoded/decoded correctly in substrait, the following queries should pass
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (partitioning.*) REDACTED
tql explain (0, 100, '1s') 
    increase(count_total{
      tag_a="ffa",
    }[1h])[12h:1h];

tql eval (0, 100, '1s') 
    increase(count_total{
      tag_a="ffa",
    }[1h])[12h:1h];

drop table count_total;
